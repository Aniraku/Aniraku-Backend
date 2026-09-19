package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Aniraku/Aniraku-Backend/internal/metadata/anilist"
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
)

// ponytail: caps page to prevent AniList abuse from deep pagination
func parsePageParam(r *http.Request, defaultVal int) int {
	v := defaultVal
	if p := r.URL.Query().Get("page"); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 && n <= 100 {
			v = n
		}
	}
	return v
}

// ponytail: caps perPage at 20 (Kitsu max)
func parsePerPageParam(r *http.Request, defaultVal int) int {
	v := defaultVal
	if pp := r.URL.Query().Get("perPage"); pp != "" {
		if n, err := strconv.Atoi(pp); err == nil && n > 0 && n <= 20 {
			v = n
		}
	}
	return v
}

type keyCacheEntry struct {
	data      []byte
	fetchedAt time.Time
}

// Browse cache entry for in-memory caching of AniList responses
type browseCacheEntry struct {
	data      *anilist.BrowseResponse
	fetchedAt time.Time
}

// tokenBucket is a simple client-side rate limiter. We hold ourselves to
// ~60 req/min (AniList's documented limit is 90/min per IP) so bursts from
// parallel page-load requests never trigger upstream 429s in the first place.

// resolveMalIDsToAniList maps a batch of MyAnimeList IDs (as returned by the
// Jikan search/browse fallback) to AniList IDs in a single GraphQL round trip
// via the idMal_in filter. IDs without a mapping are omitted from the result.
func (h *Handlers) resolveMalIDsToAniList(ctx context.Context, malIDs []int) (map[int]int, error) {
	mapped := map[int]int{}
	if len(malIDs) == 0 {
		return mapped, nil
	}
	query := `query ($ids: [Int]) { Page(perPage: 50) { media(idMal_in: $ids, type: ANIME) { id idMal } } }`
	raw, err := h.anilistClient.do(ctx, query, map[string]any{"ids": malIDs})
	if err != nil {
		return nil, fmt.Errorf("resolve mal ids: %w", err)
	}

	var out struct {
		Data struct {
			Page struct {
				Media []struct {
					ID    int `json:"id"`
					IDMal int `json:"idMal"`
				} `json:"media"`
			} `json:"Page"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("resolve mal ids: decode: %w", err)
	}
	for _, m := range out.Data.Page.Media {
		if m.IDMal > 0 && m.ID > 0 {
			mapped[m.IDMal] = m.ID
		}
	}
	return mapped, nil
}

// normalizeSearchResults rekeys Jikan search/browse results onto AniList IDs.
// Jikan results carry the MAL ID in Media.ID; every downstream consumer (stream,
// episodes, dub check, relations) is AniList-keyed, so conversion happens once
// here at the search boundary instead of at each consumer.
func (h *Handlers) normalizeSearchResults(ctx context.Context, media []anilist.Anime) error {
	malIDs := make([]int, 0, len(media))
	for i := range media {
		if media[i].ID > 0 {
			malIDs = append(malIDs, media[i].ID)
		}
	}
	mapped, err := h.resolveMalIDsToAniList(ctx, malIDs)
	if err != nil {
		return err
	}
	for i := range media {
		if anilistID, ok := mapped[media[i].ID]; ok {
			malID := media[i].ID
			media[i].ID = anilistID
			media[i].IDMal = &malID
		}
	}
	return nil
}

func (h *Handlers) getAnimeFromAniList(ctx context.Context, id int) (*anilist.Anime, error) {
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { id idMal title { romaji english native userPreferred } coverImage { extraLarge large medium color } bannerImage format status episodes duration genres averageScore popularity description season seasonYear nextAiringEpisode { episode airingAt } isAdult } }`
	raw, err := h.anilistClient.do(ctx, query, map[string]any{"id": id})
	if err != nil {
		return nil, err
	}

	var result map[string]any
	json.Unmarshal(raw, &result)
	data, _ := result["data"].(map[string]any)
	media, _ := data["Media"].(map[string]any)
	if media == nil {
		return nil, fmt.Errorf("no media found")
	}

	// Convert map to anilist.Anime
	a := &anilist.Anime{}
	if id, ok := media["id"].(float64); ok {
		a.ID = int(id)
	}
	if idMal, ok := media["idMal"].(float64); ok {
		v := int(idMal)
		a.IDMal = &v
	}
	if title, ok := media["title"].(map[string]any); ok {
		if v, ok := title["romaji"].(string); ok {
			a.Title.Romaji = &v
		}
		if v, ok := title["english"].(string); ok {
			a.Title.English = &v
		}
		if v, ok := title["native"].(string); ok {
			a.Title.Native = &v
		}
		if v, ok := title["userPreferred"].(string); ok {
			v2 := v
			a.Title.UserPreferred = &v2
		}
	}
	if genres, ok := media["genres"].([]any); ok {
		for _, g := range genres {
			if s, ok := g.(string); ok {
				a.Genres = append(a.Genres, s)
			}
		}
	}
	return a, nil
}

func (h *Handlers) GetAnime(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	// Use AniList GraphQL proxy (avoids Jikan 504s)
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { id title { romaji english native userPreferred } coverImage { extraLarge large medium color } bannerImage format status episodes duration genres averageScore popularity description season seasonYear nextAiringEpisode { episode airingAt } isAdult } }`
	raw, err := h.anilistClient.do(r.Context(), query, map[string]any{"id": id})
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to fetch anime metadata")
		return
	}

	var result map[string]any
	json.Unmarshal(raw, &result)
	data, _ := result["data"].(map[string]any)
	media, _ := data["Media"].(map[string]any)
	if media == nil {
		h.respondError(w, http.StatusBadGateway, "invalid anime data")
		return
	}

	h.respondJSON(w, http.StatusOK, media)
}

func suffixTitle(t *anilist.Title, suffix string) {
	if t.Romaji != nil && *t.Romaji != "" {
		v := *t.Romaji + " " + suffix
		t.Romaji = &v
	}
	if t.English != nil && *t.English != "" {
		v := *t.English + " " + suffix
		t.English = &v
	}
	if t.Native != nil && *t.Native != "" {
		v := *t.Native + " " + suffix
		t.Native = &v
	}
	if t.UserPreferred != nil && *t.UserPreferred != "" {
		v := *t.UserPreferred + " " + suffix
		t.UserPreferred = &v
	}
}

func (h *Handlers) GetEpisodes(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	anilistID := id

	// AniList metadata and the AniZip episode map are independent sources —
	// fetch AniZip concurrently so the AniList round trip never serializes
	// in front of it. The channel close gives the happens-before edge: the
	// write to anizipData is visible after <-anizipDone below.
	var anizipData map[string]tmdb.AniZipEpisode
	anizipDone := make(chan struct{})
	go func() {
		defer close(anizipDone)
		data, _ := tmdb.FetchAniZipEpisodes(r.Context(), h.httpClient, anilistID)
		anizipData = data
	}()

	// AniList media metadata (episode count, cover art) is a nice-to-have:
	// during AniList outages the episode list itself must still serve from
	// AniZip + TMDB, so a failed lookup here is non-fatal. A nil `media` map
	// reads as zero values below, which routes count derivation to AniZip.
	var media map[string]any
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { id title { romaji english userPreferred } coverImage { extraLarge large medium } episodes format status nextAiringEpisode { episode airingAt } } }`
	raw, err := h.anilistClient.do(r.Context(), query, map[string]any{"id": id})
	if err != nil {
		h.log.Warn().Err(err).Int("id", id).Msg("episodes: anilist metadata unavailable, serving from anizip/tmdb only")
	} else {
		var result map[string]any
		json.Unmarshal(raw, &result)
		data, _ := result["data"].(map[string]any)
		media, _ = data["Media"].(map[string]any)
	}
	<-anizipDone

	episodeCount := 0
	if eps, ok := media["episodes"].(float64); ok && eps > 0 {
		episodeCount = int(eps)
	} else if nae, ok := media["nextAiringEpisode"].(map[string]any); ok {
		if ep, ok := nae["episode"].(float64); ok && ep > 1 {
			episodeCount = int(ep) - 1
		}
	} else if status, ok := media["status"].(string); ok && (status == "RELEASING" || status == "NOT_YET_RELEASED") {
		episodeCount = 12
	}

	// AniList outages (or titles with unknown counts) leave episodeCount at
	// 0, which collapses this response to an empty list even though AniZip
	// carries the full episode map — derive the count from AniZip instead of
	// returning nothing. Hentai often returns episodes:{} from AniZip, so fall
	// back to AniBridge verified mapping ranges (e.g. anilist:368 "1-6" -> 6).
	// Anisearch titles are fetched once here and reused below for titles.
	var anisearchEps map[int]*tmdb.AnisearchEpisode
	if episodeCount == 0 {
		if azMeta, err := tmdb.FetchAniZipMediaMeta(r.Context(), h.httpClient, anilistID); err == nil && azMeta.EpisodeCount > 0 {
			episodeCount = azMeta.EpisodeCount
		}
		for k := range anizipData {
			if n, err := strconv.Atoi(k); err == nil && n > episodeCount {
				episodeCount = n
			}
		}
		if episodeCount == 0 {
			inferCtx, inferCancel := context.WithTimeout(r.Context(), 15*time.Second)
			if n := tmdb.InferEpisodeCount(inferCtx, h.httpClient, h.cfg.TMDB.ReadAccessToken, anilistID); n > 0 {
				episodeCount = n
			}
			inferCancel()
		}
		// Last-resort exact count from Anisearch (via AniZip anisearch_id):
		// covers hentai missing from AniList, AniZip episodes, AniBridge and
		// Fribb alike. No Jikan involved.
		if episodeCount == 0 {
			asiCtx, asiCancel := context.WithTimeout(r.Context(), 15*time.Second)
			anisearchEps = tmdb.FetchAnisearchEpisodes(asiCtx, h.httpClient, anilistID)
			asiCancel()
			for n := range anisearchEps {
				if n > episodeCount {
					episodeCount = n
				}
			}
		}
	}

	token := h.cfg.TMDB.ReadAccessToken

	// Manual fallback cover from AniList art (no network): decided before the
	// parallel block below so only genuinely-missing covers trigger a fetch.
	coverFallback := ""
	if img, ok := media["coverImage"].(map[string]any); ok {
		if v, _ := img["extraLarge"].(string); v != "" {
			coverFallback = v
		} else if v, _ := img["large"].(string); v != "" {
			coverFallback = v
		} else if v, _ := img["medium"].(string); v != "" {
			coverFallback = v
		}
	}

	// TMDB resolve and the AniZip cover lookup are independent once the
	// episode count is known — run them concurrently so a slow TMDB resolve
	// no longer serializes the cover fallback behind it. The channel close
	// gives the happens-before edge for the coverFallback write.
	episodeNumbers := make([]int, episodeCount)
	for i := range episodeNumbers {
		episodeNumbers[i] = i + 1
	}
	coverDone := make(chan struct{})
	if coverFallback == "" {
		go func() {
			defer close(coverDone)
			if c := tmdb.FetchAniZipCover(r.Context(), h.httpClient, anilistID); c != "" {
				coverFallback = c
			}
		}()
	} else {
		close(coverDone)
	}
	tmdbByNumber := tmdb.GetCachedEpisodes(anilistID, episodeNumbers)
	if tmdbByNumber == nil {
		// Cache miss — fetch TMDB with generous timeout (blocks until done).
		// Must stay under the server's 60s WriteTimeout or the connection is
		// killed mid-request and the client gets nothing.
		tmdbByNumber = map[int]*tmdb.EpisodeMetadata{}
		if len(episodeNumbers) > 0 {
			fetchCtx, fetchCancel := context.WithTimeout(r.Context(), 50*time.Second)
			defer fetchCancel()
			result, _ := tmdb.ResolveEpisodes(fetchCtx, h.httpClient, token, anilistID, episodeNumbers)
			if result != nil {
				for _, ep := range result.Episodes {
					tmdbByNumber[ep.Number] = ep
				}
			}
			// Only cache non-empty results; caching an empty map poisons
			// hentai titles for 30min when TMDB is briefly unreachable.
			if len(tmdbByNumber) > 0 {
				tmdb.CacheEpisodes(anilistID, tmdbByNumber)
			}
		}
	}
	<-coverDone
	if coverFallback == "" && strings.TrimSpace(token) != "" {
		covCtx, covCancel := context.WithTimeout(r.Context(), 15*time.Second)
		if p := tmdb.FetchTmdbFallbackPoster(covCtx, h.httpClient, token, anilistID); p != "" {
			coverFallback = p
		}
		covCancel()
	}

	// Never stamp a junk cover onto every episode: validate the final
	// fallback (guards a blind-trusted AniList URL).
	if coverFallback != "" && !tmdb.IsValidAniZipThumbnail(coverFallback) {
		coverFallback = ""
	}

	// Anisearch episode titles (last resort): only when AniZip + TMDB both
	// have nothing for this anime. Thumbnails intentionally stay the anime
	// poster (coverFallback) per product decision — Anisearch ships a generic
	// placeholder cover for adult titles. Reuses the fetch from the count
	// stage above when available.
	if episodeCount > 0 && anisearchEps == nil && len(anizipData) == 0 && len(tmdbByNumber) == 0 {
		asiCtx, asiCancel := context.WithTimeout(r.Context(), 15*time.Second)
		anisearchEps = tmdb.FetchAnisearchEpisodes(asiCtx, h.httpClient, anilistID)
		asiCancel()
	}

	episodes := make([]map[string]any, episodeCount)
	for i := 0; i < episodeCount; i++ {
		epNum := i + 1
		ep := map[string]any{
			"number": epNum,
			"title":  fmt.Sprintf("Episode %d", epNum),
		}

		// AniZip data (title is multi-language map, image is the thumbnail).
		anizipKey := fmt.Sprintf("%d", epNum)
		anizipEp, hasAnizip := anizipData[anizipKey]

		// Merge: AniZip title → TMDB title → Anisearch title → generic.
		// Thumbnails are validated before use: junk URLs previously flowed
		// straight to clients as broken images. TMDB stills win (verified
		// image.tmdb.org stills), else AniZip art, else the anime poster
		// (coverFallback) applied below — never an Anisearch placeholder.
		if asiEp, ok := anisearchEps[epNum]; ok && asiEp != nil {
			if cur, _ := ep["title"].(string); !tmdb.IsPublishedTitle(cur) && tmdb.IsPublishedTitle(asiEp.Title) {
				ep["title"] = asiEp.Title
			}
			if _, hasAir := ep["airdate"]; !hasAir && asiEp.Airdate != "" {
				ep["airdate"] = asiEp.Airdate
			}
		}
		if hasAnizip {
			if t := anizipEp.BestTitle(); t != "" {
				ep["title"] = t
			}
			if tmdb.IsValidAniZipThumbnail(anizipEp.Thumbnail) {
				ep["thumbnail"] = strings.TrimSpace(anizipEp.Thumbnail)
			}
			if anizipEp.Airdate != "" {
				ep["airdate"] = anizipEp.Airdate
			}
		}
		if tmdbMeta, ok := tmdbByNumber[epNum]; ok {
			if tmdbMeta.Title != "" {
				ep["title"] = tmdbMeta.Title
			}
			if tmdbMeta.Thumbnail != nil && tmdb.HasVerifiedTmdbThumbnail(*tmdbMeta.Thumbnail) {
				ep["thumbnail"] = strings.TrimSpace(*tmdbMeta.Thumbnail)
			}
			if tmdbMeta.Description != nil && *tmdbMeta.Description != "" {
				ep["description"] = *tmdbMeta.Description
			}
			if tmdbMeta.Airdate != nil && *tmdbMeta.Airdate != "" {
				ep["airdate"] = *tmdbMeta.Airdate
			}
		}
		if _, hasThumb := ep["thumbnail"]; !hasThumb && coverFallback != "" {
			ep["thumbnail"] = coverFallback
		}
		episodes[i] = ep
	}

	h.respondJSON(w, http.StatusOK, map[string]any{"episodes": episodes})
}

func (h *Handlers) GetSchedule(w http.ResponseWriter, r *http.Request) {
	page := parsePageParam(r, 1)
	perPage := parsePerPageParam(r, 50)

	results, err := h.mal.GetSchedule(r.Context(), page, perPage)
	if err != nil {
		h.respondJSON(w, http.StatusOK, map[string]any{"schedule": []any{}, "pageInfo": anilist.PageInfo{}})
		return
	}

	type scheduleItem struct {
		ID       int    `json:"id"`
		Title    any    `json:"title"`
		Cover    any    `json:"coverImage"`
		Format   string `json:"format"`
		Episode  int    `json:"episode"`
		AiringAt int    `json:"airingAt"`
		Day      string `json:"day"`
	}

	items := []scheduleItem{}
	for _, m := range results.Data.Page.Media {
		if m.NextAiringEpisode == nil {
			continue
		}
		t := time.Unix(int64(m.NextAiringEpisode.AiringAt), 0)
		items = append(items, scheduleItem{
			ID:       m.ID,
			Title:    m.Title,
			Cover:    m.CoverImage,
			Format:   m.Format,
			Episode:  m.NextAiringEpisode.Episode,
			AiringAt: m.NextAiringEpisode.AiringAt,
			Day:      t.Weekday().String(),
		})
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"schedule": items,
		"pageInfo": results.Data.Page.PageInfo,
	})
}

func (h *Handlers) GetSimilar(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	page := parsePageParam(r, 1)
	perPage := parsePerPageParam(r, 12)

	// ponytail: AniList first (get genres, then search by genre), Jikan fallback
	anime, anilistErr := h.getAnimeFromAniList(r.Context(), id)
	if anilistErr == nil && len(anime.Genres) > 0 {
		filters := anilist.BrowseFilters{
			Genre: anime.Genres[:1],
			Sort:  "SCORE_DESC",
		}
		results, err := h.browseAniList(r.Context(), filters, page, perPage)
		if err == nil {
			// filter out the original anime
			var filtered []anilist.Anime
			for _, m := range results.Data.Page.Media {
				if m.ID != id {
					filtered = append(filtered, m)
				}
			}
			if len(filtered) > perPage {
				filtered = filtered[:perPage]
			}
			results.Data.Page.Media = filtered
			h.respondJSON(w, http.StatusOK, map[string]any{
				"media":    results.Data.Page.Media,
				"pageInfo": results.Data.Page.PageInfo,
			})
			return
		}
		h.log.Warn().Err(err).Int("id", id).Msg("anilist similar failed, falling back to jikan")
	}

	results, err := h.mal.GetSimilar(r.Context(), id, page, perPage)
	if err != nil {
		h.log.Warn().Err(err).Int("id", id).Msg("failed to fetch similar anime")
		h.respondError(w, http.StatusBadGateway, "failed to fetch similar anime")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"media":    results.Data.Page.Media,
		"pageInfo": results.Data.Page.PageInfo,
	})
}

func (h *Handlers) GetTrending(w http.ResponseWriter, r *http.Request) {
	page := parsePageParam(r, 1)
	perPage := parsePerPageParam(r, 20)

	// ponytail: AniList first, Jikan fallback
	results, err := h.trendingAniList(r.Context(), page, perPage)
	if err != nil {
		h.log.Warn().Err(err).Msg("anilist trending failed, falling back to jikan")
		results, err = h.mal.GetTrending(r.Context(), page, perPage)
		if err != nil {
			h.log.Warn().Err(err).Msg("jikan trending also failed")
			h.respondError(w, http.StatusBadGateway, "failed to fetch trending")
			return
		}
	}

	for i := range results.Data.Page.Media {
		if results.Data.Page.Media[i].IsAdult {
			suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
		}
	}

	h.respondJSON(w, http.StatusOK, results.Data.Page.Media)
}

func (h *Handlers) GetSeasonal(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, []any{})
}

func (h *Handlers) Browse(w http.ResponseWriter, r *http.Request) {
	page := parsePageParam(r, 1)
	perPage := parsePerPageParam(r, 20)

	filters := anilist.BrowseFilters{
		Genre:  r.URL.Query()["genre"],
		Format: r.URL.Query()["format"],
		Status: r.URL.Query()["status"],
		Season: r.URL.Query().Get("season"),
		Sort:   r.URL.Query().Get("sort"),
		Search: r.URL.Query().Get("search"),
	}
	if y := r.URL.Query().Get("year"); y != "" {
		filters.Year, _ = strconv.Atoi(y)
	}

	isNSFW := false
	for _, g := range filters.Genre {
		if strings.EqualFold(g, "NSFW") {
			isNSFW = true
			break
		}
	}

	if isNSFW {
		results, err := h.browseAdult(r.Context(), page, 50)
		if err != nil {
			h.log.Warn().Err(err).Msg("adult browse failed")
			h.respondError(w, http.StatusBadGateway, "adult browse failed")
			return
		}
		for i := range results.Data.Page.Media {
			suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
		}
		h.respondJSON(w, http.StatusOK, map[string]any{
			"media":    results.Data.Page.Media,
			"pageInfo": results.Data.Page.PageInfo,
		})
		return
	}

	// ponytail: AniList first (fast, reliable), Jikan fallback (slow, 502-prone)
	results, err := h.browseAniList(r.Context(), filters, page, perPage)
	if err != nil {
		h.log.Warn().Err(err).Msg("anilist browse failed, falling back to jikan")
		results, err = h.mal.Browse(r.Context(), filters, page, perPage)
		if err != nil {
			h.log.Warn().Err(err).Msg("jikan browse also failed")
			h.respondError(w, http.StatusBadGateway, "browse failed")
			return
		}
	}

	for i := range results.Data.Page.Media {
		if results.Data.Page.Media[i].IsAdult {
			suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
		}
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"media":    results.Data.Page.Media,
		"pageInfo": results.Data.Page.PageInfo,
	})
}

func (h *Handlers) GetGenres(w http.ResponseWriter, r *http.Request) {
	genres, err := h.mal.GetGenres(r.Context())
	if err != nil {
		h.log.Warn().Err(err).Msg("failed to fetch genres")
		h.respondError(w, http.StatusBadGateway, "failed to fetch genres")
		return
	}
	h.respondJSON(w, http.StatusOK, genres)
}

func (h *Handlers) GetRelations(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	anilistID, err := strconv.Atoi(idStr)
	if err != nil || anilistID <= 0 {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	// Relations come from AniList directly so node IDs stay AniList-keyed,
	// matching the shape the frontend consumes on the anime detail page.
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { relations { edges { relationType node { id idMal title { romaji english native userPreferred } coverImage { extraLarge large medium } format type status } } } } }`
	raw, err := h.anilistClient.do(r.Context(), query, map[string]any{"id": anilistID})
	if err != nil {
		h.log.Warn().Err(err).Int("id", anilistID).Msg("failed to fetch relations from AniList")
		h.respondError(w, http.StatusBadGateway, "failed to fetch relations")
		return
	}

	var out struct {
		Data struct {
			Media struct {
				Relations json.RawMessage `json:"relations"`
			} `json:"Media"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to fetch relations")
		return
	}
	if len(out.Errors) > 0 {
		h.log.Warn().Str("detail", out.Errors[0].Message).Int("id", anilistID).Msg("anilist relations error")
		h.respondError(w, http.StatusBadGateway, "failed to fetch relations")
		return
	}
	if len(out.Data.Media.Relations) == 0 {
		h.respondError(w, http.StatusNotFound, "relations not found")
		return
	}

	h.respondJSON(w, http.StatusOK, out.Data.Media.Relations)
}

func (h *Handlers) trendingAniList(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	cacheKey := fmt.Sprintf("trending:%d:%d", page, perPage)
	if cached, ok := h.browseCache.Load(cacheKey); ok {
		if entry, ok := cached.(browseCacheEntry); ok && time.Since(entry.fetchedAt) < h.browseCacheTTL {
			h.log.Debug().Str("cache_key", cacheKey).Msg("trending cache hit")
			RecordBrowseCacheHit()
			return entry.data, nil
		}
		h.browseCache.Delete(cacheKey)
	}

	query := `query ($page: Int, $perPage: Int) {
		Page(page: $page, perPage: $perPage) {
			pageInfo { total lastPage hasNextPage currentPage perPage }
			media(type: ANIME, sort: TRENDING) {
				id title { romaji english native userPreferred }
				coverImage { extraLarge large medium color }
				bannerImage format status episodes averageScore popularity season seasonYear genres isAdult
				nextAiringEpisode { episode airingAt }
			}
		}
	}`
	raw, err := h.anilistClient.do(ctx, query, map[string]any{
		"page":    page,
		"perPage": perPage,
	})
	if err != nil {
		return nil, err
	}

	var result anilist.BrowseResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}

	// Cache the result
	h.browseCache.Store(cacheKey, browseCacheEntry{data: &result, fetchedAt: time.Now()})
	return &result, nil
}

func (h *Handlers) browseAniList(ctx context.Context, filters anilist.BrowseFilters, page, perPage int) (*anilist.BrowseResponse, error) {
	// Build AniList GraphQL query with variable placeholders
	variables := map[string]any{
		"page":    page,
		"perPage": perPage,
	}

	var typeArgs []string
	// AniList rejects queries that declare variables it doesn't use, so only
	// declare the ones we actually pass in the media() args.
	var varDecls []string
	typeArgs = append(typeArgs, "type: ANIME")

	if filters.Search != "" {
		typeArgs = append(typeArgs, "search: $search")
		varDecls = append(varDecls, "$search: String")
		variables["search"] = filters.Search
	}
	if len(filters.Genre) > 0 {
		typeArgs = append(typeArgs, "genre: $genre")
		varDecls = append(varDecls, "$genre: String")
		variables["genre"] = filters.Genre[0]
	}
	if len(filters.Format) > 0 {
		typeArgs = append(typeArgs, "format: $format")
		varDecls = append(varDecls, "$format: MediaFormat")
		variables["format"] = strings.ToUpper(filters.Format[0])
	}
	if len(filters.Status) > 0 {
		typeArgs = append(typeArgs, "status: $status")
		varDecls = append(varDecls, "$status: MediaStatus")
		variables["status"] = strings.ToUpper(filters.Status[0])
	}
	if filters.Season != "" {
		typeArgs = append(typeArgs, "season: $season")
		varDecls = append(varDecls, "$season: MediaSeason")
		variables["season"] = strings.ToUpper(filters.Season)
	}
	if filters.Year > 0 {
		typeArgs = append(typeArgs, "seasonYear: $year")
		varDecls = append(varDecls, "$year: Int")
		variables["year"] = filters.Year
	}

	sort := "POPULARITY_DESC"
	switch filters.Sort {
	case "SCORE_DESC":
		sort = "SCORE_DESC"
	case "START_DATE_DESC":
		sort = "START_DATE_DESC"
	case "TITLE_ROMAJI":
		sort = "TITLE_ROMAJI"
	}
	typeArgs = append(typeArgs, "sort: $sort")
	varDecls = append(varDecls, "$sort: [MediaSort]")
	variables["sort"] = sort

	// Build cache key from all filter parameters. The filter slices are
	// optional (a bare /browse call has none) — never index them without a
	// length check, or the request panics into a 500 on every filter-less
	// page load.
	first := func(s []string) string {
		if len(s) > 0 {
			return s[0]
		}
		return ""
	}
	cacheKey := fmt.Sprintf("browse:%d:%d:%s:%s:%s:%s:%s:%d:%s",
		page, perPage,
		filters.Search, first(filters.Genre), first(filters.Format), first(filters.Status),
		filters.Season, filters.Year, sort)

	if cached, ok := h.browseCache.Load(cacheKey); ok {
		if entry, ok := cached.(browseCacheEntry); ok && time.Since(entry.fetchedAt) < h.browseCacheTTL {
			h.log.Debug().Str("cache_key", cacheKey).Msg("browse cache hit")
			RecordBrowseCacheHit()
			return entry.data, nil
		}
		h.browseCache.Delete(cacheKey)
	}

	query := fmt.Sprintf(`query ($page: Int, $perPage: Int, %s) {
		Page(page: $page, perPage: $perPage) {
			pageInfo { total lastPage hasNextPage currentPage perPage }
			media(%s) {
				id title { romaji english native userPreferred }
				coverImage { extraLarge large medium color }
				bannerImage format status episodes averageScore popularity season seasonYear genres isAdult
				nextAiringEpisode { episode airingAt }
			}
		}
	}`, strings.Join(varDecls, ", "), strings.Join(typeArgs, ", "))

	raw, err := h.anilistClient.do(ctx, query, variables)
	if err != nil {
		return nil, err
	}

	var result anilist.BrowseResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}

	// Cache the result
	h.browseCache.Store(cacheKey, browseCacheEntry{data: &result, fetchedAt: time.Now()})
	return &result, nil
}

func (h *Handlers) browseAdult(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	query := `query ($page: Int, $perPage: Int) {
		Page(page: $page, perPage: $perPage) {
			pageInfo { total lastPage hasNextPage currentPage perPage }
			media(type: ANIME, isAdult: true, sort: POPULARITY_DESC) {
				id title { romaji english native userPreferred }
				coverImage { extraLarge large medium color }
				bannerImage format status episodes averageScore popularity season seasonYear genres isAdult
			}
		}
	}`
	raw, err := h.anilistClient.do(ctx, query, map[string]any{
		"page":    page,
		"perPage": perPage,
	})
	if err != nil {
		return nil, err
	}

	var result anilist.BrowseResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
