package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/auth"
)

// Import / Export of library lists between Aniraku and the user's
// connected MAL / AniList accounts. Both directions reuse the OAuth
// tokens stored by the sync feature (Settings → Library Sync).
//
//	import = pull provider list → Aniraku bookmarks + watch_history +
//	         episode_ratings (progress, status and scores are merged in:
//	         provider episodes only ever advance Aniraku's max episode,
//	         provider scores only fill titles with no local ratings)
//	export = push Aniraku favorites → provider library, writing progress
//	         (num_watched_episodes), status (watching/completed) and score
//	         in a single per-title write.
//
// Aniraku has no provider-style status column: "completed" is derived
// from watch_history (highest episode fully watched AND at/above the
// known episode total), everything else maps to watching/CURRENT.
// Planning-only titles import as bookmarks with no watch rows.
//
// All endpoints are idempotent (upsert) and capped so a single request
// stays well inside provider rate limits and the platform's timeout.

const (
	importExportCap    = 150  // max titles per request
	importBatchSize    = 200  // supabase rows per POST
	importWatchBatch   = 200  // watch_history rows per POST
	importMaxWatchRows = 2000 // max synthesized watch rows per import
	exportWriteCap     = 60   // max provider writes per export (bounds request time)
	// exportTimeBudget hard-stops an export's write loop so the JSON
	// response is always on its way well before nginx's 180s
	// proxy_read_timeout — the write deadline is lifted for these routes
	// (see liftExportWriteDeadline), so the loop is the only thing still
	// bounding the request. Past the budget the handler responds with
	// limited=true and the client re-runs to continue where it left off.
	exportTimeBudget   = 150 * time.Second
	fullEpisodeSeconds = 1440 // synthetic progress/duration for imported eps (24 min)
	// AniList now enforces 30 req/min per token/IP. Exports stay at 10
	// requests/min (one request every 6s, 3x headroom under the ceiling)
	// with up to exportAniListBatchSize title mutations packed into each
	// write request via GraphQL aliases — up to ~100 titles/min of
	// throughput while spending only 10 req/min of the budget.
	exportAniListRequestInterval = 6 * time.Second
	exportAniListBatchSize       = 10
)

// requireProviderToken returns the user's stored token for a provider,
// refreshing it first if it is near expiry. The refreshed token is
// persisted so callers never operate on stale credentials.
func (h *Handlers) requireProviderToken(ctx context.Context, userID, provider string) (syncProviderToken, error) {
	tokens, err := h.loadSyncTokens(ctx, userID)
	if err != nil {
		return syncProviderToken{}, err
	}
	token, ok := tokens[provider]
	if !ok || token.AccessToken == "" {
		return syncProviderToken{}, fmt.Errorf("%s is not connected — connect it in Settings first", provider)
	}

	// Refresh when the token is within 5 minutes of expiring or already stale.
	if token.ExpiresAt > 0 && time.Now().Unix() > token.ExpiresAt-300 {
		var refreshed syncProviderToken
		switch provider {
		case "mal":
			refreshed, err = h.refreshMALToken(ctx, token)
		case "anilist":
			refreshed, err = h.refreshAniListToken(ctx, token)
		}
		if err == nil {
			token = refreshed
			_ = h.saveSyncToken(ctx, userID, provider, refreshed)
		} else {
			h.log.Warn().Err(err).Str("provider", provider).Msg("import/export: token refresh failed, using stored token")
		}
	}
	return token, nil
}

// importFavoriteDiff inserts only the ids not already in the user's
// bookmarks, returning (newly inserted, already present). Import stays
// idempotent while the UI can show what actually changed.
func (h *Handlers) importFavoriteDiff(ctx context.Context, userID string, ids []int) (int, int, error) {
	if len(ids) == 0 {
		return 0, 0, nil
	}
	existing, err := h.loadUserFavorites(ctx, userID)
	if err != nil {
		return 0, 0, err
	}
	have := make(map[int]bool, len(existing))
	for _, id := range existing {
		have[id] = true
	}
	fresh := make([]int, 0, len(ids))
	already := 0
	for _, id := range ids {
		if have[id] {
			already++
		} else {
			fresh = append(fresh, id)
		}
	}
	meta, err := h.fetchMediaMeta(ctx, fresh)
	if err != nil {
		h.log.Warn().Err(err).Msg("import: media metadata fetch failed, importing with fallback titles")
	}
	inserted, err := h.insertBookmarks(ctx, userID, fresh, meta)
	if err != nil {
		return 0, already, err
	}
	return inserted, already, nil
}

// mediaMeta holds the display fields the UI needs for a bookmarked title.
type mediaMeta struct {
	title    string
	image    string
	episodes int // known total episode count, 0 = unknown
}

// providerEntry is a normalized provider list entry, keyed by AniList ID.
// Status is one of: completed | watching | planning | paused | dropped.
type providerEntry struct {
	AnimeID  int
	Progress int
	Score    int // 1-10, 0 = no score
	Status   string
	Total    int // known total episodes, 0 = unknown
}

func normalizeMALStatus(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "completed":
		return "completed"
	case "watching":
		return "watching"
	case "on_hold":
		return "paused"
	case "dropped":
		return "dropped"
	case "plan_to_watch":
		return "planning"
	default:
		return "watching"
	}
}

func normalizeAniListStatus(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "COMPLETED":
		return "completed"
	case "CURRENT", "REPEATING":
		return "watching"
	case "PAUSED":
		return "paused"
	case "DROPPED":
		return "dropped"
	case "PLANNING":
		return "planning"
	default:
		return "watching"
	}
}

// anilistScoreToTen converts an AniList MediaList.score value (which is
// formatted per the viewer's scoreFormat) to a 1-10 Aniraku score.
// Returns 0 when there is no score.
func anilistScoreToTen(score float64, format string) int {
	if score <= 0 {
		return 0
	}
	var ten float64
	switch strings.ToUpper(strings.TrimSpace(format)) {
	case "POINT_100":
		ten = score / 10
	case "POINT_5":
		ten = score * 2
	case "POINT_3":
		ten = score * 10 / 3
	default: // POINT_10, POINT_10_DECIMAL, unknown
		ten = score
	}
	s := int(ten + 0.5)
	if s < 1 {
		s = 1
	}
	if s > 10 {
		s = 10
	}
	return s
}

type animeWatchProgress struct {
	Episode   int
	Progress  int
	Completed bool
}

// fetchMediaMeta resolves AniList IDs to title + cover image + episode
// total in batched GraphQL round trips so imported rows render properly
// in the UI and completion can be derived from the episode total.
func (h *Handlers) fetchMediaMeta(ctx context.Context, anilistIDs []int) (map[int]mediaMeta, error) {
	meta := make(map[int]mediaMeta, len(anilistIDs))
	for start := 0; start < len(anilistIDs); start += 50 {
		end := start + 50
		if end > len(anilistIDs) {
			end = len(anilistIDs)
		}
		query := `query ($ids: [Int]) {
			Page(perPage: 50) {
				media(id_in: $ids, type: ANIME) {
					id
					episodes
					title { romaji english }
					coverImage { medium }
				}
			}
		}`
		raw, err := h.anilistClient.do(ctx, query, map[string]any{"ids": anilistIDs[start:end]})
		if err != nil {
			return nil, err
		}
		var out struct {
			Data struct {
				Page struct {
					Media []struct {
						ID       int  `json:"id"`
						Episodes *int `json:"episodes"`
						Title    struct {
							Romaji  string `json:"romaji"`
							English string `json:"english"`
						} `json:"title"`
						CoverImage struct {
							Medium string `json:"medium"`
						} `json:"coverImage"`
					} `json:"media"`
				} `json:"Page"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		for _, m := range out.Data.Page.Media {
			if m.ID <= 0 {
				continue
			}
			title := m.Title.English
			if title == "" {
				title = m.Title.Romaji
			}
			if title == "" {
				title = fmt.Sprintf("Anime %d", m.ID)
			}
			total := 0
			if m.Episodes != nil && *m.Episodes > 0 {
				total = *m.Episodes
			}
			meta[m.ID] = mediaMeta{title: title, image: m.CoverImage.Medium, episodes: total}
		}
	}
	return meta, nil
}

func (h *Handlers) insertBookmarks(ctx context.Context, userID string, ids []int, meta map[int]mediaMeta) (int, error) {
	inserted := 0
	for start := 0; start < len(ids); start += importBatchSize {
		end := start + importBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		rows := make([]map[string]any, 0, end-start)
		for _, id := range ids[start:end] {
			m := meta[id]
			title := m.title
			if title == "" {
				title = fmt.Sprintf("Anime %d", id)
			}
			rows = append(rows, map[string]any{
				"user_id":  userID,
				"anime_id": id,
				"title":    title,
				"image":    m.image,
				"added_at": time.Now().UnixMilli(),
			})
		}
		raw, _ := json.Marshal(rows)
		resp, err := h.supabaseRequest(ctx, "POST",
			"/rest/v1/bookmarks?on_conflict=user_id,anime_id",
			bytes.NewReader(raw),
			map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
		if err != nil {
			return inserted, err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			return inserted, fmt.Errorf("bookmarks insert returned %s", resp.Status)
		}
		inserted += end - start
	}
	return inserted, nil
}

func (h *Handlers) loadUserFavorites(ctx context.Context, userID string) ([]int, error) {
	resp, err := h.supabaseRequest(ctx, "GET",
		"/rest/v1/bookmarks?select=anime_id&user_id=eq."+encodePath(userID)+"&limit=500",
		nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bookmarks fetch returned %s", resp.Status)
	}
	var rows []struct {
		AnimeID int `json:"anime_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(rows))
	seen := map[int]bool{}
	for _, r := range rows {
		if r.AnimeID > 0 && !seen[r.AnimeID] {
			seen[r.AnimeID] = true
			ids = append(ids, r.AnimeID)
		}
	}
	return ids, nil
}

// loadUserWatchProgress returns the latest/highest Aniraku episode state for
// each title. Watch history stores one row per episode, so exports must fold
// those rows into the provider's anime-level progress field instead of
// treating every favorite as completed.
func (h *Handlers) loadUserWatchProgress(ctx context.Context, userID string) (map[int]animeWatchProgress, error) {
	resp, err := h.supabaseRequest(ctx, "GET",
		"/rest/v1/watch_history?select=anime_id,episode_number,progress,duration,timestamp&user_id=eq."+encodePath(userID)+"&order=timestamp.desc&limit=5000",
		nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("watch history fetch returned %s", resp.Status)
	}
	var rows []struct {
		AnimeID   int     `json:"anime_id"`
		Episode   int     `json:"episode_number"`
		Progress  float64 `json:"progress"`
		Duration  float64 `json:"duration"`
		Timestamp int64   `json:"timestamp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	result := make(map[int]animeWatchProgress)
	for _, row := range rows {
		if row.AnimeID <= 0 || row.Episode <= 0 {
			continue
		}
		progress := row.Episode
		completed := row.Duration > 0 && row.Progress >= row.Duration*0.95
		current, ok := result[row.AnimeID]
		if !ok || row.Episode > current.Episode ||
			(row.Episode == current.Episode && (completed || int(row.Progress) > current.Progress)) {
			result[row.AnimeID] = animeWatchProgress{Episode: row.Episode, Progress: progress, Completed: completed}
		}
	}
	return result, nil
}

// loadUserWatchMax returns the highest watched episode_number per anime.
// Imports use it to only ever advance Aniraku progress, never regress it.
func (h *Handlers) loadUserWatchMax(ctx context.Context, userID string) (map[int]int, error) {
	resp, err := h.supabaseRequest(ctx, "GET",
		"/rest/v1/watch_history?select=anime_id,episode_number&user_id=eq."+encodePath(userID)+"&limit=5000",
		nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("watch history fetch returned %s", resp.Status)
	}
	var rows []struct {
		AnimeID int `json:"anime_id"`
		Episode int `json:"episode_number"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	max := make(map[int]int)
	for _, r := range rows {
		if r.AnimeID > 0 && r.Episode > max[r.AnimeID] {
			max[r.AnimeID] = r.Episode
		}
	}
	return max, nil
}

// insertWatchHistoryRows upserts synthetic fully-watched episode rows
// (progress == duration, so exports read them back as completed).
func (h *Handlers) insertWatchHistoryRows(ctx context.Context, userID string, rows []map[string]any) error {
	for start := 0; start < len(rows); start += importWatchBatch {
		end := start + importWatchBatch
		if end > len(rows) {
			end = len(rows)
		}
		raw, _ := json.Marshal(rows[start:end])
		resp, err := h.supabaseRequest(ctx, "POST",
			"/rest/v1/watch_history?on_conflict=user_id,anime_id,episode_number",
			bytes.NewReader(raw),
			map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			return fmt.Errorf("watch history insert returned %s", resp.Status)
		}
	}
	return nil
}

// loadRatedAnime returns the set of anime IDs that already have at least
// one episode rating, so imports fill scores without overwriting locals.
func (h *Handlers) loadRatedAnime(ctx context.Context, userID string, animeIDs []int) (map[int]bool, error) {
	rated := map[int]bool{}
	if len(animeIDs) == 0 {
		return rated, nil
	}
	strIDs := make([]string, 0, len(animeIDs))
	for _, id := range animeIDs {
		if id > 0 {
			strIDs = append(strIDs, strconv.Itoa(id))
		}
	}
	for start := 0; start < len(strIDs); start += 100 {
		end := start + 100
		if end > len(strIDs) {
			end = len(strIDs)
		}
		resp, err := h.supabaseRequest(ctx, "GET",
			"/rest/v1/episode_ratings?select=anime_id&user_id=eq."+encodePath(userID)+
				"&anime_id=in.("+strings.Join(strIDs[start:end], ",")+")&limit=5000",
			nil, nil)
		if err != nil {
			return rated, err
		}
		var rows []struct {
			AnimeID int `json:"anime_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			resp.Body.Close()
			return rated, err
		}
		resp.Body.Close()
		for _, r := range rows {
			if r.AnimeID > 0 {
				rated[r.AnimeID] = true
			}
		}
	}
	return rated, nil
}

func (h *Handlers) insertEpisodeRatings(ctx context.Context, userID string, rows []map[string]any) error {
	for start := 0; start < len(rows); start += importBatchSize {
		end := start + importBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		raw, _ := json.Marshal(rows[start:end])
		resp, err := h.supabaseRequest(ctx, "POST",
			"/rest/v1/episode_ratings?on_conflict=user_id,anime_id,episode_number",
			bytes.NewReader(raw),
			map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			return fmt.Errorf("episode ratings insert returned %s", resp.Status)
		}
	}
	return nil
}

// loadUserAnimeScores averages per-episode ratings into a 1-10 anime
// score per title so exports can write scores alongside progress.
func (h *Handlers) loadUserAnimeScores(ctx context.Context, userID string) (map[int]int, error) {
	scores := map[int]int{}
	resp, err := h.supabaseRequest(ctx, "GET",
		"/rest/v1/episode_ratings?select=anime_id,score&user_id=eq."+encodePath(userID)+"&limit=5000",
		nil, nil)
	if err != nil {
		return scores, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return scores, fmt.Errorf("episode ratings fetch returned %s", resp.Status)
	}
	var rows []struct {
		AnimeID int `json:"anime_id"`
		Score   int `json:"score"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return scores, err
	}
	sum := map[int]int{}
	count := map[int]int{}
	for _, r := range rows {
		if r.AnimeID > 0 && r.Score >= 1 && r.Score <= 10 {
			sum[r.AnimeID] += r.Score
			count[r.AnimeID]++
		}
	}
	for id, n := range count {
		avg := (sum[id] + n/2) / n
		if avg < 1 {
			avg = 1
		}
		if avg > 10 {
			avg = 10
		}
		scores[id] = avg
	}
	return scores, nil
}

// runProviderImport merges normalized provider entries into Aniraku:
// bookmarks for every title, watch_history rows that only advance the
// local max episode, and scores only for titles with no local ratings.
// Returns (imported, already, episodesAdded, scoresAdded, limited).
func (h *Handlers) runProviderImport(ctx context.Context, userID string, entries []providerEntry, meta map[int]mediaMeta) (int, int, int, int, bool) {
	// De-duplicate: keep the best entry per title (most progress wins,
	// completed beats watching on ties, then highest score).
	best := make(map[int]providerEntry, len(entries))
	rank := func(e providerEntry) (int, int, int) {
		completed := 0
		if e.Status == "completed" {
			completed = 1
		}
		return e.Progress, completed, e.Score
	}
	for _, e := range entries {
		if e.AnimeID <= 0 {
			continue
		}
		cur, ok := best[e.AnimeID]
		if !ok {
			best[e.AnimeID] = e
			continue
		}
		ap, ac, as := rank(e)
		bp, bc, bs := rank(cur)
		if ap > bp || (ap == bp && (ac > bc || (ac == bc && as > bs))) {
			best[e.AnimeID] = e
		}
	}

	ids := make([]int, 0, len(best))
	for id := range best {
		ids = append(ids, id)
	}
	imported, already, err := h.importFavoriteDiff(ctx, userID, ids)
	if err != nil {
		h.log.Warn().Err(err).Msg("import: favorites insert failed")
	}
	// Fill display meta for the watch-history rows (bookmarks already
	// fetched their own copy inside importFavoriteDiff).
	if len(meta) == 0 {
		if m, merr := h.fetchMediaMeta(ctx, ids); merr == nil {
			meta = m
		} else {
			h.log.Warn().Err(merr).Msg("import: media metadata fetch failed, importing with fallback titles")
			meta = map[int]mediaMeta{}
		}
	}

	watchMax, err := h.loadUserWatchMax(ctx, userID)
	historyOK := err == nil
	if err != nil {
		// Fail safe: without the existing history we cannot merge, and a
		// blind upsert would overwrite real per-episode progress rows.
		h.log.Warn().Err(err).Msg("import: watch history fetch failed, skipping progress import")
		watchMax = map[int]int{}
	}
	rated, err := h.loadRatedAnime(ctx, userID, ids)
	scoresOK := err == nil
	if err != nil {
		// Fail safe: without the existing ratings we cannot tell which
		// titles are unrated, and upserting would overwrite local scores.
		h.log.Warn().Err(err).Msg("import: ratings fetch failed, skipping score import")
		rated = map[int]bool{}
	}

	now := time.Now().UnixMilli()
	watchRows := []map[string]any{}
	ratingRows := []map[string]any{}
	episodesAdded, scoresAdded := 0, 0
	limited := false
	for _, e := range best {
		m := meta[e.AnimeID]
		title := m.title
		if title == "" {
			title = fmt.Sprintf("Anime %d", e.AnimeID)
		}
		want := e.Progress
		if e.Status == "completed" && want <= 0 {
			// Provider says completed but reports no count: fall back to
			// the known episode total so the title still lands completed.
			if e.Total > 0 {
				want = e.Total
			} else if m.episodes > 0 {
				want = m.episodes
			}
		}
		if total := e.Total; total > 0 && want > total {
			want = total
		} else if total := m.episodes; total > 0 && want > total {
			want = total
		}
		// Merge-only: rows are only ever appended after the real history.
		// If the existing history could not be read we skip progress
		// entirely instead of blind-upserting over real rows.
		if historyOK {
			from := watchMax[e.AnimeID] + 1
			if from < 1 {
				from = 1
			}
			for ep := from; ep <= want; ep++ {
				if len(watchRows) >= importMaxWatchRows {
					limited = true
					break
				}
				// Backdate merged rows (1h apart, oldest first) so a bulk
				// import never outranks genuine recent watches in history /
				// continue-watching ordering.
				ts := now - int64(want-ep+1)*3600000
				watchRows = append(watchRows, map[string]any{
					"user_id":        userID,
					"anime_id":       e.AnimeID,
					"anime_title":    title,
					"anime_image":    m.image,
					"episode_number": ep,
					"progress":       fullEpisodeSeconds,
					"duration":       fullEpisodeSeconds,
					"timestamp":      ts,
				})
				episodesAdded++
			}
			if limited {
				break
			}
		}
		if !scoresOK {
			continue
		}
		if e.Score >= 1 && e.Score <= 10 && !rated[e.AnimeID] {
			rated[e.AnimeID] = true // one score row per title
			ep := want
			if ep <= 0 {
				ep = 1
			}
			ratingRows = append(ratingRows, map[string]any{
				"user_id":        userID,
				"anime_id":       e.AnimeID,
				"episode_number": ep,
				"score":          e.Score,
				"created_at":     time.Now().UTC().Format(time.RFC3339),
			})
			scoresAdded++
		}
	}
	if len(watchRows) > 0 {
		if err := h.insertWatchHistoryRows(ctx, userID, watchRows); err != nil {
			h.log.Warn().Err(err).Msg("import: watch history insert failed")
			episodesAdded = 0
		}
	}
	if len(ratingRows) > 0 {
		if err := h.insertEpisodeRatings(ctx, userID, ratingRows); err != nil {
			h.log.Warn().Err(err).Msg("import: ratings insert failed")
			scoresAdded = 0
		}
	}
	return imported, already, episodesAdded, scoresAdded, limited
}

// ────────────────────────────────────────────────────────────────
// Import: provider list → Aniraku favorites
// ────────────────────────────────────────────────────────────────

// ImportMAL pulls the user's own MyAnimeList anime list (needs the MAL
// account connected in Settings) and merges it into Aniraku: bookmarks
// for every title, watch_history advanced to the provider's episode
// count, and scores filled in where Aniraku has none.
func (h *Handlers) ImportMAL(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !h.syncConfigured("mal") {
		h.respondError(w, http.StatusNotImplemented, "MAL sync is not configured on this server")
		return
	}
	token, err := h.requireProviderToken(r.Context(), userID, "mal")
	if err != nil {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}

	// Fetch the connected user's list, paginated by offset. list_status
	// carries the per-title status, score (0-10, 0 = unset) and watched
	// episode count that the old favorites-only import threw away.
	type malEntry struct {
		MALID    int
		Status   string
		Score    int
		Progress int
	}
	byMAL := map[int]*malEntry{}
	malIDs := []int{}
	offset := 0
	for offset < 1000 {
		u := fmt.Sprintf("https://api.myanimelist.net/v2/users/@me/animelist?limit=100&offset=%d&fields=list_status", offset)
		req, err := http.NewRequestWithContext(r.Context(), "GET", u, nil)
		if err != nil {
			break
		}
		req.Header.Set("Authorization", "Bearer "+token.AccessToken)
		resp, err := h.h2Client.Do(req)
		if err != nil {
			h.respondError(w, http.StatusBadGateway, "could not reach MyAnimeList — try again")
			return
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			h.log.Warn().Msg("mal import: list fetch failed " + resp.Status)
			h.respondError(w, http.StatusBadGateway, "MyAnimeList rejected the request — reconnect the account in Settings")
			return
		}
		var page struct {
			Data []struct {
				Node struct {
					ID int `json:"id"`
				} `json:"node"`
				ListStatus struct {
					Status             string `json:"status"`
					Score              int    `json:"score"`
					NumEpisodesWatched int    `json:"num_episodes_watched"`
				} `json:"list_status"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			h.respondError(w, http.StatusBadGateway, "could not read the MyAnimeList response")
			return
		}
		if len(page.Data) == 0 {
			break
		}
		for _, e := range page.Data {
			if e.Node.ID <= 0 {
				continue
			}
			if _, ok := byMAL[e.Node.ID]; !ok {
				malIDs = append(malIDs, e.Node.ID)
				byMAL[e.Node.ID] = &malEntry{MALID: e.Node.ID}
			}
			en := byMAL[e.Node.ID]
			// Keep the best row on duplicates: most progress wins.
			if e.ListStatus.NumEpisodesWatched > en.Progress {
				en.Progress = e.ListStatus.NumEpisodesWatched
				en.Status = e.ListStatus.Status
				en.Score = e.ListStatus.Score
			} else if en.Status == "" {
				en.Status = e.ListStatus.Status
				en.Score = e.ListStatus.Score
			}
		}
		offset += len(page.Data)
	}

	anilistIDs, err := h.resolveMalIDsToAniList(r.Context(), malIDs)
	if err != nil {
		h.log.Warn().Err(err).Msg("mal import: id mapping failed")
		h.respondError(w, http.StatusBadGateway, "could not map your list to Aniraku IDs")
		return
	}
	entries := make([]providerEntry, 0, len(malIDs))
	unmapped := 0
	for _, malID := range malIDs {
		anID, ok := anilistIDs[malID]
		if !ok {
			unmapped++
			continue
		}
		en := byMAL[malID]
		score := 0
		if en.Score >= 1 && en.Score <= 10 {
			score = en.Score
		}
		progress := en.Progress
		if progress < 0 {
			progress = 0
		}
		entries = append(entries, providerEntry{
			AnimeID:  anID,
			Progress: progress,
			Score:    score,
			Status:   normalizeMALStatus(en.Status),
		})
	}

	imported, already, episodes, scores, limited := h.runProviderImport(r.Context(), userID, entries, nil)
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"provider": "mal",
		"imported": imported,
		"already":  already,
		"episodes": episodes,
		"scores":   scores,
		"total":    len(entries),
		"unmapped": unmapped,
		"limited":  limited,
	})
}

// ImportAniList pulls the user's AniList anime list (needs the AniList
// account connected in Settings) and merges it into Aniraku: bookmarks
// for every title, watch_history advanced to the provider's progress,
// and scores filled in where Aniraku has none.
func (h *Handlers) ImportAniList(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !h.syncConfigured("anilist") {
		h.respondError(w, http.StatusNotImplemented, "AniList sync is not configured on this server")
		return
	}
	token, err := h.requireProviderToken(r.Context(), userID, "anilist")
	if err != nil {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}

	// score comes back in the viewer's own scoreFormat, so fetch the
	// format alongside the entries to normalize everything to 1-10.
	// Media carries title/cover/total inline — no follow-up meta fetch.
	// The viewer ID is resolved first and passed explicitly: a null
	// userId does not reliably default to the viewer (400).
	// Imports keep the direct (unpaged) call: two requests total.
	direct := func(ctx context.Context, query string, vars map[string]any) ([]byte, error) {
		return h.anilistAuthedWithRetry(ctx, token.AccessToken, query, vars)
	}
	viewerID, err := h.fetchAniListViewerID(r.Context(), token.AccessToken, direct)
	if err != nil {
		switch {
		case isAniListAuthError(err):
			h.respondError(w, http.StatusUnauthorized, "AniList token is invalid — reconnect the account in Settings")
		case isRetryableAniListError(err):
			h.respondError(w, http.StatusBadGateway, "AniList is rate-limiting requests — wait a minute and try again")
		default:
			h.log.Warn().Err(err).Msg("anilist import: viewer lookup failed")
			h.respondError(w, http.StatusBadGateway, "AniList request failed ("+err.Error()+") — try again")
		}
		return
	}
	query := `query ($userId: Int) {
		Viewer {
			id
			mediaListOptions { scoreFormat }
		}
		MediaListCollection(userId: $userId, type: ANIME) {
			lists {
				entries {
					mediaId
					status
					progress
					score
					media {
						episodes
						title { romaji english }
						coverImage { medium }
					}
				}
			}
		}
	}`
	raw, err := h.anilistAuthedWithRetry(r.Context(), token.AccessToken, query, map[string]any{"userId": viewerID})
	if err != nil {
		switch {
		case isAniListAuthError(err):
			h.respondError(w, http.StatusUnauthorized, "AniList token is invalid — reconnect the account in Settings")
		case isRetryableAniListError(err):
			h.log.Warn().Err(err).Msg("anilist import: rate-limited or unavailable")
			h.respondError(w, http.StatusBadGateway, "AniList is rate-limiting requests — wait a minute and try again")
		default:
			h.log.Warn().Err(err).Msg("anilist import: query failed")
			h.respondError(w, http.StatusBadGateway, "AniList request failed ("+err.Error()+") — try again")
		}
		return
	}
	var out struct {
		Data struct {
			Viewer struct {
				ID               int `json:"id"`
				MediaListOptions struct {
					ScoreFormat string `json:"scoreFormat"`
				} `json:"mediaListOptions"`
			} `json:"Viewer"`
			MediaListCollection struct {
				Lists []struct {
					Entries []struct {
						MediaID  int     `json:"mediaId"`
						Status   string  `json:"status"`
						Progress int     `json:"progress"`
						Score    float64 `json:"score"`
						Media    struct {
							Episodes *int `json:"episodes"`
							Title    struct {
								Romaji  string `json:"romaji"`
								English string `json:"english"`
							} `json:"title"`
							CoverImage struct {
								Medium string `json:"medium"`
							} `json:"coverImage"`
						} `json:"media"`
					} `json:"entries"`
				} `json:"lists"`
			} `json:"MediaListCollection"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		h.respondError(w, http.StatusBadGateway, "could not read the AniList response")
		return
	}
	entries := []providerEntry{}
	meta := map[int]mediaMeta{}
	for _, list := range out.Data.MediaListCollection.Lists {
		for _, e := range list.Entries {
			if e.MediaID <= 0 {
				continue
			}
			total := 0
			if e.Media.Episodes != nil && *e.Media.Episodes > 0 {
				total = *e.Media.Episodes
			}
			title := e.Media.Title.English
			if title == "" {
				title = e.Media.Title.Romaji
			}
			if title == "" {
				title = fmt.Sprintf("Anime %d", e.MediaID)
			}
			if _, ok := meta[e.MediaID]; !ok {
				meta[e.MediaID] = mediaMeta{title: title, image: e.Media.CoverImage.Medium, episodes: total}
			}
			progress := e.Progress
			if progress < 0 {
				progress = 0
			}
			entries = append(entries, providerEntry{
				AnimeID:  e.MediaID,
				Progress: progress,
				Score:    anilistScoreToTen(e.Score, out.Data.Viewer.MediaListOptions.ScoreFormat),
				Status:   normalizeAniListStatus(e.Status),
				Total:    total,
			})
		}
	}
	if len(entries) == 0 && out.Data.Viewer.ID == 0 {
		h.respondError(w, http.StatusUnauthorized, "AniList token is invalid — reconnect the account in Settings")
		return
	}
	imported, already, episodes, scores, limited := h.runProviderImport(r.Context(), userID, entries, meta)
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"provider": "anilist",
		"imported": imported,
		"already":  already,
		"episodes": episodes,
		"scores":   scores,
		"total":    len(entries),
		"limited":  limited,
	})
}

// ────────────────────────────────────────────────────────────────
// Export: Aniraku favorites → provider library
// ────────────────────────────────────────────────────────────────

// exportCompleted reports whether Aniraku considers a title completed:
// the highest episode was fully watched AND reached the known total
// (or the total is unknown, preserving the legacy behavior).
func exportCompleted(state animeWatchProgress, total int) bool {
	if !state.Completed {
		return false
	}
	if total > 0 {
		return state.Episode >= total
	}
	return true
}

// liftExportWriteDeadline clears the server-wide 60s WriteTimeout for this
// request. A full export (favorites/watch-history/scores pre-fetch + media
// meta + up to exportWriteCap provider writes at rate-limit pace) routinely
// runs past 60s; without the lift the response write fails after the
// deadline, the connection is closed before a single header is sent, nginx
// answers 502 — and because nginx's own error page carries no
// Access-Control-Allow-Origin, browsers surface it as a CORS error even
// though CORS is configured correctly. The exportTimeBudget loop cap keeps
// the total under nginx's 180s proxy_read_timeout.
func liftExportWriteDeadline(w http.ResponseWriter) {
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
}

// anilistExportPacer spaces one export's AniList requests to at most one
// per interval (10/min against AniList's 30 req/min ceiling). A pacer is
// created per export request and shared by every AniList call that export
// makes — pre-fetch reads and batched writes alike — so the whole export
// stays inside the budget with headroom to spare.
type anilistExportPacer struct {
	mu       sync.Mutex
	last     time.Time
	interval time.Duration
}

func newAnilistExportPacer(interval time.Duration) *anilistExportPacer {
	return &anilistExportPacer{interval: interval}
}

// wait blocks until interval has passed since the previous request. The
// first call proceeds immediately.
func (p *anilistExportPacer) wait(ctx context.Context) error {
	p.mu.Lock()
	d := time.Until(p.last.Add(p.interval))
	p.mu.Unlock()
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// mark records that a request was just sent.
func (p *anilistExportPacer) mark() {
	p.mu.Lock()
	p.last = time.Now()
	p.mu.Unlock()
}

// anilistCall sends one AniList GraphQL request with a user token.
type anilistCall func(ctx context.Context, query string, variables map[string]any) ([]byte, error)

// anilistExportCall is the only way export code talks to AniList: it paces
// the request through the export's pacer, sends it with Retry-After-aware
// retries, then records the send time for the next call's spacing.
func (h *Handlers) anilistExportCall(ctx context.Context, pacer *anilistExportPacer, accessToken, query string, variables map[string]any) ([]byte, error) {
	if err := pacer.wait(ctx); err != nil {
		return nil, err
	}
	raw, err := h.anilistAuthedWithRetry(ctx, accessToken, query, variables)
	pacer.mark()
	return raw, err
}

// anilistExportWrite is one title update queued for the batched export.
type anilistExportWrite struct {
	MediaID   int
	Progress  int
	Status    string  // "CURRENT" or "COMPLETED"
	Score     float64 // scoreRaw; 0 = omit
	WantScore int     // 1..10, 0 = none (counts scoresSent on success)
}

// buildAniListBulkMutation packs a batch of title updates into a single
// GraphQL request using field aliases (m0, m1, ...). All values are ints
// from our own DB or fixed enum strings, so inlining them is safe — and one
// request carrying exportAniListBatchSize mutations spends 1 req/min of the
// AniList budget instead of 10.
func buildAniListBulkMutation(writes []anilistExportWrite) string {
	var b strings.Builder
	b.WriteString("mutation {")
	for i, w := range writes {
		fmt.Fprintf(&b, " m%d: SaveMediaListEntry(mediaId: %d, progress: %d, status: %s", i, w.MediaID, w.Progress, w.Status)
		if w.Score > 0 {
			fmt.Fprintf(&b, ", scoreRaw: %g", w.Score)
		}
		b.WriteString(") { id }")
	}
	b.WriteString(" }")
	return b.String()
}

// parseAniListBulkResult maps a bulk-mutation response back onto its
// aliases: an alias with a non-null entry id succeeded; aliases named in a
// GraphQL error path — or missing/null in data — failed. ok has one entry
// per requested alias, in order.
func parseAniListBulkResult(raw []byte, aliases int) (ok []bool, firstErr string) {
	ok = make([]bool, aliases)
	var out struct {
		Data map[string]struct {
			ID *int `json:"id"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
			Path    []any  `json:"path"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return ok, "AniList returned an unreadable response"
	}
	bad := map[string]bool{}
	for _, e := range out.Errors {
		if firstErr == "" && e.Message != "" {
			firstErr = e.Message
		}
		if len(e.Path) > 0 {
			if alias, isStr := e.Path[0].(string); isStr {
				bad[alias] = true
			}
		}
	}
	for i := range ok {
		alias := fmt.Sprintf("m%d", i)
		entry, present := out.Data[alias]
		ok[i] = present && !bad[alias] && entry.ID != nil
	}
	if firstErr == "" {
		for _, good := range ok {
			if !good {
				firstErr = "AniList rejected the update"
				break
			}
		}
	}
	return ok, firstErr
}

// ExportMAL pushes Aniraku favorites into the user's connected MyAnimeList
// library, writing progress (num_watched_episodes), status
// (watching/completed) and score in a single per-title write.
func (h *Handlers) ExportMAL(w http.ResponseWriter, r *http.Request) {
	liftExportWriteDeadline(w)
	start := time.Now()
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !h.syncConfigured("mal") {
		h.respondError(w, http.StatusNotImplemented, "MAL sync is not configured on this server")
		return
	}
	token, err := h.requireProviderToken(r.Context(), userID, "mal")
	if err != nil {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}
	anilistIDs, err := h.loadUserFavorites(r.Context(), userID)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "could not read your favorites")
		return
	}
	favoriteCount := len(anilistIDs)
	if len(anilistIDs) > importExportCap {
		anilistIDs = anilistIDs[:importExportCap]
	}

	// AniList IDs → MAL IDs (batched, single GraphQL round trip each).
	malIDs, err := h.resolveAniListIDsToMAL(r.Context(), anilistIDs)
	if err != nil {
		h.log.Warn().Err(err).Msg("mal export: id mapping failed")
		h.respondError(w, http.StatusBadGateway, "could not map your favorites to MAL IDs")
		return
	}

	// Skip titles already marked completed on MAL — no pointless writes.
	watchProgress, progressErr := h.loadUserWatchProgress(r.Context(), userID)
	if progressErr != nil {
		h.log.Warn().Err(progressErr).Msg("mal export: watch history fetch failed, exporting without progress")
		watchProgress = map[int]animeWatchProgress{}
	}
	scores, scoresErr := h.loadUserAnimeScores(r.Context(), userID)
	if scoresErr != nil {
		h.log.Warn().Err(scoresErr).Msg("mal export: ratings fetch failed, exporting without scores")
		scores = map[int]int{}
	}
	// Episode totals so "completed" means finished the whole series, not
	// just the latest watched episode of an ongoing show.
	totals := map[int]mediaMeta{}
	if m, merr := h.fetchMediaMeta(r.Context(), anilistIDs); merr == nil {
		totals = m
	} else {
		h.log.Warn().Err(merr).Msg("mal export: episode totals unavailable, using watch flags")
	}
	completed, err := h.fetchMALCompletedSet(r.Context(), token.AccessToken)
	if err != nil {
		h.log.Warn().Err(err).Msg("mal export: completed set fetch failed, exporting all")
	}

	exported, skipped, failed, scoresSent := 0, 0, 0, 0
	limited := favoriteCount > importExportCap
	processed := 0
	for _, anilistID := range anilistIDs {
		// Wall-clock budget (see exportTimeBudget): stop taking new
		// writes in time to still deliver the JSON response.
		if time.Since(start) >= exportTimeBudget {
			limited = true
			break
		}
		malID, ok := malIDs[anilistID]
		if !ok {
			continue
		}
		i := processed
		processed++
		if i > 0 && i%3 == 0 {
			select {
			case <-time.After(1100 * time.Millisecond):
			case <-r.Context().Done():
				h.respondError(w, http.StatusGatewayTimeout, "export interrupted")
				return
			}
		}
		state := watchProgress[anilistID]
		done := exportCompleted(state, totals[anilistID].episodes)
		if completed[malID] && (done || state.Episode == 0) {
			skipped++
			continue
		}
		form := url.Values{}
		if state.Episode > 0 {
			form.Set("num_watched_episodes", fmt.Sprintf("%d", state.Episode))
		}
		if done {
			form.Set("status", "completed")
		} else {
			form.Set("status", "watching")
		}
		if s := scores[anilistID]; s >= 1 && s <= 10 {
			form.Set("score", fmt.Sprintf("%d", s))
		}
		req, err := http.NewRequestWithContext(r.Context(), "PUT",
			fmt.Sprintf("https://api.myanimelist.net/v2/anime/%d/my_list_status", malID),
			strings.NewReader(form.Encode()))
		if err != nil {
			continue
		}
		req.Header.Set("Authorization", "Bearer "+token.AccessToken)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := h.h2Client.Do(req)
		if err != nil {
			failed++
			continue
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			exported++
			if s := scores[anilistID]; s >= 1 && s <= 10 {
				scoresSent++
			}
		} else {
			failed++
		}
	}
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"provider": "mal",
		"exported": exported,
		"skipped":  skipped,
		"failed":   failed,
		"scores":   scoresSent,
		"total":    len(anilistIDs),
		"limited":  limited,
	})
}

// ExportAniList pushes Aniraku favorites into the user's connected AniList
// library, writing progress, status (CURRENT/COMPLETED) and score
// (scoreRaw) in a single per-title write.
func (h *Handlers) ExportAniList(w http.ResponseWriter, r *http.Request) {
	liftExportWriteDeadline(w)
	start := time.Now()
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !h.syncConfigured("anilist") {
		h.respondError(w, http.StatusNotImplemented, "AniList sync is not configured on this server")
		return
	}
	token, err := h.requireProviderToken(r.Context(), userID, "anilist")
	if err != nil {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}
	ids, err := h.loadUserFavorites(r.Context(), userID)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "could not read your favorites")
		return
	}
	favoriteCount := len(ids)
	if favoriteCount > importExportCap {
		ids = ids[:importExportCap]
	}

	watchProgress, progressErr := h.loadUserWatchProgress(r.Context(), userID)
	if progressErr != nil {
		h.log.Warn().Err(progressErr).Msg("anilist export: watch history fetch failed, exporting without progress")
		watchProgress = map[int]animeWatchProgress{}
	}
	scores, scoresErr := h.loadUserAnimeScores(r.Context(), userID)
	if scoresErr != nil {
		h.log.Warn().Err(scoresErr).Msg("anilist export: ratings fetch failed, exporting without scores")
		scores = map[int]int{}
	}
	// Everything AniList-facing below shares one pacer and one deadline:
	// at most one request every 6s (10 batched req/min of AniList's
	// 30 req/min ceiling), and the export stops taking new requests at the
	// time budget so the JSON response still beats nginx's timeout.
	exportCtx, cancel := context.WithTimeout(r.Context(), exportTimeBudget)
	defer cancel()
	pacer := newAnilistExportPacer(exportAniListRequestInterval)
	paced := func(ctx context.Context, query string, vars map[string]any) ([]byte, error) {
		return h.anilistExportCall(ctx, pacer, token.AccessToken, query, vars)
	}

	totals := map[int]mediaMeta{}
	// The meta lookup uses the public client (up to 3 instant requests for
	// 150 titles): separate it from the paced authed burst by one interval.
	if err := pacer.wait(exportCtx); err != nil {
		h.respondError(w, http.StatusGatewayTimeout, "export interrupted")
		return
	}
	if m, merr := h.fetchMediaMeta(exportCtx, ids); merr == nil {
		totals = m
	} else {
		h.log.Warn().Err(merr).Msg("anilist export: episode totals unavailable, using watch flags")
	}
	pacer.mark()
	// Diff-then-write: fetch the provider's current state once and only
	// write titles that actually differ. This collapses repeat exports to
	// a couple of reads, and the paced batched writes below stay inside
	// AniList's 30 req/min ceiling.
	remote, err := h.fetchAniListListState(exportCtx, token.AccessToken, paced)
	if err != nil {
		h.log.Warn().Err(err).Msg("anilist export: remote state fetch failed, exporting all")
		remote = map[int]anilistListState{}
	}

	exported, skipped, failed, scoresSent := 0, 0, 0, 0
	limited := favoriteCount > importExportCap
	rateLimitedStop := false
	var firstExportErr error

	// Phase 1 — diff: collect the titles that actually need writing.
	// Skipped titles cost nothing; only pending titles consume the cap.
	var pending []anilistExportWrite
	for _, id := range ids {
		if len(pending) >= exportWriteCap {
			limited = true
			break
		}
		wp := watchProgress[id]
		done := exportCompleted(wp, totals[id].episodes)
		wantStatus := "watching"
		if done {
			wantStatus = "completed"
		}
		wantScore := scores[id]
		if wantScore < 1 || wantScore > 10 {
			wantScore = 0
		}
		if cur, ok := remote[id]; ok {
			progressOK := cur.Progress >= wp.Episode
			statusOK := (wantStatus == "completed" && cur.Status == "completed") ||
				(wantStatus == "watching" && cur.Status == "watching")
			scoreOK := wantScore == 0 || cur.Score == wantScore
			if progressOK && statusOK && scoreOK {
				skipped++
				continue
			}
		}
		// Never regress a provider lead (e.g. episodes watched on AniList
		// directly past the Aniraku max).
		progress := wp.Episode
		if cur, ok := remote[id]; ok && cur.Progress > progress {
			progress = cur.Progress
		}
		status := "CURRENT"
		if done {
			status = "COMPLETED"
		}
		w := anilistExportWrite{MediaID: id, Progress: progress, Status: status, WantScore: wantScore}
		if wantScore != 0 {
			w.Score = float64(wantScore) * 10
		}
		pending = append(pending, w)
	}

	// Phase 2 — write in batches of exportAniListBatchSize mutations per
	// request, one request per 6s (10 batched req/min). The budget check
	// before each batch guarantees the JSON response still goes out.
	for off := 0; off < len(pending); off += exportAniListBatchSize {
		if time.Since(start) >= exportTimeBudget {
			limited = true
			break
		}
		end := off + exportAniListBatchSize
		if end > len(pending) {
			end = len(pending)
		}
		batch := pending[off:end]
		raw, err := h.anilistExportCall(exportCtx, pacer, token.AccessToken, buildAniListBulkMutation(batch), nil)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				limited = true
				break
			}
			if isAniListRateLimitError(err) {
				// The retry helper already waited out full Retry-After
				// windows and the limit still holds: pause here and let
				// the client re-run — diff-then-write skips what's done.
				h.log.Warn().Err(err).Int("remaining", len(pending)-off).
					Msg("anilist export: rate limit persists, pausing for re-run")
				limited, rateLimitedStop = true, true
				break
			}
			if firstExportErr == nil {
				firstExportErr = err
			}
			failed += len(batch)
			continue
		}
		ok, msg := parseAniListBulkResult(raw, len(batch))
		if msg != "" && firstExportErr == nil {
			firstExportErr = fmt.Errorf("%s", msg)
		}
		for i, good := range ok {
			if !good {
				failed++
				continue
			}
			exported++
			if batch[i].WantScore != 0 {
				scoresSent++
			}
		}
	}
	// Every write rejected on credentials is an auth problem, not 36
	// individual failures — say so instead of reporting "N failed".
	// Otherwise report the first error verbatim so the cause is visible.
	// A rate-limit pause is not a failure: it answers 200 with
	// limited=true (+rate_limited) so the client re-runs and resumes —
	// diff-then-write skips everything already exported.
	if exported == 0 && failed > 0 && !rateLimitedStop {
		if isAniListAuthError(firstExportErr) {
			h.respondError(w, http.StatusUnauthorized, "AniList token is invalid — reconnect the account in Settings")
			return
		}
		if firstExportErr != nil {
			h.respondError(w, http.StatusBadGateway, "AniList export failed ("+firstExportErr.Error()+")")
			return
		}
	}
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"provider":     "anilist",
		"exported":     exported,
		"skipped":      skipped,
		"failed":       failed,
		"scores":       scoresSent,
		"total":        len(ids),
		"limited":      limited,
		"rate_limited": rateLimitedStop,
	})
}

// fetchAniListViewerID resolves the token owner's user ID. Collection
// queries take it explicitly: a null userId does not reliably default
// to the viewer and AniList answers such calls with a 400. call is how the
// request is sent — the plain retry helper for imports, the paced export
// call for exports.
func (h *Handlers) fetchAniListViewerID(ctx context.Context, accessToken string, call anilistCall) (int, error) {
	raw, err := call(ctx, `query { Viewer { id } }`, map[string]any{})
	if err != nil {
		return 0, err
	}
	var out struct {
		Data struct {
			Viewer struct {
				ID int `json:"id"`
			} `json:"Viewer"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, err
	}
	if out.Data.Viewer.ID <= 0 {
		return 0, fmt.Errorf("AniList returned no viewer for this token")
	}
	return out.Data.Viewer.ID, nil
}

// anilistListState is a provider-side entry normalized like providerEntry.
type anilistListState struct {
	Progress int
	Status   string // completed | watching | planning | paused | dropped
	Score    int    // 1-10, 0 = none
}

// fetchAniListListState returns the user's current AniList entries
// (progress, status, normalized score) in a single query so exports can
// diff-then-write instead of blindly rewriting every title.
func (h *Handlers) fetchAniListListState(ctx context.Context, accessToken string, call anilistCall) (map[int]anilistListState, error) {
	// The viewer ID is resolved first and passed explicitly: a null
	// userId does not reliably default to the viewer (400).
	viewerID, err := h.fetchAniListViewerID(ctx, accessToken, call)
	if err != nil {
		return nil, err
	}
	query := `query ($userId: Int) {
		Viewer { mediaListOptions { scoreFormat } }
		MediaListCollection(userId: $userId, type: ANIME) {
			lists { entries { mediaId status progress score } }
		}
	}`
	raw, err := call(ctx, query, map[string]any{"userId": viewerID})
	if err != nil {
		return nil, err
	}
	var out struct {
		Data struct {
			Viewer struct {
				MediaListOptions struct {
					ScoreFormat string `json:"scoreFormat"`
				} `json:"mediaListOptions"`
			} `json:"Viewer"`
			MediaListCollection struct {
				Lists []struct {
					Entries []struct {
						MediaID  int     `json:"mediaId"`
						Status   string  `json:"status"`
						Progress int     `json:"progress"`
						Score    float64 `json:"score"`
					} `json:"entries"`
				} `json:"lists"`
			} `json:"MediaListCollection"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	format := out.Data.Viewer.MediaListOptions.ScoreFormat
	state := map[int]anilistListState{}
	for _, list := range out.Data.MediaListCollection.Lists {
		for _, e := range list.Entries {
			if e.MediaID <= 0 {
				continue
			}
			progress := e.Progress
			if progress < 0 {
				progress = 0
			}
			cur, ok := state[e.MediaID]
			next := anilistListState{
				Progress: progress,
				Status:   normalizeAniListStatus(e.Status),
				Score:    anilistScoreToTen(e.Score, format),
			}
			// Duplicate rows across lists: keep the furthest progress.
			if !ok || next.Progress > cur.Progress {
				state[e.MediaID] = next
			}
		}
	}
	return state, nil
}

// fetchAniListCompletedSet returns the set of AniList ids the user has
// already marked completed.
func (h *Handlers) fetchAniListCompletedSet(ctx context.Context, accessToken string) (map[int]bool, error) {
	query := `query ($userId: Int, $status: MediaListStatus) {
		Viewer {
			mediaListCollection(userId: $userId, type: ANIME, status: $status) {
				lists { entries { mediaId } }
			}
		}
	}`
	raw, err := h.anilistAuthed(ctx, accessToken, query, map[string]any{"status": "COMPLETED"})
	if err != nil {
		return nil, err
	}
	var out struct {
		Data struct {
			Viewer struct {
				MediaListCollection struct {
					Lists []struct {
						Entries []struct {
							MediaID int `json:"mediaId"`
						} `json:"entries"`
					} `json:"lists"`
				} `json:"mediaListCollection"`
			} `json:"Viewer"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	completed := map[int]bool{}
	for _, list := range out.Data.Viewer.MediaListCollection.Lists {
		for _, e := range list.Entries {
			if e.MediaID > 0 {
				completed[e.MediaID] = true
			}
		}
	}
	return completed, nil
}

// resolveAniListIDsToMAL maps AniList IDs to MAL IDs in batched GraphQL
// round trips. IDs without a mapping are omitted.
func (h *Handlers) resolveAniListIDsToMAL(ctx context.Context, anilistIDs []int) (map[int]int, error) {
	mapped := map[int]int{}
	for start := 0; start < len(anilistIDs); start += 50 {
		end := start + 50
		if end > len(anilistIDs) {
			end = len(anilistIDs)
		}
		query := `query ($ids: [Int]) { Page(perPage: 50) { media(id_in: $ids, type: ANIME) { id idMal } } }`
		raw, err := h.anilistClient.do(ctx, query, map[string]any{"ids": anilistIDs[start:end]})
		if err != nil {
			return nil, err
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
			return nil, err
		}
		for _, m := range out.Data.Page.Media {
			if m.ID > 0 && m.IDMal > 0 {
				mapped[m.ID] = m.IDMal
			}
		}
	}
	return mapped, nil
}

// fetchMALCompletedSet returns the set of MAL anime ids the user has
// already marked completed, paginated by offset (status filter + fields).
func (h *Handlers) fetchMALCompletedSet(ctx context.Context, accessToken string) (map[int]bool, error) {
	completed := map[int]bool{}
	offset := 0
	for offset < 1000 {
		u := fmt.Sprintf("https://api.myanimelist.net/v2/users/@me/animelist?limit=100&offset=%d&status=completed&fields=list_status", offset)
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			break
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)
		resp, err := h.h2Client.Do(req)
		if err != nil {
			return completed, err
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return completed, fmt.Errorf("mal list fetch returned %s", resp.Status)
		}
		var page struct {
			Data []struct {
				Node struct {
					ID int `json:"id"`
				} `json:"node"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return completed, err
		}
		if len(page.Data) == 0 {
			break
		}
		for _, e := range page.Data {
			if e.Node.ID > 0 {
				completed[e.Node.ID] = true
			}
		}
		offset += len(page.Data)
	}
	return completed, nil
}

// anilistHTTPError is a non-200 GraphQL response. It carries the status code
// and the server's Retry-After so rate-limited callers can wait out the
// window instead of burning retries inside it. Error() preserves the
// historical "anilist returned %d: %s" text that isRetryableAniListError /
// isAniListAuthError string-match on.
type anilistHTTPError struct {
	status     int
	retryAfter time.Duration
	body       string
}

func (e *anilistHTTPError) Error() string {
	return fmt.Sprintf("anilist returned %d: %s", e.status, e.body)
}

// waitAfter429 is how long to pause after this error when it is a rate
// limit: the server's Retry-After, defaulting to one full minute when the
// header is missing (AniList's limit resets per minute; observed responses
// carry "Retry-After: 60"). Returns 0 for non-429 errors.
func (e *anilistHTTPError) waitAfter429() time.Duration {
	if e.status != http.StatusTooManyRequests {
		return 0
	}
	if e.retryAfter <= 0 {
		return 60 * time.Second
	}
	return e.retryAfter
}

// parseRetryAfter reads a Retry-After header given in delta-seconds form.
// HTTP-date form and garbage both yield 0 ("no server preference").
func parseRetryAfter(v string) time.Duration {
	secs, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || secs <= 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

// isAniListRateLimitError reports whether err is an AniList HTTP 429 (typed
// or matched by text, covering GraphQL-200 error payloads too).
func isAniListRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	var httpErr *anilistHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.status == http.StatusTooManyRequests
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") || strings.Contains(msg, "too many")
}

// anilistAuthed POSTs a GraphQL request to AniList with a user token.
func (h *Handlers) anilistAuthed(ctx context.Context, accessToken, query string, variables map[string]any) ([]byte, error) {
	payload, _ := json.Marshal(map[string]any{"query": query, "variables": variables})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://graphql.anilist.co", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, &anilistHTTPError{
			status:     resp.StatusCode,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
			body:       truncate(raw, 300),
		}
	}
	return raw, nil
}

// anilistAuthedWithRetry retries transient transport, rate-limit, and server
// failures. AniList can return HTTP 200 with a GraphQL errors array, so those
// responses are inspected as well instead of being reported as success.
// Rate limits (HTTP 429) are honored, not hammered: the wait follows the
// server's Retry-After header (default one full minute — AniList's window
// resets per minute), so a retry lands after the window instead of inside it.
func (h *Handlers) anilistAuthedWithRetry(ctx context.Context, accessToken, query string, variables map[string]any) ([]byte, error) {
	const maxAttempts = 4
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		raw, err := h.anilistAuthed(ctx, accessToken, query, variables)
		if err == nil {
			var out struct {
				Errors []struct {
					Message string `json:"message"`
				} `json:"errors"`
			}
			if json.Unmarshal(raw, &out) == nil && len(out.Errors) == 0 {
				return raw, nil
			}
			message := "AniList rejected the request"
			if len(out.Errors) > 0 && out.Errors[0].Message != "" {
				message = out.Errors[0].Message
			}
			lastErr = fmt.Errorf("%s", message)
		} else {
			lastErr = err
		}
		if attempt == maxAttempts-1 || !isRetryableAniListError(lastErr) {
			break
		}
		wait := time.Duration(500*(1<<attempt)) * time.Millisecond
		if isAniListRateLimitError(lastErr) {
			wait = 60 * time.Second
			var httpErr *anilistHTTPError
			if errors.As(lastErr, &httpErr) {
				if w := httpErr.waitAfter429(); w > 0 {
					wait = w
				}
			}
			h.log.Warn().Err(lastErr).Dur("wait", wait).
				Msg("anilist rate-limited, honoring retry window")
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

// isAniListAuthError reports whether an AniList call failed on credentials
// (HTTP 401/403 or an Unauthorized GraphQL error) as opposed to a network,
// rate-limit, or server problem.
func isAniListAuthError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "401") ||
		strings.Contains(message, "403") ||
		strings.Contains(message, "unauthor") ||
		strings.Contains(message, "invalid token")
}

func isRetryableAniListError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "429") ||
		strings.Contains(message, "rate") ||
		strings.Contains(message, "too many") ||
		strings.Contains(message, "temporarily") ||
		strings.Contains(message, "returned 5") ||
		strings.Contains(message, "timeout") ||
		strings.Contains(message, "connection")
}
