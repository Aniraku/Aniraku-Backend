package v1

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"

	"github.com/Aniraku/Aniraku-Backend/internal/auth"
	"github.com/Aniraku/Aniraku-Backend/internal/config"
	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/metadata/anilist"
	"github.com/Aniraku/Aniraku-Backend/internal/metadata/mal"
	"github.com/Aniraku/Aniraku-Backend/internal/streaming"
)

var (
	Version   = "0.1.0"
	Commit    = "dev"
	BuildDate = "unknown"
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

type Handlers struct {
	cfg              *config.Config
	log              zerolog.Logger
	mal              *mal.Client
	stream           *streaming.Manager
	h2Client         *http.Client
	h1Client         *http.Client
	httpClient        *http.Client
	goTLSClient       *http.Client
	miruroProxyURL    string
	miruroProxyClient *http.Client
	keyCache          sync.Map
}

func NewHandlers(cfg *config.Config, log zerolog.Logger, miruroProxyURL string) *Handlers {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 15 * time.Second}).DialContext(ctx, "udp", "8.8.8.8:53")
		},
	}
	baseDialer := &net.Dialer{Timeout: 15 * time.Second, Resolver: resolver}

	h2Transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			conn, err := baseDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				conn.Close()
				return nil, err
			}
			tlsConn := utls.UClient(conn, &utls.Config{ServerName: host}, utls.HelloChrome_Auto)
			if err := tlsConn.Handshake(); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}
	h1Transport := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := baseDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				conn.Close()
				return nil, err
			}
			tlsConn := utls.UClient(conn, &utls.Config{ServerName: host}, utls.HelloChrome_Auto)
			if err := tlsConn.Handshake(); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
		MaxIdleConnsPerHost: 10,
	}

	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{DialContext: baseDialer.DialContext},
	}
	goTLSClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:       baseDialer.DialContext,
			MaxIdleConnsPerHost: 10,
		},
	}
	h := &Handlers{
		cfg:              cfg,
		log:              log,
		mal:              mal.NewClient(log),
		stream:           streaming.NewManager(log, miruroProxyURL, httpClient),
		h2Client:         &http.Client{Timeout: 30 * time.Second, Transport: h2Transport},
		h1Client:         &http.Client{Timeout: 30 * time.Second, Transport: h1Transport},
		httpClient:       httpClient,
		goTLSClient:      goTLSClient,
		miruroProxyURL:   miruroProxyURL,
		miruroProxyClient: &http.Client{Timeout: 60 * time.Second},
	}

	// ponytail: global lock, per-account locks if throughput matters
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			h.keyCache.Range(func(k, v any) bool {
				entry, ok := v.(keyCacheEntry)
				if ok && time.Since(entry.fetchedAt) > 10*time.Minute {
					h.keyCache.Delete(k)
				}
				return true
			})
		}
	}()

	return h
}

func (h *Handlers) respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *Handlers) respondError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// mapMalToAniList returns the input ID as-is (frontend uses AniList IDs directly).
func (h *Handlers) mapMalToAniList(_ context.Context, malID int) (int, error) {
	return malID, nil
}

func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) Version(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{
		"version": Version,
		"commit":  Commit,
		"date":    BuildDate,
	})
}

func (h *Handlers) getAnimeFromAniList(ctx context.Context, id int) (*anilist.Anime, error) {
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { id title { romaji english native userPreferred } coverImage { extraLarge large medium color } bannerImage format status episodes duration genres averageScore popularity description season seasonYear nextAiringEpisode { episode airingAt } isAdult } }`
	body, _ := json.Marshal(map[string]any{"query": query, "variables": map[string]int{"id": id}})

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.anilist.co", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := h.h2Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anilist %d", resp.StatusCode)
	}

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
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
	if title, ok := media["title"].(map[string]any); ok {
		if v, ok := title["romaji"].(string); ok { a.Title.Romaji = &v }
		if v, ok := title["english"].(string); ok { a.Title.English = &v }
		if v, ok := title["native"].(string); ok { a.Title.Native = &v }
		if v, ok := title["userPreferred"].(string); ok { v2 := v; a.Title.UserPreferred = &v2 }
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
	body, _ := json.Marshal(map[string]any{"query": query, "variables": map[string]int{"id": id}})
	
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, "https://graphql.anilist.co", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	
	resp, err := h.h2Client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		h.respondError(w, http.StatusBadGateway, "failed to fetch anime metadata")
		return
	}
	defer resp.Body.Close()
	
	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
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

	// Use AniList GraphQL
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { id title { romaji english userPreferred } coverImage { extraLarge large medium } episodes format status nextAiringEpisode { episode airingAt } } }`
	body, _ := json.Marshal(map[string]any{"query": query, "variables": map[string]int{"id": id}})
	
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, "https://graphql.anilist.co", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	
	resp, err := h.h2Client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		h.respondError(w, http.StatusBadGateway, "failed to fetch anime metadata")
		return
	}
	defer resp.Body.Close()
	
	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)
	data, _ := result["data"].(map[string]any)
	media, _ := data["Media"].(map[string]any)
	if media == nil {
		h.respondError(w, http.StatusBadGateway, "invalid anime data")
		return
	}
	
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

	anilistID := id
	thumbs := h.stream.GetEpisodeThumbnails(r.Context(), anilistID)
	titles := h.stream.GetEpisodeTitles(r.Context(), anilistID)

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

	episodes := make([]map[string]any, episodeCount)
	for i := 0; i < episodeCount; i++ {
		epNum := i + 1
		ep := map[string]any{
			"number": epNum,
			"title":  fmt.Sprintf("Episode %d", epNum),
		}
		if thumb, ok := thumbs[epNum]; ok {
			ep["thumbnail"] = thumb
		} else if coverFallback != "" {
			ep["thumbnail"] = coverFallback
		}
		if title, ok := titles[epNum]; ok {
			ep["title"] = title
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
			Genre:  anime.Genres[:1],
			Sort:   "SCORE_DESC",
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

func (h *Handlers) GetManga(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	_, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid manga ID")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "manga metadata placeholder"})
}

func (h *Handlers) GetChapters(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	_, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid manga ID")
		return
	}
	h.respondJSON(w, http.StatusOK, core.ChapterList{Chapters: []core.Chapter{}})
}

func (h *Handlers) GetChapterPages(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	chStr := chi.URLParam(r, "ch")
	_, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid manga ID")
		return
	}
	_, err = strconv.Atoi(chStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid chapter number")
		return
	}
	h.respondJSON(w, http.StatusOK, core.PageList{Pages: []core.Page{}})
}

func (h *Handlers) Stream(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var req core.StreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.AnimeID == 0 || req.Episode == 0 {
		h.respondError(w, http.StatusBadRequest, "animeId and episode are required")
		return
	}

	if req.Lang == "" {
		req.Lang = "sub"
	}
	if req.Quality == "" {
		req.Quality = "auto"
	}

	ctx := r.Context()
	if req.Refresh {
		ctx = streaming.WithRefresh(ctx)
	}

	// ponytail: map MAL ID → AniList ID for Miruro streaming
	anilistID, err := h.mapMalToAniList(ctx, req.AnimeID)
	if err != nil {
		h.log.Warn().Err(err).Int("malId", req.AnimeID).Msg("failed to map MAL to AniList")
		h.respondError(w, http.StatusNotFound, "anime not found for streaming")
		return
	}

	// Find best source for the requested provider/lang only
	result, err := h.stream.GetSourcesForProvider(ctx, "", req.Episode, req.Provider, req.Lang, req.Quality, anilistID)

	if err != nil || result == nil || len(result.Sources) == 0 {
		h.log.Warn().Err(err).Int("animeId", req.AnimeID).Str("lang", req.Lang).Str("provider", req.Provider).Msg("streaming failed")
		if err != nil && (strings.Contains(err.Error(), "no source") || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "not available")) {
			h.respondError(w, http.StatusNotFound, "no streaming source found")
			return
		}
		h.respondError(w, http.StatusBadGateway, "streaming source failed")
		return
	}

	h.respondJSON(w, http.StatusOK, result)
}

func (h *Handlers) GetServers(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("animeId")
	epStr := r.URL.Query().Get("episode")
	lang := r.URL.Query().Get("lang")

	if idStr == "" || epStr == "" {
		h.respondError(w, http.StatusBadRequest, "animeId and episode are required")
		return
	}
	if lang == "" {
		lang = "sub"
	}

	animeID, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid animeId")
		return
	}
	episode, err := strconv.Atoi(epStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid episode")
		return
	}

	ctx := r.Context()
	// ponytail: map MAL ID → AniList ID for Miruro streaming
	anilistID, err := h.mapMalToAniList(ctx, animeID)
	if err != nil {
		h.log.Warn().Err(err).Int("malId", animeID).Msg("failed to map MAL to AniList")
		h.respondError(w, http.StatusNotFound, "anime not found for streaming")
		return
	}
	servers := h.stream.FindAllServers(ctx, anilistID, episode, lang)
	h.respondJSON(w, http.StatusOK, servers)
}

func (h *Handlers) Proxy(w http.ResponseWriter, r *http.Request) {
	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		h.respondError(w, http.StatusBadRequest, "url parameter required")
		return
	}

	decodedURL, err := url.QueryUnescape(targetURL)
	if err != nil {
		decodedURL = targetURL
	}

	// Basic SSRF guard: only http(s), block obvious local/metadata targets
	parsed, err := url.Parse(decodedURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		h.respondError(w, http.StatusBadRequest, "invalid URL scheme")
		return
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "0.0.0.0" ||
		host == "::1" || strings.HasPrefix(host, "10.") || strings.HasPrefix(host, "192.168.") ||
		strings.HasPrefix(host, "172.") || strings.HasPrefix(host, "169.254.") ||
		host == "metadata.google.internal" {
		h.respondError(w, http.StatusForbidden, "proxy target not allowed")
		return
	}

	// Create request with proper headers
	req, err := http.NewRequestWithContext(r.Context(), "GET", decodedURL, nil)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid URL")
		return
	}

	// Disable compression — our utls/http2 transport doesn't auto-decompress like
	// a default http.Transport does. Upstream gzipped bytes would arrive
	// garbled, corrupting AES-128 keys and segment data.
	req.Header.Set("Accept-Encoding", "identity")

	// Set headers from query param
	headersJSON := r.URL.Query().Get("headers")
	if headersJSON != "" {
		var headers map[string]string
		if json.Unmarshal([]byte(headersJSON), &headers) == nil {
			for k, v := range headers {
				// ponytail: strip dangerous headers to prevent injection
				lower := strings.ToLower(k)
				if lower == "host" || lower == "transfer-encoding" || lower == "connection" ||
					lower == "proxy-connection" || lower == "upgrade" || lower == "te" ||
					strings.HasPrefix(lower, "x-") {
					continue
				}
				req.Header.Set(k, v)
			}
		}
	}

	// Set referer for CDNs that require it — client headers take priority where provided
	if req.Header.Get("Referer") == "" {
		if strings.Contains(decodedURL, "uwucdn") || strings.Contains(decodedURL, "owocdn") || strings.Contains(decodedURL, "185.237.106.79") {
			req.Header.Set("Referer", "https://kwik.cx/")
			req.Header.Set("Origin", "https://kwik.cx")
		} else if strings.Contains(decodedURL, "senshi") || strings.Contains(decodedURL, "ninstream") {
			req.Header.Set("Referer", "https://senshi.live")
		}
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	}

	// Cache key responses — hls.js makes one request per unique KEY URI
	// (we append &sn=N to each per-segment KEY tag). The backend strips sn
	// from the upstream URL, so every request fetches the same CDN key URL.
	cacheKeyURL := decodedURL
	if idx := strings.IndexByte(decodedURL, '?'); idx >= 0 {
		baseQ, _ := url.ParseQuery(decodedURL[idx+1:])
		for _, skip := range []string{"sn", "iv", "t"} {
			baseQ.Del(skip)
		}
		if clean := baseQ.Encode(); clean != "" {
			cacheKeyURL = decodedURL[:idx+1] + clean
		} else {
			cacheKeyURL = decodedURL[:idx]
		}
	}
	isKey := strings.HasSuffix(strings.ToLower(parsed.Path), ".key")

	if isKey {
		if cached, ok := h.keyCache.Load(cacheKeyURL); ok {
			if entry, ok := cached.(keyCacheEntry); ok && time.Since(entry.fetchedAt) < 5*time.Minute {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Cache-Control", "public, max-age=300")
				w.WriteHeader(http.StatusOK)
				w.Write(entry.data)
				return
			}
			h.keyCache.Delete(cacheKeyURL)
		}
	}

	resp, err := h.doRequest(req, parsed.Scheme == "https")
	if err != nil {
		errStr := err.Error()
		h.log.Warn().Err(err).Str("proxy_url", decodedURL).Msg("proxy upstream connection failed")
		if strings.Contains(errStr, "connection refused") ||
			strings.Contains(errStr, "no route to host") ||
			strings.Contains(errStr, "i/o timeout") {
			h.respondError(w, http.StatusBadGateway, "CDN_BLOCKED: upstream refused connection - CDN blocks datacenter IPs")
			return
		}
		h.respondError(w, http.StatusBadGateway, "upstream request failed")
		return
	}
	defer resp.Body.Close()

	// ponytail: debug — log upstream status for proxy issues
	h.log.Debug().Str("proxy_url", decodedURL).Int("upstream_status", resp.StatusCode).Msg("proxy upstream response")

	if resp.StatusCode == 403 || resp.StatusCode == 502 || resp.StatusCode == 503 {
		errStr := fmt.Sprintf("upstream returned %d", resp.StatusCode)
		h.log.Warn().Str("proxy_url", decodedURL).Int("upstream_status", resp.StatusCode).Msg("proxy upstream rejected")
		if resp.StatusCode == 502 || resp.StatusCode == 403 {
			h.respondError(w, http.StatusBadGateway, "CDN_BLOCKED: upstream rejected (HTTP "+strconv.Itoa(resp.StatusCode)+")")
			return
		}
		h.respondError(w, http.StatusBadGateway, errStr)
		return
	}

	if isKey && resp.StatusCode == http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		h.keyCache.Store(cacheKeyURL, keyCacheEntry{data: body, fetchedAt: time.Now()})

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
		return
	}

	// Check if this is an HLS playlist — use parsed path, not raw URL (query params break HasSuffix)
	contentType := resp.Header.Get("Content-Type")
	pathLower := strings.ToLower(parsed.Path)
	isHLS := strings.Contains(contentType, "mpegurl") || strings.Contains(contentType, "m3u8") ||
		strings.HasSuffix(pathLower, ".m3u8") || strings.HasSuffix(pathLower, ".m3u")

	if isHLS {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			h.respondError(w, http.StatusBadGateway, "failed to read HLS playlist")
			return
		}

		// Rewrite HLS playlist to route through proxy, passing custom headers
		rewritten := h.rewriteHLSPlaylist(string(body), decodedURL, headersJSON)
		if len(rewritten) < 1500 {
			h.log.Debug().Str("playlist_body", rewritten).Str("proxy_url", decodedURL).Msg("rewritten HLS playlist")
		} else {
			h.log.Debug().Str("playlist_preview", rewritten[:1500]).Str("proxy_url", decodedURL).Msg("rewritten HLS playlist (truncated)")
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.WriteHeader(resp.StatusCode)
		w.Write([]byte(rewritten))
		return
	}
	// Stream non-HLS content directly through the proxy.
	// Force correct Content-Type for TS segments — CDN lies with "image/jpeg"
	// to bypass Cloudflare media blocking.
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	if strings.HasSuffix(pathLower, ".jpg") || strings.HasSuffix(pathLower, ".ts") {
		if !strings.HasSuffix(pathLower, ".m3u8") && !strings.HasSuffix(pathLower, ".key") {
			ct = "video/mp2t"
		}
	}
	if strings.HasSuffix(pathLower, ".key") {
		ct = "application/octet-stream"
	}

	w.Header().Set("Content-Type", ct)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (h *Handlers) doRequest(req *http.Request, https bool) (*http.Response, error) {
	if !https {
		return h.httpClient.Do(req)
	}
	resp, err := h.h2Client.Do(req)
	if err == nil {
		return resp, nil
	}
	resp, err = h.h1Client.Do(req)
	if err == nil {
		return resp, nil
	}
	// ponytail: standard Go TLS fallback — utls Chrome fingerprint triggers
	// bot detection on some CDNs (nekostream, watching.onl). Native Go TLS works fine.
	return h.goTLSClient.Do(req)
}

func (h *Handlers) rewriteHLSPlaylist(content, baseURL, headersJSON string) string {
	lines := strings.Split(content, "\n")
	baseParts := strings.Split(baseURL, "/")
	var basePrefix string
	if len(baseParts) < 4 {
		basePrefix = baseURL
	} else {
		basePrefix = strings.Join(baseParts[:len(baseParts)-1], "/")
	}

	headersParam := ""
	if headersJSON != "" {
		headersParam = "&headers=" + url.QueryEscape(headersJSON)
	}

	isEncrypted := false
	encKeyURI := ""
	segmentNumRe := regexp.MustCompile(`segment-(\d+)-`)

	for i, line := range lines {
		original := strings.TrimSpace(line)

		// Handle any HLS tag with URI="..." attribute (#EXT-X-KEY, #EXT-X-MAP, #EXT-X-MEDIA, etc.)
		if strings.HasPrefix(original, "#") && strings.Contains(original, "URI=") {
			re := regexp.MustCompile(`URI="([^"]+)"`)
			rewritten := re.ReplaceAllStringFunc(original, func(match string) string {
				parts := strings.SplitN(match, "=", 2)
				if len(parts) != 2 {
					return match
				}
				uri := strings.Trim(parts[1], "\"")
				absoluteURL := resolveURL(uri, basePrefix)
				if needsProxyRewrite(absoluteURL) || headersJSON != "" {
					proxied := fmt.Sprintf("/api/v1/proxy?url=%s%s", url.QueryEscape(absoluteURL), headersParam)
					return fmt.Sprintf("URI=\"%s\"", proxied)
				}
				return match
			})
			if strings.Contains(original, "METHOD=AES-128") {
				isEncrypted = true
				if match := regexp.MustCompile(`URI="([^"]+)"`).FindStringSubmatch(rewritten); len(match) >= 2 {
					encKeyURI = match[1]
				}
			}
			lines[i] = rewritten
			continue
		}

		// #EXTINF and other non-URI HLS tags — left unchanged
		if strings.HasPrefix(original, "#") {
			continue
		}

		// Skip empty lines
		if original == "" {
			continue
		}

		// Resolve relative URLs for segment lines
		var absoluteURL string
		if strings.HasPrefix(original, "http") {
			absoluteURL = original
		} else if strings.HasPrefix(original, "//") {
			absoluteURL = "https:" + original
		} else {
			absoluteURL = basePrefix + "/" + original
		}

		// If encrypted, insert per-segment KEY tag with file-number-based IV
		// ponytail: CDN uses file number as IV (not MEDIA-SEQUENCE), and the
		// playlist skips segments 2-3, so hls.js default MEDIA-SEQ IV is wrong.
		if isEncrypted && encKeyURI != "" {
			if matches := segmentNumRe.FindStringSubmatch(original); len(matches) >= 2 {
				num, _ := strconv.ParseInt(matches[1], 10, 64)
				iv := fmt.Sprintf("0x%032x", num)
				// hls.js indexes LevelKey by URI — same URI skips IV update.
				// Append segment number to force a separate LevelKey per segment.
				keyTag := fmt.Sprintf(`#EXT-X-KEY:METHOD=AES-128,URI="%s&sn=%d",IV=%s`, encKeyURI, num, iv)
				if headersJSON != "" || needsProxyRewrite(absoluteURL) {
					lines[i] = keyTag + "\n" + fmt.Sprintf("/api/v1/proxy?url=%s%s", url.QueryEscape(absoluteURL), headersParam)
				} else {
					lines[i] = keyTag + "\n" + absoluteURL
				}
				continue
			}
		}

		if headersJSON != "" || needsProxyRewrite(absoluteURL) {
			lines[i] = fmt.Sprintf("/api/v1/proxy?url=%s%s", url.QueryEscape(absoluteURL), headersParam)
		} else {
			lines[i] = absoluteURL
		}
	}

	result := strings.Join(lines, "\n")

	// VOD playlists without #EXT-X-ENDLIST cause hls.js to loop re-fetching.
	// Append it if missing — the CDN already returned the full segment list.
	if strings.Contains(result, "#EXT-X-PLAYLIST-TYPE:VOD") && !strings.Contains(result, "#EXT-X-ENDLIST") {
		result += "\n#EXT-X-ENDLIST"
	}

	return result
}

func needsProxyRewrite(rawURL string) bool {
	lower := strings.ToLower(rawURL)
	return strings.Contains(lower, "senshi") ||
		strings.Contains(lower, "ninstream") ||
		strings.Contains(lower, "uwucdn") ||
		strings.Contains(lower, "owocdn") ||
		strings.Contains(lower, "kotocdn") ||
		strings.Contains(lower, "vivibebe") ||
		strings.Contains(lower, "wixmp") ||
		strings.Contains(lower, "anidb") ||
		strings.Contains(lower, "animegg") ||
		strings.Contains(lower, "nekostream") ||
		strings.Contains(lower, "watching.onl") ||
		strings.Contains(lower, "krussdomi") ||
		strings.Contains(lower, "mewstream") ||
		strings.Contains(lower, "megaplay") ||
		strings.Contains(lower, "fast4speed") ||
		strings.Contains(lower, "185.237.106.79")
}

func resolveURL(uri, basePrefix string) string {
	if strings.HasPrefix(uri, "http") {
		return uri
	}
	if strings.HasPrefix(uri, "//") {
		return "https:" + uri
	}
	return basePrefix + "/" + uri
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

func (h *Handlers) SaveAnimeProgress(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "saved"})
}

func (h *Handlers) SaveMangaProgress(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "saved"})
}

type watchHistoryEntry struct {
	AnimeID     int    `json:"anime_id"`
	AnimeTitle  string `json:"anime_title"`
	AnimeImage  string `json:"anime_image"`
	Episode     int    `json:"episode_number"`
	Progress    int    `json:"progress"`
	Duration    int    `json:"duration"`
	Timestamp   int64  `json:"timestamp"`
}

type continueWatchingItem struct {
	AnimeID   int    `json:"animeId"`
	Title     string `json:"title"`
	Image     string `json:"image"`
	Episode   int    `json:"episode"`
	Time      int    `json:"time"`
	Duration  int    `json:"duration"`
	Timestamp int64  `json:"timestamp"`
}

func (h *Handlers) GetContinueWatching(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}

	supabaseURL := h.cfg.Supabase.URL
	serviceKey := h.cfg.Supabase.ServiceKey
	if supabaseURL == "" || serviceKey == "" {
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}

	reqURL := supabaseURL + "/rest/v1/watch_history?select=anime_id,anime_title,anime_image,episode_number,progress,duration,timestamp&user_id=eq." + userID + "&order=timestamp.desc&limit=30"

	req, err := http.NewRequestWithContext(r.Context(), "GET", reqURL, nil)
	if err != nil {
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}
	req.Header.Set("apikey", serviceKey)
	req.Header.Set("Authorization", "Bearer "+serviceKey)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}
	defer resp.Body.Close()

	var history []watchHistoryEntry
	if err := json.NewDecoder(resp.Body).Decode(&history); err != nil {
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}

	items := make([]continueWatchingItem, 0, len(history))
	for _, entry := range history {
		items = append(items, continueWatchingItem{
			AnimeID:   entry.AnimeID,
			Title:     entry.AnimeTitle,
			Image:     entry.AnimeImage,
			Episode:   entry.Episode,
			Time:      entry.Progress,
			Duration:  entry.Duration,
			Timestamp: entry.Timestamp,
		})
	}

	h.respondJSON(w, http.StatusOK, items)
}

func (h *Handlers) Search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		h.respondError(w, http.StatusBadRequest, "query parameter 'q' is required")
		return
	}

	page := parsePageParam(r, 1)
	perPage := parsePerPageParam(r, 20)

	if strings.EqualFold(q, "uncensored") {
		results, err := h.browseAdult(r.Context(), page, perPage)
		if err != nil {
			h.log.Warn().Err(err).Msg("adult search failed")
			h.respondError(w, http.StatusBadGateway, "search failed")
			return
		}
		for i := range results.Data.Page.Media {
			suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
		}
		h.respondJSON(w, http.StatusOK, map[string]any{
			"query":    q,
			"results":  results.Data.Page.Media,
			"pageInfo": results.Data.Page.PageInfo,
		})
		return
	}

	// If filter params are present, use Browse with the search term for richer suggestions
	genres := r.URL.Query()["genre"]
	formats := r.URL.Query()["format"]
	statuses := r.URL.Query()["status"]
	if len(genres) > 0 || len(formats) > 0 || len(statuses) > 0 {
		filters := anilist.BrowseFilters{
			Genre:  genres,
			Format: formats,
			Status: statuses,
			Search: q,
			Sort:   "SEARCH_MATCH",
		}
		results, err := h.browseAniList(r.Context(), filters, page, perPage)
		if err != nil {
			h.log.Warn().Err(err).Str("query", q).Msg("anilist filtered search failed, falling back to jikan")
			results, err = h.mal.Browse(r.Context(), filters, page, perPage)
		}
		if err != nil {
			h.log.Warn().Err(err).Str("query", q).Msg("filtered search failed")
			h.respondError(w, http.StatusBadGateway, "search failed")
			return
		}
		for i := range results.Data.Page.Media {
			if results.Data.Page.Media[i].IsAdult {
				suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
			}
		}
		h.respondJSON(w, http.StatusOK, map[string]any{
			"query":    q,
			"results":  results.Data.Page.Media,
			"pageInfo": results.Data.Page.PageInfo,
		})
		return
	}

	results, err := h.mal.Search(r.Context(), q, page, perPage)
	if err != nil {
		h.log.Warn().Err(err).Str("query", q).Msg("jikan search failed, falling back to anilist")
		// fallback: use AniList search via browseAniList with search term
		fallbackFilters := anilist.BrowseFilters{Search: q, Sort: "SEARCH_MATCH"}
		results, err = h.browseAniList(r.Context(), fallbackFilters, page, perPage)
	}
	if err != nil {
		h.log.Warn().Err(err).Str("query", q).Msg("search failed")
		h.respondError(w, http.StatusBadGateway, "search failed")
		return
	}

	for i := range results.Data.Page.Media {
		if results.Data.Page.Media[i].IsAdult {
			suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
		}
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"query":    q,
		"results":  results.Data.Page.Media,
		"pageInfo": results.Data.Page.PageInfo,
	})
}

func (h *Handlers) ImportMAL(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, http.StatusNotImplemented, "MAL import not yet implemented")
}

func (h *Handlers) ImportAniList(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, http.StatusNotImplemented, "AniList import not yet implemented")
}

func (h *Handlers) ImportStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	h.respondJSON(w, http.StatusOK, map[string]string{"jobId": jobID, "status": "pending"})
}

func (h *Handlers) GetProfile(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	h.respondJSON(w, http.StatusOK, map[string]string{"username": username, "status": "placeholder"})
}

func (h *Handlers) UpdateProfile(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *Handlers) AddFavorite(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "added"})
}

func (h *Handlers) RemoveFavorite(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func (h *Handlers) ListFavorites(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]any{"favorites": []any{}})
}

func (h *Handlers) ClientLog(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "received"})
}

func (h *Handlers) GetSetting(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	h.respondJSON(w, http.StatusOK, map[string]string{"key": key, "value": ""})
}

func (h *Handlers) UpdateSetting(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *Handlers) GetNotifications(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondJSON(w, http.StatusOK, []any{})
		return
	}
	// Notifications are stored in Supabase, frontend queries directly
	h.respondJSON(w, http.StatusOK, []any{})
}

func (h *Handlers) MarkNotificationRead(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "marked"})
}

func (h *Handlers) MarkAllNotificationsRead(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "all marked"})
}

func (h *Handlers) GetMiruroEpisodes(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	if idStr == "" {
		h.respondError(w, http.StatusBadRequest, "anime ID is required")
		return
	}

	// ponytail: map MAL ID → AniList ID for Miruro
	malID, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}
	anilistID, err := h.mapMalToAniList(r.Context(), malID)
	if err != nil {
		h.respondError(w, http.StatusNotFound, "anime not found")
		return
	}

	miruroURL := fmt.Sprintf("%s/episodes/%d", h.miruroProxyURL, anilistID)
	resp, err := h.miruroProxyClient.Get(miruroURL)
	if err != nil {
		h.log.Warn().Err(err).Str("id", idStr).Msg("miruro episodes request failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch episodes from Miruro")
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to read miruro response")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

func (h *Handlers) GetRelations(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	relations, err := h.mal.GetRelations(r.Context(), id)
	if err != nil {
		h.log.Warn().Err(err).Int("id", id).Msg("failed to fetch relations from AniList")
		h.respondError(w, http.StatusBadGateway, "failed to fetch relations")
		return
	}

	h.respondJSON(w, http.StatusOK, relations)
}

func (h *Handlers) HasDub(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	anilistID, _ := h.mapMalToAniList(r.Context(), id)
	hasDub := h.stream.HasAnimeDub(r.Context(), anilistID)
	h.respondJSON(w, http.StatusOK, map[string]bool{"hasDub": hasDub})
}

func (h *Handlers) trendingAniList(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
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
	body, _ := json.Marshal(map[string]any{
		"query": query,
		"variables": map[string]int{
			"page":    page,
			"perPage": perPage,
		},
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.anilist.co", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := h.h2Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("anilist %d: %s", resp.StatusCode, string(respBody))
	}

	var result anilist.BrowseResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (h *Handlers) browseAniList(ctx context.Context, filters anilist.BrowseFilters, page, perPage int) (*anilist.BrowseResponse, error) {
	// Build AniList GraphQL query with variable placeholders
	variables := map[string]any{
		"page":    page,
		"perPage": perPage,
	}

	var typeArgs []string
	typeArgs = append(typeArgs, "type: ANIME")

	if filters.Search != "" {
		typeArgs = append(typeArgs, "search: $search")
		variables["search"] = filters.Search
	}
	if len(filters.Genre) > 0 {
		typeArgs = append(typeArgs, "genre: $genre")
		variables["genre"] = filters.Genre[0]
	}
	if len(filters.Format) > 0 {
		typeArgs = append(typeArgs, "format: $format")
		variables["format"] = strings.ToUpper(filters.Format[0])
	}
	if len(filters.Status) > 0 {
		typeArgs = append(typeArgs, "status: $status")
		variables["status"] = strings.ToUpper(filters.Status[0])
	}
	if filters.Season != "" {
		typeArgs = append(typeArgs, "season: $season")
		variables["season"] = strings.ToUpper(filters.Season)
	}
	if filters.Year > 0 {
		typeArgs = append(typeArgs, "seasonYear: $year")
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
	variables["sort"] = sort

	query := fmt.Sprintf(`query ($page: Int, $perPage: Int, $search: String, $genre: String, $format: MediaFormat, $status: MediaStatus, $season: MediaSeason, $year: Int, $sort: [MediaSort]) {
		Page(page: $page, perPage: $perPage) {
			pageInfo { total lastPage hasNextPage currentPage perPage }
			media(%s) {
				id title { romaji english native userPreferred }
				coverImage { extraLarge large medium color }
				bannerImage format status episodes averageScore popularity season seasonYear genres isAdult
				nextAiringEpisode { episode airingAt }
			}
		}
	}`, strings.Join(typeArgs, ", "))

	body, _ := json.Marshal(map[string]any{"query": query, "variables": variables})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.anilist.co", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := h.h2Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("anilist %d: %s", resp.StatusCode, string(respBody))
	}

	var result anilist.BrowseResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
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
	body, _ := json.Marshal(map[string]any{
		"query": query,
		"variables": map[string]int{
			"page":    page,
			"perPage": perPage,
		},
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.anilist.co", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := h.h2Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result anilist.BrowseResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ponytail: simple AniList GraphQL proxy with 429 retry + backoff
func (h *Handlers) AniListProxy(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "failed to read body")
		return
	}
	defer r.Body.Close()

	var resp *http.Response
	for attempt := range 3 {
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, "https://graphql.anilist.co", bytes.NewReader(body))
		if err != nil {
			h.respondError(w, http.StatusInternalServerError, "failed to build request")
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		resp, err = h.h2Client.Do(req)
		if err != nil {
			h.respondError(w, http.StatusBadGateway, "AniList unreachable")
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			break
		}
		resp.Body.Close()
		if attempt < 2 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}
	if resp == nil {
		h.respondError(w, http.StatusTooManyRequests, "AniList rate limited after retries")
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
