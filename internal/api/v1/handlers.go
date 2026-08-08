package v1

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	utls "github.com/refraction-networking/utls"
	"github.com/rs/zerolog"
	"golang.org/x/net/http2"

	"github.com/Aniraku/Aniraku-Backend/internal/auth"
	"github.com/Aniraku/Aniraku-Backend/internal/config"
	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/metadata/anilist"
	"github.com/Aniraku/Aniraku-Backend/internal/metadata/mal"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
	"github.com/Aniraku/Aniraku-Backend/internal/streaming"
)

var (
	Version   = "0.1.0"
	Commit    = "dev"
	BuildDate = "unknown"
)

const maxProbeCacheSize = 2000

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

type probeCacheEntry struct {
	subCount  int
	dubCount  int
	playable  bool
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
type tokenBucket struct {
	mu       sync.Mutex
	capacity float64
	tokens   float64
	refill   float64 // tokens per second
	last     time.Time
}

func newTokenBucket(capacity, refillPerSec float64) *tokenBucket {
	return &tokenBucket{capacity: capacity, tokens: capacity, refill: refillPerSec, last: time.Now()}
}

// wait blocks until a token is available or the context is cancelled,
// smoothing bursts into the configured sustained rate.
func (tb *tokenBucket) wait(ctx context.Context) error {
	for {
		tb.mu.Lock()
		now := time.Now()
		tb.tokens = math.Min(tb.capacity, tb.tokens+now.Sub(tb.last).Seconds()*tb.refill)
		tb.last = now
		if tb.tokens >= 1 {
			tb.tokens--
			tb.mu.Unlock()
			return nil
		}
		need := (1 - tb.tokens) / tb.refill
		tb.mu.Unlock()
		timer := time.NewTimer(time.Duration(need*float64(time.Second)) + 10*time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// anilistClient provides retry logic, caching, and rate limiting for AniList API calls
type anilistClient struct {
	h          *Handlers
	client     *http.Client
	cache      sync.Map
	cacheTTL   time.Duration
	maxRetries int
	baseDelay  time.Duration
	limiter    *tokenBucket
}

type anilistCacheEntry struct {
	data      []byte
	fetchedAt time.Time
}

func newAnilistClient(h *Handlers) *anilistClient {
	return &anilistClient{
		h:          h,
		client:     h.h2Client,
		cacheTTL:   5 * time.Minute,
		maxRetries: 3,
		baseDelay:  1 * time.Second,
		// Burst 15, 0.9/s refill (~54/min sustained): a page load fires
		// ~10-15 parallel queries (home/catalog), and the old burst of 40
		// tripped AniList's shared-per-IP 429s on every fast scroll.
		limiter: newTokenBucket(15, 0.9),
	}
}

// circuitBreaker implements a simple circuit breaker pattern for AniList
type circuitBreaker struct {
	failures       int
	successes      int
	lastFailure    time.Time
	state          int // 0=closed, 1=open, 2=half-open
	mu             sync.Mutex
	failureThreshold int
	successThreshold int
	timeout        time.Duration
}

func newCircuitBreaker() *circuitBreaker {
	return &circuitBreaker{
		failureThreshold: 5,
		successThreshold: 2,
		timeout:          30 * time.Second,
	}
}

func (cb *circuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case 0: // closed
		return true
	case 1: // open
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = 2 // half-open
			return true
		}
		return false
	case 2: // half-open
		return true
	}
	return false
}

func (cb *circuitBreaker) recordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.successes++
	cb.failures = 0
	if cb.state == 2 && cb.successes >= cb.successThreshold {
		cb.state = 0
		cb.successes = 0
	}
}

func (cb *circuitBreaker) recordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = time.Now()
	cb.successes = 0
	if cb.failures >= cb.failureThreshold {
		cb.state = 1 // open
	}
}

func (cb *circuitBreaker) getState() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// providerHealth tracks per-provider health
type providerHealth struct {
	consecutiveFailures int
	lastSuccess         time.Time
	lastFailure         time.Time
	mu                  sync.Mutex
}

func (ph *providerHealth) recordSuccess() {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	ph.consecutiveFailures = 0
	ph.lastSuccess = time.Now()
}

func (ph *providerHealth) recordFailure() {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	ph.consecutiveFailures++
	ph.lastFailure = time.Now()
}

func (ph *providerHealth) isHealthy() bool {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	// Unhealthy after 3 consecutive failures
	return ph.consecutiveFailures < 3
}

func (ph *providerHealth) getConsecutiveFailures() int {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	return ph.consecutiveFailures
}

func (c *anilistClient) getCacheKey(query string, variables map[string]any) string {
	v, _ := json.Marshal(variables)
	return fmt.Sprintf("%s:%s", query, string(v))
}

func (c *anilistClient) do(ctx context.Context, query string, variables map[string]any) ([]byte, error) {
	// Check circuit breaker
	if c.h.anilistCircuit != nil && !c.h.anilistCircuit.allow() {
		// Circuit open - try to serve stale cache
		cacheKey := c.getCacheKey(query, variables)
		if cached, ok := c.cache.Load(cacheKey); ok {
			if entry, ok := cached.(anilistCacheEntry); ok {
				c.h.log.Warn().Str("cache_key", cacheKey).Msg("circuit open, serving stale cache")
				return entry.data, nil
			}
		}
		return nil, fmt.Errorf("anilist circuit open, rate limited")
	}

	cacheKey := c.getCacheKey(query, variables)

	// Request deduplication: check if same query is in-flight
	if inFlight, ok := c.h.anilistInflight.Load(cacheKey); ok {
		if ch, ok := inFlight.(chan struct{}); ok {
			select {
			case <-ch:
				// Original request finished; fall through to the cache lookup below.
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	// Check cache first
	if cached, ok := c.cache.Load(cacheKey); ok {
		if entry, ok := cached.(anilistCacheEntry); ok && time.Since(entry.fetchedAt) < c.cacheTTL {
			return entry.data, nil
		}
		c.cache.Delete(cacheKey)
	}

	// Create in-flight channel for deduplication. On completion we cache the
	// result first, then close the channel so all waiters wake up and serve
	// from cache (or retry themselves if the original request failed).
	inFlightChan := make(chan struct{})
	c.h.anilistInflight.Store(cacheKey, inFlightChan)
	defer func() {
		c.h.anilistInflight.Delete(cacheKey)
		close(inFlightChan)
	}()

	body, _ := json.Marshal(map[string]any{"query": query, "variables": variables})

	if err := c.limiter.wait(ctx); err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.anilist.co", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.maxRetries {
				time.Sleep(c.baseDelay * time.Duration(attempt+1))
				continue
			}
			if c.h.anilistCircuit != nil {
				c.h.anilistCircuit.recordFailure()
			}
			return nil, fmt.Errorf("anilist unreachable: %w", lastErr)
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt < c.maxRetries {
				delay := c.baseDelay * time.Duration(attempt+1)
				time.Sleep(delay)
				continue
			}
			if c.h.anilistCircuit != nil {
				c.h.anilistCircuit.recordFailure()
			}
			return nil, fmt.Errorf("anilist rate limited after retries: %s", string(respBody))
		}

		if resp.StatusCode != http.StatusOK {
			if c.h.anilistCircuit != nil {
				c.h.anilistCircuit.recordFailure()
			}
			return nil, fmt.Errorf("anilist %d: %s", resp.StatusCode, string(respBody))
		}

		// Success - record circuit breaker success and cache.
		// Waiters are released via the deferred close once we return.
		if c.h.anilistCircuit != nil {
			c.h.anilistCircuit.recordSuccess()
		}
		c.cache.Store(cacheKey, anilistCacheEntry{data: respBody, fetchedAt: time.Now()})

		return respBody, nil
	}

	if c.h.anilistCircuit != nil {
		c.h.anilistCircuit.recordFailure()
	}
	return nil, lastErr
}

type Handlers struct {
	cfg               *config.Config
	log               zerolog.Logger
	mal               *mal.Client
	stream            *streaming.Manager
	h2Client          *http.Client
	h1Client          *http.Client
	httpClient        *http.Client
	goTLSClient       *http.Client
	miruroProxyURL    string
	miruroProxyClient *http.Client
	keyCache          sync.Map
	probeCache        sync.Map
	probeCacheMu      sync.Mutex
	probeCacheCount   int
	sourceProbeMemo   sync.Map // srcType|url|headers -> sourceProbeMemoEntry (90s TTL)
	// Browse/trending cache with TTL
	browseCache       sync.Map
	browseCacheTTL    time.Duration
	anilistClient     *anilistClient
	// In-flight OAuth handshakes for MAL/AniList sync: state -> pendingOAuth.
	syncPending       sync.Map

	// --- Resilience layer ---
	anilistCircuit    *circuitBreaker
	anilistInflight   sync.Map // request deduplication
	providerHealth    sync.Map // provider -> *providerHealth
}

func NewHandlers(cfg *config.Config, log zerolog.Logger, miruroProxyURL string) *Handlers {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 15 * time.Second}).DialContext(ctx, "udp", "8.8.8.8:53")
		},
	}
	baseDialer := &net.Dialer{
		Timeout:  15 * time.Second,
		Resolver: resolver,
		// SSRF guard: every outbound socket this server dials passes through
		// this check on the final resolved IP. This is the authoritative
		// boundary that defeats DNS rebinding and redirects, because any
		// connection to a private address must eventually dial it here.
		Control: netguard.Control,
	}

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
		// Never follow redirects server-side. hls.js re-requests the
		// redirected URL through the proxy itself; following here would let
		// an upstream bounce the server at any internal endpoint.
		CheckRedirect: netguard.NoRedirects,
	}
	goTLSClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:         baseDialer.DialContext,
			MaxIdleConnsPerHost: 10,
		},
		CheckRedirect: netguard.NoRedirects,
	}
	h := &Handlers{
		cfg:               cfg,
		log:               log,
		mal:               mal.NewClient(log),
		stream:            streaming.NewManager(log, miruroProxyURL, httpClient),
		h2Client:          &http.Client{Timeout: 30 * time.Second, Transport: h2Transport, CheckRedirect: netguard.NoRedirects},
		h1Client:          &http.Client{Timeout: 30 * time.Second, Transport: h1Transport, CheckRedirect: netguard.NoRedirects},
		httpClient:        httpClient,
		goTLSClient:       goTLSClient,
		miruroProxyURL:    miruroProxyURL,
		miruroProxyClient: &http.Client{Timeout: 60 * time.Second, CheckRedirect: netguard.NoRedirects},
		browseCacheTTL:    5 * time.Minute, // 5 min cache for browse/trending
	}
	h.anilistClient = newAnilistClient(h)
	h.anilistCircuit = newCircuitBreaker()

	// ponytail: global lock, per-account locks if throughput matters
	// Provider-vouched hosts feed the media-proxy CDN allowlist as they
	// surface, so rotated CDN hostnames never 403 at the proxy gate.
	h.stream.SetHostLearner(func(host string) { LearnHostFromPlaylist(host) })

	// The media proxy is the single playback gate: providers only serve
	// sources that pass its exact request path (uTLS transport, no redirects,
	// header filtering), so "URL served" always means "URL plays".
	h.stream.SetMediaProbe(h)

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
			// Clean up browse cache
			h.browseCache.Range(func(k, v any) bool {
				entry, ok := v.(browseCacheEntry)
				if ok && time.Since(entry.fetchedAt) > h.browseCacheTTL {
					h.browseCache.Delete(k)
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

	// Use AniList GraphQL
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { id title { romaji english userPreferred } coverImage { extraLarge large medium } episodes format status nextAiringEpisode { episode airingAt } } }`
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
	fillerFlags, recapFlags := h.stream.GetEpisodeFlags(r.Context(), anilistID)

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
		if fillerFlags[epNum] {
			ep["filler"] = true
		}
		if recapFlags[epNum] {
			ep["recap"] = true
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
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	// AniList-keyed: the frontend sends AniList IDs, and Miruro sources are
	// AniList-keyed too. MAL IDs are normalized to AniList IDs at the search
	// boundary, never here.
	anilistID := req.AnimeID

	// Find best source for the requested provider/lang only
	result, err := h.stream.GetSourcesForProvider(ctx, req.Episode, req.Provider, req.Lang, req.Quality, anilistID)

	if err != nil || result == nil || len(result.Sources) == 0 {
		h.log.Warn().Err(err).Int("animeId", req.AnimeID).Str("lang", req.Lang).Str("provider", req.Provider).Msg("streaming failed")
		if err != nil && (strings.Contains(err.Error(), "no source") || strings.Contains(err.Error(), "not found") ||
			strings.Contains(err.Error(), "not available") || strings.Contains(err.Error(), "blocked") ||
			strings.Contains(err.Error(), "unreachable") || strings.Contains(err.Error(), "filtered")) {
			h.respondError(w, http.StatusNotFound, "no streaming source found")
			return
		}
		h.respondError(w, http.StatusBadGateway, "streaming source failed")
		return
	}

	h.respondJSON(w, http.StatusOK, result)
}

func (h *Handlers) LegacyEpsrc(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	epStr := r.URL.Query().Get("ep")
	lang := r.URL.Query().Get("lang")
	if lang == "" {
		lang = "sub"
	}

	if idStr == "" || epStr == "" {
		h.respondError(w, http.StatusBadRequest, "id and ep are required")
		return
	}

	animeID, err := strconv.Atoi(idStr)
	episode, err2 := strconv.Atoi(epStr)
	if err != nil || err2 != nil || animeID <= 0 || episode <= 0 {
		h.respondError(w, http.StatusBadRequest, "invalid id or ep")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	// Legacy endpoint: delegate to the streaming manager (Miruro).
	result, err := h.stream.GetSourcesForProvider(ctx, episode, "", lang, "auto", animeID)
	if err != nil || result == nil || len(result.Sources) == 0 {
		h.respondError(w, http.StatusNotFound, "no streaming source found")
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

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	if r.URL.Query().Get("refresh") == "1" || r.URL.Query().Get("refresh") == "true" {
		ctx = streaming.WithRefresh(ctx)
	}
	// AniList-keyed; IDs are normalized at the search boundary.
	anilistID := animeID

	servers := h.stream.FindAllServers(ctx, anilistID, episode, lang)
	h.respondJSON(w, http.StatusOK, servers)
}

// blockedProxyPorts are well-known non-HTTP service ports the media proxy
// refuses to dial. Everything else is allowed: CDNs legitimately serve video
// on other ports (observed: a1.mp4upload.com:183). The authoritative SSRF
// boundary stays in the dialer's resolved-IP control hook; the port list
// only stops accidental fetches of SSH/database/mail daemons.
var blockedProxyPorts = map[string]bool{
	"21": true, "22": true, "23": true, "25": true, "53": true,
	"110": true, "143": true, "389": true, "445": true, "465": true, "587": true,
	"993": true, "995": true, "1433": true, "1521": true, "2049": true,
	"2375": true, "3306": true, "3389": true, "5432": true, "5900": true,
	"6379": true, "9200": true, "11211": true, "27017": true,
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

	// SSRF guard: cheap fast-fail on obviously bad hostnames. The dialer
	// Control hook (ssrfGuardControl) remains the authoritative boundary for
	// resolved-IP checks covering DNS rebinding, redirects, and alt encodings.
	parsed, err := url.Parse(decodedURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		h.respondError(w, http.StatusBadRequest, "invalid URL scheme")
		return
	}
	if port := parsed.Port(); port != "" && blockedProxyPorts[port] {
		h.respondError(w, http.StatusForbidden, "proxy target port not allowed")
		return
	}
	if host := strings.ToLower(parsed.Hostname()); validateProxyTarget(host) != nil {
		h.respondError(w, http.StatusForbidden, "proxy target not allowed")
		return
	} else if !isAllowedProxyHost(host) {
		// The dialer SSRF guard keeps private targets out; the CDN suffix
		// allowlist keeps the proxy from being a general public relay.
		h.log.Warn().Str("proxy_host", host).Msg("proxy host not on CDN allowlist")
		h.respondError(w, http.StatusForbidden, "proxy target not allowed")
		return
	}

	// Create request with proper headers
	req, err := http.NewRequestWithContext(r.Context(), "GET", decodedURL, nil)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid URL")
		return
	}

	// Set headers from query param
	headersJSON := r.URL.Query().Get("headers")
	applyProxyQueryHeaders(req, headersJSON)

	// Forward the client's Range header for media/segment requests so the
	// video element can seek through the proxy. Without it every seek turns
	// into a fresh full-body download and playback restarts from the start.
	// Playlists and keys are excluded — their paths need full bodies (HLS
	// rewrite / key cache), and neither hls.js nor native HLS ranges them.
	if rng := r.Header.Get("Range"); rng != "" {
		p := strings.ToLower(parsed.Path)
		if !strings.HasSuffix(p, ".m3u8") && !strings.HasSuffix(p, ".m3u") && !strings.HasSuffix(p, ".key") {
			req.Header.Set("Range", rng)
		}
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

	// Learning happens where a trusted playlist names its media hosts
	// (rewriteHLSPlaylist), not here. Recording the host at this point could
	// never learn anything: this code is only reachable once the allowlist
	// gate above has already passed.

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
		scheme := "https"
		if r.TLS == nil && !strings.HasPrefix(r.Header.Get("X-Forwarded-Proto"), "https") {
			scheme = "http"
		}
		proxyBase := scheme + "://" + r.Host
		rewritten := h.rewriteHLSPlaylist(string(body), decodedURL, headersJSON, proxyBase)
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

	// Partial-content responses (206) from a forwarded Range: pass through
	// Content-Range and advertise Accept-Ranges so the video element knows it
	// can seek. Content-Length is echoed to keep the response non-chunked.
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		w.Header().Set("Content-Range", cr)
	}
	if ar := resp.Header.Get("Accept-Ranges"); ar != "" {
		w.Header().Set("Accept-Ranges", ar)
	} else if resp.StatusCode == http.StatusPartialContent {
		w.Header().Set("Accept-Ranges", "bytes")
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}

	w.Header().Set("Content-Type", ct)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (h *Handlers) doRequest(req *http.Request, https bool) (*http.Response, error) {
	// The uTLS fingerprinting chain (h2 → h1 → native Go TLS) exists to get
	// past CDN bot detection. Every transport dials through baseDialer, whose
	// ssrfGuardControl re-validates the resolved IP on each connection, so
	// redirects to private targets are rejected at dial time and no client
	// follows redirects at all (noRedirects).
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

// applyProxyQueryHeaders applies the ?headers= JSON to a proxy upstream
// request exactly as the media proxy does: filter dangerous headers, add
// the referer/UA defaults for known CDN classes, and disable compression
// (the uTLS transports don't auto-decompress; gzipped upstream bytes would
// corrupt AES-128 keys and segment data).
func applyProxyQueryHeaders(req *http.Request, headersJSON string) {
	// Disable compression — the uTLS/http2 transport doesn't auto-decompress
	// like a default http.Transport does. Upstream gzipped bytes would arrive
	// garbled, corrupting AES-128 keys and segment data.
	req.Header.Set("Accept-Encoding", "identity")

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
		u := strings.ToLower(req.URL.String())
		if strings.Contains(u, "uwucdn") || strings.Contains(u, "owocdn") || strings.Contains(u, "185.237.106.79") {
			req.Header.Set("Referer", "https://kwik.cx/")
			req.Header.Set("Origin", "https://kwik.cx")
		} else if strings.Contains(u, "senshi") || strings.Contains(u, "ninstream") {
			req.Header.Set("Referer", "https://senshi.live")
		}
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	}
}

// directProbeClient mimics a real browser client (follows redirects, no
// forced Accept-Encoding) for the direct-path verdict.
var directProbeClient = &http.Client{Timeout: 20 * time.Second}

// sourceProbeMemo short-circuits repeated playback probes of the same URL
// within a short window (a /servers call followed by /stream probes the
// same sources). Verdicts are TTL'd because CDN reachability drifts.
type sourceProbeMemoEntry struct {
	verdict streaming.PlaybackVerdict
	at      time.Time
}

// ProbePlayback ranks how a source can reach a player — a soft verdict that
// orders server lists but never filters them (CDNs serve datacenter and
// residential IPs differently, so "dead" here does not mean dead in the
// browser).
//
// Proxy path: the exact same request path the media proxy uses (uTLS
// transport chain, no redirects, filtered headers, Accept-Encoding:
// identity). HLS: the manifest must fetch 200 and parse as a playlist
// (#EXTM3U), the first referenced media playlist must fetch 200, and the
// first media segment must not be a canary (some CDNs serve real playlists
// but 1x1 PNG bytes in place of every segment — observed on vidtub/vivibebe).
// MP4: a ranged GET must return 200/206 with real media bytes, not an HTML
// error page and not image bytes (fast4speed 404s on the full body).
//
// Direct path: a plain browser-like client (redirects followed, standard
// Accept-Encoding). Image-magic rejection is intentionally NOT applied here
// — some CDNs (vidtub/vivibebe) serve image canaries only to datacenter
// fingerprints and real segments to residential ones.
func (h *Handlers) ProbePlayback(ctx context.Context, srcType, rawURL string, headers map[string]string) streaming.PlaybackVerdict {
	headersJSON, _ := json.Marshal(headers)
	probeKey := srcType + "|" + rawURL + "|" + string(headersJSON)
	if v, ok := h.sourceProbeMemo.Load(probeKey); ok {
		e := v.(sourceProbeMemoEntry)
		if time.Since(e.at) < 90*time.Second {
			return e.verdict
		}
	}

	verdict := streaming.VerdictDead
	if h.probeViaProxy(ctx, srcType, rawURL, string(headersJSON)) {
		verdict = streaming.VerdictProxy
	} else if h.probeDirect(ctx, srcType, rawURL, headers) {
		verdict = streaming.VerdictDirect
	}

	h.sourceProbeMemo.Store(probeKey, sourceProbeMemoEntry{verdict: verdict, at: time.Now()})
	return verdict
}

// probeViaProxy is the media-proxy-path playback check (see ProbePlayback).
func (h *Handlers) probeViaProxy(ctx context.Context, srcType, rawURL, headersJSON string) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if srcType == "mp4" {
		req, err := http.NewRequestWithContext(probeCtx, "GET", rawURL, nil)
		if err != nil {
			return false
		}
		applyProxyQueryHeaders(req, headersJSON)
		req.Header.Set("Range", "bytes=0-1023")
		resp, err := h.doRequest(req, strings.HasPrefix(rawURL, "https"))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return false
		}
		ct := strings.ToLower(resp.Header.Get("Content-Type"))
		if strings.Contains(ct, "text/html") {
			return false
		}
		if !strings.Contains(ct, "video") && !strings.Contains(ct, "audio") &&
			!strings.Contains(ct, "octet-stream") && !strings.Contains(ct, "binary") {
			return false
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			return false
		}
		return len(body) >= 100 && !mediaMagicImage(body)
	}

	// HLS: manifest, then the first media playlist it references, then the
	// first segment — fake sources pass the first two but serve image bytes.
	manifest, err := h.probePlaylist(probeCtx, rawURL, headersJSON)
	if err != nil {
		return false
	}
	firstChild := firstPlaylistURI(manifest)
	if firstChild == "" {
		return false
	}
	childURL, err := resolvePlaylistURL(rawURL, firstChild)
	if err != nil {
		return false
	}
	child, err := h.probePlaylist(probeCtx, childURL, headersJSON)
	if err != nil {
		return false
	}
	firstSegment := firstPlaylistURI(child)
	if firstSegment == "" {
		return false
	}
	segURL, err := resolvePlaylistURL(childURL, firstSegment)
	if err != nil {
		return false
	}
	return h.probeSegment(probeCtx, segURL, headersJSON)
}

// probeDirect checks the same source with a plain browser-like client
// (redirects followed, standard headers). Verdict is intentionally laxer
// than the proxy path: any CDN that serves real media to a generic client
// counts as directly playable, and image canaries are ignored because some
// CDNs serve them only to datacenter fingerprints.
func (h *Handlers) probeDirect(ctx context.Context, srcType, rawURL string, headers map[string]string) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, "GET", rawURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	for k, v := range headers {
		if strings.EqualFold(k, "User-Agent") {
			continue
		}
		req.Header.Set(k, v)
	}

	if srcType == "mp4" {
		req.Header.Set("Range", "bytes=0-1023")
		resp, err := directProbeClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return false
		}
		ct := strings.ToLower(resp.Header.Get("Content-Type"))
		if strings.Contains(ct, "text/html") {
			return false
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			return false
		}
		return len(body) >= 100
	}

	// HLS: manifest, first media playlist, first segment — HTML is dead.
	manifest, err := h.probeDirectPlaylist(probeCtx, req, rawURL)
	if err != nil {
		return false
	}
	firstChild := firstPlaylistURI(manifest)
	if firstChild == "" {
		return false
	}
	childURL, err := resolvePlaylistURL(rawURL, firstChild)
	if err != nil {
		return false
	}
	childReq, err := http.NewRequestWithContext(probeCtx, "GET", childURL, nil)
	if err != nil {
		return false
	}
	childReq.Header = req.Header.Clone()
	child, err := h.probeDirectPlaylist(probeCtx, childReq, childURL)
	if err != nil {
		return false
	}
	firstSegment := firstPlaylistURI(child)
	if firstSegment == "" {
		return false
	}
	segURL, err := resolvePlaylistURL(childURL, firstSegment)
	if err != nil {
		return false
	}
	segReq, err := http.NewRequestWithContext(probeCtx, "GET", segURL, nil)
	if err != nil {
		return false
	}
	segReq.Header = req.Header.Clone()
	segResp, err := directProbeClient.Do(segReq)
	if err != nil {
		return false
	}
	defer segResp.Body.Close()
	if segResp.StatusCode != http.StatusOK && segResp.StatusCode != http.StatusPartialContent {
		return false
	}
	ct := strings.ToLower(segResp.Header.Get("Content-Type"))
	if strings.Contains(ct, "text/html") {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(segResp.Body, 512))
	if err != nil {
		return false
	}
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	lower := bytes.ToLower(trimmed)
	if bytes.HasPrefix(lower, []byte("<html")) || bytes.HasPrefix(lower, []byte("<!doctype")) {
		return false
	}
	return len(body) >= 2
}

// probeDirectPlaylist fetches a playlist with the plain client and returns
// its body when it is a genuine, parseable playlist.
func (h *Handlers) probeDirectPlaylist(ctx context.Context, req *http.Request, rawURL string) (string, error) {
	resp, err := directProbeClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("playlist fetch returned %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
	if err != nil {
		return "", err
	}
	text := string(body)
	if !strings.HasPrefix(strings.TrimSpace(text), "#EXTM3U") {
		return "", fmt.Errorf("body is not an HLS playlist")
	}
	return text, nil
}

// probeSegment fetches the first media segment exactly like the proxy would
// and rejects canary sources that serve image bytes or HTML in place of video.
func (h *Handlers) probeSegment(ctx context.Context, rawURL, headersJSON string) bool {
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return false
	}
	applyProxyQueryHeaders(req, headersJSON)
	resp, err := h.doRequest(req, strings.HasPrefix(rawURL, "https"))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return false
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.Contains(ct, "text/html") {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512))
	if err != nil {
		return false
	}
	if len(body) < 2 {
		return false
	}
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	lower := bytes.ToLower(trimmed)
	if bytes.HasPrefix(lower, []byte("<html")) || bytes.HasPrefix(lower, []byte("<!doctype")) {
		return false
	}
	return !mediaMagicImage(body)
}

// mediaMagicImage reports whether b starts with a known image signature.
// Dead CDNs serve 1x1 PNGs (or other image bytes) in place of video segments.
func mediaMagicImage(b []byte) bool {
	if len(b) < 8 {
		return false
	}
	if bytes.HasPrefix(b, []byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}) {
		return true
	}
	if b[0] == 0xFF && b[1] == 0xD8 && b[2] == 0xFF {
		return true // JPEG
	}
	if len(b) >= 4 && bytes.HasPrefix(b, []byte("GIF8")) {
		return true
	}
	if len(b) >= 12 && bytes.HasPrefix(b, []byte("RIFF")) && bytes.HasPrefix(b[8:12], []byte("WEBP")) {
		return true
	}
	return false
}

// probePlaylist fetches a playlist exactly like the proxy would and returns
// its body when the fetch is a genuine, parseable playlist.
func (h *Handlers) probePlaylist(ctx context.Context, rawURL, headersJSON string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return "", err
	}
	applyProxyQueryHeaders(req, headersJSON)
	resp, err := h.doRequest(req, strings.HasPrefix(rawURL, "https"))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("playlist fetch returned %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
	if err != nil {
		return "", err
	}
	text := string(body)
	if !strings.HasPrefix(strings.TrimSpace(text), "#EXTM3U") {
		return "", fmt.Errorf("body is not an HLS playlist")
	}
	return text, nil
}

// firstPlaylistURI returns the first non-comment URI line in a playlist.
func firstPlaylistURI(content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		return line
	}
	return ""
}

// resolvePlaylistURL resolves a URI line from a playlist against the
// playlist's own URL (relative or absolute).
func resolvePlaylistURL(baseURL, ref string) (string, error) {
	if u, err := url.Parse(ref); err == nil && u.IsAbs() {
		return ref, nil
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	rel, err := url.Parse(ref)
	if err != nil {
		return "", err
	}
	return base.ResolveReference(rel).String(), nil
}

func (h *Handlers) rewriteHLSPlaylist(content, baseURL, headersJSON, proxyBase string) string {
	lines := strings.Split(content, "\n")
	baseParts := strings.Split(baseURL, "/")
	var basePrefix string
	if len(baseParts) < 4 {
		basePrefix = baseURL
	} else {
		basePrefix = strings.Join(baseParts[:len(baseParts)-1], "/")
	}

	// Only a playlist we fetched from an allowed host may vouch for the hosts
	// it references. Proxy() gates on isAllowedProxyHost before fetching, so
	// in production this holds by construction; re-checking keeps the trust
	// chain explicit and correct if this is ever called from elsewhere.
	vouching := false
	if pb, err := url.Parse(baseURL); err == nil {
		vouching = isAllowedProxyHost(strings.ToLower(pb.Hostname()))
	}
	learnPlaylistTarget := func(rawURL string) {
		if !vouching {
			return
		}
		u, err := url.Parse(rawURL)
		if err != nil {
			return
		}
		// A trusted playlist may name any media host — including extension-less
		// segment URLs (observed: p1.ipstatp.com/obj/ad-site-i18n/...). The
		// dialer SSRF guard plus static allowlist gate remain the boundary;
		// the media-path filter was dropping real segment hosts.
		LearnHostFromPlaylist(u.Hostname())
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
				learnPlaylistTarget(absoluteURL)
				if needsProxyRewrite(absoluteURL) || headersJSON != "" {
					proxied := fmt.Sprintf("%s/api/v1/proxy?url=%s%s", proxyBase, url.QueryEscape(absoluteURL), headersParam)
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
		learnPlaylistTarget(absoluteURL)

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
					lines[i] = keyTag + "\n" + fmt.Sprintf("%s/api/v1/proxy?url=%s%s", proxyBase, url.QueryEscape(absoluteURL), headersParam)
				} else {
					lines[i] = keyTag + "\n" + absoluteURL
				}
				continue
			}
		}

		if headersJSON != "" || needsProxyRewrite(absoluteURL) {
			lines[i] = fmt.Sprintf("%s/api/v1/proxy?url=%s%s", proxyBase, url.QueryEscape(absoluteURL), headersParam)
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
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return !isAllowedProxyHost(parsed.Hostname()) || hasKnownRewriteKeys(rawURL)
}

// hasKnownRewriteKeys returns true for CDN hostnames that must be proxied even
// if they're on the allowlist — these are hosts where the player's direct
// request would be rejected at the CDN without custom headers or transport
// (e.g. Senshi's TLS fingerprint filtering, uTLS-required hosts).
func hasKnownRewriteKeys(rawURL string) bool {
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
		strings.Contains(lower, "ans-bio-video") ||
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
	animeID, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime id")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	defer r.Body.Close()
	var input struct {
		Episode   int  `json:"episode"`
		Position  int  `json:"position_sec"`
		Completed bool `json:"completed"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			h.respondError(w, http.StatusBadRequest, "invalid progress payload")
			return
		}
	}

	payload := []map[string]any{{
		"user_id":      userID,
		"anime_id":     animeID,
		"episode":      input.Episode,
		"position_sec": input.Position,
		"completed":    input.Completed,
		"updated_at":   "now()",
	}}
	raw, _ := json.Marshal(payload)

	resp, err := h.supabaseRequest(r.Context(), "POST",
		"/rest/v1/anime_progress?on_conflict=user_id,anime_id",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("anime progress upsert failed")
		h.respondError(w, http.StatusBadGateway, "failed to save progress")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		h.log.Warn().Str("detail", supabaseErrorBody(resp)).Msg("anime progress upsert rejected")
		h.respondError(w, http.StatusBadGateway, "failed to save progress")
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
	mangaID, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid manga id")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	r.Body.Close()
	var input struct {
		Chapter   int  `json:"chapter"`
		Page      int  `json:"page"`
		Completed bool `json:"completed"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			h.respondError(w, http.StatusBadRequest, "invalid progress payload")
			return
		}
	}

	raw, _ := json.Marshal([]map[string]any{{
		"user_id":    userID,
		"manga_id":   mangaID,
		"chapter":    input.Chapter,
		"page":       input.Page,
		"completed":  input.Completed,
		"updated_at": "now()",
	}})

	resp, err := h.supabaseRequest(r.Context(), "POST",
		"/rest/v1/manga_progress?on_conflict=user_id,manga_id",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("manga progress upsert failed")
		h.respondError(w, http.StatusBadGateway, "failed to save progress")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		h.log.Warn().Str("detail", supabaseErrorBody(resp)).Msg("manga progress upsert rejected")
		h.respondError(w, http.StatusBadGateway, "failed to save progress")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]string{"status": "saved"})
}

type watchHistoryEntry struct {
	AnimeID    int     `json:"anime_id"`
	AnimeTitle string  `json:"anime_title"`
	AnimeImage string  `json:"anime_image"`
	Episode    int     `json:"episode_number"`
	Progress   float64 `json:"progress"`
	Duration   float64 `json:"duration"`
	Timestamp  int64   `json:"timestamp"`
}

type continueWatchingItem struct {
	AnimeID   int     `json:"animeId"`
	Title     string  `json:"title"`
	Image     string  `json:"image"`
	Episode   int     `json:"episode"`
	Time      float64 `json:"time"`
	Duration  float64 `json:"duration"`
	Timestamp int64   `json:"timestamp"`
}

func (h *Handlers) GetContinueWatching(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/watch_history?select=anime_id,anime_title,anime_image,episode_number,progress,duration,timestamp&user_id=eq."+encodePath(userID)+"&order=timestamp.desc&limit=30",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("continue-watching fetch failed")
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}

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

// AdminStats relays the server-gated admin dashboard stats. The route is
// protected by auth.RequireAdmin (server-side is_admin() check against
// Supabase); this handler then calls Supabase's admin_stats() RPC with the
// caller's JWT, which raises unless the caller's role is admin. Belt and
// suspenders: even if middleware regresses, the RPC still enforces it.
func (h *Handlers) AdminStats(w http.ResponseWriter, r *http.Request) {
	resp, err := h.supabaseRequest(r.Context(), "POST", "/rest/v1/rpc/admin_stats", nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("admin_stats rpc failed")
		h.respondError(w, http.StatusInternalServerError, "failed to load admin stats")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusForbidden, "insufficient privileges")
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		h.log.Warn().Err(err).Msg("admin_stats read failed")
		h.respondError(w, http.StatusInternalServerError, "failed to load admin stats")
		return
	}
	h.respondJSON(w, http.StatusOK, json.RawMessage(body))
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
			if err == nil {
				// Jikan results are MAL-keyed; rekey onto AniList IDs so the
				// watch flow (stream/episodes) works unchanged.
				if nerr := h.normalizeSearchResults(r.Context(), results.Data.Page.Media); nerr != nil {
					h.log.Warn().Err(nerr).Msg("failed to normalize jikan browse results")
				}
			}
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
	} else {
		// Jikan results are MAL-keyed; rekey onto AniList IDs so the watch
		// flow (stream/episodes) works unchanged.
		if nerr := h.normalizeSearchResults(r.Context(), results.Data.Page.Media); nerr != nil {
			h.log.Warn().Err(nerr).Msg("failed to normalize jikan search results")
		}
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

// ImportMAL and ImportAniList are implemented in importexport.go.
// ImportStatus reports on an import/export job; jobs run synchronously
// today, so this is a compatibility stub.
func (h *Handlers) ImportStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	h.respondJSON(w, http.StatusOK, map[string]string{"jobId": jobID, "status": "pending"})
}

func (h *Handlers) GetProfile(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	if username == "" {
		h.respondError(w, http.StatusBadRequest, "username required")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/profiles?select=username,display_name,avatar_url,bio,location,socials,created_at&username=eq."+url.QueryEscape(username)+"&limit=1",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("profile fetch failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch profile")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.respondError(w, http.StatusNotFound, "profile not found")
		return
	}

	var profiles []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&profiles); err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to decode profile")
		return
	}
	if len(profiles) == 0 {
		h.respondError(w, http.StatusNotFound, "profile not found")
		return
	}
	h.respondJSON(w, http.StatusOK, profiles[0])
}

func (h *Handlers) UpdateProfile(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	r.Body.Close()
	var input map[string]any
	if err := json.Unmarshal(body, &input); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid profile payload")
		return
	}
	// Never allow a caller to change their own role.
	delete(input, "role")
	if len(input) == 0 {
		h.respondError(w, http.StatusBadRequest, "no fields to update")
		return
	}
	input["updated_at"] = "now()"
	raw, _ := json.Marshal(input)

	resp, err := h.supabaseRequest(r.Context(), "PATCH",
		"/rest/v1/profiles?id=eq."+encodePath(userID),
		bytes.NewReader(raw), nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("profile update failed")
		h.respondError(w, http.StatusBadGateway, "failed to update profile")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to update profile")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *Handlers) AddFavorite(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	r.Body.Close()
	var input struct {
		MediaID   int    `json:"mediaId"`
		MediaType string `json:"mediaType"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.MediaID <= 0 {
		h.respondError(w, http.StatusBadRequest, "valid mediaId required")
		return
	}
	if input.MediaType == "" {
		input.MediaType = "anime"
	}
	if input.MediaType != "anime" && input.MediaType != "manga" {
		h.respondError(w, http.StatusBadRequest, "mediaType must be anime or manga")
		return
	}

	raw, _ := json.Marshal([]map[string]any{{
		"user_id":    userID,
		"media_id":   input.MediaID,
		"media_type": input.MediaType,
	}})

	resp, err := h.supabaseRequest(r.Context(), "POST",
		"/rest/v1/favorites?on_conflict=user_id,media_id,media_type",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("favorite add failed")
		h.respondError(w, http.StatusBadGateway, "failed to add favorite")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to add favorite")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "added"})
}

func (h *Handlers) RemoveFavorite(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	mediaID, err := strconv.Atoi(chi.URLParam(r, "mediaId"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid media id")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "DELETE",
		"/rest/v1/favorites?user_id=eq."+encodePath(userID)+"&media_id=eq."+strconv.Itoa(mediaID),
		nil, map[string]string{"Prefer": "return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("favorite remove failed")
		h.respondError(w, http.StatusBadGateway, "failed to remove favorite")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to remove favorite")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func (h *Handlers) ListFavorites(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/favorites?select=media_id,media_type,added_at&user_id=eq."+encodePath(userID)+"&order=added_at.desc&limit=200",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("favorites fetch failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch favorites")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to fetch favorites")
		return
	}

	var favorites []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&favorites); err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to decode favorites")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{"favorites": favorites})
}

func (h *Handlers) ClientLog(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	r.Body.Close()
	var entry struct {
		Level   string `json:"level"`
		Message string `json:"message"`
		Data    any    `json:"data"`
	}
	_ = json.Unmarshal(body, &entry)
	msg := entry.Message
	if msg == "" {
		msg = "(empty client log)"
	}
	h.log.Info().Str("user", userID).Str("level", entry.Level).Any("data", entry.Data).Msg("client log: " + msg)
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "received"})
}

func (h *Handlers) GetSetting(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	key := chi.URLParam(r, "key")
	if key == "" {
		h.respondError(w, http.StatusBadRequest, "key required")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/user_settings?select=value&user_id=eq."+encodePath(userID)+"&key=eq."+url.QueryEscape(key)+"&limit=1",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("setting fetch failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch setting")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.respondJSON(w, http.StatusOK, map[string]string{"key": key, "value": ""})
		return
	}

	var rows []struct {
		Value any `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		h.respondJSON(w, http.StatusOK, map[string]string{"key": key, "value": ""})
		return
	}
	if len(rows) == 0 {
		h.respondJSON(w, http.StatusOK, map[string]string{"key": key, "value": ""})
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{"key": key, "value": rows[0].Value})
}

func (h *Handlers) UpdateSetting(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	key := chi.URLParam(r, "key")
	if key == "" {
		h.respondError(w, http.StatusBadRequest, "key required")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	r.Body.Close()
	var input struct {
		Value any `json:"value"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid setting payload")
		return
	}

	raw, _ := json.Marshal([]map[string]any{{
		"user_id":    userID,
		"key":        key,
		"value":      input.Value,
		"updated_at": "now()",
	}})

	resp, err := h.supabaseRequest(r.Context(), "POST",
		"/rest/v1/user_settings?on_conflict=user_id,key",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("setting upsert failed")
		h.respondError(w, http.StatusBadGateway, "failed to save setting")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to save setting")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *Handlers) GetNotifications(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/notifications?select=id,type,message,anime_id,read,created_at&user_id=eq."+encodePath(userID)+"&order=created_at.desc&limit=50",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("notifications fetch failed")
		h.respondJSON(w, http.StatusOK, []any{})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondJSON(w, http.StatusOK, []any{})
		return
	}

	var notifications []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&notifications); err != nil {
		h.respondJSON(w, http.StatusOK, []any{})
		return
	}
	h.respondJSON(w, http.StatusOK, notifications)
}

func (h *Handlers) MarkNotificationRead(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	notifID := chi.URLParam(r, "id")
	if notifID == "" {
		h.respondError(w, http.StatusBadRequest, "notification id required")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "PATCH",
		"/rest/v1/notifications?id=eq."+encodePath(notifID)+"&user_id=eq."+encodePath(userID),
		bytes.NewReader([]byte(`{"read":true}`)), nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("notification read failed")
		h.respondError(w, http.StatusBadGateway, "failed to update notification")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to update notification")
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

	resp, err := h.supabaseRequest(r.Context(), "PATCH",
		"/rest/v1/notifications?user_id=eq."+encodePath(userID)+"&read=eq.false",
		bytes.NewReader([]byte(`{"read":true}`)), nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("notifications read-all failed")
		h.respondError(w, http.StatusBadGateway, "failed to update notifications")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to update notifications")
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

	// The frontend is AniList-keyed; Miruro is also AniList-keyed, so the ID
	// passes through unchanged. MAL IDs are normalized to AniList IDs at the
	// search boundary, never here.
	anilistID, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
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

// GetMiruroProbe reports whether an anime has actually playable Miruro
// streams. Unlike the raw episodes endpoint (which only lists provider
// metadata), this resolves and reachability-verifies a real source, so the
// frontend's hentai filter never surfaces titles whose first episode cannot
// play. Results are cached for 6 hours in-process.
func (h *Handlers) GetMiruroProbe(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	if idStr == "" {
		h.respondError(w, http.StatusBadRequest, "anime ID is required")
		return
	}
	anilistID, err := strconv.Atoi(idStr)
	if err != nil || anilistID <= 0 {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	// Positive results are stable — 6h. Negative results are cached much
	// shorter (15m) because they usually mean Miruro was momentarily down
	// (Cloudflare challenge, 502) rather than that the anime truly has no
	// stream. Short TTLs let hentai listings recover quickly once upstream
	// comes back, instead of hiding titles for 6 hours off one bad probe.
	if v, ok := h.probeCache.Load(anilistID); ok {
		e := v.(probeCacheEntry)
		ttl := 6 * time.Hour
		if !e.playable {
			ttl = 15 * time.Minute
		}
		if time.Since(e.fetchedAt) < ttl {
			h.writeProbeResponse(w, anilistID, e)
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	sub, dub, playable := h.stream.ProbePlayable(ctx, anilistID)
	e := probeCacheEntry{subCount: sub, dubCount: dub, playable: playable, fetchedAt: time.Now()}
	h.probeCacheMu.Lock()
	if h.probeCacheCount >= maxProbeCacheSize {
		h.probeCache.Range(func(k, _ any) bool {
			h.probeCache.Delete(k)
			return false
		})
		h.probeCacheCount--
	}
	h.probeCacheCount++
	h.probeCacheMu.Unlock()
	h.probeCache.Store(anilistID, e)
	h.writeProbeResponse(w, anilistID, e)
}

func (h *Handlers) writeProbeResponse(w http.ResponseWriter, id int, e probeCacheEntry) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=21600")
	json.NewEncoder(w).Encode(map[string]any{
		"id":       id,
		"playable": e.playable,
		"subCount": e.subCount,
		"dubCount": e.dubCount,
	})
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

func (h *Handlers) HasDub(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	// AniList-keyed, like every other route; IDs are normalized at the search
	// boundary.
	anilistID, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	hasDub := h.stream.HasAnimeDub(r.Context(), anilistID)
	h.respondJSON(w, http.StatusOK, map[string]bool{"hasDub": hasDub})
}

func (h *Handlers) trendingAniList(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	cacheKey := fmt.Sprintf("trending:%d:%d", page, perPage)
	if cached, ok := h.browseCache.Load(cacheKey); ok {
		if entry, ok := cached.(browseCacheEntry); ok && time.Since(entry.fetchedAt) < h.browseCacheTTL {
			h.log.Debug().Str("cache_key", cacheKey).Msg("trending cache hit")
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
			return entry.data, nil
		}
		h.browseCache.Delete(cacheKey)
	}

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

// ponytail: simple AniList GraphQL proxy with 429 retry + backoff
func (h *Handlers) AniListProxy(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "failed to read body")
		return
	}
	defer r.Body.Close()

	// Parse the incoming request to extract query/variables for caching
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	query, _ := payload["query"].(string)
	variables, _ := payload["variables"].(map[string]any)

	// Use anilistClient with retry logic and caching
	respBody, err := h.anilistClient.do(r.Context(), query, variables)
	if err != nil {
		if strings.Contains(err.Error(), "rate limited") {
			h.respondError(w, http.StatusTooManyRequests, err.Error())
			return
		}
		h.respondError(w, http.StatusBadGateway, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBody)
}
