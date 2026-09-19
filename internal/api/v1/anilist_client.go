package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

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
	endpoint   string
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
		endpoint:   "https://graphql.anilist.co",
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
	failures         int
	successes        int
	lastFailure      time.Time
	state            int // 0=closed, 1=open, 2=half-open
	mu               sync.Mutex
	failureThreshold int
	successThreshold int
	timeout          time.Duration
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

// staleCache returns the last successful response even after its normal fresh
// TTL. AniList metadata changes slowly compared with the cost of sending users
// a 502 during an upstream 429; callers use this only after the fresh-cache
// path or an upstream attempt has failed.
func (c *anilistClient) staleCache(cacheKey string) ([]byte, bool) {
	cached, ok := c.cache.Load(cacheKey)
	if !ok {
		return nil, false
	}
	entry, ok := cached.(anilistCacheEntry)
	if !ok || len(entry.data) == 0 {
		return nil, false
	}
	return entry.data, true
}

// The GraphQL-proxy cache (c.cache) previously had no eviction at all: every
// unique query+variables pair lived for the process lifetime and the janitor
// only trimmed keyCache/browseCache. Long-running instances leaked memory
// proportional to the variety of proxied AniList queries.
const (
	// anilistCacheTTL bounds how long an entry may serve as a stale-cache
	// fallback after its fresh 5-minute window. One hour keeps resilience
	// during AniList outages while bounding memory.
	anilistCacheTTL = 1 * time.Hour
	// anilistCacheMaxEntries caps the cache regardless of age, so even a
	// flood of unique queries cannot grow it without bound.
	anilistCacheMaxEntries = 5000
)

// evictStale drops entries older than anilistCacheTTL. Called from the
// NewHandlers janitor.
func (c *anilistClient) evictStale() {
	c.cache.Range(func(k, v any) bool {
		entry, ok := v.(anilistCacheEntry)
		if ok && time.Since(entry.fetchedAt) > anilistCacheTTL {
			c.cache.Delete(k)
		}
		return true
	})
}

// evictOldest trims the cache back to anilistCacheMaxEntries by dropping the
// oldest entries first. Only reached when the cap is exceeded (rare), so the
// O(n log n) snapshot is acceptable.
func (c *anilistClient) evictOldest() {
	type aged struct {
		key       any
		fetchedAt time.Time
	}
	entries := make([]aged, 0, 64)
	c.cache.Range(func(k, v any) bool {
		if entry, ok := v.(anilistCacheEntry); ok {
			entries = append(entries, aged{key: k, fetchedAt: entry.fetchedAt})
		}
		return true
	})
	if len(entries) <= anilistCacheMaxEntries {
		return
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].fetchedAt.Before(entries[j].fetchedAt) })
	for i := 0; i < len(entries)-anilistCacheMaxEntries; i++ {
		c.cache.Delete(entries[i].key)
	}
}

func (c *anilistClient) do(ctx context.Context, query string, variables map[string]any) ([]byte, error) {
	cacheKey := c.getCacheKey(query, variables)
	// Check circuit breaker
	if c.h.anilistCircuit != nil && !c.h.anilistCircuit.allow() {
		// Circuit open - try to serve stale cache
		if stale, ok := c.staleCache(cacheKey); ok {
			c.h.log.Warn().Str("cache_key", cacheKey).Msg("circuit open, serving stale cache")
			RecordAnilistCacheStale()
			return stale, nil
		}
		return nil, fmt.Errorf("anilist circuit open, rate limited")
	}

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
			RecordAnilistCacheHit()
			return entry.data, nil
		}
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
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
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
			RecordAnilistUpstream(false)
			if stale, ok := c.staleCache(cacheKey); ok {
				c.h.log.Warn().Err(lastErr).Str("cache_key", cacheKey).Msg("AniList unreachable, serving stale cache")
				RecordAnilistCacheStale()
				return stale, nil
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
			RecordAnilistUpstream(false)
			if stale, ok := c.staleCache(cacheKey); ok {
				c.h.log.Warn().Str("cache_key", cacheKey).Msg("AniList rate limited, serving stale cache")
				RecordAnilistCacheStale()
				return stale, nil
			}
			return nil, fmt.Errorf("anilist rate limited after retries: %s", string(respBody))
		}

		if resp.StatusCode != http.StatusOK {
			if c.h.anilistCircuit != nil {
				c.h.anilistCircuit.recordFailure()
			}
			RecordAnilistUpstream(false)
			if stale, ok := c.staleCache(cacheKey); ok {
				c.h.log.Warn().Int("status", resp.StatusCode).Str("cache_key", cacheKey).Msg("AniList upstream failed, serving stale cache")
				RecordAnilistCacheStale()
				return stale, nil
			}
			return nil, fmt.Errorf("anilist %d: %s", resp.StatusCode, string(respBody))
		}

		// Success - record circuit breaker success and cache.
		// Waiters are released via the deferred close once we return.
		if c.h.anilistCircuit != nil {
			c.h.anilistCircuit.recordSuccess()
		}
		RecordAnilistUpstream(true)
		c.cache.Store(cacheKey, anilistCacheEntry{data: respBody, fetchedAt: time.Now()})

		return respBody, nil
	}

	if c.h.anilistCircuit != nil {
		c.h.anilistCircuit.recordFailure()
	}
	return nil, lastErr
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

// ────────────────────────────────────────────────────────────────
// Episode ratings (per-episode score, aggregated to the anime score
// when syncing to MAL / AniList)
// ────────────────────────────────────────────────────────────────
