package v1

import (
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/api/middleware"
)

// AniList-path counters for the /api/v1/metrics snapshot (admin only).
// Request-status counters live in internal/api/middleware/metrics.go and are
// instrumented from the Logging middleware.
var (
	metricAnilistCacheHits   atomic.Uint64
	metricAnilistCacheStale  atomic.Uint64
	metricAnilistUpstreamOK  atomic.Uint64
	metricAnilistUpstreamErr atomic.Uint64
	metricBrowseCacheHits    atomic.Uint64
)

var metricsStartedAt = time.Now()

// RecordAnilistCacheHit / RecordAnilistCacheStale / RecordAnilistUpstream are
// called from the anilistClient fast paths.
func RecordAnilistCacheHit()   { metricAnilistCacheHits.Add(1) }
func RecordAnilistCacheStale() { metricAnilistCacheStale.Add(1) }
func RecordAnilistUpstream(ok bool) {
	if ok {
		metricAnilistUpstreamOK.Add(1)
		return
	}
	metricAnilistUpstreamErr.Add(1)
}

// RecordBrowseCacheHit counts browse-cache fast returns.
func RecordBrowseCacheHit() { metricBrowseCacheHits.Add(1) }

// Metrics serves the JSON snapshot on GET /api/v1/metrics (admin only).
func (h *Handlers) Metrics(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	cbState := "closed"
	if h.anilistCircuit != nil {
		switch h.anilistCircuit.getState() {
		case 1:
			cbState = "open"
		case 2:
			cbState = "half-open"
		}
	}

	ok2xx, other1xx3xx, clientErr4xx, serverErr5xx := middleware.SnapshotRequests()

	h.respondJSON(w, http.StatusOK, map[string]any{
		"uptime_seconds": int64(time.Since(metricsStartedAt).Seconds()),
		"version":        Version,
		"commit":         Commit,

		"requests": map[string]uint64{
			"ok_2xx":           ok2xx,
			"redirect_1xx_3xx": other1xx3xx,
			"client_err_4xx":   clientErr4xx,
			"server_err_5xx":   serverErr5xx,
		},

		"anilist": map[string]any{
			"cache_hits_fresh": metricAnilistCacheHits.Load(),
			"cache_hits_stale": metricAnilistCacheStale.Load(),
			"upstream_ok":      metricAnilistUpstreamOK.Load(),
			"upstream_errors":  metricAnilistUpstreamErr.Load(),
			"circuit_state":    cbState,
		},

		"browse_cache_hits": metricBrowseCacheHits.Load(),
		"dynamic_cdn_hosts": GetDynamicCDNCount(),

		"runtime": map[string]any{
			"goroutines":       runtime.NumGoroutine(),
			"heap_alloc_bytes": ms.HeapAlloc,
			"heap_objects":     ms.HeapObjects,
			"sys_bytes":        ms.Sys,
			"num_gc":           ms.NumGC,
		},
	})
}
