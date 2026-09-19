package middleware

import (
	"net/http"
)

// CacheShort marks a response as publicly cacheable for five minutes: the
// catalog data underneath is itself cached server-side for the same TTL, so
// a shared/browser cache can absorb repeat reads without re-fetching.
// Applied per-route to catalog endpoints only — account, sync, streaming,
// and proxy responses must never be cached by intermediaries.
func CacheShort(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=300")
		next.ServeHTTP(w, r)
	})
}
