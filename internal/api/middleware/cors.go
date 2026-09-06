package middleware

import (
	"net/http"
	"os"
	"strings"
)

func allowedOrigins() map[string]bool {
	allowed := map[string]bool{
		"http://127.0.0.1:3000":      true,
		"http://127.0.0.1:43211":     true,
		"http://localhost:3000":      true,
		"http://localhost:3001":      true,
		"http://localhost:43211":     true,
		"http://localhost:5173":      true,
		"https://aniraku.vercel.app": true,
		"https://www.aniraku.tech":   true,
		"https://test.aniraku.tech":  true,
	}

	// Comma-separated extra origins for production
	if extra := os.Getenv("ANIRAKU_CORS_ORIGINS"); extra != "" {
		for _, o := range strings.Split(extra, ",") {
			o = strings.TrimSpace(o)
			if o != "" {
				allowed[o] = true
			}
		}
	}
	return allowed
}

// IsAllowedOrigin reports whether an Origin header value is on the CORS
// allowlist (including ANIRAKU_CORS_ORIGINS extensions). Handlers that must
// answer CORS outside the middleware chain use this so they cannot drift
// from the site-wide policy.
func IsAllowedOrigin(origin string) bool {
	return allowedOrigins()[origin]
}

func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		allowed := allowedOrigins()

		if origin != "" && allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		// ponytail: removed wildcard *.vercel.app and arbitrary origin reflection.
		// Use ANIRAKU_CORS_ORIGINS env var for additional allowed origins.

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Range")
		w.Header().Set("Access-Control-Expose-Headers", "Content-Type, Content-Length, Accept-Ranges, Content-Range, Cache-Control")
		w.Header().Set("Access-Control-Max-Age", "86400")
		w.Header().Set("Vary", "Origin")

		// Security headers live in middleware.SecurityHeaders; do not
		// duplicate them here.

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
