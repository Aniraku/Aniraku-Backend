package middleware

import (
	"net/http"
	"os"
	"strings"
)

func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		allowed := map[string]bool{
			"http://127.0.0.1:3000":       true,
			"http://127.0.0.1:43211":      true,
			"http://localhost:3000":       true,
			"http://localhost:3001":       true,
			"http://localhost:43211":      true,
			"http://localhost:5173":       true,
			"https://aniraku.vercel.app":  true,
			"https://www.aniraku.tech":    true,
			"https://test.aniraku.tech":    true,
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

		// Security headers
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
