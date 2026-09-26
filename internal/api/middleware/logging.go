package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// Unwrap exposes the wrapped writer to http.ResponseController. Without it,
// handlers calling http.NewResponseController(w).SetWriteDeadline(...) fail
// with ErrNotSupported at this wrapper: the response-timeout lifts used by
// the media proxy and the long export routes would silently do nothing, and
// any response slower than the server-wide 60s WriteTimeout would be killed
// mid-flight (nginx then answers 502, which browsers report as a CORS
// error). Every other middleware in the chain passes the writer through
// unwrapped, and chi's Compress provides its own Unwrap.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

func Logging(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(wrapped, r)

			RecordRequestStatus(wrapped.status)

			// The media proxy answers every HLS segment fetch — during
			// playback it is the overwhelming majority of requests, and
			// every line carries the same path ("/api/v1/proxy"), so
			// production logged ~65MB/h of near-identical entries in the
			// container log on the 15G VM. Successful proxy responses drop
			// to Debug (disabled in production); failures stay at Info so
			// a broken source is still visible, and the proxy handler's
			// own Warn lines (429 / upstream rejected / stream aborted)
			// are untouched.
			level := log.Info()
			if wrapped.status < 400 && strings.HasPrefix(r.URL.Path, "/api/v1/proxy") {
				level = log.Debug()
			}
			level.
				Str("request_id", GetRequestID(r.Context())).
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Int("status", wrapped.status).
				Dur("duration", time.Since(start)).
				Str("remote", r.RemoteAddr).
				Msg("request")
		})
	}
}
