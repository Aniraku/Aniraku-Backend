package middleware

import (
	"net/http"
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

			log.Info().
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
