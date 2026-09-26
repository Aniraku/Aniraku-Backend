package middleware

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestWriteDeadlineLiftSurvivesLoggingWrapper proves the exact path the long
// handlers (media proxy, /servers fan-out, exports) rely on:
// http.ResponseController must unwrap through this package's responseWriter
// to reach the real connection. The handler outlives the server's
// WriteTimeout and only delivers its response because the deadline is
// lifted. Before Unwrap existed, SetWriteDeadline failed with
// ErrNotSupported, the write after the deadline killed the connection
// header-less, and nginx turned that into a 502 (which browsers report as a
// CORS error).
func TestWriteDeadlineLiftSurvivesLoggingWrapper(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	h := Logging(zerolog.Nop())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		if err := rc.SetWriteDeadline(time.Time{}); err != nil {
			t.Errorf("SetWriteDeadline through Logging wrapper: %v", err)
		}
		time.Sleep(300 * time.Millisecond) // outlive the 100ms WriteTimeout
		fmt.Fprint(w, "late-but-alive")
	}))

	srv := &http.Server{Handler: h, WriteTimeout: 100 * time.Millisecond}
	go srv.Serve(ln) //nolint:errcheck // test server; Close below
	t.Cleanup(func() { srv.Close() })

	resp, err := http.Get("http://" + ln.Addr().String() + "/") //nolint:noctx // test server
	if err != nil {
		t.Fatalf("connection killed before any response (Unwrap broken?): %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "late-but-alive" {
		t.Fatalf("body = %q, want %q", body, "late-but-alive")
	}
}

// TestResponseWriterUnwrap pins the contract itself: the wrapper exposes
// exactly its underlying writer, so http.ResponseController can skip it.
func TestResponseWriterUnwrap(t *testing.T) {
	orig := &captureWriter{}
	rw := &responseWriter{ResponseWriter: orig, status: http.StatusOK}
	if got := rw.Unwrap(); got != http.ResponseWriter(orig) {
		t.Fatalf("Unwrap() = %v, want the original writer", got)
	}
}

type captureWriter struct{ header http.Header }

func (c *captureWriter) Header() http.Header {
	if c.header == nil {
		c.header = http.Header{}
	}
	return c.header
}

func (c *captureWriter) Write(b []byte) (int, error) { return len(b), nil }

func (c *captureWriter) WriteHeader(int) {}
