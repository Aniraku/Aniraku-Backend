package streaming

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestTenshoUpstreamTimeoutCapsHang pins the per-hop ceiling for tensho's
// sequential prefix: a blackholed upstream (accepts the connection, never
// answers — the classic egress-blocked edge) must be cut at the short
// per-call budget, not the shared client's 45s. One dead hop used to pin
// the /servers fan-out for tens of seconds (see tenshoUpstreamTimeout).
func TestTenshoUpstreamTimeoutCapsHang(t *testing.T) {
	old := tenshoUpstreamTimeout
	tenshoUpstreamTimeout = 300 * time.Millisecond
	t.Cleanup(func() { tenshoUpstreamTimeout = old })

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(60 * time.Second):
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() {
		// Release the blackhole handler first so Close() never waits on a
		// lingering connection after the (already-aborted, deadline-cut)
		// request.
		close(release)
		server.Close()
	}()

	p := NewTenshoProvider(zerolog.Nop(), server.URL, server.URL+"/anilist", server.URL, server.URL)
	p.client = server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	_, err := p.FindEpisodeSource(ctx, "185874", 9, "sub")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("hung upstream must error")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("hung upstream took %v, want the ~300ms test cap", elapsed.Round(100*time.Millisecond))
	}
	t.Logf("hung upstream cut after %v", elapsed.Round(100*time.Millisecond))
}
