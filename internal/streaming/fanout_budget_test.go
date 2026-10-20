package streaming

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestFanOutCollectorsBoundedByBudget pins the cold-/servers contract: the
// response must land within the collector budget even when a collector's
// upstream blackholes, because latency IS the slowest collector
// (2026-10-20: one dead zoko upstream pinned cold fetches at 20-85s).
// The budget must cut the hung collector and still return promptly.
func TestFanOutCollectorsBoundedByBudget(t *testing.T) {
	old := collectorBudget
	t.Cleanup(func() { collectorBudget = old })
	collectorBudget = 300 * time.Millisecond

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(60 * time.Second):
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() {
		close(release)
		server.Close()
	}()

	oldBase := zokoBase
	zokoBase = server.URL
	t.Cleanup(func() { zokoBase = oldBase })

	m := NewManager(zerolog.Nop())
	p := NewZokoProvider(zerolog.Nop())
	p.client = server.Client()
	// Only the hung provider: every other collector must find nothing to
	// do rather than hitting real upstreams from the test box.
	m.providers = []Provider{p}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	out := m.FindAllServers(ctx, 185874, 9, "sub", []string{"Action"})
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Fatalf("cold fetch took %v, want the 300ms test budget respected", elapsed.Round(100*time.Millisecond))
	}
	if len(out) != 0 {
		t.Fatalf("hung provider must yield no servers, got %d", len(out))
	}
	t.Logf("cold fetch with a hung collector returned in %v", elapsed.Round(10*time.Millisecond))
}
