package streaming

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// errTransport fails every request instantly: the manager under test must
// never touch the network outside its fixture-pointed providers (kiwi
// downloads, hentai lookups).
type errTransport struct{}

func (errTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("no network in fan-out test")
}

// TestFindAllServersCompletes is the deadlock regression test for the
// fan-out WaitGroup: after the tryembed removal a stale wg.Add(11) covered
// 10 goroutines and every /servers call wedged in wg.Wait() until the
// gateway 504d (no "fan-out complete" in prod logs for 19 minutes). The
// fan-out is slice-driven now, and this test fails if it ever wedges
// again: with a single fixture provider and dead network elsewhere, the
// listing must return well inside the tripwire carrying Sunny.
func TestFindAllServersCompletes(t *testing.T) {
	f := newAnimeGGFixture(t)
	ag := NewAnimeGGProvider(zerolog.Nop(), f.base, f.base+"/anilist")
	ag.client = f.server.Client()
	m := &Manager{
		log:         zerolog.Nop(),
		providers:   []Provider{ag},
		httpClient:  &http.Client{Transport: errTransport{}},
		hentaiCache: map[int]hentaiEntry{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Run async with our own tripwire: on a fan-out wedge nothing ever
	// returns (WaitGroup ignores context), so bound the test itself.
	done := make(chan []core.Server, 1)
	go func() {
		// Genres provided so the hentai gate never dials AniList.
		done <- m.FindAllServers(ctx, 21, 1, "sub", []string{"Action"})
	}()
	var servers []core.Server
	select {
	case servers = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("FindAllServers did not return in 20s (fan-out wedged)")
	}
	if ctx.Err() != nil {
		t.Fatalf("FindAllServers hit the context deadline: %v", ctx.Err())
	}
	found := false
	for _, s := range servers {
		if s.Name == "Sunny" && s.Provider == "animegg" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Sunny missing from servers: %+v", servers)
	}
}
