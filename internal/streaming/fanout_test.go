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
// never touch the network outside its fixture-pointed providers (download
// fetches, hentai lookups).
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

func TestFindAllServersHonorsHentaiCollectorGate(t *testing.T) {
	f := newAnimeGGFixture(t)
	ag := NewAnimeGGProvider(zerolog.Nop(), f.base, f.base+"/anilist")
	ag.client = f.server.Client()
	m := &Manager{
		log:         zerolog.Nop(),
		providers:   []Provider{ag},
		httpClient:  &http.Client{Transport: errTransport{}},
		hentaiCache: map[int]hentaiEntry{},
	}

	servers := m.FindAllServers(context.Background(), 21, 1, "sub", []string{"Hentai"})
	if len(servers) != 0 {
		t.Fatalf("hentai-gated AnimeGG collector ran anyway: %+v", servers)
	}
}

func TestFindAllServersRetainsProviderAfterTransientCollectorFailure(t *testing.T) {
	f := newAnimeGGFixture(t)
	ag := NewAnimeGGProvider(zerolog.Nop(), f.base, f.base+"/anilist")
	ag.client = f.server.Client()
	m := &Manager{
		log:         zerolog.Nop(),
		providers:   []Provider{ag},
		httpClient:  &http.Client{Transport: errTransport{}},
		hentaiCache: map[int]hentaiEntry{},
	}

	first := m.FindAllServers(context.Background(), 21, 1, "sub", []string{"Action"})
	if !hasProvider(first, "animegg") {
		t.Fatalf("fixture did not produce AnimeGG on the first round: %+v", first)
	}

	// Expire only the short full-list snapshot, then make the real collector
	// fail. The per-provider last-good entry must carry Sunny into round two.
	key := serverRequestKey{animeID: 21, episode: 1, lang: "sub"}
	cache := m.stability()
	cache.mu.Lock()
	entry := cache.lists[key]
	entry.expires = time.Now().Add(-time.Second)
	cache.lists[key] = entry
	cache.mu.Unlock()
	ag.mu.Lock()
	ag.resolved = make(map[animeggResolveKey]*animeggResolvedEntry)
	ag.mu.Unlock()
	ag.client = &http.Client{Transport: errTransport{}}

	second := m.FindAllServers(context.Background(), 21, 1, "sub", []string{"Action"})
	if !hasProvider(second, "animegg") {
		t.Fatalf("transient collector failure removed AnimeGG: first=%+v second=%+v", first, second)
	}
}
