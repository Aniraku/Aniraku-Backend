package streaming

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// TestLiveProviderProbe is an env-guarded integration probe: it exercises
// every provider against the real upstreams and prints per-provider status.
// It is skipped unless explicitly requested, so CI never depends on
// third-party site availability:
//
//	ANIRAKU_LIVE_PROBE=1 go test ./internal/streaming/ -run TestLiveProviderProbe -v
//
// Optional env overrides:
//
//	ANIRAKU_LIVE_PROBE_ID      AniList ID to probe (default 1, Cowboy Bebop)
//	ANIRAKU_LIVE_PROBE_EP      episode number (default 1)
//	ANIRAKU_LIVE_PROBE_LANG    sub or dub (default sub)
func TestLiveProviderProbe(t *testing.T) {
	if os.Getenv("ANIRAKU_LIVE_PROBE") != "1" {
		t.Skip("live probe disabled (set ANIRAKU_LIVE_PROBE=1)")
	}

	animeID := 1
	if v := os.Getenv("ANIRAKU_LIVE_PROBE_ID"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			animeID = n
		}
	}
	episode := 1
	if v := os.Getenv("ANIRAKU_LIVE_PROBE_EP"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			episode = n
		}
	}

	lang := "sub"
	if v := os.Getenv("ANIRAKU_LIVE_PROBE_LANG"); v != "" {
		lang = v
	}

	log := zerolog.Nop()
	m := NewManager(log)
	anilistID := strconv.Itoa(animeID)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	type outcome struct {
		name    string
		servers int
		sources int
		err     error
	}
	results := make(chan outcome, 5)

	probe := func(name string, fn func() ([]core.Server, error)) {
		servers, err := fn()
		n, total := len(servers), 0
		for _, s := range servers {
			total += len(s.Sources)
		}
		results <- outcome{name: name, servers: n, sources: total, err: err}
	}

	go probe("anikoto", func() ([]core.Server, error) {
		return m.collectAnikotoServers(ctx, anilistID, episode, lang), nil
	})
	go probe("animex", func() ([]core.Server, error) {
		return m.collectAnimeXServers(ctx, anilistID, episode, lang), nil
	})
	go probe("zoko", func() ([]core.Server, error) {
		return m.collectZokoServers(ctx, anilistID, episode, lang), nil
	})
	go probe("flixcloud", func() ([]core.Server, error) {
		return m.collectFlixServers(ctx, anilistID, episode, lang), nil
	})

	okCount := 0
	for i := 0; i < 4; i++ {
		r := <-results
		status := "FAIL"
		if r.err == nil && r.sources > 0 {
			status = "OK"
			okCount++
		}
		t.Logf("provider %-9s %s servers=%d sources=%d err=%v", r.name, status, r.servers, r.sources, r.err)
	}
	t.Logf("probe result: %d/4 providers returned sources for anilist %d ep %d lang %s", okCount, animeID, episode, lang)
}
