package streaming

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Kaa resolved One Piece from the ANCHORED episodes page, the deep-walk
// fallback must not fire (one listing fetch total), and a completed player
// must survive a collector cut that kills a sibling player.
//

func TestKaaAnchoredPageSkipsDeepWalk(t *testing.T) {
	f := newKaaFixture(t)
	p := newKaaTestProvider(f)

	sr, err := p.FindEpisodeSource(kaaTestCtx(t), "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 || sr.ServerNames[0] != "Nico" {
		t.Fatalf("sources = %+v, want one Nico stream", sr)
	}
	if got := atomic.LoadInt64(f.episodePageHits); got != 1 {
		t.Fatalf("listing fetches = %d, want exactly 1 (anchored page, no deep walk)", got)
	}
}

func TestKaaCutCollectorShipsPartial(t *testing.T) {
	f := newKaaFixture(t)
	f.dualPlayers = true
	p := newKaaTestProvider(f)

	// Same shape the manager's collectorBudget applies: one deadline, from
	// which every inner op derives its cap.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	sr, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("a cut must not fail completed players: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 || sr.ServerNames[0] != "Nico" {
		t.Fatalf("sources = %+v, want the fast player's stream only", sr)
	}
	if time.Since(start) > 40*time.Millisecond+1500*time.Millisecond {
		t.Fatalf("cut collector ran %v", time.Since(start))
	}
}

// Kaa retry design: a deadline-shaped listing failure must NOT
// refetch the listing (a 6s fail followed by a fresh 6s walk would
// guarantee a wall cut). Every miss schedules ONE background resolve that
// fills the 10-min cache the moment kaa.lt/krussdomi cooperate, later
// rounds then serve kaa fully.
func TestKaaDeadlineFailureSkipsWalk(t *testing.T) {
	f := newKaaFixture(t)
	f.hangFirstListing.Store(true)
	p := newKaaTestProvider(f)

	ctx, cancel := context.WithTimeout(context.Background(), 1400*time.Millisecond)
	defer cancel()
	if _, err := p.FindEpisodeSource(ctx, "21", 1, "sub"); err == nil {
		t.Fatal("a hung listing draw must fail the round, not succeed")
	}
	if got := atomic.LoadInt64(f.episodePageHits); got != 1 {
		t.Fatalf("listing fetches = %d, want 1 (deadline failure never re-draws within the round)", got)
	}
}

func TestKaaBackgroundRetryFillsCache(t *testing.T) {
	f := newKaaFixture(t)
	f.hangFirstListing.Store(true)
	p := newKaaTestProvider(f)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := p.FindEpisodeSource(ctx, "21", 1, "sub"); err == nil {
		t.Fatal("first round must fail on the hung draw")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.loadResolved("naruto-f3cf", 1, "sub") != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("background resolve never filled the resolve cache")
}
