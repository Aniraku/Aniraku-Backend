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
// Tensho's 10-min resolved-episode cache skips the sequential prefix on
// repeat rounds (the skip is prefix-free per verify below), and a
// collector cut mid-slot-fan-out must ship the slots that already
// resolved; erase-on-cut is why Hoshi vanished from every One Piece
// round while its budget burned on one hung 4animo slot.

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

func TestTenshoResolvedEpisodeCacheSkipsPrefix(t *testing.T) {
	f := newTenshoFixture(t)
	p := newTenshoTestProvider(f)
	ctx := tenshoTestCtx(t)

	if _, err := p.FindEpisodeSource(ctx, "21", 1, "sub"); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	w1, s1 := f.watchHits.Load(), f.serverHits.Load()

	// Dub lane must ALSO be prefix-free: the cache holds both slot lists.
	dub, err := p.FindEpisodeSource(ctx, "21", 1, "dub")
	if err != nil {
		t.Fatalf("cached dub resolve: %v", err)
	}
	if dub == nil || len(dub.ServerNames) != 1 || dub.ServerNames[0] != "Tsuki" {
		t.Fatalf("dub = %+v, want the cached dub lane resolved", dub)
	}

	again, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("cached sub resolve: %v", err)
	}
	if w2 := f.watchHits.Load(); w2 != w1 {
		t.Fatalf("watch page fetches = %d -> %d on cached rounds: prefix reran", w1, w2)
	}
	if s2 := f.serverHits.Load(); s2 != s1 {
		t.Fatalf("server ajax calls = %d -> %d on cached rounds: prefix reran", s1, s2)
	}
	// The embeds resolved FRESH in every round, same stream set as round 1.
	if again == nil || len(again.Sources) != 3 || again.Sources[0].URL != dub.Sources[0].URL {
		t.Fatalf("cached round sources = %+v, want the full fresh embed resolution", again)
	}
}

func TestTenshoCutFanOutShipsSurvivors(t *testing.T) {
	f := newTenshoFixture(t)
	f.hangEmbed.Store(true)
	p := newTenshoTestProvider(f)

	// Collector budget shape: s-2 hangs to the cut while s-1/s-3 resolve;
	// the round must ship those two rather than erroring away.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	sr, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("survivor slots must not turn a cut into an error: %v", err)
	}
	if sr == nil {
		t.Fatal("survivor slots erased: want Tsuki/Hoshi partial")
	}
	if len(sr.Sources) != 2 {
		t.Fatalf("sources = %d, want the two survivors (s-1, s-3)", len(sr.Sources))
	}
	if got := sr.ServerNames; got[0] != "Tsuki" || got[1] != "Hoshi" {
		t.Fatalf("names = %v, want positional [Tsuki Hoshi] (dropped s-2 keeps its own name out)", got)
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
