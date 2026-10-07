package streaming

// Egress-path tests: breaker verdicts must land on the bucket of the
// egress that produced them (runner bridge vs local/proxied daemon), a
// rested path must be skipped without touching the other, "every path
// resting" is the only fail-fast condition, and a rate verdict gets
// exactly one polite Go-side retry.

import (
	"strings"
	"testing"
	"time"
)

func mkissaFreshBreaker() *mkissaThrottleBreaker {
	return &mkissaThrottleBreaker{cooldown: time.Hour, tripAfter: 2}
}

func mkissaTrippedBreaker() *mkissaThrottleBreaker {
	b := mkissaFreshBreaker()
	b.record(false, true)
	b.record(false, true)
	if !b.blocked() {
		panic("breaker must be tripped by construction")
	}
	return b
}

func TestMkissaBreakerRouting(t *testing.T) {
	if mkissaBreakerFor("bridge") != mkissaBridgeBreaker {
		t.Fatal("bridge verdict must land on the bridge bucket")
	}
	if mkissaBreakerFor("local") != mkissaLocalBreaker {
		t.Fatal("local verdict must land on the local bucket")
	}
	if mkissaBreakerFor("") != mkissaLocalBreaker {
		t.Fatal("unknown path must default to the local bucket")
	}
}

func TestMkissaNoPathUsable(t *testing.T) {
	oldBridge, oldLocal := mkissaBridgeBreaker, mkissaLocalBreaker
	defer func() {
		mkissaBridgeBreaker, mkissaLocalBreaker = oldBridge, oldLocal
	}()

	cases := []struct {
		name   string
		token  string
		bridge *mkissaThrottleBreaker
		local  *mkissaThrottleBreaker
		want   bool
	}{
		{"no token, local rested", "", mkissaFreshBreaker(), mkissaFreshBreaker(), false},
		{"no token, local resting", "", mkissaFreshBreaker(), mkissaTrippedBreaker(), true},
		{"bridge usable, local resting", "t", mkissaFreshBreaker(), mkissaTrippedBreaker(), false},
		{"bridge resting, local usable", "t", mkissaTrippedBreaker(), mkissaFreshBreaker(), false},
		{"both resting", "t", mkissaTrippedBreaker(), mkissaTrippedBreaker(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ANIRAKU_BRIDGE_TOKEN", tc.token)
			mkissaBridgeBreaker, mkissaLocalBreaker = tc.bridge, tc.local
			if got := mkissaNoPathUsable(); got != tc.want {
				t.Fatalf("mkissaNoPathUsable() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A rested bridge is skipped outright: no job is queued, no bridge wait
// is burned — the local daemon answers immediately.
func TestMkissaBridgeRestedSkipsToLocal(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "bridge-test-token")
	oldBridge := mkissaBridgeBreaker
	mkissaBridgeBreaker = mkissaTrippedBreaker()
	defer func() { mkissaBridgeBreaker = oldBridge }()

	f := newMkissaFixture(t, nil, "")
	p := newMkissaTestProvider(f)

	start := time.Now()
	srcs, path, err := p.daemon.callPath(mkissaTestCtx(t), mkissaTestShowID, "sub", "1")
	if err != nil {
		t.Fatalf("local call with rested bridge: %v", err)
	}
	if path != "local" {
		t.Fatalf("path = %q, want local", path)
	}
	if len(srcs) != 5 {
		t.Fatalf("sources = %d, want 5", len(srcs))
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("rested bridge still burned %s (wait not skipped?)", elapsed)
	}
	if left := MkissaBridgePoll(8); len(left) != 0 {
		t.Fatalf("rested bridge still got queued: %+v", left)
	}
}

// Every usable path resting: runEngine fails fast before spawning
// anything or spending the fan-out budget.
func TestMkissaAllPathsRestingFailsFast(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "")
	oldLocal := mkissaLocalBreaker
	mkissaLocalBreaker = mkissaTrippedBreaker()
	defer func() { mkissaLocalBreaker = oldLocal }()

	f := newMkissaFixture(t, nil, "")
	p := newMkissaTestProvider(f)
	p.engineBin = "/bin/false" // must never spawn

	start := time.Now()
	_, err := p.runEngine(mkissaTestCtx(t), mkissaTestShowID, "sub", "1")
	if err == nil || !strings.Contains(err.Error(), "throttled cooldown") {
		t.Fatalf("err = %v, want throttled cooldown", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("fail-fast took %s", elapsed)
	}
}

// A rate verdict ("try again in N seconds") gets one Go-side re-attempt
// after the demanded pause: line 1 of the canned daemon output is a
// throttle, line 2 the real payload — success proves the retry ran.
func TestMkissaRateVerdictGetsOneRetry(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "")
	f := newMkissaFixture(t, nil,
		`{"id":0,"showId":"`+mkissaTestShowID+`","audio":"sub","results":[{"episode":"1",`+
			`"error":"Too many requests, please try again in 1 seconds.",`+
			`"code":"Too many requests, please try again in 1 seconds."}]}`+"\n===NEXT===\n"+
			mkissaStubOutput("{{media}}"))
	p := newMkissaTestProvider(f)

	start := time.Now()
	srcs, err := p.runEngine(mkissaTestCtx(t), mkissaTestShowID, "sub", "1")
	if err != nil {
		t.Fatalf("rate verdict must succeed on the retry: %v", err)
	}
	if len(srcs) != 5 {
		t.Fatalf("sources = %d, want 5 (raw engine line)", len(srcs))
	}
	// The demanded 1s + mkissaRateExtra must actually be waited out.
	if elapsed := time.Since(start); elapsed < 1500*time.Millisecond {
		t.Fatalf("retry returned after %s — pause not honored", elapsed)
	}
}
