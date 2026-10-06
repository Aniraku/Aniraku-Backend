package streaming

// Bridge tests: the pull-worker hand-off (queue → poll → deliver → wake)
// plus every fallback edge — worker silent, queue full, worker sending
// garbage — must land on the local daemon exactly like before the bridge
// existed. The "worker" in these tests is the test goroutine itself: it
// polls MkissaBridgePoll and answers with the canned engine line.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// pollBridgeJob waits (like the real worker) until a job shows up.
func pollBridgeJob(t *testing.T) mkissaBridgeJob {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		jobs := MkissaBridgePoll(8)
		if len(jobs) > 0 {
			return jobs[0]
		}
		if time.Now().After(deadline) {
			t.Fatal("no bridge job appeared in the queue")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The happy path: Call parks a job, the worker polls it, delivers the
// canned engine line under the job's id, and the Call wakes with sources.
func TestMkissaBridgeRoundTrip(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "bridge-test-token")
	f := newMkissaFixture(t, nil, "")
	p := newMkissaTestProvider(f)
	ctx := mkissaTestCtx(t)

	type result struct {
		srcs []mkissaEngineSource
		err  error
	}
	done := make(chan result, 1)
	go func() {
		s, err := p.daemon.Call(ctx, mkissaTestShowID, "sub", "1")
		done <- result{s, err}
	}()

	job := pollBridgeJob(t)
	if job.ShowID != mkissaTestShowID || job.Audio != "sub" || job.Ep != "1" {
		t.Fatalf("queued job payload wrong: %+v", job)
	}
	// The canned line carries "id":0 — swap in the job's id like the
	// daemon stub does for stdin requests.
	stub := strings.Replace(mkissaStubOutput(f.media.URL), `"id":0`, fmt.Sprintf(`"id":%d`, job.ID), 1)
	if !MkissaBridgeDeliver(job.ID, stub, "") {
		t.Fatal("deliver refused a waiting job")
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("call failed: %v", r.err)
		}
		// Raw engine output: 5 sources (3 keepers + embed + unknown kind).
		if len(r.srcs) != 5 {
			t.Fatalf("want 5 raw engine sources, got %d: %+v", len(r.srcs), r.srcs)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("call never returned")
	}
	if left := MkissaBridgePoll(8); len(left) != 0 {
		t.Fatalf("delivered job still queued: %+v", left)
	}
}

// Nobody polls: the bridge wait expires and the local daemon (the stub
// engine) serves the call — today's pre-bridge behaviour, unchanged.
func TestMkissaBridgeTimeoutFallsBackToLocal(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "bridge-test-token")
	oldWait := mkissaBridgeWait
	mkissaBridgeWait = 250 * time.Millisecond
	defer func() { mkissaBridgeWait = oldWait }()

	f := newMkissaFixture(t, nil, "")
	p := newMkissaTestProvider(f)
	ctx := mkissaTestCtx(t)

	srcs, err := p.daemon.Call(ctx, mkissaTestShowID, "sub", "1")
	if err != nil {
		t.Fatalf("local fallback after bridge timeout failed: %v", err)
	}
	if len(srcs) != 5 {
		t.Fatalf("want 5 sources via local fallback, got %d", len(srcs))
	}
	// The unclaimed job stays queued (at-least-once) until delivered or reaped.
	if left := MkissaBridgePoll(8); len(left) != 1 {
		t.Fatalf("unclaimed job should still be queued, got %d", len(left))
	}
}

// No token: the bridge does not exist — calls go local directly and the
// queue never fills.
func TestMkissaBridgeDisabledWithoutToken(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "")
	f := newMkissaFixture(t, nil, "")
	p := newMkissaTestProvider(f)
	ctx := mkissaTestCtx(t)

	srcs, err := p.daemon.Call(ctx, mkissaTestShowID, "sub", "1")
	if err != nil {
		t.Fatalf("local call failed: %v", err)
	}
	if len(srcs) != 5 {
		t.Fatalf("want 5 sources, got %d", len(srcs))
	}
	if left := MkissaBridgePoll(8); len(left) != 0 {
		t.Fatalf("bridge queued work while disabled: %+v", left)
	}
}

// A full queue is bridge trouble, not an error the user should see:
// the call must land on the local daemon instead.
func TestMkissaBridgeQueueFullFallsToLocal(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "bridge-test-token")
	oldCap := mkissaBridgeQueueCap
	mkissaBridgeQueueCap = 1
	defer func() { mkissaBridgeQueueCap = oldCap }()

	f := newMkissaFixture(t, nil, "")
	p := newMkissaTestProvider(f)

	// Occupy the single slot with a job nobody ever claims.
	prefill, _, err := p.daemon.bridgeSend(context.Background(), mkissaTestShowID, "sub", "1")
	if err != nil {
		t.Fatalf("prefill: %v", err)
	}
	defer p.daemon.abandon(prefill)

	ctx := mkissaTestCtx(t)
	srcs, err := p.daemon.Call(ctx, mkissaTestShowID, "sub", "1")
	if err != nil {
		t.Fatalf("call with full bridge queue failed: %v", err)
	}
	if len(srcs) != 5 {
		t.Fatalf("want 5 sources via local fallback, got %d", len(srcs))
	}
}

// A worker answer that is not engine JSON is a transport-level failure:
// the call falls back to the local daemon instead of surfacing the
// decode error (an engine verdict like NEED_CAPTCHA, by contrast, is a
// real answer and must stand).
func TestMkissaBridgeMalformedAnswerFallsBackToLocal(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "bridge-test-token")
	oldWait := mkissaBridgeWait
	mkissaBridgeWait = 5 * time.Second
	defer func() { mkissaBridgeWait = oldWait }()

	f := newMkissaFixture(t, nil, "")
	p := newMkissaTestProvider(f)
	ctx := mkissaTestCtx(t)

	done := make(chan error, 1)
	go func() {
		_, err := p.daemon.Call(ctx, mkissaTestShowID, "sub", "1")
		done <- err
	}()

	job := pollBridgeJob(t)
	if !MkissaBridgeDeliver(job.ID, "this is not engine json", "") {
		t.Fatal("deliver refused a waiting job")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("malformed answer should fall back to local, got error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("call never returned")
	}
}

// Answers for ids nobody is waiting for (late, duplicate, forged) are
// dropped, never panics.
func TestMkissaBridgeDeliverUnknownID(t *testing.T) {
	f := newMkissaFixture(t, nil, "")
	_ = newMkissaTestProvider(f) // registers the daemon
	if MkissaBridgeDeliver(999999, `{"id":999999}`, "") {
		t.Fatal("deliver accepted an unknown id")
	}
	if MkissaBridgeDeliver(999998, "", "engine exploded") {
		t.Fatal("deliver accepted an unknown id via error path")
	}
}
