package streaming

// mkissa relay bridge — pull-worker hand-off to a GitHub Actions runner.
//
// mkissa's signed-call gate is per egress IP: the VPS (and every WARP exit
// from this region) escalates to NEED_CAPTCHA, while the GitHub Actions
// runner's native US egress has answered GATE_OPEN on every probe
// (2026-10-06). A runner cannot receive inbound traffic, so the direction
// is reversed: the provider enqueues each signed engine job here and
// WAITS; a worker (deploy/mkissa-relay-worker/worker.mjs, kept alive by
// the mkissa-relay workflow) polls this queue about once a second from the
// runner, executes the vendored engine with the runner's egress, and posts
// the raw engine response line back. The waiting Call matches it by id.
//
// Failure handling is deliberately boring:
//   - Any bridge trouble (queue full, no worker within mkissaBridgeWait,
//     malformed answer) makes the Call fall back to the LOCAL daemon —
//     today's pre-bridge behaviour, with breaker + cache absorbing misses.
//   - The queue holds at most mkissaBridgeQueueCap jobs and reaps entries
//     older than mkissaBridgeTTL, so a dead worker can neither grow
//     memory nor cause a stampede when it comes back.
//   - Poll hands jobs out but they only leave the queue when the result
//     arrives (at-least-once): a worker dying mid-job is covered by the
//     next one, and duplicate answers are harmless because the pending
//     entry is consumed exactly once.
//
// The endpoints answer 404 unless ANIRAKU_BRIDGE_TOKEN is set; the same
// token authenticates the worker (Bearer, constant-time compare).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Transport-level bridge failures. Call falls back to the local daemon
// for these; a DECODED engine verdict (including NEED_CAPTCHA or a
// missing episode) is the engine's real answer and stands as-is.
var (
	errBridgeMalformed = errors.New("malformed engine output from relay worker")
	errBridgeWorker    = errors.New("relay worker reported failure")
)

// Vars (not consts) so tests can shorten the wait and shrink the cap.
var (
	// mkissaBridgeWait bounds how long a Call waits for the relay worker
	// before falling back to the local daemon. 15s covers a cold worker
	// hand-off (poll ≤1s + engine answer ≤10s) while leaving slack in the
	// fan-out budget even when a local attempt follows.
	mkissaBridgeWait = 15 * time.Second
	// mkissaBridgeTTL reaps queued jobs no worker ever claimed.
	mkissaBridgeTTL = 90 * time.Second
	// mkissaBridgeQueueCap bounds the queue against a stalled worker.
	mkissaBridgeQueueCap = 32
)

// mkissaBridgeJob is one signed engine call waiting for the relay worker.
// `queued` is bookkeeping only (TTL reaping) and never serialized.
type mkissaBridgeJob struct {
	ID     uint64 `json:"id"`
	ShowID string `json:"showId"`
	Audio  string `json:"audio"`
	Ep     string `json:"ep"`
	queued time.Time
}

// MkissaBridgeToken is the shared secret gating the bridge endpoints —
// read live from the environment so a rotation needs no restart. Empty
// token = bridge disabled everywhere (provider, API, worker gets 404).
func MkissaBridgeToken() string {
	return strings.TrimSpace(os.Getenv("ANIRAKU_BRIDGE_TOKEN"))
}

// The live daemon serving bridge jobs. Producers never touch the registry
// directly (bridgeSend runs on the daemon itself); only the API's
// poll/deliver entry points dereference it, and they release this mutex
// before taking the daemon's — no nested locking in either direction.
var (
	mkissaBridgeMu     sync.Mutex
	mkissaBridgeDaemon *mkissaDaemon
)

func registerMkissaBridgeDaemon(d *mkissaDaemon) {
	mkissaBridgeMu.Lock()
	mkissaBridgeDaemon = d
	mkissaBridgeMu.Unlock()
}

// bridgeSend parks one signed call: it allocates the id, registers the
// waiter (same pending map the local daemon uses) and queues the job for
// the worker. It never spawns the local engine — that is the fallback's
// job when the bridge cannot deliver.
func (d *mkissaDaemon) bridgeSend(ctx context.Context, showID, audio, epStr string) (uint64, chan mkissaDaemonResp, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	default:
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reapBridgeLocked(time.Now())
	if len(d.bridgeQ) >= mkissaBridgeQueueCap {
		return 0, nil, fmt.Errorf("mkissa: bridge queue full (%d)", mkissaBridgeQueueCap)
	}
	id := atomic.AddUint64(&d.nextID, 1)
	ch := make(chan mkissaDaemonResp, 1)
	d.pending[id] = ch
	d.bridgeQ = append(d.bridgeQ, mkissaBridgeJob{
		ID: id, ShowID: showID, Audio: audio, Ep: epStr, queued: time.Now(),
	})
	return id, ch, nil
}

// reapBridgeLocked drops jobs no worker claimed within the TTL. The
// waiter's own timeout already abandoned its pending entry by then.
func (d *mkissaDaemon) reapBridgeLocked(now time.Time) {
	live := d.bridgeQ[:0]
	for _, j := range d.bridgeQ {
		if now.Sub(j.queued) < mkissaBridgeTTL {
			live = append(live, j)
		}
	}
	d.bridgeQ = live
}

// MkissaBridgePoll hands up to limit queued jobs to the relay worker.
// Jobs stay queued until MkissaBridgeDeliver consumes them (at-least-once:
// a worker that dies mid-job loses nothing, duplicates resolve to one
// delivered answer).
func MkissaBridgePoll(limit int) []mkissaBridgeJob {
	mkissaBridgeMu.Lock()
	d := mkissaBridgeDaemon
	mkissaBridgeMu.Unlock()
	if d == nil || limit <= 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reapBridgeLocked(time.Now())
	if len(d.bridgeQ) == 0 {
		return nil
	}
	if limit > len(d.bridgeQ) {
		limit = len(d.bridgeQ)
	}
	out := make([]mkissaBridgeJob, limit)
	copy(out, d.bridgeQ[:limit])
	return out
}

// MkissaBridgeDeliver feeds a worker's answer (raw engine line, or errMsg
// when the worker itself failed) back to the waiting Call. It reports
// whether somebody was still waiting: late answers after a local fallback,
// unknown ids and duplicate deliveries are dropped (false), but the job
// always leaves the queue.
func MkissaBridgeDeliver(id uint64, line, errMsg string) bool {
	var resp mkissaDaemonResp
	if errMsg != "" {
		resp.err = fmt.Errorf("%w: %s", errBridgeWorker, errMsg)
	} else {
		var out mkissaEngineOutput
		if err := json.Unmarshal([]byte(line), &out); err != nil {
			resp.err = fmt.Errorf("%w: %v", errBridgeMalformed, err)
		} else {
			resp.out = out
		}
	}
	mkissaBridgeMu.Lock()
	d := mkissaBridgeDaemon
	mkissaBridgeMu.Unlock()
	if d == nil {
		return false
	}
	d.mu.Lock()
	for i, j := range d.bridgeQ {
		if j.ID == id {
			d.bridgeQ = append(d.bridgeQ[:i], d.bridgeQ[i+1:]...)
			break
		}
	}
	ch, ok := d.pending[id]
	if ok {
		delete(d.pending, id)
	}
	d.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- resp:
	default:
	}
	return true
}
