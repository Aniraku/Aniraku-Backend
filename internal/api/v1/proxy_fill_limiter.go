package v1

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// Origin fill limiter (2026-10-16, memory hardening after the OOM wedges).
//
// The origin box runs on 836 MB of RAM. Every /proxy cache miss is one
// upstream connection + goroutine + buffers held for the whole paced copy,
// so the proxy's memory profile is unbounded exactly when it matters:
// evening peak, when fills stack up faster than the pacer drains them.
// The kernel OOM-killed aniraku-server three times on Oct 15-16
// (kernel: oom-kill task=aniraku-server, restart_count=3) and each wedge
// required a manual reboot.
//
// The limiter bounds how many upstream fills may run at once. Requests
// that find all slots busy queue briefly — bounded by the shorter of the
// client's remaining budget and the queue hold — then get a clean 503
// (Retry-After) they can retry, instead of silently pushing the box into
// the swap thrash that previously only a reboot cleared. Cache hits (VOD
// cache, key cache) bypass the limiter on purpose: they cost no upstream
// fill. The hop is taken AFTER the SSRF/allowlist gates so garbage URLs
// never occupy slots.
//
// ANIRAKU_PROXY_MAX_STREAMS overrides the cap; 0 disables the limiter
// entirely (the pre-Oct-16 behavior). The value is read at process
// start; changing it requires a restart (documented for operators, same
// contract as the shaping envar in proxy_throttle.go).
const (
	limiterEnv        = "ANIRAKU_PROXY_MAX_STREAMS"
	limiterDefaultCap = 48
	limiterQueueHold  = 10 * time.Second
)

// fillLimiter is a counting semaphore over upstream fills.
type fillLimiter struct {
	slots chan struct{}
}

var proxyFills = newFillLimiter()

func newFillLimiter() *fillLimiter {
	if n := parseFillCap(os.Getenv(limiterEnv)); n > 0 {
		return &fillLimiter{slots: make(chan struct{}, n)}
	}
	return &fillLimiter{} // 0 = unlimited: pre-limiter behavior
}

// parseFillCap maps the envar to a slot count. Garbage and empty strings
// fall back to the default; negatives and 0 disable the limiter.
func parseFillCap(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return limiterDefaultCap
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return limiterDefaultCap
	}
	if n < 0 {
		return 0
	}
	return n
}

// acquire occupies one fill slot or reports ok=false to shed the request.
// The wait is bounded by the client context so a browser that walks away
// never leaves a request queued behind a slot, and by limiterQueueHold so
// load-shed happens as a clean retry error instead of a runaway queue.
func (f *fillLimiter) acquire(ctx context.Context) (released func(), ok bool) {
	if f.slots == nil {
		return func() {}, true
	}
	select {
	case f.slots <- struct{}{}:
		return func() { <-f.slots }, true
	default:
	}
	hold := time.NewTimer(limiterQueueHold)
	defer hold.Stop()
	select {
	case f.slots <- struct{}{}:
		return func() { <-f.slots }, true
	case <-hold.C:
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}

// Memory caps for the two paths that buffer upstream bodies before
// rewriting (2026-10-16). Playlists are tens of KiB (the VOD cache itself
// stores at most 256 KiB) and encryption keys are 16 bytes, so anything
// approaching these ceilings is not the media type the URL claims; past
// the cap the bytes are streamed instead of buffered.
const (
	hlsMaxRead      = 1 << 20
	keyCacheMaxRead = 256 << 10
)

// streamKeepCloser joins a capped head with the remainder of an upstream
// body while keeping its Close wired to resp.Body. Swapping resp.Body for
// a plain NopCloser over a MultiReader would strand the real connection
// body: the handler's deferred Close would then close nothing under load,
// leaking upstream FDs until GC got around to finalizing them.
func streamKeepCloser(head io.Reader, body io.ReadCloser) io.ReadCloser {
	return struct {
		io.Reader
		io.Closer
	}{io.MultiReader(head, body), body}
}
