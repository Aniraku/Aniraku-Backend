package streaming

// Persistent mkissa engine supervisor.
//
// Rationale: api.mkissa.net throttles per egress IP, so every HTTP call
// counts. A one-shot engine run replays the full JS-chunk discovery crawl
// (~17 requests) + bootstrap on EVERY resolve — a self-inflicted stampede.
// The daemon (mkissa_daemon.mjs) stays alive: discovery and the lane key
// persist across requests, and each episode costs one signed call. Go
// serializes calls globally with a gap (shared IP bucket) and restarts
// the daemon if it ever dies or stops answering.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type mkissaDaemonResp struct {
	out mkissaEngineOutput
	err error
}

type mkissaDaemon struct {
	log      zerolog.Logger
	provider *MkissaProvider

	mu      sync.Mutex
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	pending map[uint64]chan mkissaDaemonResp
	nextID  uint64
	// bridgeQ holds signed jobs waiting for the relay worker (see
	// mkissa_bridge.go); guarded by mu like pending.
	bridgeQ []mkissaBridgeJob
}

func newMkissaDaemon(log zerolog.Logger, p *MkissaProvider) *mkissaDaemon {
	d := &mkissaDaemon{log: log, provider: p, pending: map[uint64]chan mkissaDaemonResp{}}
	// Publish for the bridge's poll/deliver entry points (last daemon wins
	// — prod constructs exactly one provider).
	registerMkissaBridgeDaemon(d)
	return d
}

// Call resolves one episode, preferring the relay bridge when
// ANIRAKU_BRIDGE_TOKEN enables it: the signed call then leaves from the
// GitHub runner's gate-clean US egress instead of this host. Transport
// trouble of any kind (queue full, silent worker, garbage answer) falls
// back to the local daemon; a decoded engine verdict does not — the
// engine said what it said. Only a caller whose own context is done
// skips the fallback (there is no time left for a local attempt either).
// callPath is Call with transport attribution: it reports which egress
// served (or failed) so the breaker can police each per-IP bucket
// independently — a dry runner bucket must rest the bridge without
// resting the local proxy path, and vice versa. Call stays as the
// transport-agnostic wrapper for tests and simple callers.
func (d *mkissaDaemon) callPath(ctx context.Context, showID, audio, epStr string) ([]mkissaEngineSource, string, error) {
	if MkissaBridgeToken() != "" {
		if mkissaBridgeBreaker.blocked() {
			d.log.Debug().Str("showId", showID).Str("audio", audio).Str("ep", epStr).
				Msg("mkissa: relay bridge resting (throttled cooldown), using local engine")
		} else {
			resp, err := d.callBridge(ctx, showID, audio, epStr)
			if err == nil {
				srcs, derr := decodeDaemonResp(resp, epStr)
				switch {
				case derr == nil:
					return srcs, "bridge", nil
				case ctx.Err() != nil:
					return nil, "bridge", derr
				case errors.Is(derr, errBridgeMalformed), errors.Is(derr, errBridgeWorker):
					d.log.Warn().Err(derr).Str("showId", showID).Str("audio", audio).Str("ep", epStr).
						Msg("mkissa: relay bridge trouble, falling back to local engine")
				default:
					// A decoded engine verdict (429, captcha, …) rides the
					// bridge's bucket: the runner's egress produced it.
					return nil, "bridge", derr
				}
			} else if ctx.Err() != nil {
				return nil, "bridge", err
			} else {
				d.log.Warn().Err(err).Str("showId", showID).Str("audio", audio).Str("ep", epStr).
					Msg("mkissa: relay bridge unavailable, falling back to local engine")
			}
		}
	}
	if mkissaLocalBreaker.blocked() {
		return nil, "local", fmt.Errorf("mkissa: local engine resting (throttled cooldown)")
	}
	srcs, err := d.callLocal(ctx, showID, audio, epStr)
	return srcs, "local", err
}

func (d *mkissaDaemon) Call(ctx context.Context, showID, audio, epStr string) ([]mkissaEngineSource, error) {
	srcs, _, err := d.callPath(ctx, showID, audio, epStr)
	return srcs, err
}

// callBridge queues the signed job and waits for the worker's answer,
// bounded by the caller's context and mkissaBridgeWait.
func (d *mkissaDaemon) callBridge(ctx context.Context, showID, audio, epStr string) (mkissaDaemonResp, error) {
	started := time.Now()
	id, ch, err := d.bridgeSend(ctx, showID, audio, epStr)
	if err != nil {
		return mkissaDaemonResp{}, err
	}
	bctx, cancel := context.WithTimeout(ctx, mkissaBridgeWait)
	defer cancel()
	select {
	case <-bctx.Done():
		d.abandon(id)
		if ctx.Err() != nil {
			return mkissaDaemonResp{}, ctx.Err()
		}
		return mkissaDaemonResp{}, fmt.Errorf("mkissa: relay worker did not answer within %s", mkissaBridgeWait)
	case r := <-ch:
		d.log.Info().Uint64("callId", id).Str("showId", showID).Str("audio", audio).Str("ep", epStr).
			Int64("ms", time.Since(started).Milliseconds()).
			Str("engineErr", firstNonEmpty(r.out.Error, errText(r.err))).Msg("mkissa: engine call returned (bridge)")
		return r, nil
	}
}

// callLocal is the pre-bridge path: one request straight to the persistent
// local daemon, starting (or restarting) it on demand.
func (d *mkissaDaemon) callLocal(ctx context.Context, showID, audio, epStr string) ([]mkissaEngineSource, error) {
	started := time.Now()
	id, ch, err := d.send(ctx, showID, audio, epStr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, mkissaEngineTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		d.abandon(id)
		return nil, ctx.Err()
	case <-cctx.Done():
		d.kill("call timeout")
		d.log.Warn().Uint64("callId", id).Str("showId", showID).Str("ep", epStr).
			Int64("ms", time.Since(started).Milliseconds()).
			Msg("mkissa: engine call timed out with the child alive")
		return nil, fmt.Errorf("mkissa: engine timeout")
	case r := <-ch:
		d.log.Info().Uint64("callId", id).Str("showId", showID).Str("audio", audio).Str("ep", epStr).
			Int64("ms", time.Since(started).Milliseconds()).
			Str("engineErr", firstNonEmpty(r.out.Error, errText(r.err))).Msg("mkissa: engine call returned")
		return decodeDaemonResp(r, epStr)
	}
}

// decodeDaemonResp turns one daemon/bridge response into episode sources.
// Shared by both transports so a bridge answer is interpreted exactly
// like a local one.
func decodeDaemonResp(r mkissaDaemonResp, epStr string) ([]mkissaEngineSource, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.out.Error != "" {
		return nil, fmt.Errorf("mkissa: engine error: %s", r.out.Error)
	}
	for _, res := range r.out.Results {
		if res.Episode == epStr {
			if res.Error != "" {
				return nil, fmt.Errorf("mkissa: episode %s: %s", epStr, res.Error)
			}
			return res.Sources, nil
		}
	}
	return nil, fmt.Errorf("mkissa: engine returned no result for episode %s", epStr)
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func (d *mkissaDaemon) send(ctx context.Context, showID, audio, epStr string) (uint64, chan mkissaDaemonResp, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.ensureStartedLocked(); err != nil {
		return 0, nil, err
	}
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	default:
	}
	id := atomic.AddUint64(&d.nextID, 1)
	line, err := json.Marshal(map[string]any{"id": id, "showId": showID, "audio": audio, "episodes": []string{epStr}})
	if err != nil {
		return 0, nil, err
	}
	ch := make(chan mkissaDaemonResp, 1)
	d.pending[id] = ch
	if _, err := d.stdin.Write(append(line, '\n')); err != nil {
		delete(d.pending, id)
		d.killLocked("stdin write failed")
		return 0, nil, fmt.Errorf("mkissa: engine write failed: %w", err)
	}
	return id, ch, nil
}

func (d *mkissaDaemon) abandon(id uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.pending, id)
}

func (d *mkissaDaemon) kill(reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.killLocked(reason)
}

// killLocked terminates the child and fails every pending call. The next
// Call restarts it (fresh discovery + lane key).
func (d *mkissaDaemon) killLocked(reason string) {
	d.log.Warn().Str("reason", reason).Msg("mkissa: engine daemon killed, will restart on demand")
	if d.cmd != nil && d.cmd.Process != nil {
		_ = d.cmd.Process.Kill()
		d.cmd = nil
		d.stdin = nil
	}
	for id, ch := range d.pending {
		delete(d.pending, id)
		select {
		case ch <- mkissaDaemonResp{err: fmt.Errorf("mkissa: engine %s", reason)}:
		default:
		}
	}
}

func (d *mkissaDaemon) ensureStartedLocked() error {
	if d.cmd != nil {
		return nil
	}
	bin, script, err := d.provider.daemonCommand()
	if err != nil {
		return err
	}
	cmd := exec.Command(bin, script)
	// Egress proxy scope: ONLY the engine child gets proxy env. The API
	// process must never see it — http.DefaultTransport honors
	// HTTP(S)_PROXY, so a process-wide setting would drag playback
	// relays and every other provider through the slow egress hop.
	// bun's NODE_USE_ENV_PROXY covers the engine's plain-fetch calls
	// (bootstrap/discovery; http proxies only) and MKISSA_PROXY is read
	// natively by wreq for the signed gate calls (socks4/socks5/http).
	if proxy := strings.TrimSpace(os.Getenv("MKISSA_PROXY")); proxy != "" {
		cmd.Env = append(os.Environ(),
			"MKISSA_PROXY="+proxy,
			"NODE_USE_ENV_PROXY=1",
			"HTTP_PROXY="+proxy,
			"HTTPS_PROXY="+proxy,
			"ALL_PROXY="+proxy,
		)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("mkissa: engine stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("mkissa: engine stdout: %w", err)
	}
	// Engine diagnostics: bun writes crashes, module errors and fetch
	// failures to stderr — discarding it made prod hangs undebuggable
	// (2026-10-04: every engine call timed out while the same script
	// answered in 2s by hand).
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("mkissa: engine stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("mkissa: engine start: %w", err)
	}
	d.cmd = cmd
	d.stdin = stdin
	d.log.Info().Str("bin", bin).Str("script", script).Msg("mkissa: engine started")
	go d.readLoop(cmd, stdout)
	go d.logStderr(cmd, stderr)
	go func() { _ = cmd.Wait() }()
	return nil
}

// logStderr forwards the engine's stderr into the app log so a failing
// bun child is diagnosable from `docker logs` alone.
func (d *mkissaDaemon) logStderr(cmd *exec.Cmd, r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if len(line) > 500 {
			line = line[:500] + "…"
		}
		d.log.Warn().Str("line", line).Msg("mkissa: engine stderr")
	}
}

func (d *mkissaDaemon) readLoop(cmd *exec.Cmd, stdout io.Reader) {
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		raw := strings.TrimSpace(sc.Text())
		if raw == "" {
			continue
		}
		var out mkissaEngineOutput
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			line := raw
			if len(line) > 300 {
				line = line[:300] + "…"
			}
			// The decode reason matters: a struct/field type mismatch
			// reads exactly like malformed JSON and silently drops the
			// reply (that is how the priority int/float bug hid).
			d.log.Warn().Str("decodeErr", err.Error()).Str("line", line).
				Msg("mkissa: engine stdout failed to decode")
			continue
		}
		d.mu.Lock()
		ch, ok := d.pending[out.ID]
		if ok {
			delete(d.pending, out.ID)
		}
		d.mu.Unlock()
		if ok {
			select {
			case ch <- mkissaDaemonResp{out: out}:
			default:
			}
		} else {
			// A response nobody is waiting for (id drift or a late reply
			// after an abandoned call) reads as a hang on the request
			// side — make it visible instead.
			line := raw
			if len(line) > 300 {
				line = line[:300] + "…"
			}
			d.log.Warn().Uint64("responseId", out.ID).Str("line", line).
				Msg("mkissa: engine response matched no pending call")
		}
	}
	// EOF or error: the child is gone; fail everything pending so the
	// next call restarts it.
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cmd == cmd {
		d.cmd = nil
		d.stdin = nil
		for id, ch := range d.pending {
			delete(d.pending, id)
			select {
			case ch <- mkissaDaemonResp{err: fmt.Errorf("mkissa: engine exited")}:
			default:
			}
		}
	}
}
