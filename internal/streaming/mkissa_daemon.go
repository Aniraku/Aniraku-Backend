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
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"

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
}

func newMkissaDaemon(log zerolog.Logger, p *MkissaProvider) *mkissaDaemon {
	return &mkissaDaemon{log: log, provider: p, pending: map[uint64]chan mkissaDaemonResp{}}
}

// Call sends one episode request to the daemon, starting (or restarting)
// it on demand.
func (d *mkissaDaemon) Call(ctx context.Context, showID, audio, epStr string) ([]mkissaEngineSource, error) {
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
		return nil, fmt.Errorf("mkissa: engine timeout")
	case r := <-ch:
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
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("mkissa: engine stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("mkissa: engine stdout: %w", err)
	}
	cmd.Stderr = nil // engine errors surface as JSON responses
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("mkissa: engine start: %w", err)
	}
	d.cmd = cmd
	d.stdin = stdin
	go d.readLoop(cmd, stdout)
	go func() { _ = cmd.Wait() }()
	return nil
}

func (d *mkissaDaemon) readLoop(cmd *exec.Cmd, stdout io.Reader) {
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var out mkissaEngineOutput
		if err := json.Unmarshal(sc.Bytes(), &out); err != nil {
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
