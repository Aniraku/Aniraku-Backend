package streaming

// Cloudflare Turnstile solving for mkissa.net episode queries.
//
// mkissa's API answers NEED_CAPTCHA when the per-IP anti-abuse budget is
// exhausted. The site's own client then renders a Turnstile widget
// (siteKey 0x4AAAAAADXpHZ1lTeqKwhch, provider "turnstile1") and retries the
// same GraphQL POST with extensions.captcha = {token, provider}. This file
// produces such tokens without a browser:
//
//	MKISSA_CAPSOLVER_KEY      CapSolver AntiTurnstileTaskProxyLess
//	MKISSA_2CAPTCHA_KEY       2Captcha TurnstileTaskProxyless
//	MKISSA_TURNSTILE_TOKEN_FILE  manual mode: a token pasted from a real
//	                          browser session (the file's mtime invalidates
//	                          the cache, so refreshing the file rotates it)
//
// Solvers are only constructed on the first NEED_CAPTCHA, so an unsolved
// deployment costs nothing and the provider degrades to its throttle
// backoff instead.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	mkissaTurnstileSiteKey = "0x4AAAAAADXpHZ1lTeqKwhch"
	mkissaTurnstilePage    = "https://mkissa.to/"
	mkissaTurnstileProv    = "turnstile1"
)

// mkissaTurnstileToken returns a fresh Turnstile token, using whichever
// solver the environment provides. ok=false when no solver is configured.
func (p *MkissaProvider) mkissaTurnstileToken(ctx context.Context) (string, bool) {
	p.turnMu.Lock()
	solver := p.turnSolver
	if solver == nil {
		solver = newMkissaTurnstileSolver()
		p.turnSolver = solver
	}
	p.turnMu.Unlock()
	if solver == nil {
		return "", false
	}
	// Brief token cache: Turnstile tokens live a few minutes and single
	// solves cover short bursts, but they are single-use server-side, so
	// keep the window small.
	p.turnTokMu.Lock()
	defer p.turnTokMu.Unlock()
	if !p.turnTokAt.IsZero() && time.Since(p.turnTokAt) < 45*time.Second && p.turnTok != "" {
		return p.turnTok, true
	}
	tok, err := solver.solve(ctx)
	if err != nil {
		p.log.Warn().Err(err).Str("solver", solver.name()).Msg("mkissa: turnstile solve failed")
		return "", false
	}
	p.turnTok = tok
	p.turnTokAt = time.Now()
	return tok, true
}

// mkissaCaptchaRetry POSTs payload again with extensions.captcha attached,
// mirroring the client's withCaptchaRetry wrapper (which also forces the
// POST form for the retry).
func (p *MkissaProvider) mkissaCaptchaRetry(ctx context.Context, payload map[string]any) ([]byte, error) {
	tok, ok := p.mkissaTurnstileToken(ctx)
	if !ok {
		return nil, fmt.Errorf("mkissa: NEED_CAPTCHA and no turnstile solver configured")
	}
	ext, _ := payload["extensions"].(map[string]any)
	if ext == nil {
		ext = map[string]any{}
		payload["extensions"] = ext
	}
	ext["captcha"] = map[string]any{"token": tok, "provider": mkissaTurnstileProv}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBase, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Referer", mkissaReferer)
	req.Header.Set("Origin", mkissaOrigin)
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("x-build-id", mkissaBuildID)
	p.acquireAPI(ctx)
	resp, err := p.client.Do(req)
	p.releaseAPI()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mkissa: captcha retry POST -> HTTP %d", resp.StatusCode)
	}
	return rb, nil
}

type mkissaSolver interface {
	solve(ctx context.Context) (string, error)
	name() string
}

// newMkissaTurnstileSolver picks the first configured solver.
func newMkissaTurnstileSolver() mkissaSolver {
	if k := strings.TrimSpace(os.Getenv("MKISSA_CAPSOLVER_KEY")); k != "" {
		return &mkissaPollSolver{key: k, base: "https://api.capsolver.com", taskType: "AntiTurnstileTaskProxyLess", label: "capsolver"}
	}
	if k := strings.TrimSpace(os.Getenv("MKISSA_2CAPTCHA_KEY")); k != "" {
		return &mkissaPollSolver{key: k, base: "https://api.2captcha.com", taskType: "TurnstileTaskProxyless", label: "2captcha"}
	}
	if f := strings.TrimSpace(os.Getenv("MKISSA_TURNSTILE_TOKEN_FILE")); f != "" {
		return &mkissaManualSolver{file: f}
	}
	return nil
}

// mkissaPollSolver covers the createTask/getTaskResult shape shared by
// CapSolver and 2Captcha.
type mkissaPollSolver struct {
	key      string
	base     string
	taskType string
	label    string
}

func (s *mkissaPollSolver) name() string { return s.label }

func (s *mkissaPollSolver) solve(ctx context.Context) (string, error) {
	createBody, _ := json.Marshal(map[string]any{
		"clientKey": s.key,
		"task": map[string]any{
			"type":       s.taskType,
			"websiteURL": mkissaTurnstilePage,
			"websiteKey": mkissaTurnstileSiteKey,
		},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/createTask", bytes.NewReader(createBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	var cr struct {
		ErrorID int    `json:"errorId"`
		TaskID  string `json:"taskId"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if err := json.Unmarshal(b, &cr); err != nil || cr.TaskID == "" {
		return "", fmt.Errorf("%s createTask: %s", s.label, strings.TrimSpace(string(b)))
	}
	// Poll up to ~90s.
	resBody, _ := json.Marshal(map[string]any{"clientKey": s.key, "taskId": cr.TaskID})
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(3 * time.Second):
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+"/getTaskResult", bytes.NewReader(resBody))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		var rr struct {
			Status   string `json:"status"`
			Solution struct {
				Token string `json:"token"`
			} `json:"solution"`
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if json.Unmarshal(b, &rr) != nil {
			continue
		}
		if rr.Solution.Token != "" {
			return rr.Solution.Token, nil
		}
	}
	return "", fmt.Errorf("%s: solve timeout", s.label)
}

// mkissaManualSolver reads a pasted token from a file; the file's mtime
// invalidates the cached value so the operator can rotate it.
type mkissaManualSolver struct {
	file   string
	mu     sync.Mutex
	token  string
	modAt  time.Time
	readAt time.Time
}

func (s *mkissaManualSolver) name() string { return "manual" }

func (s *mkissaManualSolver) solve(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Re-stat every read; a fresh mtime means the operator pasted a new token.
	st, err := os.Stat(s.file)
	if err != nil {
		return "", fmt.Errorf("turnstile token file: %w", err)
	}
	if s.token != "" && st.ModTime().Equal(s.modAt) && time.Since(s.readAt) < 90*time.Second {
		return s.token, nil
	}
	b, err := os.ReadFile(s.file)
	if err != nil {
		return "", fmt.Errorf("turnstile token file: %w", err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("turnstile token file empty")
	}
	s.token = tok
	s.modAt = st.ModTime()
	s.readAt = time.Now()
	return tok, nil
}
