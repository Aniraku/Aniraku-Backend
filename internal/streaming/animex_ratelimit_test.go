package streaming

import (
	"errors"
	"testing"
	"time"
)

// TestRateLimitNeverRetried guards the throttle death spiral: a 429 must
// neither trigger the immediate fast-failure retry nor a hedged duplicate —
// every extra hit while throttled extends the throttle.
func TestRateLimitNeverRetried(t *testing.T) {
	limited := &animexRateLimitError{status: 429, retryAfter: 20 * time.Second}
	if shouldRetryProvider(limited, 100*time.Millisecond) {
		t.Error("429 must not be retried")
	}
	if shouldRetryProvider(limited, 10*time.Second) {
		t.Error("slow 429 must not be retried either")
	}
	// Ordinary fast errors still retry; slow ones still don't.
	if !shouldRetryProvider(errors.New("boom"), 100*time.Millisecond) {
		t.Error("fast transient error should still retry")
	}
	if shouldRetryProvider(errors.New("boom"), 10*time.Second) {
		t.Error("slow error must not retry")
	}
}

func TestParseRateLimit(t *testing.T) {
	if got := parseRateLimit([]byte(`{"error":"too_many_requests","retry_after":27}`)); got != 27*time.Second {
		t.Errorf("got %v, want 27s", got)
	}
	if got := parseRateLimit([]byte(`{}`)); got != 30*time.Second {
		t.Errorf("empty body should fall back to 30s, got %v", got)
	}
	if got := parseRateLimit([]byte(`{"retry_after":3600}`)); got != 2*time.Minute {
		t.Errorf("absurd cooldown must cap at 2m, got %v", got)
	}
}

func TestCooldownWindow(t *testing.T) {
	p := &AnimeXProvider{}
	if p.coolingDown() {
		t.Fatal("fresh provider must not be cooling down")
	}
	p.setCooldown(50 * time.Millisecond)
	if !p.coolingDown() {
		t.Fatal("provider must cool down after setCooldown")
	}
	time.Sleep(80 * time.Millisecond)
	if p.coolingDown() {
		t.Fatal("cooldown must expire")
	}
	// Longer windows win; shorter ones never shrink the window.
	p.setCooldown(time.Minute)
	before := time.Now().Add(time.Minute)
	p.setCooldown(time.Second)
	p.limitMu.Lock()
	until := p.limitUntil
	p.limitMu.Unlock()
	if until.Before(before.Add(-5 * time.Second)) {
		t.Error("shorter cooldown must not shrink the window")
	}
}
