package v1

import (
	"context"
	"testing"
	"time"
)

// The limiter is the OOM fix on an 836 MB origin box: it must shed beyond
// its cap with a bounded wait, free a slot for the next waiter on
// release, and give up as soon as the client's own budget expires.
func TestFillLimiterShedsBeyondCap(t *testing.T) {
	f := &fillLimiter{slots: make(chan struct{}, 2)}
	r1, ok := f.acquire(context.Background())
	if !ok {
		t.Fatal("first acquire must succeed")
	}
	r2, ok := f.acquire(context.Background())
	if !ok {
		t.Fatal("second acquire must succeed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if released, ok := f.acquire(ctx); ok {
		released()
		t.Fatal("acquire beyond cap must shed once the wait expires")
	}

	r1()
	r3, ok := f.acquire(context.Background())
	if !ok {
		t.Fatal("a freed slot must admit the next acquisition")
	}
	r3()
	r2()
}

func TestFillLimiterAbortsOnClientContext(t *testing.T) {
	f := &fillLimiter{slots: make(chan struct{}, 1)}
	r1, ok := f.acquire(context.Background())
	if !ok {
		t.Fatal("cap-1 first acquire must succeed")
	}
	defer r1()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if released, ok := f.acquire(ctx); ok {
		released()
		t.Fatal("acquire must abort when the client context ends, not wait out the full hold")
	}
}

func TestParseFillCapEnvContract(t *testing.T) {
	cases := map[string]int{
		"":        limiterDefaultCap, // unset behaves like the coded default
		"   ":     limiterDefaultCap,
		"96":      96,
		"0":       0, // explicit unlimited
		"-5":      0, // negatives read as unlimited, never a zero cap
		"garbage": limiterDefaultCap,
	}
	for raw, want := range cases {
		if got := parseFillCap(raw); got != want {
			t.Errorf("parseFillCap(%q) = %d, want %d", raw, got, want)
		}
	}
}

// Unlimited configuration never sheds and releases are no-ops.
func TestFillLimiterUnlimited(t *testing.T) {
	f := &fillLimiter{} // nil slots
	for i := 0; i < 10; i++ {
		rel, ok := f.acquire(context.Background())
		if !ok {
			t.Fatal("unlimited limiter must never shed")
		}
		rel()
	}
}
