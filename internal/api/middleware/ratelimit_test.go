package middleware

import (
	"fmt"
	"testing"
	"time"
)

func TestRateLimiterCapEvictsOldest(t *testing.T) {
	rl := &RateLimiter{
		visitors:    make(map[string]*visitor),
		rate:        1,
		burst:       10,
		interval:    time.Second,
		maxVisitors: 3,
	}

	if !rl.allow("1.1.1.1") || !rl.allow("2.2.2.2") || !rl.allow("3.3.3.3") {
		t.Fatal("initial visitors should be allowed")
	}
	if len(rl.visitors) != 3 {
		t.Fatalf("visitors = %d, want 3", len(rl.visitors))
	}

	time.Sleep(time.Millisecond)
	if !rl.allow("4.4.4.4") {
		t.Fatal("new visitor should be allowed while evicting the oldest")
	}
	if len(rl.visitors) != 3 {
		t.Fatalf("visitors after cap = %d, want 3", len(rl.visitors))
	}
	if _, ok := rl.visitors["1.1.1.1"]; ok {
		t.Error("oldest visitor 1.1.1.1 should have been evicted")
	}
	if _, ok := rl.visitors["4.4.4.4"]; !ok {
		t.Error("newest visitor 4.4.4.4 should be present")
	}
}

func TestRateLimiterTokenBucketSemantics(t *testing.T) {
	rl := &RateLimiter{
		visitors:    make(map[string]*visitor),
		rate:        1,
		burst:       2,
		interval:    time.Hour,
		maxVisitors: 10,
	}

	// Burst of 2: two requests pass, third is limited.
	if !rl.allow("1.1.1.1") || !rl.allow("1.1.1.1") {
		t.Fatal("first two requests should be allowed")
	}
	if rl.allow("1.1.1.1") {
		t.Fatal("third request within burst should be limited")
	}

	// Simulate refill: mark lastSeen far enough in the past.
	v := rl.visitors["1.1.1.1"]
	v.lastSeen = v.lastSeen.Add(-2 * time.Hour)
	if !rl.allow("1.1.1.1") {
		t.Fatalf("visitor should be refilled: %+v", v)
	}

	// Distinct IPs are independent buckets.
	for i := 0; i < 5; i++ {
		if !rl.allow(fmt.Sprintf("10.0.%d.%d", i, i)) {
			t.Fatalf("distinct IP %d should not share a bucket", i)
		}
	}
}
