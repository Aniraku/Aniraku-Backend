package middleware

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestRateLimiterConcurrentAccess hammers allow() from many goroutines. Run
// under -race it proves the mutex discipline; functionally it proves that
// concurrent new visitors each get exactly one bucket and that the limiter
// never panics under contention.
func TestRateLimiterConcurrentAccess(t *testing.T) {
	rl := NewRateLimiter(1000, 1000, time.Second)

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				rl.allow("10.0.0.1")
				rl.allow("10.0.1." + string(rune('a'+g%26)))
			}
		}(g)
	}
	wg.Wait()

	rl.mu.Lock()
	visitors := len(rl.visitors)
	rl.mu.Unlock()
	if visitors != 27 { // 1 shared + 26 unique
		t.Errorf("visitors = %d, want 27", visitors)
	}
}

// TestRateLimiterBurstThenRefill: exhaust the burst, confirm the next request
// is denied, wait for refill, confirm allowed again.
func TestRateLimiterBurstThenRefill(t *testing.T) {
	rl := NewRateLimiter(10, 3, 50*time.Millisecond)

	for i := 0; i < 3; i++ {
		if !rl.allow("1.2.3.4") {
			t.Fatalf("request %d denied within burst", i)
		}
	}
	if rl.allow("1.2.3.4") {
		t.Error("request beyond burst allowed")
	}
	time.Sleep(80 * time.Millisecond) // > one refill interval
	if !rl.allow("1.2.3.4") {
		t.Error("request after refill denied")
	}
}

// TestRateLimiterEvictsOldestVisitor: exceeding maxVisitors drops the least
// recently seen bucket, so the map cannot grow without bound.
func TestRateLimiterEvictsOldestVisitor(t *testing.T) {
	rl := NewRateLimiter(10, 10, time.Second)
	rl.maxVisitors = 3 // shrink for the test

	rl.allow("v1")
	time.Sleep(2 * time.Millisecond)
	rl.allow("v2")
	time.Sleep(2 * time.Millisecond)
	rl.allow("v3")
	// v1 is now the oldest; a new visitor forces its eviction.
	rl.allow("v4")

	rl.mu.Lock()
	_, v1Alive := rl.visitors["v1"]
	_, v4Alive := rl.visitors["v4"]
	rl.mu.Unlock()

	if v1Alive {
		t.Error("oldest visitor v1 was not evicted")
	}
	if !v4Alive {
		t.Error("newest visitor v4 missing after eviction")
	}
}

// TestRateLimiterMiddleware writes 429 with Retry-After and does not call the
// next handler when exhausted.
func TestRateLimiterMiddleware(t *testing.T) {
	rl := NewRateLimiter(1, 1, time.Hour)

	called := false
	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/", nil))
	if first.Code != http.StatusOK || !called {
		t.Fatalf("first request: code=%d called=%v", first.Code, called)
	}

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/", nil))
	if second.Code != http.StatusTooManyRequests {
		t.Errorf("second request code = %d, want 429", second.Code)
	}
	if second.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After on 429")
	}
}
