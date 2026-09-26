package v1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// A 429 carrying Retry-After must be honored with one honest wait and a
// single retry — not burned through with short blind sleeps inside the ban.
func TestAniListClientHonorsRetryAfter(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"data":null}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"ok":true}}`))
	}))
	defer server.Close()

	h := &Handlers{}
	client := newAnilistClient(h)
	client.client = server.Client()
	client.endpoint = server.URL

	start := time.Now()
	got, err := client.do(context.Background(), "query { x }", nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("do() error = %v, want success after honoring Retry-After", err)
	}
	if string(got) != `{"data":{"ok":true}}` {
		t.Fatalf("do() = %s", got)
	}
	if hits.Load() != 2 {
		t.Fatalf("hits = %d, want exactly 2 (one 429, one retry)", hits.Load())
	}
	if elapsed < time.Second {
		t.Fatalf("elapsed = %v, want >= 1s Retry-After wait", elapsed)
	}
}

// The bucket math behind the 30/min ceiling: capacity burst, then the
// configured sustained rate.
func TestTokenBucketSustainedRate(t *testing.T) {
	tb := newTokenBucket(2, 50) // burst 2, then 1 token per 20ms
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 2; i++ {
		if err := tb.wait(ctx); err != nil {
			t.Fatalf("burst wait %d: %v", i, err)
		}
	}
	if time.Since(start) > 20*time.Millisecond {
		t.Fatalf("burst of 2 should be immediate, took %v", time.Since(start))
	}
	if err := tb.wait(ctx); err != nil {
		t.Fatalf("third wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("third wait not rate-limited: total %v", elapsed)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tb.wait(cancelCtx); err == nil {
		t.Fatal("wait on a cancelled context must fail")
	}
}
