package streaming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNeedsWorkerRetry(t *testing.T) {
	t.Parallel()

	respWith := func(status int) *http.Response {
		return &http.Response{StatusCode: status}
	}
	cases := []struct {
		name string
		resp *http.Response
		err  error
		want bool
	}{
		{name: "429", resp: respWith(429), want: true},
		{name: "403", resp: respWith(403), want: true},
		{name: "500", resp: respWith(500), want: true},
		{name: "502", resp: respWith(502), want: true},
		{name: "503", resp: respWith(503), want: true},
		{name: "504", resp: respWith(504), want: true},
		{name: "520", resp: respWith(520), want: true},
		{name: "524", resp: respWith(524), want: true},
		{name: "200", resp: respWith(200), want: false},
		{name: "404", resp: respWith(404), want: false},
		{name: "transport error", err: fmt.Errorf("connection reset"), want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := needsWorkerRetry(tc.resp, tc.err); got != tc.want {
				t.Fatalf("needsWorkerRetry(status=%v, err=%v) = %v, want %v",
					statusOf(tc.resp), tc.err, got, tc.want)
			}
		})
	}
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func TestMegaplayWorkerBaseFromEnv(t *testing.T) {
	t.Setenv("ANIRAKU_MEGAPLAY_WORKER", "https://w.example/proxy")
	if got := megaplayWorkerBaseFromEnv(); got != "https://w.example/proxy" {
		t.Fatalf("env override = %q", got)
	}
}

// TestFetchWithWorkerFallback pins the fallback contract against fake
// servers: direct success never touches the worker; direct CF-failure
// retries once through it; worker failure returns the original outcome.
func TestFetchWithWorkerFallback(t *testing.T) {
	var workerHits atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workerHits.Add(1)
		if r.URL.Query().Get("url") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"proxied":true}`))
	}))
	defer worker.Close()
	oldBase := megaplayWorkerBase
	megaplayWorkerBase = worker.URL
	t.Cleanup(func() { megaplayWorkerBase = oldBase })

	newReq := func(t *testing.T, url string) *http.Request {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		return req
	}
	client := worker.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("direct success skips worker", func(t *testing.T) {
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"direct":true}`))
		}))
		defer origin.Close()
		before := workerHits.Load()
		resp, err := fetchWithWorkerFallback(ctx, client, newReq(t, origin.URL+"/x"))
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if got := workerHits.Load(); got != before {
			t.Fatalf("worker hits moved %d -> %d on direct success", before, got)
		}
	})

	t.Run("CF failure retries through worker", func(t *testing.T) {
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`blocked`))
		}))
		defer origin.Close()
		before := workerHits.Load()
		resp, err := fetchWithWorkerFallback(ctx, client, newReq(t, origin.URL+"/x"))
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d, want worker 200", resp.StatusCode)
		}
		if got := workerHits.Load(); got != before+1 {
			t.Fatalf("worker hits moved %d -> %d, want exactly one retry", before, got)
		}
	})

	t.Run("worker failure preserves original", func(t *testing.T) {
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`denied`))
		}))
		defer origin.Close()
		megaplayWorkerBase = "http://127.0.0.1:1/"
		defer func() { megaplayWorkerBase = worker.URL }()
		resp, err := fetchWithWorkerFallback(ctx, client, newReq(t, origin.URL+"/x"))
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		defer resp.Body.Close()
		// Worker unreachable: the original 403 must come back, not a
		// worker transport error.
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want original 403", resp.StatusCode)
		}
	})
}
