package v1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAniListClientServesStaleCacheAfterRateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer server.Close()

	h := &Handlers{}
	client := newAnilistClient(h)
	client.client = server.Client()
	client.endpoint = server.URL
	client.maxRetries = 0
	query := "query { Media { id } }"
	variables := map[string]any{"id": 130298}
	want := []byte(`{"data":{"Media":{"id":130298}}}`)
	client.cache.Store(client.getCacheKey(query, variables), anilistCacheEntry{
		data:      want,
		fetchedAt: time.Now().Add(-10 * time.Minute),
	})

	got, err := client.do(context.Background(), query, variables)
	if err != nil {
		t.Fatalf("do() error = %v, want stale response", err)
	}
	if string(got) != string(want) {
		t.Fatalf("do() = %s, want %s", got, want)
	}
}
