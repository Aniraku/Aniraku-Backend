package streaming

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// kiwiFixture serves token + download-list endpoints with counters.
// When aniEmpty, ani-namespaced requests get empty lists (proving the
// MAL-keyed fallback); mal-namespaced requests always serve qualities.
type kiwiFixture struct {
	server     *httptest.Server
	tokenHits  atomic.Int32
	listHits   atomic.Int32
	emptyTrack string
	aniEmpty   bool
}

func newKiwiFixture(t *testing.T) *kiwiFixture {
	t.Helper()
	f := &kiwiFixture{}
	var mux http.ServeMux
	mux.HandleFunc("/api/downloads/token/", func(w http.ResponseWriter, _ *http.Request) {
		f.tokenHits.Add(1)
		_, _ = w.Write([]byte(`{"token":"tok123","exp":9999999999,"ttl":600}`))
	})
	mux.HandleFunc("/api/downloads/", func(w http.ResponseWriter, r *http.Request) {
		f.listHits.Add(1)
		if f.emptyTrack != "" && len(r.URL.Path) >= len(f.emptyTrack) &&
			r.URL.Path[len(r.URL.Path)-len(f.emptyTrack):] == f.emptyTrack {
			_, _ = w.Write([]byte(`{"details":{},"downloads":[]}`))
			return
		}
		if f.aniEmpty && strings.Contains(r.URL.Path, "/downloads/ani/") {
			_, _ = w.Write([]byte(`{"details":{},"downloads":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"details":{},"downloads":[` +
			`{"kind":"sub","type":"download","label":"1080p","url":"https://pahe.example/v1080"},` +
			`{"kind":"sub","type":"download","label":"720p","url":"https://pahe.example/v720"},` +
			`{"kind":"sub","type":"download","label":"720p","url":"https://pahe.example/v720"}` +
			`]}`))
	})
	f.server = httptest.NewServer(&mux)
	t.Cleanup(f.server.Close)
	return f
}

func withZokoBase(t *testing.T, base string) {
	t.Helper()
	old := zokoBase
	zokoBase = base
	t.Cleanup(func() { zokoBase = old })
	// Token cache is global: clear so tests never share tokens.
	kiwiTokenMu.Lock()
	kiwiTokens = map[string]kiwiTokenEntry{}
	kiwiTokenMu.Unlock()
}

func TestFetchKiwiDownloads(t *testing.T) {
	f := newKiwiFixture(t)
	withZokoBase(t, f.server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	links := fetchKiwiDownloads(ctx, f.server.Client(), "21", 1, "sub")
	if len(links) != 2 {
		t.Fatalf("links = %+v, want 2 deduped qualities", links)
	}
	if links[0].Label != "1080p" || links[1].Label != "720p" {
		t.Fatalf("labels = %v", links)
	}
	// Token fetched once; second call reuses it (list refetches).
	links2 := fetchKiwiDownloads(ctx, f.server.Client(), "21", 1, "sub")
	if len(links2) != 2 {
		t.Fatalf("second fetch = %+v", links2)
	}
	if got := f.tokenHits.Load(); got != 1 {
		t.Fatalf("token hits = %d, want 1 (cached)", got)
	}
	if got := f.listHits.Load(); got != 2 {
		t.Fatalf("list hits = %d, want 2 (fresh lists)", got)
	}
}

func TestFetchKiwiDownloadsEmpty(t *testing.T) {
	f := newKiwiFixture(t)
	f.emptyTrack = "/dub"
	withZokoBase(t, f.server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// malID 0 skips the MAL attempt (and its network lookups) entirely.
	if links := fetchKiwiWithMAL(ctx, f.server.Client(), "21", 0, 1, "dub"); len(links) != 0 {
		t.Fatalf("empty track must yield nil, got %+v", links)
	}
}

// TestFetchKiwiMALFallback pins the Overflow case: ani-keyed empty,
// MAL-keyed serving qualities — the MAL attempt must fire and win.
func TestFetchKiwiMALFallback(t *testing.T) {
	f := newKiwiFixture(t)
	f.aniEmpty = true
	withZokoBase(t, f.server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	links := fetchKiwiWithMAL(ctx, f.server.Client(), "113417", 40746, 1, "sub")
	if len(links) != 2 {
		t.Fatalf("MAL fallback = %+v, want 2 qualities", links)
	}
	if links[0].Label != "1080p" {
		t.Fatalf("first label = %q, want 1080p", links[0].Label)
	}
}

func TestAttachKiwiDownloads(t *testing.T) {
	t.Parallel()

	mkServer := func(name, stype string, dls []core.DownloadLink) core.Server {
		return core.Server{Name: name, Sources: []core.Source{{URL: "http://x/v.m3u8", Type: stype}}, Downloads: dls}
	}
	links := []core.DownloadLink{{URL: "https://pahe.example/a", Label: "1080p"}}
	// Embed servers skipped, streams merged, existing kept first, no dups.
	in := []core.Server{
		mkServer("Yuta", "embed", nil),
		mkServer("Niko", "hls", []core.DownloadLink{{URL: "https://old.example/x", Label: "Old"}}),
		mkServer("Miru", "hls", nil),
	}
	out := attachKiwiDownloads(in, links)
	if len(out[0].Downloads) != 0 {
		t.Fatalf("embed server must stay download-free: %+v", out[0].Downloads)
	}
	if len(out[1].Downloads) != 2 || out[1].Downloads[0].URL != "https://old.example/x" {
		t.Fatalf("existing first, kiwi appended: %+v", out[1].Downloads)
	}
	if len(out[2].Downloads) != 1 {
		t.Fatalf("stream server must gain links: %+v", out[2].Downloads)
	}
	// Empty links: untouched.
	if got := attachKiwiDownloads(in, nil); len(got) != 3 {
		t.Fatalf("empty links must not touch servers")
	}
}
