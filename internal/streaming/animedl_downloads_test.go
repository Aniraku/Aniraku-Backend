package streaming

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// animeDlFixture serves /api/anilist/{id}/{ep} with a hit counter.
// Episodes: 1 = two release groups, sub+dub, quality ladder incl. a
// non-numeric label and a cross-group duplicate URL; 2 = 404 error JSON.
type animeDlFixture struct {
	server *httptest.Server
	hits   atomic.Int32
}

func newAnimeDlFixture(t *testing.T) *animeDlFixture {
	t.Helper()
	f := &animeDlFixture{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/anilist/", func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		switch r.URL.Path {
		case "/api/anilist/21/1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"MTBB": {
					"sub": {"download": {
						"360p": "https://animedl.to/download/mtbb360",
						"720p": "https://animedl.to/download/mtbb720",
						"1080p": "https://animedl.to/download/mtbb1080",
						"mirror": "https://animedl.to/download/mtbbmir"}},
					"dub": {"download": {
						"360p": "https://animedl.to/download/mtbb360d",
						"720p": "https://animedl.to/download/mtbb720d",
						"1080p": "https://animedl.to/download/mtbb1080d"}}
				},
				"SubsPlease": {
					"sub": {"download": {
						"720p": "https://animedl.to/download/sp720",
						"1080p": "https://animedl.to/download/mtbb1080"}}
				},
				"status": {"time": 1, "serves_from": "cache"}
			}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"No AniList entry matches that anilist id.","status":{"serves_from":"upstream"}}`))
		}
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

// withAnimeDlBase points the fetcher at a fixture and clears the shared
// cache so tests never observe each other's entries.
func withAnimeDlBase(t *testing.T, base string) {
	t.Helper()
	old := animeDlBase
	animeDlBase = base
	t.Cleanup(func() { animeDlBase = old })
	resetAnimeDlCache()
	t.Cleanup(resetAnimeDlCache)
}

func TestFetchAnimeDlDownloads(t *testing.T) {
	f := newAnimeDlFixture(t)
	withAnimeDlBase(t, f.server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// sub lane: quality-first ordering across groups (1080p, then 720p
	// ladder, then non-numeric), cross-group duplicate URL deduped.
	links := fetchAnimeDlDownloads(ctx, f.server.Client(), "21", 1, "sub")
	wantLabels := []string{"1080p", "720p", "720p", "360p", "mirror"}
	wantURLs := []string{
		"https://animedl.to/download/mtbb1080",
		"https://animedl.to/download/mtbb720",
		"https://animedl.to/download/sp720",
		"https://animedl.to/download/mtbb360",
		"https://animedl.to/download/mtbbmir",
	}
	if len(links) != len(wantLabels) {
		t.Fatalf("sub links = %+v, want %d entries", links, len(wantLabels))
	}
	for i := range links {
		if links[i].Label != wantLabels[i] || links[i].URL != wantURLs[i] {
			t.Fatalf("sub[%d] = %+v, want label=%q url=%q", i, links[i], wantLabels[i], wantURLs[i])
		}
	}

	// dub lane: only its own track.
	dub := fetchAnimeDlDownloads(ctx, f.server.Client(), "21", 1, "dub")
	if len(dub) != 3 || dub[0].Label != "1080p" || dub[2].Label != "360p" {
		t.Fatalf("dub links = %+v, want 360p..1080p dub ladder", dub)
	}
	for _, d := range dub {
		if d.URL[len(d.URL)-1] != 'd' {
			t.Fatalf("dub lane leaked non-dub URL: %+v", d)
		}
	}

	// Cached: both lane reads cost one upstream fetch total.
	if got := f.hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1 (sub+dub from one cached fetch)", got)
	}
	if again := fetchAnimeDlDownloads(ctx, f.server.Client(), "21", 1, "sub"); len(again) != len(links) {
		t.Fatalf("cached refetch = %+v", again)
	}
	if got := f.hits.Load(); got != 1 {
		t.Fatalf("upstream hits after cached read = %d, want 1", got)
	}
}

// TestFetchAnimeDlDownloadsMiss pins the graceful-empty rule: a 404/error
// response yields nil and is negatively cached, so a hot page does not
// hammer their flapping upstream.
func TestFetchAnimeDlDownloadsMiss(t *testing.T) {
	f := newAnimeDlFixture(t)
	withAnimeDlBase(t, f.server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if links := fetchAnimeDlDownloads(ctx, f.server.Client(), "999", 1, "sub"); links != nil {
		t.Fatalf("miss must return nil, got %+v", links)
	}
	if got := f.hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1", got)
	}
	// Negative cache: immediate retry costs no upstream call.
	if links := fetchAnimeDlDownloads(ctx, f.server.Client(), "999", 1, "dub"); links != nil {
		t.Fatalf("negative-cached miss must return nil, got %+v", links)
	}
	if got := f.hits.Load(); got != 1 {
		t.Fatalf("upstream hits after negative-cached read = %d, want 1", got)
	}
	// Empty AniList ID never dials upstream.
	if links := fetchAnimeDlDownloads(ctx, f.server.Client(), "", 1, "sub"); links != nil {
		t.Fatalf("empty id must return nil, got %+v", links)
	}
	if got := f.hits.Load(); got != 1 {
		t.Fatalf("upstream hits after empty-id short circuit = %d, want 1", got)
	}
}

func TestFetchAnimeDlDownloadsUnreachable(t *testing.T) {
	withAnimeDlBase(t, "http://127.0.0.1:1") // closed port: instant dial error
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if links := fetchAnimeDlDownloads(ctx, http.DefaultClient, "21", 1, "sub"); links != nil {
		t.Fatalf("unreachable upstream must return nil, got %+v", links)
	}
}

func TestQualityNum(t *testing.T) {
	t.Parallel()
	cases := map[string]int{"1080p": 1080, "800p": 800, "1080p mirror": 1080, "360p": 360, "mirror": -1, "Kiwi-Stream": -1, "": -1}
	for label, want := range cases {
		if got := qualityNum(label); got != want {
			t.Errorf("qualityNum(%q) = %d, want %d", label, got, want)
		}
	}
}

func TestAttachDownloadLinks(t *testing.T) {
	t.Parallel()

	mk := func(name, provider, stype string, dls []core.DownloadLink) core.Server {
		return core.Server{Name: name, Provider: provider,
			Sources: []core.Source{{URL: "http://x/v.m3u8", Type: stype}}, Downloads: dls}
	}
	links := []core.DownloadLink{{URL: "https://pahe.example/a", Label: "1080p"}}
	// Only FlixCloud is excluded; an embed-typed server from any other
	// provider receives the links ("all sources without the FlixCloud").
	in := []core.Server{
		mk("Yuta", "flixcloud", "embed", nil),
		mk("Niko", "anikoto", "embed", nil),
		mk("Zoko", "zoko", "hls", []core.DownloadLink{{URL: "https://old.example/x", Label: "Old"}}),
		mk("Miru", "miruro", "hls", nil),
	}
	out := attachDownloadLinks(in, links)
	if len(out[0].Downloads) != 0 {
		t.Fatalf("flixcloud must stay download-free: %+v", out[0].Downloads)
	}
	if len(out[1].Downloads) != 1 {
		t.Fatalf("non-flixcloud embed must gain links: %+v", out[1].Downloads)
	}
	if len(out[2].Downloads) != 2 || out[2].Downloads[0].URL != "https://old.example/x" {
		t.Fatalf("provider links stay first, animedl appended: %+v", out[2].Downloads)
	}
	if len(out[3].Downloads) != 1 {
		t.Fatalf("stream server must gain links: %+v", out[3].Downloads)
	}
	// Empty links: untouched.
	if got := attachDownloadLinks(in, nil); len(got) != 4 {
		t.Fatalf("empty links must not touch servers")
	}
}
