package streaming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// When megaplay.buzz rate-limits this egress, every edge variant would 429
// identically (same host, same limiter, milliseconds apart). The variant
// loop must fail fast on the first 429 instead of burning ~3 requests per
// remaining variant on a verdict that's already known.
func TestResolveMegaPlayFailFastOn429(t *testing.T) {
	var embedHits atomic.Int32
	var sourcesHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/stream/getSources") {
			sourcesHits.Add(1)
		} else {
			embedHits.Add(1)
		}
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := server.Client()

	// No ?s= variant -> variants ["", "tcdn", "bcdn"]: 3 embed fetches
	// without the fail-fast, exactly 1 with it.
	_, _, _, _, _, _, err := resolveMegaPlayPlayable(ctx, client, server.URL+"/stream/ani/1/1/sub", nil)
	if err == nil {
		t.Fatal("expected the 429 error back, got nil")
	}
	if !isMegaPlayRateLimited(err) {
		t.Fatalf("error should be recognized as rate-limited, got: %v", err)
	}
	if got := embedHits.Load(); got != 1 {
		t.Fatalf("embed page hits = %d, want exactly 1 (fail fast on 429)", got)
	}
	if got := sourcesHits.Load(); got != 0 {
		t.Fatalf("getSources hits = %d, want 0 (never reached past the 429)", got)
	}
}

func TestIsMegaPlayRateLimited(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "embed 429", err: errTestStr("embed page returned HTTP 429: <html>"), want: true},
		{name: "too many requests", err: errTestStr("Too Many Requests."), want: true},
		{name: "generic failure", err: errTestStr("embed returned no file"), want: false},
		{name: "missing id", err: errTestStr("embed file id not found"), want: false},
		{name: "decrypt failure", err: errTestStr("embed enc decrypt failed: bad key"), want: false},
		{name: "transport error", err: errTestStr(`Get "https://x": connection refused`), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMegaPlayRateLimited(tc.err); got != tc.want {
				t.Fatalf("isMegaPlayRateLimited(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

type errTestStr string

func (e errTestStr) Error() string { return string(e) }

func TestIsMegaPlayMissing(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "no data-id", err: errTestStr("embed file id not found"), want: true},
		{name: "wrapped missing", err: errTestStr("megaplay direct: embed file id not found"), want: true},
		{name: "rate limit is not missing", err: errTestStr("embed page returned HTTP 429: <html>"), want: false},
		{name: "decrypt failure is not missing", err: errTestStr("embed enc decrypt failed: bad key"), want: false},
		{name: "empty file is not missing", err: errTestStr("embed returned no file"), want: false},
		{name: "blocked edge is not missing", err: errTestStr("CDN blocked manifest"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMegaPlayMissing(tc.err); got != tc.want {
				t.Fatalf("isMegaPlayMissing(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// The MAL-keyed attempt fires when the title is not carried ani-keyed —
// and never when the host is rate-limiting us (same limiter). Now covered
// through megaplayDirectKeys below; the routing rule lives in its gate
// (malID > 0 && !isMegaPlayRateLimited(aniErr)).

// megaplayTestServer serves ani/mal pages plus a getSourcesNew that maps
// data-ids to files, so megaplayDirectKeys runs hermetically (megaplayBase
// override). Files served under /f/<name>.m3u8 carry #EXTM3U.
func megaplayTestServer(files map[string]string, ani, mal map[string]string, ani429 bool) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
	var aniHits, malHits atomic.Int32
	var mux http.ServeMux
	page := func(id string) string {
		if id == "" {
			return `<html><body>not found</body></html>`
		}
		return `<html><body><div data-id="` + id + `"></div></body></html>`
	}
	mux.HandleFunc("/stream/ani/", func(w http.ResponseWriter, r *http.Request) {
		aniHits.Add(1)
		if ani429 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		}
		_, _ = w.Write([]byte(page(ani[r.URL.Path])))
	})
	mux.HandleFunc("/stream/mal/", func(w http.ResponseWriter, r *http.Request) {
		malHits.Add(1)
		_, _ = w.Write([]byte(page(mal[r.URL.Path])))
	})
	mux.HandleFunc("/stream/getSourcesNew", func(w http.ResponseWriter, r *http.Request) {
		f, ok := files[r.URL.Query().Get("id")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		host := "http://" + r.Host
		_, _ = fmt.Fprintf(w, `{"sources":{"file":%q},"tracks":[]}`, host+"/f/"+f)
	})
	mux.HandleFunc("/stream/getSources", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"sources":{},"tracks":[]}`))
	})
	mux.HandleFunc("/f/", func(w http.ResponseWriter, r *http.Request) {
		// Playable chain for the segment-depth probe: <name>.m3u8 acts as
		// master -> media.m3u8 -> seg.ts (TS bytes), all relative like
		// real CDN layouts.
		p := strings.TrimPrefix(r.URL.Path, "/f/")
		switch {
		case p == "media.m3u8":
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1\nseg.ts\n"))
		case p == "seg.ts":
			_, _ = w.Write(append([]byte{0x47}, []byte("video-segment-bytes-ok")...))
		case strings.HasSuffix(p, ".m3u8"):
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nmedia.m3u8\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	return httptest.NewServer(&mux), &aniHits, &malHits
}

func withMegaplayBase(t *testing.T, base string) {
	t.Helper()
	old := megaplayBase
	megaplayBase = base
	t.Cleanup(func() { megaplayBase = old })
}

// Both keys resolve different files: Niko (ani) + Momo (mal) with slot
// names that survive into the server list.
func TestMegaplayDirectKeysNikoMomo(t *testing.T) {
	server, _, _ := megaplayTestServer(
		map[string]string{"1111": "a.m3u8", "2222": "b.m3u8"},
		map[string]string{"/stream/ani/7/1/sub": "1111"},
		map[string]string{"/stream/mal/8/1/sub": "2222"},
		false,
	)
	defer server.Close()
	withMegaplayBase(t, server.URL)

	p := NewAnikotoProvider(zerolog.Nop())
	p.client = server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.megaplayDirectKeys(ctx, "7", 8, 1, "sub")
	if err != nil {
		t.Fatalf("keys resolve: %v", err)
	}
	if len(sr.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 (Niko + Momo)", len(sr.Sources))
	}
	if len(sr.ServerNames) != 2 || sr.ServerNames[0] != "Niko" || sr.ServerNames[1] != "Momo" {
		t.Fatalf("ServerNames = %v, want [Niko Momo]", sr.ServerNames)
	}
	if !strings.HasSuffix(sr.Sources[0].URL, "/f/a.m3u8") || !strings.HasSuffix(sr.Sources[1].URL, "/f/b.m3u8") {
		t.Fatalf("unexpected source URLs: %v", sr.Sources)
	}
}

// Same file on both keys collapses to Niko only — no duplicate slot.
func TestMegaplayDirectKeysSameFile(t *testing.T) {
	server, _, _ := megaplayTestServer(
		map[string]string{"1111": "a.m3u8", "2222": "a.m3u8"},
		map[string]string{"/stream/ani/7/1/sub": "1111"},
		map[string]string{"/stream/mal/8/1/sub": "2222"},
		false,
	)
	defer server.Close()
	withMegaplayBase(t, server.URL)

	p := NewAnikotoProvider(zerolog.Nop())
	p.client = server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.megaplayDirectKeys(ctx, "7", 8, 1, "sub")
	if err != nil {
		t.Fatalf("keys resolve: %v", err)
	}
	if len(sr.Sources) != 1 || len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Niko" {
		t.Fatalf("sources=%d names=%v, want 1 source named Niko", len(sr.Sources), sr.ServerNames)
	}
}

// Ani-keyed miss routes to MAL (Momo-only result keeps its slot name),
// and a 429 on ani skips the MAL page entirely (same limiter).
func TestMegaplayDirectKeysMalOnlyAnd429Skip(t *testing.T) {
	t.Run("mal only", func(t *testing.T) {
		server, _, malHits := megaplayTestServer(
			map[string]string{"2222": "b.m3u8"},
			map[string]string{},
			map[string]string{"/stream/mal/8/1/sub": "2222"},
			false,
		)
		defer server.Close()
		withMegaplayBase(t, server.URL)

		p := NewAnikotoProvider(zerolog.Nop())
		p.client = server.Client()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		sr, err := p.megaplayDirectKeys(ctx, "7", 8, 1, "sub")
		if err != nil {
			t.Fatalf("mal resolve: %v", err)
		}
		if len(sr.Sources) != 1 || len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Momo" {
			t.Fatalf("sources=%d names=%v, want 1 source named Momo", len(sr.Sources), sr.ServerNames)
		}
		if got := malHits.Load(); got != 1 {
			t.Fatalf("mal page hits = %d, want 1", got)
		}
	})

	t.Run("ani 429 skips mal", func(t *testing.T) {
		server, _, malHits := megaplayTestServer(nil, nil, nil, true)
		defer server.Close()
		withMegaplayBase(t, server.URL)

		p := NewAnikotoProvider(zerolog.Nop())
		p.client = server.Client()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		sr, err := p.megaplayDirectKeys(ctx, "7", 8, 1, "sub")
		if err == nil || sr != nil {
			t.Fatalf("want the ani 429 back, got sr=%v err=%v", sr, err)
		}
		if !isMegaPlayRateLimited(err) {
			t.Fatalf("error should read as rate-limited, got: %v", err)
		}
		if got := malHits.Load(); got != 0 {
			t.Fatalf("mal page hits = %d, want 0 (same limiter, skip)", got)
		}
	})
}

// ServerNames preference is covered through the Keys tests above; the
// per-URL resolver path is exercised there too.

// TestAnikotoStaleCache pins the flap guard: last-good results serve
// while fresh and refuse when expired.
func TestAnikotoStaleCache(t *testing.T) {
	t.Parallel()

	mkRes := func(url string) *SourceResult {
		return &SourceResult{
			Sources:     []core.Source{{URL: url}},
			ServerNames: []string{"Niko"},
		}
	}
	t.Run("store then serve", func(t *testing.T) {
		t.Parallel()
		p := NewAnikotoProvider(zerolog.Nop())
		p.storeAnikotoStale("k", mkRes("http://x/a.m3u8"))
		got := p.loadAnikotoStale("k")
		if got == nil || len(got.Sources) != 1 || got.Sources[0].URL != "http://x/a.m3u8" {
			t.Fatalf("loadAnikotoStale = %+v", got)
		}
	})
	t.Run("expired refuses", func(t *testing.T) {
		t.Parallel()
		p := NewAnikotoProvider(zerolog.Nop())
		p.staleMu.Lock()
		p.stale["k"] = &animexStaleEntry{res: mkRes("http://x/a.m3u8"), fetchedAt: time.Now().Add(-animexStaleTTL - time.Minute)}
		p.staleMu.Unlock()
		if got := p.loadAnikotoStale("k"); got != nil {
			t.Fatalf("expired entry must refuse, got %+v", got)
		}
	})
	t.Run("missing is nil", func(t *testing.T) {
		t.Parallel()
		p := NewAnikotoProvider(zerolog.Nop())
		if got := p.loadAnikotoStale("nope"); got != nil {
			t.Fatalf("missing entry must be nil, got %+v", got)
		}
	})
}
