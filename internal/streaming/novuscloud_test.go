package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// novuscloudFixture serves the embed page, the getSources API, and the media
// hops (master → media → segment) so every probe runs end to end.
type novuscloudFixture struct {
	server  *httptest.Server
	media   *httptest.Server
	blocked map[string]bool
	mu      sync.Mutex
}

func newNovuscloudFixture(t *testing.T) *novuscloudFixture {
	f := &novuscloudFixture{blocked: map[string]bool{}}

	// Single server serves both the embed page and the getSources API,
	// mirroring the real novuscloud.buzz where both live on the same host.
	mux := http.NewServeMux()

	// Embed page: returns HTML with a sourcesUrl token.
	mux.HandleFunc("/embed/", func(w http.ResponseWriter, r *http.Request) {
		token := "test-token-" + r.URL.Path
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, `<html><body><script>
sourcesUrl = '/stream/getSources?t=%s';
</script></body></html>`, token)
	})

	// getSources API: returns HLS source URL.
	mux.HandleFunc("/stream/getSources", func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("t")
		if token == "" {
			http.Error(w, "no token", http.StatusBadRequest)
			return
		}
		hlsURL := f.media.URL + "/master.m3u8"
		resp := novuscloudGetSourcesResponse{
			Sources: []novuscloudSource{
				{File: hlsURL, Type: "hls"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)

	// Media server: master → media playlist → segment.
	mmu := http.NewServeMux()
	mmu.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1200000\nmedia.m3u8\n")
	})
	mmu.HandleFunc("/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10,\nseg.ts\n")
	})
	mmu.HandleFunc("/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte{0x47, 0x40, 0x11, 0x10})
	})
	f.media = httptest.NewServer(mmu)
	t.Cleanup(f.media.Close)

	return f
}

func (f *novuscloudFixture) site() string {
	return f.server.URL
}

func newNovuscloudTestProvider(f *novuscloudFixture) *NovuscloudProvider {
	p := NewNovuscloudProvider(zerolog.Nop(), f.site(), "hd-1", "Nova")
	// httptest servers are loopback: the netguard transport's SSRF guard
	// would block them, so tests use a plain client (production always
	// uses the guarded one built by NewNovuscloudProvider).
	p.client = &http.Client{Timeout: 30 * time.Second}
	return p
}

func TestNovuscloudResolvesAllMirrors(t *testing.T) {
	f := newNovuscloudFixture(t)
	p := newNovuscloudTestProvider(f)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) == 0 {
		t.Fatal("expected sources from at least one mirror")
	}
	validNames := map[string]bool{
		"Nova": true, "Nebula": true,
		"Stella": true, "Lyra": true,
	}
	seen := map[string]bool{}
	for i, name := range sr.ServerNames {
		if !validNames[name] {
			t.Fatalf("server %d = %q, want one of Novus-hd1..hd4", i, name)
		}
		seen[name] = true
		if sr.Sources[i].Type != "hls" || sr.Sources[i].Verification != "proxy" {
			t.Fatalf("source %d = %+v, want a proxied HLS master", i, sr.Sources[i])
		}
		if !strings.HasPrefix(sr.Sources[i].URL, f.media.URL) {
			t.Fatalf("source %d URL = %q, want media server URL", i, sr.Sources[i].URL)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no mirrors resolved")
	}
}

func TestNovuscloudDubResolves(t *testing.T) {
	f := newNovuscloudFixture(t)
	p := newNovuscloudTestProvider(f)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "dub")
	if err != nil {
		t.Fatalf("FindEpisodeSource dub: %v", err)
	}
	if sr == nil || len(sr.Sources) == 0 {
		t.Fatal("expected dub sources")
	}
}
