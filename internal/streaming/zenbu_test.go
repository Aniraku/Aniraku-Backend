package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// zenbuFixture serves the index shape and the media hops it points at, so
// every probe runs against the same three requests a real resolve costs:
// master -> child playlist -> segment.
type zenbuFixture struct {
	server *httptest.Server

	mu      sync.Mutex
	blocked map[string]bool

	noDub    atomic.Bool
	indexHit atomic.Int64
}

func newZenbuFixture(t *testing.T) *zenbuFixture {
	t.Helper()
	f := &zenbuFixture{blocked: map[string]bool{}}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *zenbuFixture) mediaURL(name, file string) string {
	return fmt.Sprintf("%s/hls/%s/%s", f.server.URL, name, file)
}

func (f *zenbuFixture) setBlocked(masterURL string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocked[masterURL] = true
}

func (f *zenbuFixture) isBlocked(masterURL string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blocked[masterURL]
}

func (f *zenbuFixture) handle(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/episodes/") {
		f.indexHit.Add(1)
		f.serveIndex(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/hls/") {
		switch {
		case strings.HasSuffix(r.URL.Path, "/master.m3u8"):
			master := f.server.URL + r.URL.Path
			if f.isBlocked(master) {
				http.Error(w, "blocked", http.StatusForbidden)
				return
			}
			fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1200000\nv.m3u8\n")
		case strings.HasSuffix(r.URL.Path, "/v.m3u8"):
			fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10,\nseg.ts\n")
		case strings.HasSuffix(r.URL.Path, "/seg.ts"):
			_, _ = w.Write([]byte{0x47, 0x40, 0x11, 0x10})
		default:
			http.NotFound(w, r)
		}
		return
	}
	http.NotFound(w, r)
}

func (f *zenbuFixture) serveIndex(w http.ResponseWriter, r *http.Request) {
	providerID := r.URL.Query().Get("provider")
	subtitle := func(slug string) []zenbuSubtitle {
		return []zenbuSubtitle{
			{URL: f.mediaURL(slug, "eng.vtt"), Label: "English", Kind: "captions"},
			{URL: f.mediaURL(slug, "thumbnails.vtt"), Kind: "captions"},
		}
	}
	makeEntry := func(audio, slug string) zenbuSource {
		return zenbuSource{
			ProviderID: providerID,
			Resolution: "auto",
			Format:     "hls_m3u8",
			URL:        f.mediaURL(slug, "master.m3u8"),
			Audio:      audio,
			Subtitles:  subtitle(slug),
		}
	}

	var data []zenbuSource
	switch providerID {
	case zenbuPrimaryProvider:
		data = append(data, makeEntry("sub", "shorekeeper-a"), makeEntry("sub", "shorekeeper-b"))
		if !f.noDub.Load() {
			data = append(data, makeEntry("dub", "shorekeeper-a"), makeEntry("dub", "shorekeeper-b"))
		}
	case zenbuMirrorProvider:
		for _, slug := range []string{"encore-a", "encore-b", "encore-c", "encore-d"} {
			data = append(data, makeEntry("sub", slug))
			if !f.noDub.Load() {
				data = append(data, makeEntry("dub", slug))
			}
		}
	case zenbuSubProxyDefault, zenbuSubAltProvider:
		data = append(data, makeEntry("sub", providerID+"-a"), makeEntry("sub", providerID+"-b"))
	default:
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(zenbuResponse{Success: true, Data: data})
}

func newZenbuTestProvider(f *zenbuFixture) *ZenbuProvider {
	p := NewZenbuProvider(zerolog.Nop(), f.server.URL, "https://zen-bu.test")
	// httptest servers are loopback: the netguard transport's SSRF guard
	// would block them, so tests use a plain client (production always
	// uses the guarded one built by NewZenbuProvider).
	p.client = &http.Client{Timeout: 30 * time.Second}
	return p
}

func zenbuTestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestZenbuListsEveryPlayablePool(t *testing.T) {
	f := newZenbuFixture(t)
	p := newZenbuTestProvider(f)

	sr, err := p.FindEpisodeSource(zenbuTestCtx(t), "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	want := []string{"Yuzu", "Ume", "Ringo", "Budo", "Kaki", "Kuri", "Biwa", "Ichijiku", "Melon", "Suika"}
	if sr == nil || len(sr.Sources) != len(want) {
		t.Fatalf("sources = %+v, want %d named streams", sr, len(want))
	}
	for i, name := range want {
		if sr.ServerNames[i] != name {
			t.Fatalf("server %d = %q, want %q (names: %v)", i, sr.ServerNames[i], name, sr.ServerNames)
		}
		if sr.Sources[i].Type != "hls" || sr.Sources[i].Verification != "proxy" {
			t.Fatalf("source %d = %+v, want a proxied HLS master", i, sr.Sources[i])
		}
		if len(sr.Sources[i].Subtitles) != 1 || sr.Sources[i].Subtitles[0].Label != "English" {
			t.Fatalf("subtitles %d = %+v, want the English caption track only", i, sr.Sources[i].Subtitles)
		}
	}
}

func TestZenbuDubUsesOnlyDubPools(t *testing.T) {
	f := newZenbuFixture(t)
	p := newZenbuTestProvider(f)

	sr, err := p.FindEpisodeSource(zenbuTestCtx(t), "21", 1, "dub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	want := []string{"Yuzu", "Ume", "Ringo", "Budo", "Kaki", "Kuri"}
	if sr == nil || len(sr.Sources) != len(want) {
		t.Fatalf("dub sources = %+v, want %d", sr, len(want))
	}
	for i, name := range want {
		if sr.ServerNames[i] != name {
			t.Fatalf("dub server %d = %q, want %q", i, sr.ServerNames[i], name)
		}
	}
}

func TestZenbuDubMissingStaysAbsent(t *testing.T) {
	f := newZenbuFixture(t)
	f.noDub.Store(true)
	p := newZenbuTestProvider(f)

	sr, err := p.FindEpisodeSource(zenbuTestCtx(t), "21", 1, "dub")
	if err != nil {
		t.Fatalf("a title without dub entries must be a logical miss: %v", err)
	}
	if sr != nil {
		t.Fatalf("dub sources = %+v, want none (never borrow the sub master)", sr)
	}
}

func TestZenbuDropsBlockedMirror(t *testing.T) {
	f := newZenbuFixture(t)
	f.setBlocked(f.mediaURL("shorekeeper-a", "master.m3u8"))
	p := newZenbuTestProvider(f)

	sr, err := p.FindEpisodeSource(zenbuTestCtx(t), "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 9 {
		t.Fatalf("sources = %+v, want the nine reachable slots", sr)
	}
	for _, name := range sr.ServerNames {
		if name == "Yuzu" {
			t.Fatal("blocked mirror stayed listed")
		}
	}
}

func TestZenbuIndexFailureSurfaces(t *testing.T) {
	f := newZenbuFixture(t)
	p := NewZenbuProvider(zerolog.Nop(), "http://127.0.0.1:1", "https://zen-bu.test")

	if _, err := p.FindEpisodeSource(zenbuTestCtx(t), "21", 1, "sub"); err == nil {
		t.Fatal("all-pool transport failure must surface as an error")
	}
	_ = f
}
