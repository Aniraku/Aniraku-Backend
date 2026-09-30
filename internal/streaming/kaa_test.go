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
)

func kaaTestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type kaaFixture struct {
	t             *testing.T
	kaa           *httptest.Server
	anilist       *httptest.Server
	masterHits    *int64
	segmentOrigin *string
	episodesBody  string
}

func newKaaFixture(t *testing.T) *kaaFixture {
	t.Helper()
	f := &kaaFixture{t: t, masterHits: new(int64), segmentOrigin: new(string),
		episodesBody: `{"result":[{"episode_number":1,"slug":"2da064","title":"Enter"}],"pages":[]}`}
	var kaaURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"slug":"naruto-f3cf","title":"Naruto","year":2002,"type":"tv","start_date":"2002-10-03"}]`)
	})
	mux.HandleFunc("/api/show/naruto-f3cf/episodes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, f.episodesBody)
	})
	mux.HandleFunc("/naruto-f3cf/ep-1-2da064", func(w http.ResponseWriter, r *http.Request) {
		player := kaaURL + "/cat-player/player?id=abc&source=vidstream&ln=ja-JP"
		fmt.Fprintf(w, `<html>{name:"VidStreaming",shortName:"Vid",src:"%s"}</html>`, player)
	})
	mux.HandleFunc("/cat-player/player", func(w http.ResponseWriter, r *http.Request) {
		master := strings.ReplaceAll(kaaURL, "/", `\/`) + `\/master.m3u8`
		vtt := strings.ReplaceAll(kaaURL, "/", `\/`) + `\/en.vtt`
		fmt.Fprintf(w, `<html>"%s" "%s"</html>`, master, vtt)
	})
	mux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(f.masterHits, 1)
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=3543681,RESOLUTION=1280x720\nq720/playlist.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=1032414,RESOLUTION=640x360\nq360/playlist.m3u8\n")
	})
	mux.HandleFunc("/q720/playlist.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg-1.jpg\n")
	})
	mux.HandleFunc("/q360/playlist.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg-1.jpg\n")
	})
	mux.HandleFunc("/q720/seg-1.jpg", func(w http.ResponseWriter, r *http.Request) {
		*f.segmentOrigin = r.Header.Get("Origin")
		w.Header().Set("Content-Type", "image/jpeg")
		w.Write(append([]byte{0x47}, bytes_1k...))
	})
	f.kaa = httptest.NewServer(mux)
	kaaURL = f.kaa.URL
	amux := http.NewServeMux()
	amux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"Media":{"title":{"romaji":"Naruto","english":"Naruto"},"startDate":{"year":2002}}}}`)
	})
	f.anilist = httptest.NewServer(amux)
	t.Cleanup(func() { f.kaa.Close(); f.anilist.Close() })
	return f
}

var bytes_1k = make([]byte, 1023)

func newKaaTestProvider(f *kaaFixture) *KaaProvider {
	p := NewKaaProvider(zerolog.Nop(), f.kaa.URL, f.anilist.URL)
	// httptest servers are loopback: the netguard transport's SSRF guard
	// would block them, so tests use a plain client (production always
	// uses the guarded one built by NewKaaProvider).
	p.client = &http.Client{Timeout: 30 * time.Second}
	return p
}

// Full chain: search -> episodes -> watch -> player -> master -> variants,
// with the Origin header asserted on the segment request.
func TestKaaFindEpisodeSource(t *testing.T) {
	f := newKaaFixture(t)
	p := newKaaTestProvider(f)
	sr, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if len(sr.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 variants", len(sr.Sources))
	}
	if sr.Sources[0].Quality != "720p" || sr.Sources[1].Quality != "360p" {
		t.Fatalf("qualities = %q/%q, want 720p/360p", sr.Sources[0].Quality, sr.Sources[1].Quality)
	}
	for _, s := range sr.Sources {
		if s.Type != "hls" || s.Verification != "proxy" {
			t.Fatalf("source = %+v, want hls/proxy", s)
		}
		if !strings.HasPrefix(s.URL, f.kaa.URL+"/q") || !strings.HasSuffix(s.URL, "playlist.m3u8") {
			t.Fatalf("variant URL not resolved absolute: %q", s.URL)
		}
	}
	if sr.Headers["Origin"] != kaaKrussOrigin || sr.Headers["Referer"] != kaaKrussRef {
		t.Fatalf("headers = %v, want Referer+Origin krussdomi", sr.Headers)
	}
	if len(sr.ServerNames) != 2 || sr.ServerNames[0] != "VidStreaming" {
		t.Fatalf("server names = %v, want VidStreaming", sr.ServerNames)
	}
	if len(sr.Sources[0].Subtitles) != 1 || !strings.HasSuffix(sr.Sources[0].Subtitles[0].URL, "/en.vtt") {
		t.Fatalf("subtitles = %+v, want the vtt link", sr.Sources[0].Subtitles)
	}
	if got := *f.segmentOrigin; got != kaaKrussOrigin {
		t.Fatalf("segment Origin = %q, want %q", got, kaaKrussOrigin)
	}
}

// Dual-language rule: sub then dub must fetch the master exactly once.
func TestKaaDualLangShared(t *testing.T) {
	f := newKaaFixture(t)
	p := newKaaTestProvider(f)
	sub, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("sub: %v", err)
	}
	dub, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "dub")
	if err != nil {
		t.Fatalf("dub: %v", err)
	}
	if sub.Sources[0].URL != dub.Sources[0].URL {
		t.Fatalf("sub/dub URLs differ: %q vs %q", sub.Sources[0].URL, dub.Sources[0].URL)
	}
	if n := atomic.LoadInt64(f.masterHits); n != 1 {
		t.Fatalf("master fetched %d times, want 1 (shared across langs)", n)
	}
}

// Empty episode list fails clean instead of hanging the chain.
func TestKaaMissingEpisode(t *testing.T) {
	f := newKaaFixture(t)
	f.episodesBody = `{"result":[],"pages":[]}`
	p := newKaaTestProvider(f)
	if _, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 99, "sub"); err == nil {
		t.Fatal("expected error for unlisted episode")
	}
}

func TestKaaParseVariants(t *testing.T) {
	body := "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100,RESOLUTION=640x360\n//cdn.example.com/q/playlist.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=300,RESOLUTION=1280x720\nrel/playlist.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=50\nplain.m3u8\n"
	got := kaaParseVariants(body, "https://hls.example.com/m/master.m3u8")
	if len(got) != 3 {
		t.Fatalf("variants = %d, want 3", len(got))
	}
	// Sorted by bandwidth desc: 720p (relative) first, 360p
	// (protocol-relative) second, auto last.
	if got[0].quality != "720p" || got[1].quality != "360p" || got[2].quality != "auto" {
		t.Fatalf("order/labels = %v", got)
	}
	if got[1].url != "https://cdn.example.com/q/playlist.m3u8" {
		t.Fatalf("protocol-relative not resolved: %q", got[1].url)
	}
	if got[0].url != "https://hls.example.com/m/rel/playlist.m3u8" {
		t.Fatalf("relative not resolved: %q", got[0].url)
	}
}
