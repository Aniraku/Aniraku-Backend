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
	emptyEpisodes bool
	// dubNoPlayers emulates shows whose en-US listing page carries zero
	// embeds (observed: Noblesse ep1) — strict per-lang means dub fails.
	dubNoPlayers bool
	// floatPages emulates page markers with fractional episode numbers
	// (observed 14.5) that must not break episode listing.
	floatPages bool
	// fractionalEpisode emulates result entries with fractional episode
	// numbers (observed 1004.5 on long runners).
	fractionalEpisode bool
}

func newKaaFixture(t *testing.T) *kaaFixture {
	t.Helper()
	f := &kaaFixture{t: t, masterHits: new(int64), segmentOrigin: new(string)}
	var kaaURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"slug":"naruto-f3cf","title":"Naruto","year":2002,"type":"tv","start_date":"2002-10-03"}]`)
	})
	mux.HandleFunc("/api/show/naruto-f3cf/episodes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Lang-aware watch slugs (like production): each lang page has
		// its own player page and subtitle set.
		slug := "subslug"
		if r.URL.Query().Get("lang") == "en-US" {
			slug = "dubslug"
		}
		if f.emptyEpisodes {
			fmt.Fprint(w, `{"result":[],"pages":[]}`)
			return
		}
		if f.dubNoPlayers && r.URL.Query().Get("lang") == "en-US" {
			slug = "dubempty"
		}
		pages := `[]`
		if f.floatPages {
			pages = `[{"number":1,"from":"01","to":"13","eps":[1,14.5]}]`
		}
		result := fmt.Sprintf(`[{"episode_number":1,"slug":%q,"title":"Enter"}]`, slug)
		if f.fractionalEpisode {
			// Long runners list fractional specials (observed 1004.5):
			// they must not break the listing unmarshal.
			result = `[{"episode_number":1004.5,"slug":"frac","title":"Special"},` + result[1:]
		}
		fmt.Fprintf(w, `{"result":%s,"pages":%s}`, result, pages)
	})
	mux.HandleFunc("/naruto-f3cf/ep-1-subslug", func(w http.ResponseWriter, r *http.Request) {
		player := kaaURL + "/subplayer?id=1"
		fmt.Fprintf(w, `<html>{name:"VidStreaming",shortName:"Vid",src:"%s"}</html>`, player)
	})
	mux.HandleFunc("/naruto-f3cf/ep-1-dubslug", func(w http.ResponseWriter, r *http.Request) {
		player := kaaURL + "/dubplayer?id=2"
		fmt.Fprintf(w, `<html>{name:"VidStreaming",shortName:"Vid",src:"%s"}</html>`, player)
	})
	mux.HandleFunc("/naruto-f3cf/ep-1-dubempty", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>no embeds on this page</body></html>`)
	})
	mux.HandleFunc("/subplayer", func(w http.ResponseWriter, r *http.Request) {
		master := strings.ReplaceAll(kaaURL, "/", `\/`) + `\/master.m3u8`
		fmt.Fprintf(w, `<html>"%s" "%s/vtt-sub.vtt"</html>`, master, strings.ReplaceAll(kaaURL, "/", `\/`))
	})
	mux.HandleFunc("/dubplayer", func(w http.ResponseWriter, r *http.Request) {
		master := strings.ReplaceAll(kaaURL, "/", `\/`) + `\/master.m3u8`
		fmt.Fprintf(w, `<html>"%s" "%s/vtt-dub.vtt"</html>`, master, strings.ReplaceAll(kaaURL, "/", `\/`))
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

// Full chain: search -> episodes -> watch -> player -> master source,
// with the Origin header asserted on the segment request.
func TestKaaFindEpisodeSource(t *testing.T) {
	f := newKaaFixture(t)
	p := newKaaTestProvider(f)
	sr, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	// Masters, not variants: exactly one source per player, so the media
	// proxy's al=sub/dub audio strip can run (variants carry no AUDIO).
	if len(sr.Sources) != 1 {
		t.Fatalf("sources = %d, want 1 master", len(sr.Sources))
	}
	s := sr.Sources[0]
	if s.Type != "hls" || s.Verification != "proxy" || s.Quality != "auto" {
		t.Fatalf("source = %+v, want hls/proxy/auto master", s)
	}
	if want := f.kaa.URL + "/master.m3u8"; s.URL != want {
		t.Fatalf("source URL = %q, want master %q", s.URL, want)
	}
	if sr.Headers["Origin"] != kaaKrussOrigin || sr.Headers["Referer"] != kaaKrussRef {
		t.Fatalf("headers = %v, want Referer+Origin krussdomi", sr.Headers)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Nico" {
		t.Fatalf("server names = %v, want [Nico]", sr.ServerNames)
	}
	if len(s.Subtitles) != 1 || !strings.HasSuffix(s.Subtitles[0].URL, "/vtt-sub.vtt") {
		t.Fatalf("subtitles = %+v, want the sub page vtt", s.Subtitles)
	}
	if got := *f.segmentOrigin; got != kaaKrussOrigin {
		t.Fatalf("segment Origin = %q, want %q", got, kaaKrussOrigin)
	}
}

// Strict per-lang rule: sub reads ja-JP, dub reads en-US, each resolves
// independently. The dub resolve carries its own (en-US) page files here;
// Manager.withDubSubtitles replaces them with the sub page's files.
func TestKaaStrictPerLang(t *testing.T) {
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
	// Independent upstream resolves per lang (no cross-lang cache share).
	if n := atomic.LoadInt64(f.masterHits); n != 2 {
		t.Fatalf("master fetched %d times, want 2 (one per lang)", n)
	}
	if len(dub.Sources[0].Subtitles) != 1 || !strings.HasSuffix(dub.Sources[0].Subtitles[0].URL, "/vtt-dub.vtt") {
		t.Fatalf("provider dub subtitles = %+v, want the resolving (en-US) page vtt", dub.Sources[0].Subtitles)
	}
	if len(sub.Sources[0].Subtitles) != 1 || !strings.HasSuffix(sub.Sources[0].Subtitles[0].URL, "/vtt-sub.vtt") {
		t.Fatalf("sub subtitles = %+v, want the SUB page vtt", sub.Sources[0].Subtitles)
	}
}

// No dub embeds, no dub server: an en-US page without players fails the
// dub resolve while sub keeps working (fresh provider = empty cache).
func TestKaaDubMissingWhenNoPlayers(t *testing.T) {
	f := newKaaFixture(t)
	f.dubNoPlayers = true
	p := newKaaTestProvider(f)
	if _, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "dub"); err == nil {
		t.Fatal("expected dub to fail when the en-US page has no players")
	}
	if _, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub"); err != nil {
		t.Fatalf("sub must still resolve: %v", err)
	}
}

// Fractional page markers (observed 14.5) must not break episode listing.
func TestKaaFloatPageMarkers(t *testing.T) {
	f := newKaaFixture(t)
	f.floatPages = true
	p := newKaaTestProvider(f)
	sr, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource with float page markers: %v", err)
	}
	if len(sr.Sources) != 1 {
		t.Fatalf("sources = %d, want 1", len(sr.Sources))
	}
}

// Cached resolve results are isolated copies: mutating a returned result
// (as the quality filter / proxy wrap path does with its own copies) must
// never poison the cache for later requests.
func TestKaaResolveCacheIsolation(t *testing.T) {
	f := newKaaFixture(t)
	p := newKaaTestProvider(f)
	sr, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("sub: %v", err)
	}
	sr.Sources[0].URL = "MUTATED"
	again, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("sub again: %v", err)
	}
	if again.Sources[0].URL == "MUTATED" {
		t.Fatal("cache poisoned through the previously returned slice")
	}
}

// Production CatStream shape: protocol-relative master, triple-slash srt
// subs, and a preview thumbnail track that is not a subtitle.
func TestKaaFindMastersProtocolRelative(t *testing.T) {
	txt := `"manifest":[0,"//bl.krussdomi.com/playlist/6798cdea169c31976b2d8f22/master.m3u8"],"x":"https://hls.krussdomi.com/manifest/abc/master.m3u8"`
	got := kaaFindMasters(txt)
	want := []string{
		"https://bl.krussdomi.com/playlist/6798cdea169c31976b2d8f22/master.m3u8",
		"https://hls.krussdomi.com/manifest/abc/master.m3u8",
	}
	if len(got) != len(want) {
		t.Fatalf("masters = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("masters = %v, want %v", got, want)
		}
	}
}

func TestKaaFindSubtitlesSrtAndPreview(t *testing.T) {
	txt := `"src":[0,"https:///subbl.krussdomi.com/6798cdea/309567_en.srt"],` +
		`"src":[0,"https:///subbl.krussdomi.com/6798cdea/1617262191272_vi.srt"],` +
		`"thumbnails":[0,"https:///subbl.krussdomi.com/6798cdea/preview-RCmUA.vtt"],` +
		`"x":"//cdn.example.com/subs/en.vtt"`
	got := kaaFindSubtitles(txt)
	want := []string{
		"https://subbl.krussdomi.com/6798cdea/309567_en.srt",
		"https://subbl.krussdomi.com/6798cdea/1617262191272_vi.srt",
		"https://cdn.example.com/subs/en.vtt",
	}
	if len(got) != len(want) {
		t.Fatalf("subtitles = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("subtitles = %v, want %v", got, want)
		}
	}
}

// Fractional result entries (observed 1004.5) must not break listing;
// integer episodes still resolve, fractionals never match an int request.
func TestKaaFractionalEpisodeNumber(t *testing.T) {
	f := newKaaFixture(t)
	f.fractionalEpisode = true
	p := newKaaTestProvider(f)
	sr, err := p.FindEpisodeSource(kaaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource with fractional entry: %v", err)
	}
	if len(sr.Sources) != 1 {
		t.Fatalf("sources = %d, want 1", len(sr.Sources))
	}
	eps, err := p.FindEpisodes(kaaTestCtx(t), "naruto-f3cf")
	if err != nil {
		t.Fatalf("FindEpisodes: %v", err)
	}
	if len(eps) != 1 || eps[0].Number != 1 {
		t.Fatalf("episodes = %+v, want [1]", eps)
	}
}

func TestKaaSubtitleTrack(t *testing.T) {
	for _, tc := range []struct {
		url       string
		fallback  string
		wantLang  string
		wantLabel string
	}{
		{"https://subbl.krussdomi.com/abc/309567_en.srt", "sub", "en", "English"},
		{"https://subbl.krussdomi.com/abc/60596_th.srt", "sub", "th", "Thai"},
		{"https://subbl.krussdomi.com/abc/1617262191272_vi.srt", "dub", "vi", "Vietnamese"},
		{"https://subbl.krussdomi.com/abc/1617629104889_id.srt", "sub", "id", "Indonesian"},
		{"https://subbl.krussdomi.com/abc/1617629724071_ms.srt", "sub", "ms", "Malay"},
		{"https://subbl.krussdomi.com/abc/278072_zh-Hans.srt", "sub", "zh-hans", "Chinese"},
		{"https:///subbl.krussdomi.com/abc/309567_EN.srt", "sub", "en", "English"},
		{"https://subst.krussdomi.com/abc/64b0e386970810335d81b379.vtt", "sub", "sub", "sub"},
		{"https://subst.krussdomi.com/abc/64b0e386970810335d81b379.vtt", "dub", "dub", "dub"},
	} {
		got := kaaSubtitleTrack(tc.url, tc.fallback)
		if got.URL != tc.url || got.Lang != tc.wantLang || got.Label != tc.wantLabel {
			t.Errorf("kaaSubtitleTrack(%q) = %+v, want lang=%q label=%q", tc.url, got, tc.wantLang, tc.wantLabel)
		}
	}
}

func TestKaaServerName(t *testing.T) {
	want := map[int]string{0: "Nico", 1: "robin", 2: "D'Luff", 3: "zoro", 10: "jimbei", 11: "kaa-12", 25: "kaa-26"}
	for i, w := range want {
		if got := kaaServerName(i); got != w {
			t.Fatalf("kaaServerName(%d) = %q, want %q", i, got, w)
		}
	}
}

// Empty episode list fails clean instead of hanging the chain.
func TestKaaMissingEpisode(t *testing.T) {
	f := newKaaFixture(t)
	f.emptyEpisodes = true
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
