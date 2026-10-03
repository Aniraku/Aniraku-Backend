package streaming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

func TestMegaVidFileKey(t *testing.T) {
	a := megavidFileKey("https://fetch.nexabloom.top/anime/ab/cd/master.m3u8?token=XYZ")
	b := megavidFileKey("https://fetch.nexabloom.top/anime/ab/cd/master.m3u8?token=OTHER&x=1")
	if a != b || !strings.HasSuffix(a, "/master.m3u8") {
		t.Fatalf("token stripping failed: %q vs %q", a, b)
	}
	c := megavidFileKey("https://HLS.DRAMAHOT.TOP/v/x/master.m3u8")
	if !strings.HasPrefix(c, "https://hls.dramahot.top/") {
		t.Fatalf("host not lowercased: %q", c)
	}
}

func TestMegaVidVerdict(t *testing.T) {
	sub := map[string]bool{"s1": true}
	dub := map[string]bool{"d1": true}
	if c, s := megavidVerdict("s1", sub, dub); !c || s {
		t.Errorf("same-lang file must confirm, got confirmed=%v swapped=%v", c, s)
	}
	if c, s := megavidVerdict("d1", sub, dub); c || !s {
		t.Errorf("other-lang file must flag swap, got confirmed=%v swapped=%v", c, s)
	}
	if c, s := megavidVerdict("xx", sub, dub); c || s {
		t.Errorf("unknown file must stay unknown, got confirmed=%v swapped=%v", c, s)
	}
	// Identical files both langs (dual-audio single encode): indistinguishable.
	same := map[string]bool{"only": true}
	if c, s := megavidVerdict("only", same, same); !c || s {
		t.Errorf("indistinguishable file must confirm, got confirmed=%v swapped=%v", c, s)
	}
}

// megavidFixture serves the /source JSON (animex-encoded master URL) plus
// an HLS chain with real playlist/segment bytes.
type megavidFixture struct {
	server *httptest.Server
	base   string
}

func newMegaVidFixture(t *testing.T) *megavidFixture {
	t.Helper()
	var base string
	mux := http.NewServeMux()
	mux.HandleFunc("/ani/7/1/sub/source", func(w http.ResponseWriter, r *http.Request) {
		master := base + "/m/master.m3u8"
		token := EncodeAnimeXProxyURL(master, "https://megaplay.buzz/", "")
		token = token[strings.Index(token, "/uwu/")+5:]
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","source":"https://cdnx.aniwatchtv.site/uwu/%s","provider":"zuna",`+
			`"tracks":[{"file":%q,"label":"English","kind":"captions"}],`+
			`"chapters":[{"title":"Intro","start":31,"end":111},{"title":"Outro","start":1376,"end":1447}],"type":"hls"}`,
			token, base+"/s/en.vtt")
	})
	mux.HandleFunc("/ani/7/1/dub/source", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"error"}`)
	})
	mux.HandleFunc("/m/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\n/m/media.m3u8\n")
	})
	mux.HandleFunc("/m/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:10,\n/m/seg.ts\n")
	})
	mux.HandleFunc("/m/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	srv := httptest.NewServer(mux)
	base = srv.URL
	t.Cleanup(srv.Close)
	return &megavidFixture{server: srv, base: base}
}

func TestMegaVidSubResolve(t *testing.T) {
	f := newMegaVidFixture(t)
	p := NewMegaVidProvider(zerolog.Nop(), f.base)
	p.client = f.server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want 1 source, got %+v", sr)
	}
	src := sr.Sources[0]
	if !strings.HasSuffix(src.URL, "/m/master.m3u8") {
		t.Errorf("URL = %q, want decoded fixture master", src.URL)
	}
	if src.Type != "hls" || src.Quality != "auto" || src.Verification != "proxy" {
		t.Errorf("meta = type %q quality %q verification %q", src.Type, src.Quality, src.Verification)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Vidy" {
		t.Errorf("ServerNames = %v, want [Vidy]", sr.ServerNames)
	}
	if len(src.Subtitles) != 1 || src.Subtitles[0].Label != "English" {
		t.Errorf("subtitles = %+v, want English track", src.Subtitles)
	}
	if sr.Intro == nil || sr.Intro.Start != 31 || sr.Outro == nil || sr.Outro.End != 1447 {
		t.Errorf("skips = %+v / %+v", sr.Intro, sr.Outro)
	}
}

func TestMegaVidMissingLang(t *testing.T) {
	f := newMegaVidFixture(t)
	p := NewMegaVidProvider(zerolog.Nop(), f.base)
	p.client = f.server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "dub")
	if err != nil {
		t.Fatalf("FindEpisodeSource dub: %v", err)
	}
	if sr != nil {
		t.Fatalf("missing dub must resolve nothing, got %+v", sr)
	}
}

func TestMegaVidBadAnilistID(t *testing.T) {
	p := NewMegaVidProvider(zerolog.Nop(), "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.FindEpisodeSource(ctx, "xx", 1, "sub"); err == nil {
		t.Fatal("want error for bad anilist id")
	}
}

func TestNormalizeAudioLang(t *testing.T) {
	cases := map[string]string{
		"en": "en", "eng": "en", "English": "en", "EN": "en",
		"ja": "ja", "jpn": "ja", "Japanese": "ja",
		"es-419": "es", "zh-Hans": "zh", "pt-BR": "pt",
		"Latin": "", "": "", "und": "",
	}
	for in, want := range cases {
		if got := normalizeAudioLang(in); got != want {
			t.Errorf("normalizeAudioLang(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestM3uAudioLangs(t *testing.T) {
	multi := []byte("#EXTM3U\n" +
		"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",LANGUAGE=\"en\",NAME=\"English\",DEFAULT=YES,URI=\"en.m3u8\"\n" +
		"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",LANGUAGE=\"es\",NAME=\"Espanol\",URI=\"es.m3u8\"\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=1000,AUDIO=\"a\"\nlow.m3u8\n")
	got := m3uAudioLangs(multi)
	if !got["en"] || !got["es"] || len(got) != 2 {
		t.Fatalf("multi langs = %v", got)
	}
	muxed := []byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nlow.m3u8\n")
	if got := m3uAudioLangs(muxed); len(got) != 0 {
		t.Fatalf("muxed master must declare nothing, got %v", got)
	}
	if got := m3uAudioLangs([]byte("<html>nope</html>")); len(got) != 0 {
		t.Fatalf("block page must declare nothing, got %v", got)
	}
}

func TestVerifyM3uMismatchDrops(t *testing.T) {
	var base string
	mux := http.NewServeMux()
	mux.HandleFunc("/lang/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n"+
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",LANGUAGE=\"en\",NAME=\"English\",DEFAULT=YES,URI=\"en.m3u8\"\n"+
			"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",LANGUAGE=\"es\",NAME=\"Espanol\",URI=\"es.m3u8\"\n"+
			"#EXT-X-STREAM-INF:BANDWIDTH=1000,AUDIO=\"a\"\n/m/media.m3u8\n")
	})
	mux.HandleFunc("/m/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:10,\n/m/seg.ts\n")
	})
	mux.HandleFunc("/m/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	base = srv.URL

	m := &Manager{log: zerolog.Nop(), httpClient: srv.Client(), hentaiCache: map[int]hentaiEntry{}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mk := func(lang string) []core.Server {
		return []core.Server{{
			Name: "Vidy", Provider: "megavid", Lang: lang,
			Sources: []core.Source{{URL: base + "/lang/master.m3u8", Type: "hls"}},
		}}
	}
	// Master declares en+es only: sub (ja) is a proven mismatch, dub (en)
	// is confirmed — decided at layer 1 with zero reference fetches.
	if got := m.verifyMegaVidLang(ctx, mk("sub"), nil, "7", 1, "sub"); len(got) != 0 {
		t.Fatalf("sub listing must drop en/es-only master, got %+v", got)
	}
	if got := m.verifyMegaVidLang(ctx, mk("dub"), nil, "7", 1, "dub"); len(got) != 1 {
		t.Fatalf("dub listing must keep en-declaring master, got %+v", got)
	}
}

func TestVerifyNeverHidesWithoutReference(t *testing.T) {
	// No Anikoto anywhere (same-set empty): even an other-lang-shaped
	// file must list — without a same-lang reference the comparison
	// proves nothing and must not hide Vidy. Fully hermetic (mp4 skips
	// the playlist layer, empty providers skip every reference fetch).
	m := &Manager{log: zerolog.Nop(), hentaiCache: map[int]hentaiEntry{}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	mv := []core.Server{{
		Name: "Vidy", Provider: "megavid", Lang: "sub",
		Sources: []core.Source{{URL: "https://cdn.example/dub-sounding-file.mp4", Type: "mp4"}},
	}}
	if got := m.verifyMegaVidLang(ctx, mv, nil, "7", 1, "sub"); len(got) != 1 {
		t.Fatalf("Vidy must list without a same-lang reference, got %+v", got)
	}
}

func TestMegaVidRefererFor(t *testing.T) {
	cases := map[string]struct{ raw, embedded, want string }{
		"vid gateway":    {"https://megavid.buzz/vid/abc/def", "", megavidReferer},
		"embedded wins":  {"https://megavid.buzz/vid/abc", "https://zokoanime.video/", "https://zokoanime.video/"},
		"codec fallback": {"https://hls.dramahot.top/v/x/master.m3u8", "", "https://megaplay.buzz/"},
		"other host":     {"https://cdn.example/x.m3u8", "", "https://megaplay.buzz/"},
	}
	for name, tc := range cases {
		if got := megavidRefererFor(tc.raw, tc.embedded); got != tc.want {
			t.Errorf("%s: got %q want %q", name, got, tc.want)
		}
	}
}
