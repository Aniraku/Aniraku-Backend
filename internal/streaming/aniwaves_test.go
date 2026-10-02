package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestAniWavesDice(t *testing.T) {
	if got := aniwavesDice("One Piece", "One Piece"); got != 1.0 {
		t.Fatalf("identical dice = %v, want 1.0", got)
	}
	if got := aniwavesDice("One Piece", "Naruto Shippuden"); got >= 0.5 {
		t.Fatalf("distinct dice = %v, want < 0.5", got)
	}
}

func TestAniWavesCuteNames(t *testing.T) {
	cases := map[string]string{"Vidplay": "Nami", "vidplay": "Nami", "MyCloud": "Coral", "DatSaV": "Pearl", "savedly": "Pearl"}
	for upstream, want := range cases {
		if got := aniwavesCuteName(upstream, 0); got != want {
			t.Errorf("cuteName(%q) = %q, want %q", upstream, got, want)
		}
	}
	if got := aniwavesCuteName("FutureCDN", 0); got != "Wavy" {
		t.Errorf("fallback cuteName = %q, want Wavy", got)
	}
	if got := aniwavesCuteName("FutureCDN", 9); got == "Wavy" {
		t.Errorf("fallback index 9 must be unique, got %q", got)
	}
}

func TestAniWavesSavedlyRank(t *testing.T) {
	if aniwavesSavedlyRank("1080") <= aniwavesSavedlyRank("720") {
		t.Fatal("1080 must outrank 720")
	}
	if aniwavesSavedlyRank("HD") <= aniwavesSavedlyRank("SD") {
		t.Fatal("HD must outrank SD")
	}
}

func TestAniWavesParseCards(t *testing.T) {
	htmlBody := `<a class="name d-title" href="/watch/one-piece-21" data-jp="JP">One Piece</a>
<a class="name d-title" href="/watch/naruto">No ID</a>
<a class="other" href="/watch/bleach-5">Wrong class</a>`
	got := aniwavesParseCards(htmlBody)
	if len(got) != 1 {
		t.Fatalf("cards = %+v, want 1", got)
	}
	if got[0].slug != "one-piece-21" || got[0].siteID != 21 || got[0].japanese != "JP" {
		t.Errorf("card = %+v", got[0])
	}
}

func TestAniWavesParseEpisodes(t *testing.T) {
	htmlBody := `<a data-num="1" data-ids="a" data-slug="1" data-sub="1" data-dub="0">1 Pilot</a>
<a data-num="2" data-ids="" data-sub="1">no ids</a>
<a data-num="1" data-ids="b" data-sub="1">dup</a>
<a data-num="x" data-ids="c">bad num</a>`
	got := aniwavesParseEpisodes(htmlBody)
	if len(got) != 1 {
		t.Fatalf("episodes = %+v, want 1", got)
	}
	if got[0].number != 1 || got[0].title != "Pilot" || !got[0].hasSub || got[0].hasDub {
		t.Errorf("episode = %+v", got[0])
	}
}

func TestAniWavesParseServerGroups(t *testing.T) {
	htmlBody := `<div data-type="sub"><ul><li data-link-id="L1" data-sv-id="1">Vidplay</li></ul></div>` +
		`<div data-type="dub"><ul><li data-link-id="L2">MyCloud</li><li>no link</li></ul></div>`
	got := aniwavesParseServerGroups(htmlBody)
	if len(got) != 2 {
		t.Fatalf("servers = %+v, want 2", got)
	}
	if got[0].audio != "sub" || got[0].linkID != "L1" || got[0].name != "Vidplay" {
		t.Errorf("servers[0] = %+v", got[0])
	}
	if got[1].audio != "dub" || got[1].linkID != "L2" {
		t.Errorf("servers[1] = %+v", got[1])
	}
}

func TestAniWavesAlign(t *testing.T) {
	eps := []aniwavesEpisode{
		{number: 1, title: "Part 1 ep"}, {number: 2, title: "Part 1 ep"},
		{number: 3, title: "Part 2 ep"}, {number: 4, title: "Part 2 ep"},
	}
	got := aniwavesAlign(eps, []string{"Show Part 2"}, 2)
	if len(got) != 2 || got[0].number != 1 || got[1].number != 2 {
		t.Fatalf("aligned = %+v, want renumbered part 2", got)
	}
	if got[0].sourceNum != "" && got[0].title != "Part 2 ep" {
		t.Errorf("aligned[0] = %+v", got[0])
	}
	// No ordinal in titles: returned as-is.
	if same := aniwavesAlign(eps, []string{"Show"}, 2); len(same) != 4 {
		t.Fatalf("unaligned = %+v, want 4 untouched", same)
	}
}

func TestAniWavesSearchQueriesBounded(t *testing.T) {
	titles := []string{"One Piece", "ONE PIECE", "ワンピース", "OP", "One Piece Dub", "One Piece Season 1 Part 1 Final Chapter Movie"}
	if got := aniwavesSearchQueries(titles); len(got) > 18 {
		t.Fatalf("%d queries, want <= 18", len(got))
	}
}

func TestAniWavesResolveDirectPassthrough(t *testing.T) {
	p := NewAniWavesProvider(zerolog.Nop(), "", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Embed-only hosts (no direct file) resolve to nothing — never shipped.
	if got := p.resolveDirect(ctx, "https://mfw09.org/e/abc123?v=1"); len(got) != 0 {
		t.Fatalf("embed-only = %+v, want none", got)
	}
	if got := p.resolveDirect(ctx, "https://cdn.example/x/master.m3u8?token=1"); len(got) != 1 || got[0].typ != "hls" {
		t.Fatalf("m3u8 = %+v, want 1 hls", got)
	}
	if got := p.resolveDirect(ctx, "https://cdn.example/x/file.mp4"); len(got) != 1 || got[0].typ != "mp4" {
		t.Fatalf("mp4 = %+v, want 1 mp4", got)
	}
}

// aniwavesFixture serves a full AniWaves site: anilist, filter, detail,
// episode list, server list, sources, echovideo getSources and HLS
// playlists (master + media, so the playlist probe passes).
type aniwavesFixture struct {
	server *httptest.Server
	base   string
}

func newAniWavesFixture(t *testing.T) *aniwavesFixture {
	t.Helper()
	var base string
	mux := http.NewServeMux()
	mux.HandleFunc("/anilist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"Media":{"id":21,"title":{"english":"One Piece","romaji":"ONE PIECE","native":""},"synonyms":[],"status":"RELEASING","format":"TV","episodes":12,"seasonYear":1999,"startDate":{"year":1999}}}}`)
	})
	mux.HandleFunc("/filter", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<a class="name d-title" href="/watch/one-piece-21" data-jp="JP">One Piece</a>`)
	})
	mux.HandleFunc("/watch/one-piece-21", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<div>Type: <span>TV</span></div><div>Date aired: <span>Oct 20, 1999</span></div><div>Episodes: <span>12</span></div><h1>One Piece</h1>`)
	})
	ajax := func(w http.ResponseWriter, result string) {
		w.Header().Set("Content-Type", "application/json")
		raw, _ := json.Marshal(result)
		fmt.Fprintf(w, `{"status":200,"result":%s}`, raw)
	}
	mux.HandleFunc("/ajax/episode/list/21", func(w http.ResponseWriter, r *http.Request) {
		ajax(w, `<a data-num="1" data-ids="e1" data-slug="1" data-sub="1" data-dub="1">1 Romance Dawn</a>`)
	})
	mux.HandleFunc("/ajax/server/list", func(w http.ResponseWriter, r *http.Request) {
		ajax(w, `<div data-type="sub"><ul><li data-link-id="L1" data-sv-id="7">Vidplay</li></ul></div>`)
	})
	mux.HandleFunc("/ajax/sources", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":200,"result":{"url":"%s/embed-1/abc","skip_data":{"intro":[31,111],"outro":[1376,1447]}}}`, base)
	})
	mux.HandleFunc("/embed-1/getSources", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"sources":["%s/hls/master.m3u8"]}`, base)
	})
	mux.HandleFunc("/hls/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000,RESOLUTION=640x360\n/media.m3u8\n")
	})
	mux.HandleFunc("/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXTINF:10,\nseg.ts\n")
	})
	mux.HandleFunc("/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.Write([]byte{0x47, 0x40, 0x00, 0x10, 0x00, 0x01, 0x02, 0x03})
	})
	srv := httptest.NewServer(mux)
	base = srv.URL
	t.Cleanup(srv.Close)
	return &aniwavesFixture{server: srv, base: base}
}

func TestAniWavesSubResolve(t *testing.T) {
	f := newAniWavesFixture(t)
	p := NewAniWavesProvider(zerolog.Nop(), f.base, f.base+"/anilist")
	p.client = f.server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want 1 source, got %+v", sr)
	}
	src := sr.Sources[0]
	if !strings.HasSuffix(src.URL, "/hls/master.m3u8") {
		t.Errorf("URL = %q, want HLS master", src.URL)
	}
	if src.Type != "hls" || src.Quality != "auto" || src.Verification != "proxy" {
		t.Errorf("meta = type %q quality %q verification %q", src.Type, src.Quality, src.Verification)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Nami" {
		t.Errorf("ServerNames = %v, want [Nami] (Vidplay cute name)", sr.ServerNames)
	}
	if sr.Headers["Referer"] != aniwavesPlaybackReferer {
		t.Errorf("Referer = %q, want %q", sr.Headers["Referer"], aniwavesPlaybackReferer)
	}
	if sr.Intro == nil || sr.Intro.Start != 31 || sr.Intro.End != 111 {
		t.Errorf("Intro = %+v, want 31..111", sr.Intro)
	}
	if sr.Outro == nil || sr.Outro.Start != 1376 || sr.Outro.End != 1447 {
		t.Errorf("Outro = %+v, want 1376..1447", sr.Outro)
	}
}

func TestAniWavesDeadSegmentsDropped(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/dead-master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\n/dead-media.m3u8\n")
	})
	mux.HandleFunc("/dead-media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:10,\n/dead.ts\n")
	})
	mux.HandleFunc("/fallback-master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=5000,RESOLUTION=1920x1080\n/hi.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=500,RESOLUTION=640x360\n/lo.m3u8\n")
	})
	mux.HandleFunc("/hi.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:10,\n/hi.ts\n")
	})
	mux.HandleFunc("/lo.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:10,\n/lo.ts\n")
	})
	mux.HandleFunc("/lo.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	p := NewAniWavesProvider(zerolog.Nop(), srv.URL, srv.URL)
	p.client = srv.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Playlists serve but segments 404: the mirror must not list.
	if p.probeAniWavesMaster(ctx, srv.URL+"/dead-master.m3u8", "https://x/") {
		t.Fatal("master with dead segments must fail the probe")
	}
	// Dead top rendition falls through to the live one.
	if !p.probeAniWavesMaster(ctx, srv.URL+"/fallback-master.m3u8", "https://x/") {
		t.Fatal("dead 1080p rendition must fall back to live 360p")
	}
}
