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
)

func TestAnimeGGDice(t *testing.T) {
	if got := animeggDice("One Piece", "One Piece"); got != 1.0 {
		t.Fatalf("identical dice = %v, want 1.0", got)
	}
	if got := animeggDice("One Piece", "Naruto"); got >= 0.5 {
		t.Fatalf("distinct dice = %v, want < 0.5", got)
	}
	if got := animeggDice("", "x"); got != 0.0 {
		t.Fatalf("short dice = %v, want 0.0", got)
	}
}

func TestAnimeGGTitleScoreNumberPenalty(t *testing.T) {
	plain := animeggTitleScore("One Piece", "One Piece", "one-piece")
	mismatch := animeggTitleScore("One Piece", "One Piece 2", "one-piece-2")
	// qn empty + sn=2 -> 1-0.06*(2-1) = 0.94 factor on a near-perfect base.
	if mismatch >= plain {
		t.Fatalf("number-mismatch score %v should be below plain %v", mismatch, plain)
	}
	if mismatch <= 0 || mismatch >= 1 {
		t.Fatalf("number-mismatch score %v out of range", mismatch)
	}
	season := animeggTitleScore("Attack on Titan Season 2", "Attack on Titan Season 2", "attack-on-titan-season-2")
	other := animeggTitleScore("Attack on Titan Season 2", "Attack on Titan Season 3", "attack-on-titan-season-3")
	if other >= season {
		t.Fatalf("wrong-season score %v should be below exact %v", other, season)
	}
}

func TestAnimeGGQualityRank(t *testing.T) {
	cases := map[string]int{"1080p": 1080, "720p": 720, "480p": 480, "360p": 360, "unknown": 0, "": 0}
	for label, want := range cases {
		if got := animeggQualityRank(label); got != want {
			t.Errorf("rank(%q) = %d, want %d", label, got, want)
		}
	}
}

func TestAnimeGGParseSeries(t *testing.T) {
	htmlBody := `<ul>
<li><a class="anm_det_pop" href="/one-piece-ep-1-abc#x"><strong>Episode 1-3</strong><i class="anititle">Romance Dawn</i></a><span class="btn-subbed">SUB</span></li>
<li><a class="anm_det_pop" href="/one-piece-ep-2-def"><strong>Episode 2</strong></a><span class="btn-dubbed">DUB</span></li>
<li><a class="other" href="/nope"><strong>Episode 9</strong></a></li>
</ul>`
	eps := animeggParseSeries(htmlBody)
	if len(eps) != 2 {
		t.Fatalf("episodes = %+v, want 2", eps)
	}
	if eps[0].number != 1 || eps[0].epSlug != "one-piece-ep-1-abc" || !eps[0].hasSub || eps[0].hasDub {
		t.Errorf("ep1 = %+v, want number 1 sub-only with slug", eps[0])
	}
	if eps[1].number != 2 || !eps[1].hasDub || eps[1].hasSub {
		t.Errorf("ep2 = %+v, want number 2 dub-only", eps[1])
	}
}

func TestAnimeGGParseTabs(t *testing.T) {
	htmlBody := `<div class="info"><a>One Piece</a></div>
<a data-toggle="tab" data-id="101" data-mirror="MirrorA" data-version="subbed">S</a>
<a data-toggle="tab" data-id="102" data-mirror="MirrorA" data-version="dubbed">D</a>
<a data-toggle="tab" data-version="subbed">noid</a>`
	sub := animeggParseTabs(htmlBody, "sub")
	if len(sub) != 1 || sub[0].embedID != "101" || sub[0].mirror != "MirrorA" {
		t.Fatalf("sub tabs = %+v, want [101/MirrorA]", sub)
	}
	dub := animeggParseTabs(htmlBody, "dub")
	if len(dub) != 1 || dub[0].embedID != "102" {
		t.Fatalf("dub tabs = %+v, want [102]", dub)
	}
}

func TestAnimeGGParseVideoSources(t *testing.T) {
	embed := `<script>var videoSources = [{file: '/play/1/video.mp4?for=9', label: '360p', bk: '', isBk: false },{file: "https://cdn.example/x.mp4", label: "1080p"}];</script>`
	got := animeggParseVideoSources(embed)
	if len(got) != 2 {
		t.Fatalf("sources = %+v, want 2", got)
	}
	if got[0].url != "/play/1/video.mp4?for=9" || got[0].quality != "360p" {
		t.Errorf("sources[0] = %+v", got[0])
	}
	if got[1].url != "https://cdn.example/x.mp4" || got[1].quality != "1080p" {
		t.Errorf("sources[1] = %+v", got[1])
	}
	if len(animeggParseVideoSources("<html>no sources</html>")) != 0 {
		t.Fatal("want no sources from empty page")
	}
}

// animeggFixture serves a full AnimeGG site: anilist, search, series,
// watch (sub + dub tabs), embeds (sub 360p+1080p, dub 360p-only) and mp4
// file bytes with real ftyp magic so the probe passes.
type animeggFixture struct {
	server *httptest.Server
	base   string
}

func newAnimeGGFixture(t *testing.T) *animeggFixture {
	t.Helper()
	var base string
	mux := http.NewServeMux()
	mux.HandleFunc("/anilist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"Media":{"id":21,"title":{"english":"One Piece","romaji":"ONE PIECE","native":""},"synonyms":[],"status":"RELEASING","format":"TV","episodes":null,"seasonYear":1999,"startDate":{"year":1999},"relations":{"edges":[]}}}}`)
	})
	mux.HandleFunc("/search/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<a class="mse" href="/series/one-piece"><strong>One Piece</strong></a>`)
	})
	mux.HandleFunc("/series/one-piece", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<li><a class="anm_det_pop" href="/one-piece-ep1"><strong>Episode 1</strong><i class="anititle">Romance Dawn</i></a><span class="btn-subbed">SUB</span><span class="btn-dubbed">DUB</span></li>`)
	})
	mux.HandleFunc("/one-piece-ep1", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<div class="info"><a>One Piece</a></div>`+
			`<a data-toggle="tab" data-id="101" data-mirror="AnimeGG" data-version="subbed">S</a>`+
			`<a data-toggle="tab" data-id="102" data-mirror="AnimeGG" data-version="dubbed">D</a>`)
	})
	mux.HandleFunc("/embed/101", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<script>var videoSources = [{file: "%s/play/360.mp4", label: "360p", bk: "", isBk: false },{file: "%s/play/redir.mp4", label: "1080p", bk: "", isBk: false }];</script>`, base, base)
	})
	mux.HandleFunc("/embed/102", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<script>var videoSources = [{file: "%s/play/360.mp4", label: "360p", bk: "", isBk: false }];</script>`, base)
	})
	mp4 := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.Write([]byte{0, 0, 0, 24, 'f', 't', 'y', 'p', 'i', 's', 'o', 'm', 0, 1, 2, 3})
	}
	mux.HandleFunc("/play/360.mp4", mp4)
	mux.HandleFunc("/play/1080.mp4", mp4)
	// /play/ hop 302s to the file host (vidcache in prod): the provider
	// must ship the final URL since the media proxy refuses redirects.
	mux.HandleFunc("/play/redir.mp4", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, base+"/file.mp4", http.StatusFound)
	})
	mux.HandleFunc("/file.mp4", mp4)
	srv := httptest.NewServer(mux)
	base = srv.URL
	t.Cleanup(srv.Close)
	return &animeggFixture{server: srv, base: base}
}

func newAnimeGGTestProvider(f *animeggFixture) *AnimeGGProvider {
	p := NewAnimeGGProvider(zerolog.Nop(), f.base, f.base+"/anilist")
	// Plain client: netguard SSRF-blocks the 127.0.0.1 fixture server.
	p.client = f.server.Client()
	return p
}

func TestAnimeGGSubHighestQuality(t *testing.T) {
	f := newAnimeGGFixture(t)
	p := newAnimeGGTestProvider(f)
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
	if !strings.HasSuffix(src.URL, "/file.mp4") {
		t.Errorf("URL = %q, want final redirect target /file.mp4 (never the /play/ hop)", src.URL)
	}
	if strings.Contains(src.URL, "/play/") {
		t.Errorf("URL = %q, must not contain the unredirected /play/ hop", src.URL)
	}
	if src.Type != "mp4" || src.Quality != "1080p" || src.Verification != "proxy" {
		t.Errorf("meta = type %q quality %q verification %q", src.Type, src.Quality, src.Verification)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Sunny" {
		t.Errorf("ServerNames = %v, want [Sunny]", sr.ServerNames)
	}
	if sr.Headers["Referer"] != animeggReferer {
		t.Errorf("Referer = %q, want %q", sr.Headers["Referer"], animeggReferer)
	}
	// Cached resolve serves the same result without upstream contact.
	sr2, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil || sr2 == nil || sr2.Sources[0].URL != src.URL {
		t.Fatalf("cached resolve = %+v, err %v", sr2, err)
	}
}

func TestAnimeGGLowQualityDubHidden(t *testing.T) {
	f := newAnimeGGFixture(t)
	p := newAnimeGGTestProvider(f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "dub")
	if err != nil {
		t.Fatalf("FindEpisodeSource dub: %v", err)
	}
	if sr != nil {
		t.Fatalf("360p-only dub must be hidden, got %+v", sr)
	}
}

func TestAnimeGGDeletedTopFileFallsBack(t *testing.T) {
	var base string
	mux := http.NewServeMux()
	mux.HandleFunc("/anilist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"Media":{"id":7,"title":{"english":"Show Seven","romaji":"","native":""},"synonyms":[],"status":"FINISHED","format":"TV","episodes":12,"seasonYear":2020,"startDate":{"year":2020},"relations":{"edges":[]}}}}`)
	})
	mux.HandleFunc("/search/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<a class="mse" href="/series/show-seven"><strong>Show Seven</strong></a>`)
	})
	mux.HandleFunc("/series/show-seven", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<li><a class="anm_det_pop" href="/ss-ep1"><strong>Episode 1</strong></a><span class="btn-subbed">SUB</span></li>`)
	})
	mux.HandleFunc("/ss-ep1", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<a data-toggle="tab" data-id="201" data-mirror="AnimeGG" data-version="subbed">S</a>`)
	})
	mux.HandleFunc("/embed/201", func(w http.ResponseWriter, r *http.Request) {
		// 1080p deleted (404), 720p live: the mirror must survive on 720p.
		fmt.Fprintf(w, `<script>var videoSources = [{file: "%s/play/gone.mp4", label: "1080p"},{file: "%s/play/ok.mp4", label: "720p"}];</script>`, base, base)
	})
	mux.HandleFunc("/play/ok.mp4", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.Write([]byte{0, 0, 0, 24, 'f', 't', 'y', 'p', 0, 1, 2, 3})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	base = srv.URL
	p := NewAnimeGGProvider(zerolog.Nop(), srv.URL, srv.URL+"/anilist")
	p.client = srv.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want 1 fallback source, got %+v", sr)
	}
	if got := sr.Sources[0].Quality; got != "720p" {
		t.Fatalf("quality = %q, want 720p fallback for deleted 1080p", got)
	}
}
