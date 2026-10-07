package streaming

import (
	"bytes"
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

// tenshoFixture serves the zangetsu.cc flow: /search poster grid, watch
// page with a fresh AJAX_TOKEN, /ajax/episodes, /ajax/server — plus both
// player embed flows the provider decrypts (FlixEra inline token, ReCloud
// getSources) down to a probeable /p?t= HLS chain. The stale token test
// makes the first /ajax/server call answer 403, forcing the provider to
// reload the watch page and retry; deadStreams makes every /p? stream
// 404 so the segment probe drops every slot.
type tenshoFixture struct {
	srv         *httptest.Server
	watchHits   atomic.Int64
	serverHits  atomic.Int64
	rejectFirst atomic.Bool // first /ajax/server call answers 403 (stale token)
	deadStreams atomic.Bool // /p? streams 404 -> probes fail -> slots dropped
}

const (
	tenshoShowPath = "naruto-shippuden-1493"
	tenshoShowID   = "1493"
	tenshoToken    = "254640a56e0bdfad8233fa93a051de1a"
	tenshoEp1ID    = "2630"
	tenshoEp2ID    = "2631"
	tenshoAniID    = "1735/1"
	tenshoTitleEN  = "Naruto Shippuden"
	tenshoTitleROM = "NARUTO: Shippuuden"
	tenshoWatchURL = "/watch/" + tenshoShowPath + "?ep=1"
	tenshoSubNames = "Tsuki,Kaze,Hoshi"
	tenshoDubNames = "Tsuki"
)

func newTenshoFixture(t *testing.T) *tenshoFixture {
	t.Helper()
	f := &tenshoFixture{}
	mux := http.NewServeMux()

	// Poster grid: wrong title first — exact matching must win.
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("keyword") == "" {
			t.Errorf("search: missing keyword")
		}
		fmt.Fprintf(w, `<a class="film-poster-ahref" href="/naruto-11"
 title="Naruto"></a>
<a class="film-poster-ahref" href="/%s"
 title="%s"></a>`, tenshoShowPath, tenshoTitleEN)
	})

	mux.HandleFunc("/watch/"+tenshoShowPath, func(w http.ResponseWriter, r *http.Request) {
		f.watchHits.Add(1)
		// The real site binds the token to a session cookie — /ajax/*
		// 403s without it.
		w.Header().Add("Set-Cookie", "PHPSESSID=testsession00000000000000000001; Path=/; HttpOnly")
		fmt.Fprintf(w, `<html><head><script>window.AJAX_TOKEN = %q;</script></head><body></body></html>`, tenshoToken)
	})

	mux.HandleFunc("/ajax/episodes", func(w http.ResponseWriter, r *http.Request) {
		assertAjaxHeaders(t, r, tenshoToken)
		if r.URL.Query().Get("animeId") != tenshoShowID {
			t.Errorf("episodes: animeId %q, want %q", r.URL.Query().Get("animeId"), tenshoShowID)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"success":true,"episodes":[
 {"id":%q,"number":1,"sub":true,"dub":true,"ani":%q,"mal":%q,"filler":false},
 {"id":%q,"number":2,"sub":true,"dub":false,"ani":"1736/1","mal":"1736/1","filler":true}
]}`, tenshoEp1ID, tenshoAniID, tenshoAniID, tenshoEp2ID)
	})

	mux.HandleFunc("/ajax/server", func(w http.ResponseWriter, r *http.Request) {
		f.serverHits.Add(1)
		if r.URL.Query().Get("episodeId") == "" {
			t.Errorf("server: missing episodeId")
		}
		// Stale-token simulation: reject whatever token arrives first,
		// accept after the provider has refetched the watch page.
		if f.rejectFirst.Load() {
			f.rejectFirst.Store(false)
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, "<title>Forbidden</title>")
			return
		}
		assertAjaxHeaders(t, r, tenshoToken)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"sub":[
 {"serverId":"2630-s1","serverName":"s-1","index":3},
 {"serverId":"2630-s2","serverName":"s-2","index":2},
 {"serverId":"2630-s3","serverName":"s-3","index":1}],
 "dub":[{"serverId":"2630-s1","serverName":"s-1","index":3}]}`)
	})

	mux.HandleFunc("/anilist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":{"Media":{"title":{"english":%q,"romaji":%q,"native":"NARUTO -ナルト- 疾風伝"},"synonyms":["Naruto Shippuden"]}}}`,
			tenshoTitleEN, tenshoTitleROM)
	})

	// FlixEra embed: the page inlines the playback proxy path (the HLS
	// master) and subtitle tracks — this is all the decrypt step consumes.
	mux.HandleFunc("/embed/ani/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><head><script>window.__EMBED_PROXY__ = "/p?t=flix-master";`+
			`window.__EMBED_TRACKS__ = [{"file":"/p?t=flix-sub-en","label":"English","kind":"captions","default":true}];`+
			`</script></head><body></body></html>`)
	})

	// ReCloud (4animo hd-1/hd-2) embed: the page names its getSources
	// endpoint instead of inlining a stream.
	reCloudShell := func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><head><script>var sourcesUrl = '/stream/getSources?t=page-token';</script></head><body></body></html>`)
	}
	mux.HandleFunc("/embed/hd-1/", reCloudShell)
	mux.HandleFunc("/embed/hd-2/", reCloudShell)

	// getSources answers master + tracks (and an intro/outro the provider
	// ignores — skip segments come from Aniskip), and rejects requests
	// without a Referer live (404 without — measured).
	mux.HandleFunc("/stream/getSources", func(w http.ResponseWriter, r *http.Request) {
		if r.Referer() == "" {
			t.Errorf("getSources: missing Referer (live endpoint 404s without it)")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"sources":[{"file":"/p?t=animo-master","type":"hls"}],
 "tracks":[{"file":"/p?t=animo-sub-en","label":"English","kind":"captions","default":true}],
 "intro":{"start":138,"end":215},"outro":{"start":1452,"end":1542},
 "server":"hd-1","episodeTitle":"Episode 1"}`)
	})

	// /p?t= is the ReCloud proxy path both players end on: master ->
	// media -> segment (the chain the probe walks) plus subtitle files.
	mux.HandleFunc("/p", func(w http.ResponseWriter, r *http.Request) {
		if f.deadStreams.Load() {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.URL.Query().Get("t") {
		case "flix-master", "animo-master":
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			fmt.Fprintf(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1920x1080\n%s/p?t=%s-media\n",
				f.srv.URL, strings.TrimSuffix(r.URL.Query().Get("t"), "-master"))
		case "flix-media", "animo-media":
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\n%s/p?t=%s-seg\n#EXT-X-ENDLIST\n",
				f.srv.URL, strings.TrimSuffix(r.URL.Query().Get("t"), "-media"))
		case "flix-seg", "animo-seg":
			// MPEG-TS sync bytes — judged playable, never HTML.
			w.Header().Set("Content-Type", "video/mp2t")
			w.Write(bytes.Repeat([]byte{0x47, 0x00, 0x10, 0x00}, 47))
		case "flix-sub-en", "animo-sub-en":
			w.Header().Set("Content-Type", "text/vtt")
			fmt.Fprint(w, "WEBVTT\n\n00:00:00.000 --> 00:00:02.000\nHello\n")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// assertAjaxHeaders checks the session pair the site's JS always sends:
// token + referer headers and the PHPSESSID cookie from the same watch
// fetch (token without cookie is rejected live — measured 403).
func assertAjaxHeaders(t *testing.T, r *http.Request, wantToken string) {
	t.Helper()
	if got := r.Header.Get("X-Requested-With"); got != "XMLHttpRequest" {
		t.Errorf("X-Requested-With = %q", got)
	}
	if got := r.Header.Get("X-Page-Token"); got != wantToken {
		t.Errorf("X-Page-Token = %q, want %q", got, wantToken)
	}
	if !strings.HasSuffix(r.Header.Get("Referer"), tenshoWatchURL) {
		t.Errorf("Referer = %q, want the watch page", r.Header.Get("Referer"))
	}
	if !strings.Contains(r.Header.Get("Cookie"), "PHPSESSID=") {
		t.Errorf("Cookie = %q, want PHPSESSID session", r.Header.Get("Cookie"))
	}
}

func newTenshoTestProvider(f *tenshoFixture) *TenshoProvider {
	// All three hosts point at the fixture: the whole decrypt flow (embed
	// page -> token -> stream JSON -> m3u8 probe) must run locally.
	p := NewTenshoProvider(zerolog.Nop(), f.srv.URL, f.srv.URL+"/anilist", f.srv.URL, f.srv.URL)
	// httptest servers are loopback: the netguard transport's SSRF guard
	// would block them, so tests use a plain client (production always
	// uses the guarded one built by NewTenshoProvider).
	p.client = &http.Client{Timeout: 30 * time.Second}
	return p
}

func tenshoTestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestTenshoSearch(t *testing.T) {
	f := newTenshoFixture(t)
	p := newTenshoTestProvider(f)

	results, err := p.Search(tenshoTestCtx(t), tenshoTitleEN)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	if results[1].ID != tenshoShowPath || results[1].Title != tenshoTitleEN {
		t.Fatalf("result = %+v, want path %q title %q", results[1], tenshoShowPath, tenshoTitleEN)
	}
}

// Full sub resolve: title match → fresh token → episode list → server
// slots → each embed decrypted to its direct m3u8, probed, and labeled
// Tsuki/Kaze/Hoshi positionally.
func TestTenshoFindEpisodeSourceSub(t *testing.T) {
	f := newTenshoFixture(t)
	p := newTenshoTestProvider(f)

	sr, err := p.FindEpisodeSource(tenshoTestCtx(t), "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 3 {
		t.Fatalf("sources = %+v, want 3 direct streams", sr)
	}
	wantURLs := []string{
		f.srv.URL + "/p?t=flix-master",
		f.srv.URL + "/p?t=animo-master",
		f.srv.URL + "/p?t=animo-master",
	}
	for i, s := range sr.Sources {
		if s.Type != "hls" || s.Verification != "proxy" {
			t.Fatalf("source %d = %+v, want hls/proxy (decrypted, never embed)", i, s)
		}
		if s.URL != wantURLs[i] {
			t.Fatalf("source %d URL = %q, want %q", i, s.URL, wantURLs[i])
		}
	}
	// Both players ship subtitle tracks alongside the stream token.
	if subs := sr.Sources[0].Subtitles; len(subs) != 1 ||
		subs[0].URL != f.srv.URL+"/p?t=flix-sub-en" || subs[0].Lang != "en" || subs[0].Label != "English" {
		t.Fatalf("flixera subs = %+v", subs)
	}
	if subs := sr.Sources[1].Subtitles; len(subs) != 1 || subs[0].URL != f.srv.URL+"/p?t=animo-sub-en" {
		t.Fatalf("animo subs = %+v", subs)
	}
	// The players' intro/outro ride along in the live JSON but must NOT
	// reach the result — the frontend uses Aniskip for skip segments.
	if sr.Intro != nil || sr.Outro != nil {
		t.Fatalf("Intro/Outro = %+v/%+v, want nil (Aniskip is the frontend's skip source)", sr.Intro, sr.Outro)
	}
	// Playback needs no Referer upstream (measured) — ship no headers.
	if len(sr.Headers) != 0 {
		t.Fatalf("Headers = %+v, want none", sr.Headers)
	}
	if got := strings.Join(sr.ServerNames, ","); got != tenshoSubNames {
		t.Fatalf("ServerNames = %q, want %q", got, tenshoSubNames)
	}
	if sr.ServerName != "Tsuki" {
		t.Fatalf("ServerName = %q, want Tsuki", sr.ServerName)
	}
}

// Dub lane: episode 1 has dub → one decrypted flixera dub stream; episode 2 is
// sub-only → dub request returns nil (strict per-lang, no cross fallback).
func TestTenshoDubLane(t *testing.T) {
	f := newTenshoFixture(t)
	p := newTenshoTestProvider(f)
	ctx := tenshoTestCtx(t)

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "dub")
	if err != nil {
		t.Fatalf("dub FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("dub sources = %+v, want single decrypted flixera dub", sr)
	}
	if sr.Sources[0].URL != f.srv.URL+"/p?t=flix-master" ||
		sr.Sources[0].Type != "hls" || sr.Sources[0].Verification != "proxy" {
		t.Fatalf("dub source = %+v, want direct hls/proxy", sr.Sources[0])
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != tenshoDubNames {
		t.Fatalf("dub names = %v, want [%s]", sr.ServerNames, tenshoDubNames)
	}

	sr2, err := p.FindEpisodeSource(ctx, "21", 2, "dub")
	if err != nil {
		t.Fatalf("sub-only episode dub resolve: %v", err)
	}
	if sr2 != nil {
		t.Fatalf("dub source = %+v, want nil for sub-only episode", sr2)
	}
}

// A stale token (403) must trigger exactly one watch-page refresh and a
// successful retry — the failure mode observed on the live site.
func TestTenshoStaleTokenRefresh(t *testing.T) {
	f := newTenshoFixture(t)
	f.rejectFirst.Store(true)
	p := newTenshoTestProvider(f)

	sr, err := p.FindEpisodeSource(tenshoTestCtx(t), "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource after 403: %v", err)
	}
	if sr == nil || len(sr.Sources) != 3 {
		t.Fatalf("sources = %+v, want 3 after token refresh", sr)
	}
	if got := f.watchHits.Load(); got != 2 {
		t.Fatalf("watch page hits = %d, want 2 (initial + refresh)", got)
	}
}

func TestTenshoMissingEpisode(t *testing.T) {
	f := newTenshoFixture(t)
	p := newTenshoTestProvider(f)

	sr, err := p.FindEpisodeSource(tenshoTestCtx(t), "21", 99, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr != nil {
		t.Fatalf("source = %+v, want nil for unknown episode", sr)
	}
}

func TestTenshoFindEpisodes(t *testing.T) {
	f := newTenshoFixture(t)
	p := newTenshoTestProvider(f)

	eps, err := p.FindEpisodes(tenshoTestCtx(t), tenshoShowPath)
	if err != nil {
		t.Fatalf("FindEpisodes: %v", err)
	}
	if len(eps) != 2 || eps[0].Number != 1 || eps[1].Number != 2 {
		t.Fatalf("episodes = %+v, want [1 2]", eps)
	}
	if !eps[1].Filler {
		t.Fatal("episode 2 filler flag lost")
	}
	if _, err := p.FindEpisodes(tenshoTestCtx(t), "badpath"); err == nil {
		t.Fatal("expected error for non-path provider id")
	}
}

// When every decrypted stream fails the segment probe (CDN blocked from
// this egress), the provider must return nil — not embed URLs, not an
// error — so the fan-out moves on cleanly.
func TestTenshoDeadStreamsDrop(t *testing.T) {
	f := newTenshoFixture(t)
	f.deadStreams.Store(true)
	p := newTenshoTestProvider(f)

	sr, err := p.FindEpisodeSource(tenshoTestCtx(t), "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr != nil {
		t.Fatalf("sources = %+v, want nil when every probe fails", sr)
	}
}
