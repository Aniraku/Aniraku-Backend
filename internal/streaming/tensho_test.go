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

// tenshoFixture serves the zangetsu.cc flow: /search poster grid, watch
// page with a fresh AJAX_TOKEN, /ajax/episodes and /ajax/server. The stale
// token test makes the first /ajax/server call answer 403, forcing the
// provider to reload the watch page and retry.
type tenshoFixture struct {
	srv         *httptest.Server
	watchHits   atomic.Int64
	serverHits  atomic.Int64
	rejectFirst atomic.Bool // first /ajax/server call answers 403 (stale token)
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
	tenshoFlixSub  = "https://flixera.co/embed/ani/" + tenshoAniID + "/sub?autoplay=0&skipintro=0&skipoutro=0"
	tenshoHd1Sub   = "https://cdn.4animo.xyz/embed/hd-1/" + tenshoEp1ID + "/sub?k=1&autoPlay=0&skipIntro=0&skipOutro=0"
	tenshoHd2Sub   = "https://cdn.4animo.xyz/embed/hd-2/ani/" + tenshoAniID + "/sub?k=1&autoPlay=0&skipIntro=0&skipOutro=0"
	tenshoFlixDub  = "https://flixera.co/embed/ani/" + tenshoAniID + "/dub?autoplay=0&skipintro=0&skipoutro=0"
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

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

// assertAjaxHeaders checks the token trio the site's JS always sends.
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
}

func newTenshoTestProvider(f *tenshoFixture) *TenshoProvider {
	p := NewTenshoProvider(zerolog.Nop(), f.srv.URL, f.srv.URL+"/anilist")
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
// slots → three positional embed URLs (flixera, hd-1, hd-2).
func TestTenshoFindEpisodeSourceSub(t *testing.T) {
	f := newTenshoFixture(t)
	p := newTenshoTestProvider(f)

	sr, err := p.FindEpisodeSource(tenshoTestCtx(t), "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 3 {
		t.Fatalf("sources = %+v, want 3 embeds", sr)
	}
	wantURLs := []string{tenshoFlixSub, tenshoHd1Sub, tenshoHd2Sub}
	for i, s := range sr.Sources {
		if s.Type != "embed" || s.Verification != "embed" {
			t.Fatalf("source %d = %+v, want embed/embed", i, s)
		}
		if s.URL != wantURLs[i] {
			t.Fatalf("source %d URL = %q, want %q", i, s.URL, wantURLs[i])
		}
	}
	if got := strings.Join(sr.ServerNames, ","); got != tenshoSubNames {
		t.Fatalf("ServerNames = %q, want %q", got, tenshoSubNames)
	}
	if sr.ServerName != "Tsuki" {
		t.Fatalf("ServerName = %q, want Tsuki", sr.ServerName)
	}
}

// Dub lane: episode 1 has dub → one flixera dub embed; episode 2 is
// sub-only → dub request returns nil (strict per-lang, no cross fallback).
func TestTenshoDubLane(t *testing.T) {
	f := newTenshoFixture(t)
	p := newTenshoTestProvider(f)
	ctx := tenshoTestCtx(t)

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "dub")
	if err != nil {
		t.Fatalf("dub FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 || sr.Sources[0].URL != tenshoFlixDub {
		t.Fatalf("dub sources = %+v, want single flixera dub", sr)
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
