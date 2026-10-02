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

// leeFixture serves the full watch chain: ani mapping, bootstrap,
// settlar session, embed session, HLS playlists with real bytes.
type leeFixture struct {
	server *httptest.Server
	base   string
}

func newLeeFixture(t *testing.T) *leeFixture {
	t.Helper()
	var base string
	mux := http.NewServeMux()
	mux.HandleFunc("/api/anime/ani/7", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"settlarId":900,"malId":700,"episodes":[
			{"number":1,"sourceNumber":1,"title":"Episode 1","sub":false,"dub":true},
			{"number":2,"sourceNumber":2,"title":"Episode 2","sub":true,"dub":true},
			{"number":3,"sourceNumber":3,"title":"Episode 3","sub":false,"dub":false}]}`)
	})
	mux.HandleFunc("/api/anime/playback-bootstrap/settlar/900", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		if q.Get("ep") == "1" {
			fmt.Fprint(w, `{"availability":{"sub":false,"dub":true},"settlarSelection":"SEL-1"}`)
			return
		}
		fmt.Fprint(w, `{"availability":{"sub":true,"dub":true},"settlarSelection":"SEL-2"}`)
	})
	mux.HandleFunc("/api/anime/settlar/session", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"embedUrl":%q}`, base+"/embed/v1?t=TOK")
	})
	mux.HandleFunc("/api/embed/session", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("t") != "TOK" {
			http.Error(w, "bad token", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"source":%q,"kind":"hls","audioLang":"en","subtitles":[{"url":%q,"label":"English","lang":"en"}]}`,
			base+"/m/master.m3u8", base+"/s/en.vtt")
	})
	mux.HandleFunc("/api/anime/skip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"intro":{"start":10,"end":70},"outro":{"start":1300,"end":1400}}`)
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
	return &leeFixture{server: srv, base: base}
}

func newLeeTestProvider(f *leeFixture) *LeeProvider {
	p := NewLeeProvider(zerolog.Nop(), f.base, f.base)
	p.client = f.server.Client()
	return p
}

func TestLeeDubResolve(t *testing.T) {
	f := newLeeFixture(t)
	p := newLeeTestProvider(f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "dub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want 1 source, got %+v", sr)
	}
	src := sr.Sources[0]
	if !strings.HasSuffix(src.URL, "/m/master.m3u8") {
		t.Errorf("URL = %q, want fixture master", src.URL)
	}
	if src.Type != "hls" || src.Quality != "auto" || src.Verification != "proxy" {
		t.Errorf("meta = type %q quality %q verification %q", src.Type, src.Quality, src.Verification)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Lee" {
		t.Errorf("ServerNames = %v, want [Lee]", sr.ServerNames)
	}
	if len(src.Subtitles) != 1 || src.Subtitles[0].Lang != "en" {
		t.Errorf("subtitles = %+v, want 1 English track", src.Subtitles)
	}
	if sr.Intro == nil || sr.Intro.Start != 10 || sr.Outro == nil || sr.Outro.End != 1400 {
		t.Errorf("skips = %+v / %+v", sr.Intro, sr.Outro)
	}
	if sr.Headers["Referer"] != f.base+"/anime/" {
		t.Errorf("Referer = %q, want fixture base", sr.Headers["Referer"])
	}
}

func TestLeeDubOnlyTitleHidesSub(t *testing.T) {
	f := newLeeFixture(t)
	p := newLeeTestProvider(f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Strict per-lang: dub-only ep1 must not surface in a sub listing even
	// though the API would answer with an effectiveLanguage fallback.
	sr, err := p.FindEpisodeSource(ctx, "7", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource sub: %v", err)
	}
	if sr != nil {
		t.Fatalf("dub-only episode must hide sub, got %+v", sr)
	}
}

func TestLeeBadAnilistID(t *testing.T) {
	p := NewLeeProvider(zerolog.Nop(), "", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.FindEpisodeSource(ctx, "xx", 1, "dub"); err == nil {
		t.Fatal("want error for bad anilist id")
	}
}
