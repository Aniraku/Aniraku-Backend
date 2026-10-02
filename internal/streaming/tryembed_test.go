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

func TestTryEmbedCuteNames(t *testing.T) {
	cases := map[string]string{"astra": "Astro", "beta": "Beta", "sora": "Skye", "zen": "Zen"}
	for id, want := range cases {
		if got := tryembedCuteName(id); got != want {
			t.Errorf("cuteName(%q) = %q, want %q", id, got, want)
		}
	}
	if got := tryembedCuteName("nope"); got != "" {
		t.Errorf("cuteName(unknown) = %q, want empty", got)
	}
	// "Sora" must never appear: animex owns that name (nico-subtitle rule).
	for _, s := range tryembedServers {
		if s.name == "Sora" {
			t.Errorf("tryembed must not use the Sora display name")
		}
	}
}

func TestTryEmbedPickQuality(t *testing.T) {
	h720, h1080 := 720, 1080
	qs := []tryembedQuality{
		{Name: "720p", Height: &h720, Token: "t720"},
		{Name: "Auto", Token: "tauto"},
		{Name: "1080p", Height: &h1080, Token: "t1080"},
		{Name: "empty", Token: ""},
	}
	if got := tryembedPickQuality(qs); got == nil || got.Token != "tauto" {
		t.Fatalf("want Auto track, got %+v", got)
	}
	qs2 := []tryembedQuality{{Name: "720p", Height: &h720, Token: "t720"}, {Name: "1080p", Height: &h1080, Token: "t1080"}}
	if got := tryembedPickQuality(qs2); got == nil || got.Token != "t1080" {
		t.Fatalf("want tallest track, got %+v", got)
	}
	if got := tryembedPickQuality([]tryembedQuality{{Name: "x"}}); got != nil {
		t.Fatalf("tokenless list must yield nothing, got %+v", got)
	}
}

func TestTryEmbedTicketRegex(t *testing.T) {
	m := tryembedTicketRe.FindStringSubmatch(`window.BOOTSTRAP_TICKET="abc123_";`)
	if len(m) != 2 || m[1] != "abc123_" {
		t.Fatalf("ticket regex = %v", m)
	}
}

// tryembedFixture serves the full player flow: watch page (ticket),
// bootstrap (nonce), per-server stream_data (quality tokens), signed /s/
// URLs (302 to playlist chain with real bytes).
type tryembedFixture struct {
	server *httptest.Server
	base   string
}

func newTryEmbedFixture(t *testing.T) *tryembedFixture {
	t.Helper()
	var base string
	mux := http.NewServeMux()
	mux.HandleFunc("/embed/anime/7/1/sub", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><script>window.BOOTSTRAP_TICKET="fixture-ticket";</script></html>`)
	})
	mux.HandleFunc("/api/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-TryEmbed-Bootstrap") != "fixture-ticket" {
			http.Error(w, "bad ticket", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"embedNonce":"fixture-nonce"}`)
	})
	mux.HandleFunc("/api/stream_data", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("nonce") != "fixture-nonce" || r.Header.Get("X-Embed-Nonce") != "fixture-nonce" {
			http.Error(w, "bad nonce", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch q.Get("server") {
		case "astra":
			fmt.Fprintf(w, `{"providers":[{"id":"astra","name":"Astra","type":"hls","status":"ready","qualities":[{"name":"Auto","token":"TOK-A"}],"captions":[{"label":"English","lang":"en","url":"%s/subs/en.vtt"}]}],"intro":{"start":31,"end":111},"outro":{"start":1376,"end":1447}}`, base)
		case "beta":
			// Offered but tokenless: skipped silently.
			fmt.Fprint(w, `{"providers":[{"id":"beta","name":"Beta","type":"hls","status":"idle","qualities":[]}]}`)
		default:
			fmt.Fprint(w, `{"providers":[]}`)
		}
	})
	mux.HandleFunc("/s/TOK-A.m3u8", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, base+"/signed/master.m3u8?exp=9&sig=x", http.StatusFound)
	})
	mux.HandleFunc("/signed/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\n/media.m3u8\n")
	})
	mux.HandleFunc("/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:10,\n/seg.ts\n")
	})
	mux.HandleFunc("/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	srv := httptest.NewServer(mux)
	base = srv.URL
	t.Cleanup(srv.Close)
	return &tryembedFixture{server: srv, base: base}
}

func TestTryEmbedSubResolve(t *testing.T) {
	f := newTryEmbedFixture(t)
	p := NewTryEmbedProvider(zerolog.Nop(), f.base)
	// Plain transport: netguard SSRF-blocks the 127.0.0.1 fixture (the
	// provider builds session clients off p.transport).
	p.transport = f.server.Client().Transport
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want 1 source (beta tokenless skipped), got %+v", sr)
	}
	src := sr.Sources[0]
	if !strings.HasSuffix(src.URL, "/signed/master.m3u8?exp=9&sig=x") {
		t.Errorf("URL = %q, want followed signed target", src.URL)
	}
	if src.Type != "hls" || src.Quality != "auto" || src.Verification != "proxy" {
		t.Errorf("meta = type %q quality %q verification %q", src.Type, src.Quality, src.Verification)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Astro" {
		t.Errorf("ServerNames = %v, want [Astro]", sr.ServerNames)
	}
	if len(src.Subtitles) != 1 || src.Subtitles[0].Lang != "en" {
		t.Errorf("subtitles = %+v, want 1 English track", src.Subtitles)
	}
	if sr.Intro == nil || sr.Intro.Start != 31 || sr.Outro == nil || sr.Outro.End != 1447 {
		t.Errorf("skips = %+v / %+v", sr.Intro, sr.Outro)
	}
}

func TestTryEmbedGatedSkipsSilently(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/embed/anime/7/1/sub", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><script>window.BOOTSTRAP_TICKET="t";</script></html>`)
	})
	mux.HandleFunc("/api/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"embedNonce":"n"}`)
	})
	mux.HandleFunc("/api/stream_data", func(w http.ResponseWriter, r *http.Request) {
		// Datacenter-egress shape: page+bootstrap pass, stream_data 403s.
		http.Error(w, "Attention Required", http.StatusForbidden)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	p := NewTryEmbedProvider(zerolog.Nop(), srv.URL)
	p.transport = srv.Client().Transport
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "sub")
	if err != nil {
		t.Fatalf("gated resolve must not error, got %v", err)
	}
	if sr != nil {
		t.Fatalf("gated resolve must skip, got %+v", sr)
	}
}
