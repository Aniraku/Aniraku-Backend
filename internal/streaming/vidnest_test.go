package streaming

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// vidnestEncode mirrors the watch page codec (test-side only): standard
// base64 translated into the shuffled alphabet.
func vidnestEncode(raw []byte) string {
	const std = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
	enc := base64.StdEncoding.EncodeToString(raw)
	var b strings.Builder
	for _, r := range enc {
		if r == '=' {
			b.WriteRune('=')
			continue
		}
		b.WriteByte(vidnestAlphabet[strings.IndexRune(std, r)])
	}
	return b.String()
}

func TestVidNestDecodeRoundTrip(t *testing.T) {
	want := `{"sources":[{"file":"https://cdn.example/m/master.m3u8","type":"hls"}],"status":200,"success":true}`
	raw, err := vidnestDecode(vidnestEncode([]byte(want)))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(raw) != want {
		t.Fatalf("round trip = %q, want %q", raw, want)
	}
	// The exact production alphabet must survive refactors: this vector
	// was captured live from new.vidnest.fun.
	if _, err := vidnestDecode("lOyGDqyzDTEhETEU"); err != nil {
		t.Fatalf("live vector decode: %v", err)
	}
	if _, err := vidnestDecode("!!!not-base64!!!"); err == nil {
		t.Fatal("garbage must fail decode")
	}
	if _, err := vidnestDecode(""); err == nil {
		t.Fatal("empty payload must fail decode")
	}
}

func TestVidNestBadAnilistID(t *testing.T) {
	p := NewVidNestProvider(zerolog.Nop(), "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := p.FindEpisodeSource(ctx, "abc", 1, "sub"); err == nil {
		t.Fatal("want error for bad anilist id")
	}
}

// vidnestFixture serves the VidNest API (custom-b64 envelope) plus an HLS
// chain with real playlist/segment bytes.
type vidnestFixture struct {
	server *httptest.Server
	base   string
}

func newVidNestFixture(t *testing.T) *vidnestFixture {
	t.Helper()
	var base string
	mux := http.NewServeMux()
	mux.HandleFunc("/hianime/anime/7/1/sub/hd-2", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Referer"), "https://vidnest.fun/") && !strings.HasPrefix(r.Referer(), base) {
			http.Error(w, "bad referer", http.StatusForbidden)
			return
		}
		payload, _ := json.Marshal(map[string]any{
			"sources": []any{map[string]any{"file": base + "/m/master.m3u8", "type": "hls"}},
			"tracks":  []any{map[string]any{"file": base + "/subs/en.vtt", "label": "English", "lang": "en"}},
			"intro":   map[string]any{"start": 31, "end": 111},
			"outro":   map[string]any{"start": 1376, "end": 1447},
			"status":  200, "success": true,
		})
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":%q,"encrypted":true}`, vidnestEncode(payload))
	})
	mux.HandleFunc("/m/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
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
	return &vidnestFixture{server: srv, base: base}
}

func TestVidNestSubResolve(t *testing.T) {
	f := newVidNestFixture(t)
	p := NewVidNestProvider(zerolog.Nop(), f.base)
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
		t.Errorf("URL = %q, want fixture master", src.URL)
	}
	if src.Type != "hls" || src.Quality != "auto" || src.Verification != "proxy" {
		t.Errorf("meta = type %q quality %q verification %q", src.Type, src.Quality, src.Verification)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Nest" {
		t.Errorf("ServerNames = %v, want [Nest]", sr.ServerNames)
	}
	if len(src.Subtitles) != 1 || src.Subtitles[0].Lang != "en" {
		t.Errorf("subtitles = %+v, want 1 English track", src.Subtitles)
	}
	if sr.Intro == nil || sr.Intro.Start != 31 || sr.Outro == nil || sr.Outro.End != 1447 {
		t.Errorf("skips = %+v / %+v", sr.Intro, sr.Outro)
	}
	if sr.Headers["Referer"] != vidnestReferer {
		t.Errorf("Referer = %q, want %q", sr.Headers["Referer"], vidnestReferer)
	}
}

func TestVidNestGatedSkipsSilently(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Datacenter-egress shape: Cloudflare 403 on the API.
		http.Error(w, "Attention Required", http.StatusForbidden)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	p := NewVidNestProvider(zerolog.Nop(), srv.URL)
	p.client = srv.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "sub")
	if err != nil {
		t.Fatalf("gated resolve must not error, got %v", err)
	}
	if sr != nil {
		t.Fatalf("gated resolve must skip, got %+v", sr)
	}
}
