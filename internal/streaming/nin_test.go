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

// ninFixture serves Supaplay's API shapes (nested z-6, two-step
// episode-by-anilist -> episode-cache) plus playable master->media->seg
// chains and a blocked relay that must drop.
type ninFixture struct {
	server *httptest.Server
}

func newNinFixture(t *testing.T) *ninFixture {
	t.Helper()
	f := &ninFixture{}
	var mux http.ServeMux

	episodeJSON := func(r *http.Request, masterPath string) string {
		host := "http://" + r.Host
		return fmt.Sprintf(`{"success":true,"data":{"success":true,"data":{`+
			`"episodeId":2142,"type":"sub",`+
			`"m3u8":%q,`+
			`"rawM3u8":"https://cdn.imgnex.top/anime/x/61b8/master.m3u8",`+
			`"subtitles":[`+
			`{"file":"https://supaplay.fun/api/subtitle-proxy?url=x","rawFile":"https://1oe.lostproject.club/anime/x/subtitles/eng-2.vtt","label":"English","kind":"captions"},`+
			`{"file":"","rawFile":"","label":"Signs","kind":"signs"}],`+
			`"intro":{"start":31,"end":111},"outro":{"start":1376,"end":1447}}}}`,
			host+masterPath)
	}
	mux.HandleFunc("/api/z-6/21/1/sub", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(episodeJSON(r, "/hls/master.m3u8")))
	})
	// z-6502s (hentai/missing-episode shape) -> forces the two-step fallback.
	mux.HandleFunc("/api/z-6/20/1/sub", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "error code: 502", http.StatusBadGateway)
	})
	mux.HandleFunc("/api/episode-by-anilist/20/1/sub", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"episode_embed_id":"12352","type":"sub","anilist_id":20}`))
	})
	mux.HandleFunc("/api/episode-cache/12352/sub", func(w http.ResponseWriter, r *http.Request) {
		host := "http://" + r.Host
		_, _ = w.Write([]byte(fmt.Sprintf(`{"success":true,"data":{`+
			`"episodeId":12352,"type":"sub","m3u8":%q,`+
			`"subtitles":[{"file":"","rawFile":"https://raw.host/eng.vtt","label":"English"}]}}`,
			host+"/hls2/master.m3u8")))
	})
	// Relay whose manifest is an HTML block page: must drop, both UAs.
	mux.HandleFunc("/api/z-6/16498/1/sub", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(episodeJSON(r, "/blocked/master.m3u8")))
	})

	chain := func(master, media, seg string) {
		mux.HandleFunc(master, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n" + media + "\n"))
		})
		mux.HandleFunc(media, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1\n" + seg + "\n"))
		})
		mux.HandleFunc(seg, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("G@videobytes-ok"))
		})
	}
	chain("/hls/master.m3u8", "/hls/media.m3u8", "/hls/seg.ts")
	chain("/hls2/master.m3u8", "/hls2/media.m3u8", "/hls2/seg.ts")
	mux.HandleFunc("/blocked/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>cf block page</html>"))
	})

	f.server = httptest.NewServer(&mux)
	t.Cleanup(f.server.Close)
	return f
}

func newNinTestProvider(t *testing.T, f *ninFixture) *NiNProvider {
	t.Helper()
	t.Setenv("ANIRAKU_SUPAPLAY_BASE", f.server.URL)
	p := NewNiNProvider(zerolog.Nop())
	// Plain client: the netguard transport SSRF-blocks the 127.0.0.1
	// fixture server (same override pattern as the other provider tests).
	p.client = f.server.Client()
	return p
}

func TestNiNZ6Resolve(t *testing.T) {
	f := newNinFixture(t)
	p := newNinTestProvider(t, f)
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
		t.Errorf("source URL = %q, want /hls/master.m3u8", src.URL)
	}
	if src.Type != "hls" || src.Quality != "auto" || src.Verification != "proxy" {
		t.Errorf("source meta = type %q quality %q verification %q", src.Type, src.Quality, src.Verification)
	}
	if got, want := sr.ServerNames, []string{"NiN"}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("ServerNames = %v, want [NiN]", got)
	}
	if ref := sr.Headers["Referer"]; ref != f.server.URL+"/" {
		t.Errorf("Referer = %q, want %q", ref, f.server.URL+"/")
	}
	// Supaplay's own tracks are dead from every path: the provider ships
	// NONE even though the payload carries them — the fan-out borrows
	// Niko/Mochi/Zoko subs instead (mergeNiNSubtitles).
	if len(src.Subtitles) != 0 {
		t.Errorf("subtitles = %+v, want none (borrowed at fan-out level)", src.Subtitles)
	}
	if sr.Intro == nil || sr.Intro.Start != 31 || sr.Intro.End != 111 {
		t.Errorf("Intro = %+v, want 31..111", sr.Intro)
	}
	if sr.Outro == nil || sr.Outro.Start != 1376 || sr.Outro.End != 1447 {
		t.Errorf("Outro = %+v, want 1376..1447", sr.Outro)
	}
}

func TestNiNEpisodeCacheFallback(t *testing.T) {
	f := newNinFixture(t)
	p := newNinTestProvider(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "20", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want fallback source, got %+v", sr)
	}
	if !strings.HasSuffix(sr.Sources[0].URL, "/hls2/master.m3u8") {
		t.Errorf("source URL = %q, want /hls2/master.m3u8", sr.Sources[0].URL)
	}
}

func TestNiNBlockedRelayDropped(t *testing.T) {
	f := newNinFixture(t)
	p := newNinTestProvider(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "16498", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr != nil {
		t.Fatalf("blocked relay must drop, got %+v", sr)
	}
}

func mkNinServer(name, provider string, subs []core.Subtitle) core.Server {
	return core.Server{
		Name:     name,
		Provider: provider,
		Sources:  []core.Source{{URL: "https://cdn.example/master.m3u8", Type: "hls", Subtitles: subs}},
	}
}

func TestMergeNiNSubtitles(t *testing.T) {
	nikoSubs := []core.Subtitle{{URL: "https://niko.example/en.vtt", Lang: "en", Label: "English"}}
	mochiSubs := []core.Subtitle{{URL: "https://mochi.example/en.vtt", Lang: "en", Label: "English"}}
	zokoSubs := []core.Subtitle{{URL: "https://zoko.example/en.vtt", Lang: "en", Label: "English"}}
	// Fresh per case: the merge writes into the NiN sources in place.
	nn := func() []core.Server {
		return []core.Server{mkNinServer("NiN", "nin", nil)}
	}

	// Priority: Niko wins when present.
	out := mergeNiNSubtitles(nn(),
		[]core.Server{mkNinServer("Niko", "anikoto", nikoSubs)},
		[]core.Server{mkNinServer("Mochi", "animex", mochiSubs)},
		[]core.Server{mkNinServer("Zoko", "zoko", zokoSubs)})
	if got := out[0].Sources[0].Subtitles; len(got) != 1 || got[0].URL != nikoSubs[0].URL {
		t.Fatalf("with Niko present: subs = %+v, want Niko's", got)
	}

	// Niko missing -> animex (Mochi) next.
	out = mergeNiNSubtitles(nn(), nil,
		[]core.Server{mkNinServer("Mochi", "animex", mochiSubs)},
		[]core.Server{mkNinServer("Zoko", "zoko", zokoSubs)})
	if got := out[0].Sources[0].Subtitles; len(got) != 1 || got[0].URL != mochiSubs[0].URL {
		t.Fatalf("without Niko: subs = %+v, want Mochi's", got)
	}

	// Only Zoko left.
	out = mergeNiNSubtitles(nn(), nil, nil, []core.Server{mkNinServer("Zoko", "zoko", zokoSubs)})
	if got := out[0].Sources[0].Subtitles; len(got) != 1 || got[0].URL != zokoSubs[0].URL {
		t.Fatalf("only Zoko: subs = %+v, want Zoko's", got)
	}

	// No donor anywhere: NiN keeps shipping nothing (never its own).
	out = mergeNiNSubtitles(nn(), nil, nil, nil)
	if got := out[0].Sources[0].Subtitles; len(got) != 0 {
		t.Fatalf("no donor: subs = %+v, want none", got)
	}
}
