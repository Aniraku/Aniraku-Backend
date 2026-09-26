package streaming

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// TestAnimeXBlockedProvidersFiltered pins that the loli (Anzu) sub-provider —
// which serves image-segment playlists and does not play — is filtered from
// every provider list before any network call, while playable providers
// (yuki/Mochi, beep/Lumi, neko/Chibi, sora/Sora, zuna/Kira) are never touched.
func TestAnimeXBlockedProvidersFiltered(t *testing.T) {
	t.Parallel()

	got := filterBlockedProviders([]string{"beep", "yuki", "neko", "sora", "loli"})
	want := []string{"beep", "yuki", "neko", "sora"}
	if len(got) != len(want) {
		t.Fatalf("filterBlockedProviders = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("filterBlockedProviders = %v, want %v", got, want)
		}
	}
	if got := filterBlockedProviders(nil); len(got) != 0 {
		t.Fatalf("filterBlockedProviders(nil) = %v, want empty", got)
	}
	if got := filterBlockedProviders([]string{"loli"}); len(got) != 0 {
		t.Fatalf("filterBlockedProviders([loli]) = %v, want empty", got)
	}
}

// TestAnimeXProviderTimeoutCapsHang pins the per-provider ceiling: a
// blackholed sub-provider API (accepts the connection, never responds —
// the classic egress-blocked edge) must be cut at ~10s, not the shared
// client's 45s, so one hung provider cannot hold a semaphore slot and the
// whole tail that long.
func TestAnimeXProviderTimeoutCapsHang(t *testing.T) {
	old := animexProviderTimeout
	animexProviderTimeout = 300 * time.Millisecond
	t.Cleanup(func() { animexProviderTimeout = old })

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(60 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer server.Close()

	p := NewAnimeXProvider(zerolog.Nop(), server.URL)
	p.client = server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	_, err := p.resolveProvider(ctx, "21", 1, "sub", "beep")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("hanging provider must error")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("hung provider took %v, want the ~300ms test cap", elapsed)
	}
	t.Logf("hung provider cut after %v", elapsed.Round(100*time.Millisecond))
}

// TestProbeFirstSegment pins the lenient honesty rule: definitive blocks
// (unreachable, 403, HTML error pages) drop the source, but ambiguous
// payloads (image cloaks some CDNs serve probes while playing video fine)
// keep it listed — worst case is today's behavior, never a regression on
// a working stream. probeMux lives in anikoto_probe_test.go (same package).
func TestProbeFirstSegment(t *testing.T) {
	tsSeg := append([]byte{0x47}, []byte("video-bytes-here-ok")...)
	htmlSeg := []byte("<!DOCTYPE html><html><head><title>403</title></head></html>")
	pngSeg := append([]byte("\x89PNG\r\n\x1a\n"), []byte("cloak")...)

	newProvider := func(server *httptest.Server) *AnimeXProvider {
		p := NewAnimeXProvider(zerolog.Nop(), server.URL)
		p.client = server.Client()
		return p
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("playable chain", func(t *testing.T) {
		server := probeMux(200, tsSeg)
		defer server.Close()
		if !newProvider(server).probeFirstSegment(ctx, server.URL+"/master.m3u8", server.URL, "") {
			t.Fatal("healthy chain must pass")
		}
	})
	t.Run("blocked segment", func(t *testing.T) {
		server := probeMux(403, htmlSeg)
		defer server.Close()
		if newProvider(server).probeFirstSegment(ctx, server.URL+"/master.m3u8", server.URL, "") {
			t.Fatal("403 HTML segment must drop the source")
		}
	})
	t.Run("ambiguous payload kept", func(t *testing.T) {
		server := probeMux(200, pngSeg)
		defer server.Close()
		if !newProvider(server).probeFirstSegment(ctx, server.URL+"/master.m3u8", server.URL, "") {
			t.Fatal("non-HTML payload must keep the source listed (no regression)")
		}
	})
}

// TestShouldRetryProvider pins the retry policy: fast real errors (edge
// flaps) get one more chance, slow failures and legitimate empties don't.
func TestShouldRetryProvider(t *testing.T) {
	t.Parallel()

	if !shouldRetryProvider(errTestStr("boom"), 500*time.Millisecond) {
		t.Fatal("fast error must retry")
	}
	if shouldRetryProvider(errTestStr("boom"), 5*time.Second) {
		t.Fatal("slow failure must not retry (avoid piling onto hangs)")
	}
	if shouldRetryProvider(nil, 100*time.Millisecond) {
		t.Fatal("nil error (legitimate empty) must not retry")
	}
}

// TestAnimeXStaleCache pins the flicker guard: a failed fresh resolve
// serves the last good result while fresh, refuses expired entries, and
// never shares mutable state with live results.
func TestAnimeXStaleCache(t *testing.T) {
	t.Parallel()

	mkRes := func(url string) *SourceResult {
		return &SourceResult{
			Sources: []core.Source{{URL: url, Subtitles: []core.Subtitle{{URL: url + ".vtt"}}}},
			Headers: map[string]string{"Referer": "https://x/"},
		}
	}
	t.Run("store then serve", func(t *testing.T) {
		t.Parallel()
		p := NewAnimeXProvider(zerolog.Nop(), "")
		p.storeStale("k", mkRes("http://x/a.m3u8"))
		got := p.loadStale("k")
		if got == nil || len(got.Sources) != 1 || got.Sources[0].URL != "http://x/a.m3u8" {
			t.Fatalf("loadStale = %+v", got)
		}
	})
	t.Run("expired refuses", func(t *testing.T) {
		t.Parallel()
		p := NewAnimeXProvider(zerolog.Nop(), "")
		p.staleMu.Lock()
		p.stale["k"] = &animexStaleEntry{res: mkRes("http://x/a.m3u8"), fetchedAt: time.Now().Add(-animexStaleTTL - time.Minute)}
		p.staleMu.Unlock()
		if got := p.loadStale("k"); got != nil {
			t.Fatalf("expired entry must refuse, got %+v", got)
		}
		p.staleMu.Lock()
		_, stillThere := p.stale["k"]
		p.staleMu.Unlock()
		if stillThere {
			t.Fatal("expired entry must be deleted on read")
		}
	})
	t.Run("missing is nil", func(t *testing.T) {
		t.Parallel()
		p := NewAnimeXProvider(zerolog.Nop(), "")
		if got := p.loadStale("nope"); got != nil {
			t.Fatalf("missing entry must be nil, got %+v", got)
		}
	})
	t.Run("stored copy is isolated", func(t *testing.T) {
		t.Parallel()
		p := NewAnimeXProvider(zerolog.Nop(), "")
		orig := mkRes("http://x/a.m3u8")
		p.storeStale("k", orig)
		orig.Sources[0].URL = "MUTATED"
		orig.Headers["Referer"] = "MUTATED"
		orig.Sources[0].Subtitles[0].URL = "MUTATED"
		got := p.loadStale("k")
		if got == nil || got.Sources[0].URL != "http://x/a.m3u8" ||
			got.Headers["Referer"] != "https://x/" ||
			got.Sources[0].Subtitles[0].URL != "http://x/a.m3u8.vtt" {
			t.Fatalf("stored copy mutated: %+v", got)
		}
	})
	t.Run("key format", func(t *testing.T) {
		t.Parallel()
		if got := animexStaleKey("21", 1, "sub", "yuki"); got != "21/1/sub/yuki" {
			t.Fatalf("key = %q", got)
		}
	})
}

// TestAnimeXPlayableProvidersNotBlocked guards against future foot-guns:
// every known-playable sub-provider must stay out of the blocklist.
func TestAnimeXPlayableProvidersNotBlocked(t *testing.T) {
	t.Parallel()

	for _, playable := range []string{"beep", "yuki", "neko", "sora", "zuna", "uwu"} {
		if animexBlockedProviders[playable] {
			t.Errorf("playable provider %q must not be in animexBlockedProviders", playable)
		}
	}
}

// TestSanitizeSubtitleURL pins the repair of malformed upstream subtitle
// URLs (empty host from a dropped slash) and the rejection of unusable ones
// so broken tracks never list.
func TestSanitizeSubtitleURL(t *testing.T) {
	t.Parallel()

	if got, ok := sanitizeSubtitleURL("https:///subbl.krussdomi.com/679705bd169c31976bd92812/1617629103629_th.srt"); !ok || got != "https://subbl.krussdomi.com/679705bd169c31976bd92812/1617629103629_th.srt" {
		t.Fatalf("repair = %q, %v", got, ok)
	}
	good := []string{
		"https://cdn.example/subs/eng-2.vtt",
		"https://stream.animeparadise.moe/captions?url=abc",
		"//cdn.example/subs/x.vtt",
	}
	for _, u := range good {
		if _, ok := sanitizeSubtitleURL(u); !ok {
			t.Fatalf("sanitizeSubtitleURL(%q) rejected a good URL", u)
		}
	}
	bad := []string{
		"",
		"   ",
		"/subs/x.vtt",
		"subs/x.vtt",
		"ftp://cdn.example/x.vtt",
		"https://",
		":://",
	}
	for _, u := range bad {
		if got, ok := sanitizeSubtitleURL(u); ok {
			t.Fatalf("sanitizeSubtitleURL(%q) = %q, want rejection", u, got)
		}
	}
}
