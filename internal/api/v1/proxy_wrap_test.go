package v1

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// Regression: proxySources used to rewrite source URLs in place. Provider
// resolve caches (kaa, animex/anikoto stale) share their SourceResult
// backing arrays across requests, so the first request's proxyBase + al
// froze into every later response, production sub requests served local
// 127.0.0.1 links with al=dub. Wrapping must copy and leave the input
// untouched, so each request gets its own base + al.
func TestProxySourcesNoCrossRequestPoison(t *testing.T) {
	srcs := []core.Source{{
		URL: "https://hls.krussdomi.com/manifest/x/master.m3u8", Type: "hls", Quality: "auto",
		Subtitles: []core.Subtitle{{URL: "https://cdn.example/subs/en.vtt", Lang: "en", Label: "English"}},
	}}
	headers := map[string]string{"Referer": "https://upstream.example/"}

	local := httptest.NewRequest("GET", "http://127.0.0.1:43211/api/v1/servers?animeId=1&episode=1&lang=dub", nil)
	dub := proxySources(local, srcs, headers, "dub")

	prod := httptest.NewRequest("GET", "https://api.aniraku.tech/api/v1/servers?animeId=1&episode=1&lang=sub", nil)
	sub := proxySources(prod, srcs, headers, "sub")
	if strings.Contains(srcs[0].URL, "/api/v1/proxy?") {
		t.Fatalf("input mutated: %q", srcs[0].URL)
	}
	if !strings.HasPrefix(dub[0].URL, "http://127.0.0.1:43211/api/v1/proxy?") || !strings.Contains(dub[0].URL, "al=dub") {
		t.Fatalf("dub wrap = %q, want local base + al=dub", dub[0].URL)
	}
	if !strings.HasPrefix(sub[0].URL, "https://api.aniraku.tech/api/v1/proxy?") || !strings.Contains(sub[0].URL, "al=sub") {
		t.Fatalf("sub wrap = %q, want prod base + al=sub", sub[0].URL)
	}
	// Subtitles ride the same proxy as the video (url= + headers=), so the
	// client fetches them with the provider's sealed UA/Referer instead of
	// a bare CDN URL the CDN answers with 403.
	subURL := sub[0].Subtitles[0].URL
	if !strings.HasPrefix(subURL, "https://api.aniraku.tech/api/v1/proxy?url=") ||
		!strings.Contains(subURL, url.QueryEscape("https://cdn.example/subs/en.vtt")) ||
		!strings.Contains(subURL, "headers=") {
		t.Fatalf("subtitle not proxied: %q", subURL)
	}
	// The provider's cached track slice must stay raw (shallow copy would
	// have written the proxy URL into it).
	if srcs[0].Subtitles[0].URL != "https://cdn.example/subs/en.vtt" {
		t.Fatalf("input subtitle mutated: %q", srcs[0].Subtitles[0].URL)
	}
	// Wrapping the same input again must not double-wrap.
	again := proxySources(prod, sub, headers, "sub")
	if strings.Count(again[0].URL, "/api/v1/proxy?") != 1 {
		t.Fatalf("double wrap: %q", again[0].URL)
	}
	if strings.Count(again[0].Subtitles[0].URL, "/api/v1/proxy?") != 1 {
		t.Fatalf("subtitle double wrap: %q", again[0].Subtitles[0].URL)
	}
}

// Relative and non-http subtitle URLs cannot be fetched upstream, and a
// nested proxy URL must not be wrapped again: both pass through untouched
// instead of producing a proxy link that 400s or loops back to 127.0.0.1.
func TestProxySourcesSubtitlePassthrough(t *testing.T) {
	req := httptest.NewRequest("GET", "https://api.aniraku.tech/api/v1/servers?lang=sub", nil)
	srcs := []core.Source{{
		URL: "https://hls.krussdomi.com/master.m3u8", Type: "hls",
		Subtitles: []core.Subtitle{
			{URL: "/subs/relative.vtt", Lang: "en"},
			{URL: "https://api.aniraku.tech/api/v1/proxy?url=https%3A%2F%2Fcdn.example%2Fen.vtt", Lang: "en"},
		},
	}}
	got := proxySources(req, srcs, nil, "sub")
	for i, sub := range got[0].Subtitles {
		if sub.URL != srcs[0].Subtitles[i].URL {
			t.Fatalf("subtitle %d rewritten: %q", i, sub.URL)
		}
	}
}

// mp4 direct sources (mp4upload, animex variants) wrap through the
// proxy like hls, never raw, but carry no al (single-audio muxed).
func TestProxySourcesWrapsMP4WithoutAl(t *testing.T) {
	req := httptest.NewRequest("GET", "https://api.aniraku.tech/api/v1/servers?lang=dub", nil)
	srcs := []core.Source{{URL: "https://www.mp4upload.com/file.mp4", Type: "mp4", Quality: "auto"}}
	got := proxySources(req, srcs, map[string]string{"Referer": "https://x/"}, "dub")
	if !strings.HasPrefix(got[0].URL, "https://api.aniraku.tech/api/v1/proxy?url=") {
		t.Fatalf("mp4 not wrapped: %q", got[0].URL)
	}
	if strings.Contains(got[0].URL, "&al=") {
		t.Fatalf("mp4 must not carry al: %q", got[0].URL)
	}
	if strings.Contains(srcs[0].URL, "/api/v1/proxy?") {
		t.Fatal("input mutated")
	}
}
