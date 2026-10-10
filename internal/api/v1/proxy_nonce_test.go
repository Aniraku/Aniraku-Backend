package v1

import (
	"strings"
	"testing"
)

// TestRewriteEmitsStableURLs guards the egress optimisation: rewritten
// child URLs must carry no per-session nonce, so identical upstream bytes
// get identical proxy URLs and a shared edge cache can serve repeat views
// instead of every playback re-burning EC2 egress. Two rewrites of the
// same playlist must be byte-identical.
func TestRewriteEmitsStableURLs(t *testing.T) {
	h := &Handlers{}
	in := "#EXTM3U\n" +
		"#EXT-X-VERSION:3\n" +
		"#EXT-X-TARGETDURATION:10\n" +
		"#EXTINF:10.0,\n" +
		"seg-1.ts\n" +
		"#EXTINF:10.0,\n" +
		"seg-2.ts?token=abc123\n" +
		"#EXT-X-ENDLIST\n"
	base := "https://cdn.example/a/b/master.m3u8"
	first := h.rewriteHLSPlaylist(in, base, `{"Referer":"https://example.com/"}`, "https://api.test", "")
	second := h.rewriteHLSPlaylist(in, base, `{"Referer":"https://example.com/"}`, "https://api.test", "")
	if first != second {
		t.Fatalf("rewrite not stable across calls:\n%s\n---\n%s", first, second)
	}
	if strings.Contains(first, "rn=") {
		t.Errorf("rewritten playlist contains cache-busting rn nonce:\n%s", first)
	}
	if !strings.Contains(first, "/api/v1/proxy?url=") {
		t.Errorf("segments were not proxied:\n%s", first)
	}
	if !strings.Contains(first, "token%3Dabc123") && !strings.Contains(first, "token=abc123") {
		t.Errorf("upstream token lost in rewrite:\n%s", first)
	}
}

// TestStripProxyNoncePreservesSignedQueries guards the two properties of
// the cache-busting strip: our rn param disappears, and every other param
// keeps its exact bytes and order. A ParseQuery+Encode round-trip instead
// reorders params and appends "=" to nameless ones — animeheaven signs
// ?<md5>&<md5> order-sensitively and answers 404 to the re-encoded form
// (observed live: direct fetch 206, through the proxy 404).
func TestStripProxyNoncePreservesSignedQueries(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "nameless signed params untouched",
			in:   "https://ax.animeheaven.me/video.mp4?74073c8cd9cc129bd13fc7407fa81220&6162a8cdcd9b9c5d6162a8cdcd9b9c5d",
			want: "https://ax.animeheaven.me/video.mp4?74073c8cd9cc129bd13fc7407fa81220&6162a8cdcd9b9c5d6162a8cdcd9b9c5d",
		},
		{
			name: "no rn keeps order and encoding",
			in:   "https://cdn.example/play.m3u8?b=2&a=1&c=%2Fx",
			want: "https://cdn.example/play.m3u8?b=2&a=1&c=%2Fx",
		},
		{
			name: "rn removed, survivors byte-identical",
			in:   "https://cdn.example/a/seg.ts?b=2&a=1&rn=1791369826000000000",
			want: "https://cdn.example/a/seg.ts?b=2&a=1",
		},
		{
			name: "rn in front",
			in:   "https://cdn.example/a.m3u8?rn=42&t=abc",
			want: "https://cdn.example/a.m3u8?t=abc",
		},
		{
			name: "rn only drops the query",
			in:   "https://cdn.example/a.m3u8?rn=42",
			want: "https://cdn.example/a.m3u8",
		},
		{
			name: "encoded rn name still recognized",
			in:   "https://cdn.example/a.m3u8?a=1&%72n=9",
			want: "https://cdn.example/a.m3u8?a=1",
		},
		{
			name: "no query unchanged",
			in:   "https://cdn.example/a.m3u8",
			want: "https://cdn.example/a.m3u8",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripProxyNonce(tc.in); got != tc.want {
				t.Errorf("stripProxyNonce(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
