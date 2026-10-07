package v1

import "testing"

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
