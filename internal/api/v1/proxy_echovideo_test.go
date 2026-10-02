package v1

import (
	"strings"
	"testing"
)

// Root-relative, relative, protocol-relative and absolute child URIs must
// all resolve exactly like a player (RFC 3986 against the playlist URL).
// String concatenation used to mangle root-relative legs: echovideo
// variant legs (/cdn/<hash> under base https://host/cdn/<hash>?t.m3u8)
// became host/cdn//cdn/<hash>.
func TestResolveURLForms(t *testing.T) {
	base := "https://ru-cdn1.echovideo.to/cdn/abc123?t.m3u8"
	cases := map[string]string{
		"/cdn/def456":                       "https://ru-cdn1.echovideo.to/cdn/def456",
		"seg-1.ts":                          "https://ru-cdn1.echovideo.to/cdn/seg-1.ts",
		"//other.example/x.m3u8":            "https://other.example/x.m3u8",
		"https://cdn.example/absolute.m3u8": "https://cdn.example/absolute.m3u8",
	}
	for in, want := range cases {
		if got := resolveURL(in, base); got != want {
			t.Errorf("resolveURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// isPlaylistBody is the backstop for playlist legs that no URL or
// content-type hint identifies (query-suffixed masters, extension-less
// variant URLs, cloaked image/jpeg types) — and the guard that keeps
// binary tripping a hint out of the playlist rewriter.
func TestIsPlaylistBody(t *testing.T) {
	if !isPlaylistBody([]byte("#EXTM3U\n#EXT-X-VERSION:3\n")) {
		t.Error("plain playlist not detected")
	}
	if !isPlaylistBody([]byte("\n  #EXTM3U\n")) {
		t.Error("playlist with leading whitespace not detected")
	}
	for _, not := range [][]byte{
		[]byte("<html><body>error</body></html>"),
		{0, 0, 0, 24, 'f', 't', 'y', 'p'},
		{},
		[]byte("EXTM3U without the marker"),
	} {
		if isPlaylistBody(not) {
			t.Errorf("non-playlist %q detected as playlist", not)
		}
	}
}

// The exact echovideo master shape: root-relative variant legs under a
// query-suffixed master URL. Every leg must come out proxied-absolute —
// raw or double-pathed legs 404 against our own domain and black out.
func TestRewriteRootRelativeVariantLegs(t *testing.T) {
	h := &Handlers{}
	in := `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=264000,RESOLUTION=640x360,NAME="360"
/cdn/092e3d2d14736a0ad53867907007e779f696322a8909cd71a3d6168d
#EXT-X-STREAM-INF:PROGRAM-ID=1,BANDWIDTH=1828000,RESOLUTION=1920x1080,NAME="1080"
/cdn/092e3d2d14736a0ad538679073b9222b1d092e3d2d14736a0ad5386
`
	out := h.rewriteHLSPlaylist(in, "https://ru-cdn1.echovideo.to/cdn/092e3d2d14736a0ad53867906eb1495fdefdbcea5dbde581b?t.m3u8", `{"Referer":"https://play.echovideo.ru/"}`, "https://api.test", "")
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "https://api.test/api/v1/proxy?url=") {
			t.Errorf("variant leg not proxied: %q", line)
		}
		if strings.Contains(line, "cdn//cdn") {
			t.Errorf("double-pathed leg: %q", line)
		}
	}
	if !strings.Contains(out, "ru-cdn1.echovideo.to%2Fcdn%2F092e3d2d14736a0ad53867907007e779f696322a8909cd71a3d6168d") {
		t.Errorf("360p leg missing from rewrite:\n%s", out)
	}
}
