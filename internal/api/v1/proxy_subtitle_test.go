package v1

import (
	"testing"
)

// Pins which URLs the proxy relabels text/vtt: subtitle tracks and caption
// endpoints (moe /captions answers application/octet-stream, which browsers
// download instead of parsing as cues). Video segments, playlists and keys
// must never match.
func TestIsSubtitleURL(t *testing.T) {
	t.Parallel()

	subs := []string{
		"/captions?url=abc123",
		"https://stream.animeparadise.moe/captions?url=xyz",
		"https://cdn.example/subs/eng-2.vtt",
		"https://cdn.example/subtitles/ep1.srt",
		"https://cdn.example/subtitle/track",
	}
	for _, u := range subs {
		if !isSubtitleURL(u) {
			t.Fatalf("isSubtitleURL(%q) = false, want true", u)
		}
	}
	notSubs := []string{
		"/anime/abc/master.m3u8",
		"/seg-1-f1-v1-a1.jpg",
		"/seg-1.ts",
		"/hls/key.key",
		"/stream/ani/21/1/sub",
		"/api/v1/proxy?url=https%3A%2F%2Fx",
		"",
	}
	for _, u := range notSubs {
		if isSubtitleURL(u) {
			t.Fatalf("isSubtitleURL(%q) = true, want false", u)
		}
	}
}
