package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestClassifySegmentHead pins the magic-byte classification used by the
// image-segment guard. HLS players ignore extensions and sniff bytes, so the
// same is done here: disguised video (.jpg-named MP4/TS) must classify as
// video, real image payloads as image, anything undecidable as unknown.
func TestClassifySegmentHead(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		b    []byte
		want string
	}{
		// Disguised video: extensions say .jpg, bytes say otherwise.
		{"jpg-named mpegts", append([]byte{0x47, 0x40, 0x00, 0x10}, make([]byte, 16)...), "video"},
		{"jpg-named ftyp mp4", append([]byte{0, 0, 0, 0x18}, []byte("ftypisom")...), "video"},
		{"fragmented mp4 moof", []byte("moof...."), "video"},
		{"styp box", append([]byte{0, 0, 0, 0x10}, []byte("stypmsdh")...), "video"},

		// Genuine image payloads.
		{"jpeg", []byte{0xFF, 0xD8, 0xFF, 0xE0}, "image"},
		{"png", []byte{0x89, 'P', 'N', 'G', 0x0D}, "image"},
		{"gif", []byte("GIF89a"), "image"},
		{"webp", append([]byte("RIFF"), append(make([]byte, 4), []byte("WEBP")...)...), "image"},
		{"avif brand", append([]byte{0, 0, 0, 0x18}, []byte("ftypavif")...), "image"},
		{"heic brand", append([]byte{0, 0, 0, 0x18}, []byte("ftypheic")...), "image"},

		// Undecidable → unknown (fail-open).
		{"empty", nil, "unknown"},
		{"text", []byte("hello world"), "unknown"},
	}
	for _, tc := range cases {
		if got := classifySegmentHead(tc.b); got != tc.want {
			t.Errorf("%s: classifySegmentHead = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestFirstPlaylistURIAndResolve pins head parsing and reference resolution.
func TestFirstPlaylistURIAndResolve(t *testing.T) {
	t.Parallel()

	master := "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nv/720p.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2\nv/1080p.m3u8\n"
	if got := firstPlaylistURI([]byte(master)); got != "v/720p.m3u8" {
		t.Errorf("firstPlaylistURI(master) = %q", got)
	}
	media := "#EXTM3U\n#EXTINF:5.0,\n000.jpg?token=x\n"
	if got := firstPlaylistURI([]byte(media)); got != "000.jpg?token=x" {
		t.Errorf("firstPlaylistURI(media) = %q", got)
	}
	if got := firstPlaylistURI([]byte("#EXTM3U\n# only comments\n")); got != "" {
		t.Errorf("firstPlaylistURI(comments) = %q, want empty", got)
	}

	got, ok := resolveReference("https://cdn.example/stream/hls/master.m3u8", "000.jpg")
	if !ok || got != "https://cdn.example/stream/hls/000.jpg" {
		t.Errorf("resolveReference relative = %q ok=%v", got, ok)
	}
	got, ok = resolveReference("https://cdn.example/stream/hls/master.m3u8", "https://other.cdn/x.ts")
	if !ok || got != "https://other.cdn/x.ts" {
		t.Errorf("resolveReference absolute = %q ok=%v", got, ok)
	}
}

// TestAnimeXLoliGroundTruth is an env-guarded live check that documents the
// actual payload type of the loli (Anzu) playlist's first segment. It is the
// authoritative answer to "is Anzu an image slideshow or disguised video":
// the guard keeps the source when bytes say video, drops it when they say
// image, and stays silent (checked=false) when nothing can be proven.
func TestAnimeXLoliGroundTruth(t *testing.T) {
	if os.Getenv("ANIRAKU_LIVE_PROBE") != "1" {
		t.Skip("live probe disabled (set ANIRAKU_LIVE_PROBE=1)")
	}
	p := NewAnimeXProvider(zerolog.Nop(), "")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Direct API call for the loli provider, bypassing nothing — exactly what
	// the site's player does.
	slug, _, err := p.fetchPlyrData(ctx, "1", 1, "sub")
	if err != nil {
		slug = "1"
	}
	apiURL := fmt.Sprintf("%s/rest/api/sources?id=%s&epNum=%d&type=%s&providerId=%s",
		p.apiBase, url.PathEscape(slug), 1, "sub", "loli")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	req.Header.Set("User-Agent", animexPlayerUA)
	req.Header.Set("Referer", animexPlyrBase+"/")
	resp, err := p.client.Do(req)
	if err != nil {
		t.Logf("loli api unreachable: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Logf("loli api HTTP %d", resp.StatusCode)
		return
	}
	var payload struct {
		Sources []struct {
			URL string `json:"url"`
		} `json:"sources"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil || len(payload.Sources) == 0 {
		t.Logf("loli: no decodable sources (%v)", err)
		return
	}
	raw := payload.Sources[0].URL
	if decoded, _, _ := DecodeAnimeXProxyURL(raw); decoded != raw {
		raw = decoded
	}
	for _, rw := range animexGlobalRewrites {
		raw = rw(raw)
	}
	referer := animexProviderDefaultReferer["loli"]
	head, ok := p.fetchHead(ctx, raw, referer, "", 8192)
	if !ok || !strings.Contains(string(head), "#EXTM3U") {
		t.Logf("loli manifest unreachable or not HLS from this egress: %s", raw)
		return
	}
	verdict := p.playlistSegmentVerdict(ctx, raw, referer, "", head, 0)
	switch verdict {
	case "image":
		t.Logf("CONFIRMED: loli first segment is IMAGE data — guard drops it (correct)")
	case "video":
		t.Logf("loli first segment is VIDEO data (disguised as images) — guard keeps it playable")
	case "unreachable":
		t.Logf("loli first segment unreachable from this egress — guard drops it (matches your One Piece 403 log)")
	default:
		t.Logf("loli: undecidable — guard keeps the source (fail-open)")
	}
}
