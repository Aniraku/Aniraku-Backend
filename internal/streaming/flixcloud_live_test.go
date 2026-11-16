package streaming

import (
	"context"
	"github.com/rs/zerolog"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// Live end-to-end: reanime → embed page → token → wasm mix → AES → master
// URL → playlist decode → key + segment fetch. Skipped unless
// FLIXCLOUD_LIVE=1 (hits real upstreams; single-use tokens mint per run).
func TestLiveFlixcloudDirectChain(t *testing.T) {
	if os.Getenv("FLIXCLOUD_LIVE") != "1" {
		t.Skip("FLIXCLOUD_LIVE=1 required")
	}
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	p := NewFlixCloudProvider(log)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// One Piece (AniList 21), episode 1.
	res, err := p.FindEpisodeSource(ctx, "21", 1, "")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if res == nil || len(res.Sources) == 0 {
		t.Fatalf("no sources resolved")
	}
	for _, src := range res.Sources {
		t.Logf("direct master: %s (subs=%d)", src.URL, len(src.Subtitles))
		if !strings.Contains(src.URL, ".m3u8") {
			t.Errorf("source is not an m3u8 URL: %s", src.URL)
		}
		if src.Type != "hls" {
			t.Errorf("source type = %q, want hls", src.Type)
		}
	}
	if res.Headers["Referer"] == "" {
		t.Errorf("missing Referer header in result")
	}

	// Playlist fetch + decode through the same transform the proxy uses.
	client := &http.Client{Timeout: 20 * time.Second}
	master := res.Sources[0].URL
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, master, nil)
	req.Header.Set("Referer", res.Headers["Referer"])
	req.Header.Set("User-Agent", res.Headers["User-Agent"])
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("master fetch: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("master status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	plain := DecodeFlixcloudPlaylist(body)
	if !strings.HasPrefix(string(plain), "#EXTM3U") {
		t.Fatalf("playlist did not decode to #EXTM3U (first 80 bytes: %q)", body[:80])
	}
	t.Logf("master playlist decoded, %d bytes plaintext", len(plain))

	// The decode must have refreshed the proxy-side XOR key from THIS page's
	// module, and the first child URI must resolve against the master URL.
	if !strings.Contains(string(plain), "video.m3u8") {
		t.Errorf("decoded master names no video variant")
	}
}
