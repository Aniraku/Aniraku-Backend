package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Live probe: confirm the captcha-retry POST shape against api.mkissa.net.
// Run with MKISSA_CAPTCHA_PROBE=1. Sends the episode query with a valid
// aaReq plus extensions.captcha (dummy token) and prints the server's
// error signature so the shape can be verified without a real Turnstile
// solve.
func TestLiveMkissaCaptchaPOSTShape(t *testing.T) {
	if os.Getenv("MKISSA_CAPTCHA_PROBE") != "1" {
		t.Skip("MKISSA_CAPTCHA_PROBE=1 required")
	}
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	p := NewMkissaProvider(log)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	showID := os.Getenv("MKISSA_PROBE_SHOW")
	if showID == "" {
		showID = "tRZHHbEiL3AzBjZqN" // Inexpressive Kashiwada and Expressive Oota
	}
	epStr := os.Getenv("MKISSA_PROBE_EP")
	if epStr == "" {
		epStr = "1"
	}

	boot, laneKey, err := p.getLane(ctx)
	if err != nil {
		t.Fatalf("getLane: %v", err)
	}
	t.Logf("bootstrap epoch=%d lane=%s", boot.Epoch, boot.K)

	ts := time.Now().UnixMilli() / 300000 * 300000
	aaReq, err := mkissaAaReq(laneKey, boot.Epoch, mkissaBuildID, mkissaEpisodeQueryHash, ts)
	if err != nil {
		t.Fatalf("aaReq: %v", err)
	}

	for _, tc := range []struct {
		name    string
		captcha bool
	}{
		{"no-captcha", false},
		{"dummy-captcha", true},
	} {
		body := map[string]any{
			"variables": map[string]any{
				"showId":          showID,
				"translationType": "sub",
				"episodeString":   epStr,
			},
			"extensions": map[string]any{
				"persistedQuery": map[string]any{"version": 1, "sha256Hash": mkissaEpisodeQueryHash},
				"k":              mkissaLane,
				"aaReq":          aaReq,
			},
		}
		if tc.captcha {
			body["extensions"].(map[string]any)["captcha"] = map[string]any{
				"token":    "dummy.turnstile.token.probe",
				"provider": "turnstile1",
			}
		}
		b, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBase, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Referer", mkissaReferer)
		req.Header.Set("Origin", mkissaOrigin)
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("x-build-id", mkissaBuildID)

		p.acquireAPI(ctx)
		resp, err := p.client.Do(req)
		p.releaseAPI()
		if err != nil {
			t.Fatalf("%s: POST: %v", tc.name, err)
		}
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		pretty := string(rb)
		if len(pretty) > 600 {
			pretty = pretty[:600]
		}
		t.Logf("%s: HTTP %d body: %s", tc.name, resp.StatusCode, pretty)

		var parsed struct {
			Data struct {
				Tobeparsed string `json:"tobeparsed"`
			} `json:"data"`
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		_ = json.Unmarshal(rb, &parsed)
		if len(parsed.Errors) > 0 {
			t.Logf("%s: error messages: %v", tc.name, parsed.Errors[0].Message)
		}
		if parsed.Data.Tobeparsed != "" {
			pt, derr := mkissaDecryptTobeparsed(laneKey, parsed.Data.Tobeparsed)
			if derr != nil {
				t.Logf("%s: tobeparsed decrypt failed: %v", tc.name, derr)
			} else {
				t.Logf("%s: DECRYPTED (%d bytes): %.400s", tc.name, len(pt), string(pt))
			}
		}
	}
}

var _ = fmt.Sprintf // keep fmt imported for future probes

// Live probe: dump every sourceUrl entry (kind/type/url) for an episode.
func TestLiveMkissaDumpSources(t *testing.T) {
	if os.Getenv("MKISSA_CAPTCHA_PROBE") != "1" {
		t.Skip("MKISSA_CAPTCHA_PROBE=1 required")
	}
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	p := NewMkissaProvider(log)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	showID := os.Getenv("MKISSA_PROBE_SHOW")
	if showID == "" {
		showID = "tRZHHbEiL3AzBjZqN"
	}
	if al := strings.TrimSpace(os.Getenv("MKISSA_PROBE_ANILIST")); al != "" {
		id, _ := strconv.Atoi(al)
		sid, _, err := p.resolveShow(ctx, id, "sub")
		if err != nil {
			t.Fatalf("resolveShow: %v", err)
		}
		showID = sid
	}
	epStr := os.Getenv("MKISSA_PROBE_EP")
	if epStr == "" {
		epStr = "1"
	}
	langKey := "sub"

	boot, laneKey, err := p.getLane(ctx)
	if err != nil {
		t.Fatalf("getLane: %v", err)
	}
	ts := time.Now().UnixMilli() / 300000 * 300000
	aaReq, err := mkissaAaReq(laneKey, boot.Epoch, mkissaBuildID, mkissaEpisodeQueryHash, ts)
	if err != nil {
		t.Fatalf("aaReq: %v", err)
	}
	body := map[string]any{
		"variables": map[string]any{"showId": showID, "translationType": langKey, "episodeString": epStr},
		"extensions": map[string]any{
			"persistedQuery": map[string]any{"version": 1, "sha256Hash": mkissaEpisodeQueryHash},
			"k":              mkissaLane,
			"aaReq":          aaReq,
		},
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBase, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Referer", mkissaReferer)
	req.Header.Set("Origin", mkissaOrigin)
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("x-build-id", mkissaBuildID)
	p.acquireAPI(ctx)
	resp, err := p.client.Do(req)
	p.releaseAPI()
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	resp.Body.Close()
	var parsed struct {
		Data struct {
			Tobeparsed string `json:"tobeparsed"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rb, &parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	pt, err := mkissaDecryptTobeparsed(laneKey, parsed.Data.Tobeparsed)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	var dec mkissaDecrypted
	if err := json.Unmarshal(pt, &dec); err != nil {
		t.Fatalf("decrypted decode: %v", err)
	}
	for i, s := range dec.sources() {
		t.Logf("src[%d] kind=%q name=%q sourceName=%q type=%q stype=%q prio=%.2f url=%.140s", i, s.kind(), s.Name, s.SourceName, s.Type, s.Stype, s.Priority, s.directURL())
	}
}
