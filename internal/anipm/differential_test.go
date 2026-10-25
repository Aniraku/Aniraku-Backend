package anipm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// The reference SDK is NOT vendored in this repository. It is either a local,
// git-ignored copy at sdkLocal or fetched from sdkURL, and in both cases it must
// hash to sdkSHA256 before it is trusted. Update the pin deliberately if ani.pm
// ships a new bundle.
const (
	sdkURL    = "https://ani.pm/embed/sdk/v1.js"
	sdkSHA256 = "48f6b2dd442fdc366b641e99df4caeac661ffa0a6eb46d9e7a68cf665d4f4109"
	sdkLocal  = "testdata/sdk-v1.js"
	harnessJS = "testdata/embedurl_harness.js"
)

// TestDifferentialEmbedURL is the core reconstruction check: it builds a large,
// deterministic matrix of options, computes the Go EmbedURL for each, runs the
// ORIGINAL SDK over the identical JSON through Node, and requires every URL and
// error code/message to match.
//
// Node is required. The test skips (with a clear reason) when Node is absent, or
// when the SDK is neither available locally nor fetchable — the repository
// carries no third-party bundle, so an offline run without a local copy simply
// skips rather than fail.
func TestDifferentialEmbedURL(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found in PATH; skipping differential test against the original SDK")
	}
	sdkPath := resolveSDK(t)

	matrix := embedURLMatrix()

	opts, err := json.Marshal(matrix)
	if err != nil {
		t.Fatalf("marshal matrix: %v", err)
	}
	dir := t.TempDir()
	optsFile := filepath.Join(dir, "options.json")
	if err := os.WriteFile(optsFile, opts, 0o600); err != nil {
		t.Fatalf("write options: %v", err)
	}

	cmd := exec.Command(node, harnessJS, sdkPath, optsFile)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("node harness: %v\n%s", err, out)
	}

	var results []struct {
		URL   string `json:"url"`
		Error *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(out, &results); err != nil {
		t.Fatalf("decode harness output: %v\n%s", err, out)
	}
	if len(results) != len(matrix) {
		t.Fatalf("harness returned %d results for %d cases", len(results), len(matrix))
	}

	mismatches := 0
	for i, o := range matrix {
		gotURL, gotErr := EmbedURL(o)
		want := results[i]

		if want.Error == nil {
			if gotErr != nil {
				mismatches++
				t.Errorf("case %d %s: Go errored (%v) but SDK returned %q", i, jmarshal(o), gotErr, want.URL)
				continue
			}
			if gotURL != want.URL {
				mismatches++
				t.Errorf("case %d %s:\n  SDK  %q\n  Go   %q", i, jmarshal(o), want.URL, gotURL)
			}
			continue
		}

		if gotErr == nil {
			mismatches++
			t.Errorf("case %d %s: SDK errored (%s) but Go returned %q", i, jmarshal(o), want.Error.Code, gotURL)
			continue
		}
		e, ok := gotErr.(*Error)
		if !ok {
			mismatches++
			t.Errorf("case %d %s: Go error type %T", i, jmarshal(o), gotErr)
			continue
		}
		if e.Code != want.Error.Code || e.Message != want.Error.Message {
			mismatches++
			t.Errorf("case %d %s:\n  SDK  %s: %s\n  Go   %s: %s",
				i, jmarshal(o), want.Error.Code, want.Error.Message, e.Code, e.Message)
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d/%d cases diverged from the original SDK", mismatches, len(matrix))
	}
	t.Logf("all %d cases match the original SDK", len(matrix))
}

// resolveSDK returns a path to a SHA-256-verified copy of the SDK: a local,
// git-ignored testdata/sdk-v1.js when present, otherwise a fetch written to a
// temp dir. A hash mismatch is fatal (a changed upstream must be reviewed), a
// missing/local-offline case skips.
func resolveSDK(t *testing.T) string {
	t.Helper()
	if data, err := os.ReadFile(sdkLocal); err == nil {
		if got := sha256Hex(data); got != sdkSHA256 {
			t.Fatalf("%s SHA-256 = %s, want %s (delete the stale copy; the test will re-fetch)", sdkLocal, got, sdkSHA256)
		}
		abs, err := filepath.Abs(sdkLocal)
		if err != nil {
			t.Fatal(err)
		}
		return abs
	}

	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodGet, sdkURL, nil)
	if err != nil {
		t.Skipf("SDK fetch request failed (%v); skipping differential test", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/javascript,text/javascript,*/*")
	resp, err := client.Do(req)
	if err != nil {
		t.Skipf("SDK not available locally and fetch failed (%v); skipping differential test", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("SDK fetch returned HTTP %d; skipping differential test", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Skipf("SDK fetch read failed (%v); skipping differential test", err)
	}
	if got := sha256Hex(data); got != sdkSHA256 {
		t.Fatalf("fetched SDK SHA-256 = %s, want %s (ani.pm changed the bundle; update the pin deliberately)", got, sdkSHA256)
	}
	path := filepath.Join(t.TempDir(), "sdk-v1.js")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fetched SDK: %v", err)
	}
	return path
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func jmarshal(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// embedURLMatrix builds a deterministic, reproducible set of option objects: a
// full cross-product of the id/episode/language axes plus a seeded random subset
// of the remaining option fields.
func embedURLMatrix() []Options {
	animes := []any{154587, "154587", "mal-52991", 52991, "0", "mal-0", "1234567890", true, "", " 154587 ", 9999999999999}
	episodes := []any{nil, 1, 0, -1, 1.5, "3", "", "abc", true, "1e2", 100}
	langs := []any{nil, "dub", "sub", "DUB", 1}
	boolPool := []any{nil, true, false, "true", "1", "0", 0, 1, 2, "yes"}
	startPool := []any{nil, 0, -5, 10, "10.9", "0", "abc", true, "3600"}
	colorPool := []any{nil, "#a1b2c3", "a1b2c3", "#ABC", "zzzzzz", "#12345", "#1234567", "A1B2C3"}
	subPool := []any{nil, "", "English", "English (US)", "日本語", "a~b*c'd"}
	apiPool := []any{nil, true, false, "false", 0}
	originPool := []any{nil, "", "https://cdn.example.com", "https://cdn.example.com///", "http://localhost:8080"}
	adultPool := []any{nil, true, false, "true", "false", "0", 0, 1, "yes"}
	keyPool := []any{nil, "", "pk_live_" + rep("a", 24), "pk_live_short", "pk_live_" + rep("a", 23), "pk_live_" + rep("Z", 24)}

	var out []Options
	for _, a := range animes {
		for _, e := range episodes {
			for _, l := range langs {
				out = append(out, Options{Anime: a, Episode: e, Lang: l})
			}
		}
	}

	// Seeded LCG for a reproducible random subset of the remaining fields.
	seed := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 {
		seed = seed*6364136223846793005 + 1442695040888963407
		return seed >> 33
	}
	pick := func(pool []any) any { return pool[next()%uint64(len(pool))] }
	maybe := func(pool []any) any {
		if next()%2 == 0 {
			return nil
		}
		return pick(pool)
	}

	for i := 0; i < 2000; i++ {
		out = append(out, Options{
			Key:       maybe(keyPool),
			Anime:     pick(animes),
			Episode:   pick(episodes),
			Lang:      maybe(langs),
			Hardsub:   maybe(boolPool),
			Start:     maybe(startPool),
			Autoplay:  maybe(boolPool),
			Muted:     maybe(boolPool),
			Poster:    maybe(boolPool),
			AutoNext:  maybe(boolPool),
			AutoSkip:  maybe(boolPool),
			Theater:   maybe(boolPool),
			Episodes:  maybe(boolPool),
			Color:     maybe(colorPool),
			Subtitles: maybe(subPool),
			API:       maybe(apiPool),
			Origin:    maybe(originPool),
			Adult:     maybe(adultPool),
		})
	}
	return out
}
