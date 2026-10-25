package anipm

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestDifferentialEmbedURL is the core reconstruction check: it builds a large,
// deterministic matrix of options, computes the Go EmbedURL for each, runs the
// ORIGINAL SDK (testdata/sdk-v1.js) over the identical JSON through Node, and
// requires every URL and error code/message to match.
//
// Node is required; the test skips (with a clear reason) when it is absent so
// the package still builds and tests offline.
func TestDifferentialEmbedURL(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found in PATH; skipping differential test against the original SDK")
	}

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

	harness := filepath.Join("testdata", "embedurl_harness.js")
	cmd := exec.Command(node, harness, optsFile)
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
	} else {
		t.Logf("all %d cases match the original SDK", len(matrix))
	}
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
