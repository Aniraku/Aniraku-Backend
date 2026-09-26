package v1

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want time.Duration
	}{
		{name: "delta seconds", in: "60", want: 60 * time.Second},
		{name: "with whitespace", in: "  30 ", want: 30 * time.Second},
		{name: "empty", in: "", want: 0},
		{name: "garbage", in: "soon", want: 0},
		{name: "http-date form unsupported", in: "Wed, 21 Oct 2015 07:28:00 GMT", want: 0},
		{name: "zero", in: "0", want: 0},
		{name: "negative", in: "-5", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRetryAfter(tc.in); got != tc.want {
				t.Fatalf("parseRetryAfter(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestAniListHTTPErrorWait(t *testing.T) {
	// Error text keeps the historical "anilist returned %d: %s" shape the
	// string-matchers (isRetryable/isAniListAuth) rely on.
	err := &anilistHTTPError{status: 429, retryAfter: 60 * time.Second, body: `{"errors":[]}`}
	if !strings.Contains(err.Error(), "anilist returned 429") {
		t.Fatalf("Error() = %q, want it to contain the status", err.Error())
	}
	if got := err.waitAfter429(); got != 60*time.Second {
		t.Fatalf("waitAfter429() = %v, want 60s (server header)", got)
	}
	noHeader := &anilistHTTPError{status: 429, body: "x"}
	if got := noHeader.waitAfter429(); got != 60*time.Second {
		t.Fatalf("waitAfter429() without header = %v, want 60s default", got)
	}
	notRatelimit := &anilistHTTPError{status: 500, retryAfter: 60 * time.Second, body: "x"}
	if got := notRatelimit.waitAfter429(); got != 0 {
		t.Fatalf("waitAfter429() on 500 = %v, want 0", got)
	}
}

func TestIsAniListRateLimitError(t *testing.T) {
	if !isAniListRateLimitError(&anilistHTTPError{status: 429, body: "x"}) {
		t.Fatal("typed 429 should be a rate-limit error")
	}
	if isAniListRateLimitError(&anilistHTTPError{status: 500, body: "x"}) {
		t.Fatal("typed 500 must not be a rate-limit error")
	}
	if !isAniListRateLimitError(errTest("Too Many Requests.")) {
		t.Fatal("GraphQL 'Too Many Requests.' should be a rate-limit error")
	}
	if isAniListRateLimitError(errTest("AniList token is invalid")) {
		t.Fatal("auth error must not be a rate-limit error")
	}
	if isAniListRateLimitError(nil) {
		t.Fatal("nil must not be a rate-limit error")
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }

func TestBuildAniListBulkMutation(t *testing.T) {
	q := buildAniListBulkMutation([]anilistExportWrite{
		{MediaID: 21, Progress: 12, Status: "COMPLETED", Score: 85, WantScore: 8},
		{MediaID: 1, Progress: 1, Status: "CURRENT"},
	})
	for _, want := range []string{
		"m0: SaveMediaListEntry(mediaId: 21, progress: 12, status: COMPLETED, scoreRaw: 85)",
		"m1: SaveMediaListEntry(mediaId: 1, progress: 1, status: CURRENT)",
		"{ id }",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("bulk mutation missing %q:\n%s", want, q)
		}
	}
	if strings.Contains(q, "scoreRaw: 0") {
		t.Fatalf("zero score must be omitted, got:\n%s", q)
	}
	if !strings.HasPrefix(q, "mutation {") {
		t.Fatalf("must be a bare mutation block, got:\n%s", q)
	}
}

func TestParseAniListBulkResult(t *testing.T) {
	t.Run("all succeed", func(t *testing.T) {
		ok, msg := parseAniListBulkResult(
			[]byte(`{"data":{"m0":{"id":1},"m1":{"id":2}}}`), 2)
		if msg != "" || len(ok) != 2 || !ok[0] || !ok[1] {
			t.Fatalf("ok=%v msg=%q", ok, msg)
		}
	})
	t.Run("partial failure by error path", func(t *testing.T) {
		raw := []byte(`{"data":{"m0":{"id":1},"m1":null},` +
			`"errors":[{"message":"Invalid score","path":["m1","scoreRaw"]}]}`)
		ok, msg := parseAniListBulkResult(raw, 2)
		if !ok[0] || ok[1] {
			t.Fatalf("ok=%v, want [true false]", ok)
		}
		if !strings.Contains(msg, "Invalid score") {
			t.Fatalf("msg=%q, want the provider message", msg)
		}
	})
	t.Run("top-level errors fail everything", func(t *testing.T) {
		ok, msg := parseAniListBulkResult(
			[]byte(`{"data":null,"errors":[{"message":"Too Many Requests."}]}`), 2)
		if ok[0] || ok[1] || msg == "" {
			t.Fatalf("ok=%v msg=%q", ok, msg)
		}
	})
	t.Run("garbage body", func(t *testing.T) {
		ok, msg := parseAniListBulkResult([]byte(`<html>challenge</html>`), 1)
		if ok[0] || msg == "" {
			t.Fatalf("ok=%v msg=%q", ok, msg)
		}
	})
}

func TestAnilistExportPacer(t *testing.T) {
	p := newAnilistExportPacer(60 * time.Millisecond)
	ctx := context.Background()
	start := time.Now()
	if err := p.wait(ctx); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	if time.Since(start) > 30*time.Millisecond {
		t.Fatalf("first wait should be immediate, took %v", time.Since(start))
	}
	p.mark()
	if err := p.wait(ctx); err != nil {
		t.Fatalf("second wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Fatalf("second wait not spaced: total %v", elapsed)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	p.mark() // fresh spacing window, so wait must block (and notice ctx)
	if err := p.wait(cancelCtx); err == nil {
		t.Fatal("wait on a cancelled context must fail")
	}
}
