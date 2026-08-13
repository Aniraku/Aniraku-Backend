package streaming

import (
	"context"
	"net/http"
	"testing"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

type verdictProbe struct {
	verdicts map[string]PlaybackVerdict
}

func (p verdictProbe) ProbePlayback(_ context.Context, _ string, rawURL string, _ map[string]string) PlaybackVerdict {
	if verdict, ok := p.verdicts[rawURL]; ok {
		return verdict
	}
	return VerdictDead
}

func TestVerdictResultDropsIndividuallyDeadSources(t *testing.T) {
	deadURL := "https://203.188.166.234/v4/dead/master.m3u8"
	goodURL := "https://cdn.example.test/v4/good/master.m3u8"
	provider := &MiruroProvider{
		log: zerolog.Nop(),
		probe: verdictProbe{verdicts: map[string]PlaybackVerdict{
			deadURL: VerdictDead,
			goodURL: VerdictProxy,
		}},
	}

	ranked, best := provider.verdictResult(context.Background(), &SourceResult{
		Sources: []core.Source{
			{URL: deadURL, Type: "hls", Quality: "1080p"},
			{URL: goodURL, Type: "hls", Quality: "720p"},
		},
	})

	if ranked == nil {
		t.Fatal("verdictResult returned nil with one playable source")
	}
	if best != VerdictProxy {
		t.Fatalf("best verdict = %v, want proxy", best)
	}
	if len(ranked.Sources) != 1 {
		t.Fatalf("returned %d sources, want exactly 1 playable source", len(ranked.Sources))
	}
	if ranked.Sources[0].URL != goodURL {
		t.Fatalf("returned URL %q, want %q", ranked.Sources[0].URL, goodURL)
	}
	if ranked.Sources[0].Verification != "proxy" {
		t.Fatalf("verification = %q, want proxy", ranked.Sources[0].Verification)
	}
}

func TestVerifySourceURLBlocksObservedDeadCDNRange(t *testing.T) {
	provider := &MiruroProvider{}
	for _, rawURL := range []string{
		"https://203.188.166.234/v4/dead/master.m3u8",
		"https://203.188.166.228/v4/dead/master.m3u8",
	} {
		if err := provider.verifySourceURL(context.Background(), rawURL); err == nil {
			t.Fatalf("verifySourceURL(%q) allowed known dead CDN range", rawURL)
		}
	}

	if err := provider.verifySourceURL(context.Background(), "https://cdn.example.test/master.m3u8"); err != nil {
		t.Fatalf("verifySourceURL rejected unrelated CDN: %v", err)
	}
}

func TestVerdictResultDropsAllDeadSources(t *testing.T) {
	provider := &MiruroProvider{
		log: zerolog.Nop(),
		probe: verdictProbe{verdicts: map[string]PlaybackVerdict{
			"https://cdn.example.test/dead.m3u8": VerdictDead,
		}},
	}

	ranked, best := provider.verdictResult(context.Background(), &SourceResult{
		Sources: []core.Source{{URL: "https://cdn.example.test/dead.m3u8", Type: "hls"}},
	})
	if ranked != nil || best != VerdictDead {
		t.Fatalf("dead-only result = %#v, %v; want nil, dead", ranked, best)
	}
}

func TestClassifyStreamType(t *testing.T) {
	tests := []struct {
		name     string
		typeHint string
		url      string
		want     string
	}{
		{name: "hls hint", typeHint: "hls", url: "https://cdn.test/video", want: "hls"},
		{name: "m3u8 url", typeHint: "", url: "https://cdn.test/master.m3u8?token=1", want: "hls"},
		{name: "dash hint", typeHint: "dash", url: "https://cdn.test/manifest", want: "dash"},
		{name: "mpd url", typeHint: "", url: "https://cdn.test/manifest.mpd", want: "dash"},
		{name: "mp4 url", typeHint: "hls", url: "https://cdn.test/video.mp4", want: "mp4"},
		{name: "webm", typeHint: "video/webm", url: "https://cdn.test/video", want: "webm"},
		{name: "ogg", typeHint: "", url: "https://cdn.test/video.ogv", want: "ogg"},
		{name: "mpeg", typeHint: "", url: "https://cdn.test/video.mpg", want: "mpeg"},
		{name: "embed", typeHint: "embed", url: "https://player.test/watch/1", want: "embed"},
		{name: "extensionless native", typeHint: "video/x-custom", url: "https://cdn.test/stream?id=1", want: "native"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyStreamType(tt.typeHint, tt.url); got != tt.want {
				t.Fatalf("classifyStreamType(%q, %q) = %q, want %q", tt.typeHint, tt.url, got, tt.want)
			}
		})
	}
}

func TestVerdictResultRetainsVerifiedEmbed(t *testing.T) {
	provider := &MiruroProvider{
		log:   zerolog.Nop(),
		probe: verdictProbe{},
	}
	embedURL := "https://player.example.test/watch/1"
	ranked, best := provider.verdictResult(context.Background(), &SourceResult{
		Sources: []core.Source{{URL: embedURL, Type: "embed", Quality: "auto"}},
	})
	if ranked == nil || best != VerdictEmbed {
		t.Fatalf("verified embed result = %#v, %v; want one embed source", ranked, best)
	}
	if len(ranked.Sources) != 1 || ranked.Sources[0].Verification != "embed" {
		t.Fatalf("embed result = %#v, want verification=embed", ranked.Sources)
	}
}

func TestTestEmbedReachabilityRejectsInvalidURL(t *testing.T) {
	if err := testEmbedReachability(context.Background(), "not-a-url", nil, http.DefaultClient); err == nil {
		t.Fatal("testEmbedReachability accepted an invalid URL")
	}
}

func TestEmbedFrameBlockReason(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		blocked bool
	}{
		{name: "x-frame deny", headers: http.Header{"X-Frame-Options": []string{"DENY"}}, blocked: true},
		{name: "x-frame same origin", headers: http.Header{"X-Frame-Options": []string{"SAMEORIGIN"}}, blocked: true},
		{name: "csp self only", headers: http.Header{"Content-Security-Policy": []string{"default-src 'self'; frame-ancestors 'self'"}}, blocked: true},
		{name: "aniraku allowed", headers: http.Header{"Content-Security-Policy": []string{"frame-ancestors https://www.aniraku.tech"}}, blocked: false},
		{name: "no framing policy", headers: make(http.Header), blocked: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := embedFrameBlockReason(&http.Response{Header: tt.headers}) != ""
			if got != tt.blocked {
				t.Fatalf("embedFrameBlockReason blocked=%v, want %v", got, tt.blocked)
			}
		})
	}
}
