package streaming

import (
	"context"
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
