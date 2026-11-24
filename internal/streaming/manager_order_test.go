package streaming

import (
	"testing"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// The operator's playback priority is the primary sort key: AniPM, Kaa,
// FlixCloud, MegaPlay, AnV, AnimeX, then everyone else. A better verdict
// inside a lower-priority provider must not lift it above a higher-priority
// one.
func TestProviderPlayOrder(t *testing.T) {
	mk := func(provider, verification string) core.Server {
		return core.Server{
			Name:     provider,
			Provider: provider,
			Sources:  []core.Source{{URL: "https://x/" + provider, Verification: verification}},
		}
	}
	ordered := []core.Server{
		mk("anipm", "embed"), // worst verdict, still first
		mk("kaa", "embed"),
		mk("flixcloud", "embed"),
		mk("megavid", "embed"),
		mk("anv", "embed"),
		mk("animex", "embed"),
		mk("anikoto", "proxy"), // best verdict, still after animex
	}
	for i := 0; i < len(ordered)-1; i++ {
		ri, rj := serverVerdictRank(ordered[i]), serverVerdictRank(ordered[i+1])
		if ri <= rj {
			t.Fatalf("%s (rank %d) must outrank %s (rank %d)", ordered[i].Provider, ri, ordered[i+1].Provider, rj)
		}
	}
}
