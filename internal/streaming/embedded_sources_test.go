package streaming

import (
	"fmt"
	"testing"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

func TestAnikotoEmbeddedSourceURLTemplates(t *testing.T) {
	for _, lang := range []string{"sub", "dub"} {
		got := fmt.Sprintf(anikotoEmbedURLTemplate, "21", lang, 1)
		want := fmt.Sprintf("https://anivexa-api-tu4a.onrender.com/watch/anikoto/21/%s/anikoto-1", lang)
		if got != want {
			t.Fatalf("Anikoto embedded URL = %q, want %q", got, want)
		}
	}
}

func TestCustomServerNamesRemainStable(t *testing.T) {
	if got, want := anikotoServers, [2]string{"Niko", "Momo"}; got != want {
		t.Fatalf("Anikoto server names = %v, want %v", got, want)
	}

	flixcloudNames := []string{"Yuta", "Syota"}
	if flixcloudNames[0] != "Yuta" || flixcloudNames[1] != "Syota" {
		t.Fatalf("FlixCloud server names = %v, want [Yuta Syota]", flixcloudNames)
	}
}

// A result may name its own sources (Anikoto: Niko for the AniList-keyed
// stream, Momo for the MAL-keyed one). Those win over positional naming so
// each key keeps its slot even when the other key is missing; results
// without slot names behave exactly as before.
func TestAppendNamedServersPrefersSlotNames(t *testing.T) {
	mkSources := func(urls ...string) []core.Source {
		out := make([]core.Source, 0, len(urls))
		for _, u := range urls {
			out = append(out, core.Source{URL: u})
		}
		return out
	}
	names := func(out []core.Server) []string {
		got := make([]string, 0, len(out))
		for _, s := range out {
			got = append(got, s.Name)
		}
		return got
	}

	// Both slots: positional fallback would also give Niko/Momo here.
	sr := &SourceResult{
		Sources:     mkSources("http://x/a.m3u8", "http://x/b.m3u8"),
		ServerNames: []string{"Niko", "Momo"},
	}
	if got := names(appendNamedServers(nil, anikotoServers[:], "anikoto", "sub", sr)); len(got) != 2 || got[0] != "Niko" || got[1] != "Momo" {
		t.Fatalf("both slots = %v, want [Niko Momo]", got)
	}

	// MAL-only: single source keeps the Momo slot instead of sliding
	// into Niko positionally.
	sr = &SourceResult{
		Sources:     mkSources("http://x/b.m3u8"),
		ServerNames: []string{"Momo"},
	}
	if got := names(appendNamedServers(nil, anikotoServers[:], "anikoto", "sub", sr)); len(got) != 1 || got[0] != "Momo" {
		t.Fatalf("mal only = %v, want [Momo]", got)
	}

	// No slot names: legacy positional behavior unchanged.
	sr = &SourceResult{Sources: mkSources("http://x/a.m3u8", "http://x/b.m3u8")}
	if got := names(appendNamedServers(nil, anikotoServers[:], "anikoto", "sub", sr)); len(got) != 2 || got[0] != "Niko" || got[1] != "Momo" {
		t.Fatalf("unnamed = %v, want [Niko Momo]", got)
	}

	// Length mismatch: ignored, positional behavior unchanged.
	sr = &SourceResult{Sources: mkSources("http://x/a.m3u8"), ServerNames: []string{"Niko", "Momo"}}
	if got := names(appendNamedServers(nil, anikotoServers[:], "anikoto", "sub", sr)); len(got) != 1 || got[0] != "Niko" {
		t.Fatalf("mismatched = %v, want [Niko]", got)
	}
}
