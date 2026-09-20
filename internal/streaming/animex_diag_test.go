package streaming

import (
	"testing"
)

// TestAnimeXBlockedProvidersFiltered pins that the loli (Anzu) sub-provider —
// which serves image-segment playlists and does not play — is filtered from
// every provider list before any network call, while playable providers
// (yuki/Mochi, beep/Lumi, neko/Chibi, sora/Sora, zuna/Kira) are never touched.
func TestAnimeXBlockedProvidersFiltered(t *testing.T) {
	t.Parallel()

	got := filterBlockedProviders([]string{"beep", "yuki", "neko", "sora", "loli"})
	want := []string{"beep", "yuki", "neko", "sora"}
	if len(got) != len(want) {
		t.Fatalf("filterBlockedProviders = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("filterBlockedProviders = %v, want %v", got, want)
		}
	}
	if got := filterBlockedProviders(nil); len(got) != 0 {
		t.Fatalf("filterBlockedProviders(nil) = %v, want empty", got)
	}
	if got := filterBlockedProviders([]string{"loli"}); len(got) != 0 {
		t.Fatalf("filterBlockedProviders([loli]) = %v, want empty", got)
	}
}

// TestAnimeXPlayableProvidersNotBlocked guards against future foot-guns:
// every known-playable sub-provider must stay out of the blocklist.
func TestAnimeXPlayableProvidersNotBlocked(t *testing.T) {
	t.Parallel()

	for _, playable := range []string{"beep", "yuki", "neko", "sora", "zuna", "uwu"} {
		if animexBlockedProviders[playable] {
			t.Errorf("playable provider %q must not be in animexBlockedProviders", playable)
		}
	}
}
