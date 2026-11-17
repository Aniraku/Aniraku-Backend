package streaming

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// The static fallback key is the one every sampled module embeds; a resolve
// derived key is only a cached generation. If the candidate set ever stops
// trying the fallback, every flixcloud body decoded after that point fails
// while the same bytes decode fine in a fresh process — which is exactly the
// "works right after restart, dead minutes later" signature.
func TestFlixcloudPKSetAlwaysTriesFallback(t *testing.T) {
	reset := func() {
		flixcloudPKMu.Lock()
		defer flixcloudPKMu.Unlock()
		flixcloudPKSet = map[string][]byte{flixcloudPKFallbackHex: flixcloudPKFallback}
		flixcloudPKOrder = []string{flixcloudPKFallbackHex}
	}
	reset()
	t.Cleanup(reset)

	for i := 0; i < 20; i++ {
		flixcloudStorePlaylistKey(bytes.Repeat([]byte{byte(i + 1)}, 32))
	}

	snap := flixcloudPKSnapshot()
	if len(snap) == 0 {
		t.Fatal("candidate set is empty")
	}
	for i, pk := range snap {
		if hex.EncodeToString(pk) == flixcloudPKFallbackHex {
			if i != 0 {
				t.Fatalf("fallback present but not tried first (index %d)", i)
			}
			return
		}
	}
	t.Fatalf("fallback key evicted from the candidate set (%d candidates left)", len(snap))
}