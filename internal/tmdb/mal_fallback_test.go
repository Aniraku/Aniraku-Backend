package tmdb

import (
	"encoding/json"
	"testing"
)

// TestFribbMalIDField guards the struct decode: the MAL fallback reads
// fribbEntry.MalID from the anime-list JSON.
func TestFribbMalIDField(t *testing.T) {
	var e fribbEntry
	if err := json.Unmarshal([]byte(`{"anilist_id":206814,"mal_id":63367}`), &e); err != nil {
		t.Fatal(err)
	}
	if e.MalID != 63367 {
		t.Fatalf("MalID = %d, want 63367", e.MalID)
	}
	if e.AnilistID != 206814 {
		t.Fatalf("AnilistID = %d, want 206814", e.AnilistID)
	}
}
