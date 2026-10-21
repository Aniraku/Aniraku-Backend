package streaming

import "testing"

// TestAnimeGGSpecificityMismatch guards the wrong-show mapping: when the
// site lacks the specific entry (unreleased sequel/spinoff), the parent
// franchise entry must not win on overlap alone (observed: a 2026
// "Dragon Ball Super: Beerus" query matching the 2015 "Dragon Ball
// Super" listing at 0.824 and shipping its episode 1 as the wrong show).
// Season continuations stay exempt, merged listings cover those via the
// stripped query variants and episode offsets.
func TestAnimeGGSpecificityMismatch(t *testing.T) {
	reject := [][3]string{
		{"Dragon Ball Super: Beerus", "Dragon Ball Super", "dragon-ball-super"},
		{"Naruto", "Naruto Shippuden", "naruto-shippuden"},
	}
	for _, c := range reject {
		if s := animeggTitleScore(c[0], c[1], c[2]); s >= 0.5 {
			t.Errorf("query=%q cand=%q must score below cutoff, got %.3f", c[0], c[1], s)
		}
	}
	keep := [][3]string{
		{"Dragon Ball Super: Beerus", "Dragon Ball Super Beerus", "dragon-ball-super-beerus"},
		{"Naruto Shippuden", "Naruto Shippuuden", "naruto-shippuuden"},
		{"One Piece", "One Piece", "one-piece"},
	}
	for _, c := range keep {
		if s := animeggTitleScore(c[0], c[1], c[2]); s < 0.5 {
			t.Errorf("query=%q cand=%q must stay matchable, got %.3f", c[0], c[1], s)
		}
	}
}
