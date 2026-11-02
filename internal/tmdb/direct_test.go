package tmdb

import "testing"

func TestPickDirectSeason(t *testing.T) {
	seasons := []tmdbSeasonInfo{{1, "Season 1", 12}, {2, "Season 2", 12}, {3, "Season 3", 24}}
	cases := []struct {
		name       string
		title      string
		knownTotal int
		want       int
	}{
		{"explicit season", "Some Anime Season 2", 0, 2},
		{"ordinal season", "Some Anime 3rd Season", 0, 3},
		{"part marker", "Some Anime Part 3", 0, 3},
		{"unknown total defaults to first", "Some Anime", 0, 1},
		{"unique episode count", "Some Anime", 24, 3},
		{"ambiguous episode count falls back", "Some Anime", 12, 1},
		{"marker wins over count", "Some Anime Season 2", 24, 2},
	}
	for _, tc := range cases {
		if got := pickDirectSeason(seasons, []string{tc.title}, tc.knownTotal); got != tc.want {
			t.Errorf("%s: pickDirectSeason(%q, %d) = %d, want %d", tc.name, tc.title, tc.knownTotal, got, tc.want)
		}
	}
	if got := pickDirectSeason([]tmdbSeasonInfo{{4, "Season 4", 10}}, []string{"Some Anime"}, 0); got != 4 {
		t.Errorf("single season: got %d, want 4", got)
	}
}

// The season marker may ride on any title variant: romaji titles carry
// "Season 2" markers just as often as english ones.
func TestPickDirectSeasonMultiTitle(t *testing.T) {
	seasons := []tmdbSeasonInfo{{1, "Season 1", 12}, {2, "Season 2", 12}}
	if got := pickDirectSeason(seasons, []string{"No Marker Here", "Romaji Season 2"}, 0); got != 2 {
		t.Errorf("romaji marker: got %d, want 2", got)
	}
	if got := pickDirectSeason(seasons, []string{}, 0); got != 1 {
		t.Errorf("empty candidates: got %d, want 1", got)
	}
}

func TestAssignDirectSlots(t *testing.T) {
	seasons := []tmdbSeasonInfo{{1, "Season 1", 12}, {2, "Season 2", 12}}

	slots, needed := assignDirectSlots(seasons, 1, []int{1, 12, 13, 24, 25})
	want := map[int]directSlot{
		1:  {1, 1},
		12: {1, 12},
		13: {2, 1},
		24: {2, 12},
	}
	for n, w := range want {
		if slots[n] != w {
			t.Errorf("slot %d = %+v, want %+v", n, slots[n], w)
		}
	}
	if _, ok := slots[25]; ok {
		t.Error("slot 25 should be absent (past the last season)")
	}
	if len(needed) != 2 || !needed[1] || !needed[2] {
		t.Errorf("needed seasons = %v, want {1,2}", needed)
	}

	slots, needed = assignDirectSlots(seasons, 2, []int{1, 5})
	if slots[1] != (directSlot{2, 1}) || slots[5] != (directSlot{2, 5}) {
		t.Errorf("start season 2 slots = %+v", slots)
	}
	if len(needed) != 1 || !needed[2] {
		t.Errorf("start season 2 needed = %v, want {2}", needed)
	}
}

func TestDirectSeasonsFiltersSpecials(t *testing.T) {
	show := map[string]any{"seasons": []any{
		map[string]any{"season_number": float64(0), "episode_count": float64(5)},
		map[string]any{"season_number": float64(2), "episode_count": float64(12), "name": "Season 2"},
		map[string]any{"season_number": float64(1), "episode_count": float64(24), "name": "Season 1"},
	}}
	got := directSeasons(show)
	if len(got) != 2 || got[0].Number != 1 || got[1].Number != 2 || got[1].Name != "Season 2" {
		t.Fatalf("directSeasons = %+v", got)
	}
}
