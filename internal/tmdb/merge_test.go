package tmdb

import (
	"testing"
)

func TestIsGenericEpisodeLabel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"", true},
		{"12", true},
		{"Episode 12", true},
		{"ep 3", true},
		{"1 · 24p", true},
		{"1 · 24m", false}, // duration-style label is NOT matched by the pattern
		{"3.5p", true},
		{"The Promised Episode", false},
		{"Escalation", false},
		{"  7  ", true},
	}
	for _, c := range cases {
		if got := IsGenericEpisodeLabel(c.in); got != c.want {
			t.Errorf("IsGenericEpisodeLabel(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestIsPublishedTitle(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"   ", false},
		{"Episode 5", false}, // generic label
		{"TBA", false},       // placeholder
		{"tbd", false},       // placeholder (case-insensitive)
		{"Untitled", false},  // placeholder
		{"Unknown", false},   // placeholder
		{"The Blade", true},  // real title
		{"Romaji Title 7", true},
	}
	for _, c := range cases {
		if got := IsPublishedTitle(c.in); got != c.want {
			t.Errorf("IsPublishedTitle(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestIsValidAniZipThumbnail(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"", false},
		{"   ", false},
		{"http://example.com/img.jpg", false}, // http rejected — https only
		{"https://episodes.anizip.dev/abc.jpg", true},
		{"https://image.tmdb.org/t/p/w780/x.jpg", true},
		{"https://has space .com/x.jpg", false}, // whitespace rejected
		{"not a url", false},
	}
	for _, c := range cases {
		if got := IsValidAniZipThumbnail(c.in); got != c.want {
			t.Errorf("IsValidAniZipThumbnail(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestAniZipBestTitlePrecedence(t *testing.T) {
	t.Parallel()

	// en wins over x-jat and ja
	a := AniZipEpisode{Title: map[string]string{"en": "EN Title", "x-jat": "JAT", "ja": "JA"}}
	if got := a.BestTitle(); got != "EN Title" {
		t.Errorf("BestTitle = %q, want EN", got)
	}
	// x-jat wins when en missing
	b := AniZipEpisode{Title: map[string]string{"x-jat": "JAT", "ja": "JA"}}
	if got := b.BestTitle(); got != "JAT" {
		t.Errorf("BestTitle = %q, want JAT", got)
	}
	// ja is last resort
	c := AniZipEpisode{Title: map[string]string{"ja": "JA"}}
	if got := c.BestTitle(); got != "JA" {
		t.Errorf("BestTitle = %q, want JA", got)
	}
	// empty map → empty string
	d := AniZipEpisode{Title: map[string]string{}}
	if got := d.BestTitle(); got != "" {
		t.Errorf("BestTitle = %q, want empty", got)
	}
}

func TestEpisodeCacheRoundTrip(t *testing.T) {
	id := 999999001
	data := map[int]*EpisodeMetadata{
		1: {Title: "Cache Me"},
		2: {Title: "And Me"},
	}
	CacheEpisodes(id, data)

	got := GetCachedEpisodes(id, []int{1, 2})
	if got == nil {
		t.Fatal("cache hit expected right after CacheEpisodes")
	}
	if got[1] == nil || got[1].Title != "Cache Me" {
		t.Errorf("episode 1 = %+v", got[1])
	}
	if got[2] == nil || got[2].Title != "And Me" {
		t.Errorf("episode 2 = %+v", got[2])
	}

	// Partial coverage returns the covered subset (handler treats any
	// non-nil result as "skip the TMDB fetch"); only a zero-coverage
	// request misses.
	partial := GetCachedEpisodes(id, []int{1, 2, 3})
	if partial == nil || len(partial) != 2 {
		t.Errorf("partial coverage = %v, want the 2 cached episodes", partial)
	}
	if miss := GetCachedEpisodes(id, []int{50}); miss != nil {
		t.Errorf("zero coverage returned %v, want nil", miss)
	}

	// Unknown ID misses.
	if miss := GetCachedEpisodes(999999002, []int{1}); miss != nil {
		t.Errorf("unknown ID returned %v, want nil", miss)
	}
}
