package tmdb

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// seasonMarkers find an explicit season number in an AniList title
// ("Season 2", "2nd Season", "Part 3", "Cour 2").
var seasonMarkers = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(?:season|part|cour)\s*(\d+)`),
	regexp.MustCompile(`(?i)\b(\d+)(?:st|nd|rd|th)\s+season`),
}

type tmdbSeasonInfo struct {
	Number   int
	Name     string
	Episodes int
}

type directSlot struct {
	Season  int
	Episode int
}

// ResolveEpisodesDirect resolves episode metadata straight from TMDB by
// human title, trying every candidate (AniList english, romaji,
// userPreferred) until one matches. It serves titles AniZip has no episode
// map for yet, where AniBridge's incrementally verified range still lags or
// is missing entirely. TMDB stores some anime as seasons of a single show,
// so the season the AniList title refers to is selected first and numbering
// stays continuous across following seasons (split cours).
//
// knownTotal is AniList's episode count (0 when unknown); it only breaks a
// tie between several TMDB seasons of the same length when the title has no
// season marker.
func ResolveEpisodesDirect(ctx context.Context, client *http.Client, token string, anilistID int, titles []string, episodeNumbers []int, knownTotal int) (*ResolveResult, error) {
	if client == nil {
		client = &http.Client{Timeout: RequestTimeout}
	}
	showID := 0
	for _, title := range titles {
		if strings.TrimSpace(title) == "" {
			continue
		}
		if showID = resolveTmdbIDByTitle(ctx, client, token, "tv", title); showID > 0 {
			break
		}
	}
	if showID <= 0 {
		return nil, newResolverError("TMDB_MEDIA_MAPPING_NOT_FOUND", "No TMDB show matches this title.", 404)
	}
	var nums []int
	{ // dedupe & sort
		uniq := map[int]bool{}
		for _, n := range episodeNumbers {
			if n > 0 {
				uniq[n] = true
			}
		}
		for n := range uniq {
			nums = append(nums, n)
		}
		sort.Ints(nums)
	}
	if len(nums) == 0 {
		return nil, newResolverError("INVALID_EPISODES", "Provide one or more positive episode numbers.", 400)
	}
	if len(nums) > MaxEpisodeNumbers {
		return nil, newResolverError("TOO_MANY_EPISODES", fmt.Sprintf("Request at most %d episode numbers.", MaxEpisodeNumbers), 400)
	}

	show, err := getTmdbShow(ctx, client, token, showID)
	if err != nil {
		return nil, err
	}
	seasons := directSeasons(show)
	if len(seasons) == 0 {
		return nil, newResolverError("TMDB_MEDIA_MAPPING_NOT_FOUND", "TMDB show has no regular seasons.", 404)
	}
	start := pickDirectSeason(seasons, titles, knownTotal)
	slots, needed := assignDirectSlots(seasons, start, nums)

	neededSeasons := make([]int, 0, len(needed))
	for sn := range needed {
		neededSeasons = append(neededSeasons, sn)
	}
	sort.Ints(neededSeasons)
	entries := make(map[int]map[int]map[string]any, len(neededSeasons))
	for _, sn := range neededSeasons {
		// Fresh fetch: the 24h season cache can predate episodes an airing
		// show has since published, and this fallback runs for exactly the
		// brand-new titles where that staleness is most visible.
		payload, err := fetchTmdbSeasonFresh(ctx, client, token, showID, sn)
		if err != nil {
			return nil, err
		}
		byNumber := map[int]map[string]any{}
		if eps, ok := payload["episodes"].([]any); ok {
			for _, raw := range eps {
				entry, _ := raw.(map[string]any)
				if entry == nil {
					continue
				}
				var epNum int
				switch v := entry["episode_number"].(type) {
				case float64:
					epNum = int(v)
				case int:
					epNum = v
				}
				if epNum > 0 {
					byNumber[epNum] = entry
				}
			}
		}
		entries[sn] = byNumber
	}

	found := map[int]bool{}
	episodes := make([]*EpisodeMetadata, 0, len(nums))
	for _, n := range nums {
		slot, ok := slots[n]
		if !ok {
			continue
		}
		if meta := toTmdbEpisodeMetadata(entries[slot.Season][slot.Episode], n, slot.Episode); meta != nil {
			episodes = append(episodes, meta)
			found[n] = true
		}
	}
	var mapped, missing []int
	for _, n := range nums {
		if found[n] {
			mapped = append(mapped, n)
		} else {
			missing = append(missing, n)
		}
	}
	return &ResolveResult{
		AnilistID:    anilistID,
		Source:       "tmdb-direct",
		CacheSeconds: int(EpisodeTTL.Seconds()),
		Episodes:     episodes,
		Mapped:       mapped,
		Missing:      missing,
		Mapping:      map[string]any{"provider": "tmdb-direct", "showId": showID, "startSeason": start},
	}, nil
}

// DirectSeasonEpisodeCount returns the episode count of the TMDB season the
// title refers to, trying every candidate title. AniZip-less shows otherwise
// stop at AniBridge's verified frontier (or the last aired episode) while
// TMDB already lists the whole current season. Returns 0 when TMDB has no
// match.
func DirectSeasonEpisodeCount(ctx context.Context, client *http.Client, token string, titles []string, knownTotal int) int {
	if strings.TrimSpace(token) == "" {
		return 0
	}
	if client == nil {
		client = &http.Client{Timeout: RequestTimeout}
	}
	showID := 0
	for _, title := range titles {
		if strings.TrimSpace(title) == "" {
			continue
		}
		if showID = resolveTmdbIDByTitle(ctx, client, token, "tv", title); showID > 0 {
			break
		}
	}
	if showID <= 0 {
		return 0
	}
	show, err := getTmdbShow(ctx, client, token, showID)
	if err != nil {
		return 0
	}
	seasons := directSeasons(show)
	if len(seasons) == 0 {
		return 0
	}
	start := pickDirectSeason(seasons, titles, knownTotal)
	for _, s := range seasons {
		if s.Number == start {
			return s.Episodes
		}
	}
	return 0
}

// directSeasons extracts regular TMDB seasons (specials excluded) in order.
func directSeasons(show map[string]any) []tmdbSeasonInfo {
	raw, _ := show["seasons"].([]any)
	seasons := make([]tmdbSeasonInfo, 0, len(raw))
	for _, r := range raw {
		m, _ := r.(map[string]any)
		if m == nil {
			continue
		}
		var num int
		switch v := m["season_number"].(type) {
		case float64:
			num = int(v)
		case int:
			num = v
		}
		var count int
		switch v := m["episode_count"].(type) {
		case float64:
			count = int(v)
		case int:
			count = v
		}
		if num <= 0 || count <= 0 {
			continue
		}
		seasons = append(seasons, tmdbSeasonInfo{Number: num, Name: text(m["name"]), Episodes: count})
	}
	sort.Slice(seasons, func(i, j int) bool { return seasons[i].Number < seasons[j].Number })
	return seasons
}

// pickDirectSeason chooses the TMDB season an AniList entry represents.
// Explicit season markers win (checked across every title candidate —
// romaji and english carry the "Season 2"/"Part 3" markers equally), then a
// unique episode-count match, then the first regular season (most AniList
// entries are the show's first season).
func pickDirectSeason(seasons []tmdbSeasonInfo, titles []string, knownTotal int) int {
	if len(seasons) == 0 {
		return 0
	}
	has := func(n int) bool {
		for _, s := range seasons {
			if s.Number == n {
				return true
			}
		}
		return false
	}
	for _, title := range titles {
		for _, re := range seasonMarkers {
			if m := re.FindStringSubmatch(title); m != nil {
				if n, _ := strconv.Atoi(m[1]); n > 0 && has(n) {
					return n
				}
			}
		}
	}
	if len(seasons) == 1 {
		return seasons[0].Number
	}
	if knownTotal > 0 {
		match := 0
		for _, s := range seasons {
			if s.Episodes != knownTotal {
				continue
			}
			if match != 0 {
				match = 0 // ambiguous
				break
			}
			match = s.Number
		}
		if match > 0 {
			return match
		}
	}
	return seasons[0].Number
}

// assignDirectSlots maps AniList episode numbers onto (season, episode)
// slots starting at the chosen season, continuing into following seasons so
// a split cour stored as separate TMDB seasons keeps one continuous
// AniList numbering. Unavailable numbers stay absent.
func assignDirectSlots(seasons []tmdbSeasonInfo, start int, nums []int) (map[int]directSlot, map[int]bool) {
	slots := map[int]directSlot{}
	needed := map[int]bool{}
	req := make(map[int]bool, len(nums))
	maxNum := 0
	for _, n := range nums {
		req[n] = true
		if n > maxNum {
			maxNum = n
		}
	}
	offset := 0
	started := false
	for _, s := range seasons {
		if !started {
			if s.Number != start {
				continue
			}
			started = true
		}
		for n := offset + 1; n <= offset+s.Episodes; n++ {
			if req[n] {
				slots[n] = directSlot{Season: s.Number, Episode: n - offset}
				needed[s.Number] = true
			}
		}
		offset += s.Episodes
		if offset >= maxNum {
			break
		}
	}
	return slots, needed
}
