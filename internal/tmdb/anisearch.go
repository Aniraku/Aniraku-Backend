package tmdb

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// --- Anisearch episode-title fallback (last resort, no Jikan) ---
//
// AniZip mappings still carry an anisearch_id even when the episode map is
// empty (e.g. anilist:368 -> anisearch:3035) and TMDB has no usable entry.
// anisearch.com publishes per-episode titles at /anime/{id},{slug}/episodes
// (EN + JA + air date), which covers hentai that all other sources miss.
//
// Thumbnails intentionally stay the existing anime poster (coverFallback in
// the handler): Anisearch ships a generic placeholder cover for adult titles,
// so per-episode stills from it would be a downgrade.

const (
	AnisearchBase = "https://www.anisearch.com"
	AnisearchTTL  = 24 * time.Hour
)

const anisearchUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

// AnisearchEpisode is one parsed row of the Anisearch episodes page.
type AnisearchEpisode struct {
	Number  int
	Title   string // EN preferred, JA fallback
	Airdate string // ISO yyyy-mm-dd, "" when unparseable
}

var (
	anisearchRowSplit = regexp.MustCompile(`<tr data-episode="true"`)
	anisearchNumRe    = regexp.MustCompile(`<b>(\d+)</b>`)
	anisearchTitleRe  = map[string]*regexp.Regexp{
		"en": regexp.MustCompile(`(?s)<div lang="en">.*?<span[^>]*>([^<]*)</span>`),
		"ja": regexp.MustCompile(`(?s)<div lang="ja">.*?<span[^>]*>([^<]*)</span>`),
	}
	anisearchDateTDRe  = regexp.MustCompile(`(?s)<td data-title="Date of Original Release">(.*?)</td>`)
	anisearchDateDivRe = regexp.MustCompile(`<div[^>]*>([^<]*)</div>`)
)

var anisearchMonths = map[string]string{
	"jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
	"jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

// FetchAnisearchID resolves the AniZip anisearch_id for an AniList ID.
// Returns 0 when unavailable.
func FetchAnisearchID(ctx context.Context, client *http.Client, anilistID int) int {
	if anilistID <= 0 {
		return 0
	}
	if client == nil {
		client = &http.Client{Timeout: RequestTimeout}
	}
	u := fmt.Sprintf("%s/mappings?anilist_id=%d", AniZipBase, anilistID)
	key := cacheKey("anisearch-id", anilistID)
	val, err := cached(key, AnisearchTTL, func() (any, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("anizip returned %d", resp.StatusCode)
		}
		var raw struct {
			Mappings struct {
				AnisearchID int `json:"anisearch_id"`
			} `json:"mappings"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&raw); err != nil {
			return nil, err
		}
		return raw.Mappings.AnisearchID, nil
	})
	if err != nil {
		return 0
	}
	n, _ := val.(int)
	if n <= 0 {
		return 0
	}
	return n
}

// FetchAnisearchEpisodes returns per-episode titles keyed by episode number
// for an AniList ID, via its AniZip anisearch_id. Returns nil on any failure.
// Results are cached 24h; callers treat nil/empty as "no fallback".
func FetchAnisearchEpisodes(ctx context.Context, client *http.Client, anilistID int) map[int]*AnisearchEpisode {
	anisearchID := FetchAnisearchID(ctx, client, anilistID)
	if anisearchID <= 0 {
		return nil
	}
	if client == nil {
		client = &http.Client{Timeout: RequestTimeout}
	}
	key := cacheKey("anisearch-eps", anisearchID)
	val, err := cached(key, AnisearchTTL, func() (any, error) {
		pageURL, err := anisearchEpisodesURL(ctx, client, anisearchID)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, "GET", pageURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "text/html")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("User-Agent", anisearchUA)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("anisearch returned %d", resp.StatusCode)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		if err != nil {
			return nil, err
		}
		eps := parseAnisearchEpisodes(string(body))
		if len(eps) == 0 {
			return nil, fmt.Errorf("anisearch: no episodes parsed for %d", anisearchID)
		}
		return eps, nil
	})
	if err != nil {
		return nil
	}
	eps, _ := val.(map[int]*AnisearchEpisode)
	return eps
}

// anisearchEpisodesURL resolves the canonical /episodes page URL.
// anisearch.com 301s bare/wrong slugs to canonical, and our HTTP clients
// refuse redirects — so read the Location header manually (no follow needed).
func anisearchEpisodesURL(ctx context.Context, client *http.Client, anisearchID int) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/anime/%d", AnisearchBase, anisearchID), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "text/html")
	req.Header.Set("User-Agent", anisearchUA)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	loc := strings.TrimSpace(resp.Header.Get("Location"))
	if loc == "" && resp.Request != nil && resp.Request.URL != nil {
		// Client followed the redirect itself (e.g. default transport):
		// the final URL is already canonical.
		loc = strings.TrimSpace(resp.Request.URL.String())
	}
	if loc == "" {
		return "", fmt.Errorf("anisearch: no canonical URL for %d", anisearchID)
	}
	// Location may be absolute or root-relative.
	if strings.HasPrefix(loc, "/") {
		loc = AnisearchBase + loc
	}
	if !strings.HasPrefix(loc, AnisearchBase+"/anime/") {
		return "", fmt.Errorf("anisearch: unexpected canonical URL %q", loc)
	}
	return strings.TrimSuffix(loc, "/") + "/episodes", nil
}

func parseAnisearchEpisodes(page string) map[int]*AnisearchEpisode {
	out := map[int]*AnisearchEpisode{}
	rows := anisearchRowSplit.Split(page, -1)
	for _, row := range rows[1:] {
		m := anisearchNumRe.FindStringSubmatch(row)
		if m == nil {
			continue
		}
		num, err := strconv.Atoi(m[1])
		if err != nil || num <= 0 {
			continue
		}
		title := cleanAnisearchText(firstGroup(anisearchTitleRe["en"], row))
		if title == "" {
			title = cleanAnisearchText(firstGroup(anisearchTitleRe["ja"], row))
		}
		if title == "" {
			continue
		}
		ep := &AnisearchEpisode{Number: num, Title: title}
		if d := parseAnisearchDate(row); d != "" {
			ep.Airdate = d
		}
		out[num] = ep
	}
	return out
}

func firstGroup(re *regexp.Regexp, s string) string {
	if m := re.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return ""
}

func cleanAnisearchText(s string) string {
	t := html.UnescapeString(s)
	// &nbsp; survives UnescapeString as \u00a0.
	t = strings.ReplaceAll(t, "\u00a0", " ")
	return strings.TrimSpace(t)
}

// parseAnisearchDate converts "21. Jul 2001" -> "2001-07-21".
func parseAnisearchDate(row string) string {
	td := firstGroup(anisearchDateTDRe, row)
	if td == "" {
		return ""
	}
	for _, m := range anisearchDateDivRe.FindAllStringSubmatch(td, -1) {
		t := cleanAnisearchText(m[1])
		if t == "" {
			continue
		}
		parts := strings.Fields(strings.ReplaceAll(t, ".", ""))
		if len(parts) != 3 {
			continue
		}
		mon, ok := anisearchMonths[strings.ToLower(parts[1])]
		if !ok || len(parts[2]) != 4 {
			continue
		}
		day, err1 := strconv.Atoi(parts[0])
		year, err2 := strconv.Atoi(parts[2])
		if err1 != nil || err2 != nil || day < 1 || day > 31 || year < 1900 || year > 2100 {
			continue
		}
		return fmt.Sprintf("%04d-%s-%02d", year, mon, day)
	}
	return ""
}
