package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Title-matching helpers for catalog sites that are searched by title
// instead of by AniList id (heave, tensho). The rule: an exact normalized
// match always wins, otherwise accept containment either way (site titles
// drop season suffixes, punctuation and "The" prefixes; AniList titles
// carry romaji/native variants the site never lists).

// fetchAnilistTitles returns every display title of one AniList media
// (english, romaji, native + synonyms) — the candidate set for title-keyed
// site searches.
func fetchAnilistTitles(ctx context.Context, client *http.Client, anilistURL, idStr string) ([]string, error) {
	id, err := strconv.Atoi(idStr)
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("bad anilist id %q", idStr)
	}
	q := `query($id:Int){Media(id:$id,type:ANIME){title{english romaji native} synonyms}}`
	b, err := json.Marshal(map[string]any{
		"query":     q,
		"variables": map[string]any{"id": id},
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, anilistURL, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", browserUA)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anilist HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var out struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
		Data struct {
			Media *struct {
				Title struct {
					English string `json:"english"`
					Romaji  string `json:"romaji"`
					Native  string `json:"native"`
				} `json:"title"`
				Synonyms []string `json:"synonyms"`
			} `json:"Media"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if len(out.Errors) > 0 {
		return nil, fmt.Errorf("anilist: %s", out.Errors[0].Message)
	}
	if out.Data.Media == nil {
		return nil, fmt.Errorf("no anilist data for %d", id)
	}
	var titles []string
	for _, t := range []string{
		out.Data.Media.Title.English,
		out.Data.Media.Title.Romaji,
		out.Data.Media.Title.Native,
	} {
		if t = strings.TrimSpace(t); t != "" {
			titles = append(titles, t)
		}
	}
	for _, t := range out.Data.Media.Synonyms {
		if t = strings.TrimSpace(t); t != "" {
			titles = append(titles, t)
		}
	}
	return titles, nil
}

// normTitleKey lowercases and keeps only letters/digits so that
// "Mob Psycho 100", "mob-psycho100" and "MobPsycho100" compare equal.
func normTitleKey(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			continue
		}
		// Keep non-ASCII letters/digits (CJK titles) as-is.
		if r > 127 {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// pickProviderTitle returns the search result that best matches one of the
// AniList titles: exact normalized equality first, then containment (so
// "One Piece" picks the main show, not "One Piece Heroines" listed first).
func pickProviderTitle(results []SearchResult, titles []string) (SearchResult, bool) {
	type cand struct {
		res SearchResult
		key string
	}
	cands := make([]cand, 0, len(results))
	for _, r := range results {
		if k := normTitleKey(r.Title); k != "" {
			cands = append(cands, cand{res: r, key: k})
		}
	}
	if len(cands) == 0 {
		return SearchResult{}, false
	}
	for _, t := range titles {
		tk := normTitleKey(t)
		if len(tk) < 3 {
			continue
		}
		for _, c := range cands {
			if c.key == tk {
				return c.res, true
			}
		}
	}
	for _, t := range titles {
		tk := normTitleKey(t)
		if len(tk) < 3 {
			continue
		}
		for _, c := range cands {
			// The site result must CONTAIN the AniList title (site adds
			// suffixes like "(Dub)" / season markers). The reverse would
			// let a short site title ("Naruto") claim a longer AniList
			// show ("Naruto Shippuden").
			if len(c.key) >= 3 && strings.Contains(c.key, tk) {
				return c.res, true
			}
		}
	}
	return SearchResult{}, false
}
