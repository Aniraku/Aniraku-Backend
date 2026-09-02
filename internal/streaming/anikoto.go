package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

const (
	anikotoBase             = "https://anikototv.to"
	anikotoEmbedURLTemplate = "https://anivexa-api-tu4a.onrender.com/watch/anikoto/%s/%s/anikoto-%d"
)

// anikotoServers maps API server names to display names.
// The first server from the API becomes Niko, second becomes Momo.
var anikotoServers = [2]string{"Niko", "Momo"}

// AnikotoProvider scrapes anikototv.to for streaming sources.
// Two servers per episode: Niko (1st) and Momo (2nd).
// Supports sub and dub via separate data-ids per language.
type AnikotoProvider struct {
	client *http.Client
	log    zerolog.Logger
}

func NewAnikotoProvider(log zerolog.Logger) *AnikotoProvider {
	return &AnikotoProvider{
		client: &http.Client{Timeout: 15 * time.Second},
		log:    log,
	}
}

func (p *AnikotoProvider) Name() string { return "anikoto" }

func (p *AnikotoProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("anikoto search not implemented")
}

func (p *AnikotoProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("anikoto episode listing not implemented")
}

// FindEpisodeSource fetches Anikoto’s two embedded-player streams directly
// from Anivexa using the AniList ID supplied by the frontend.
func (p *AnikotoProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}

	endpoint := fmt.Sprintf(anikotoEmbedURLTemplate, providerID, lang, episode)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("anikoto anivexa request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anikoto anivexa returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Streams []struct {
			URL       string          `json:"url"`
			Type      string          `json:"type"`
			Server    string          `json:"server"`
			EmbedURL  string          `json:"embedUrl"`
			Referer   string          `json:"referer"`
			Subtitles []core.Subtitle `json:"subtitles"`
			Intro     *struct {
				Start float64 `json:"start"`
				End   float64 `json:"end"`
			} `json:"intro"`
			Outro *struct {
				Start float64 `json:"start"`
				End   float64 `json:"end"`
			} `json:"outro"`
		} `json:"streams"`
		Downloads []struct {
			URL   string `json:"url"`
			Label string `json:"label"`
		} `json:"downloads"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("anikoto anivexa response decode failed: %w", err)
	}

	var sources []core.Source
	var intro, outro *core.SkipTimestamp
	for _, stream := range payload.Streams {
		hlsURL := strings.TrimSpace(stream.URL)
		if hlsURL == "" {
			hlsURL = strings.TrimSpace(stream.EmbedURL)
		}
		if hlsURL == "" {
			continue
		}
		srcType := strings.ToLower(strings.TrimSpace(stream.Type))
		if srcType == "" {
			if strings.Contains(strings.ToLower(hlsURL), ".m3u8") {
				srcType = "hls"
			} else {
				srcType = "hls"
			}
		}
		if srcType == "hls" || strings.Contains(hlsURL, ".m3u8") {
			srcType = "hls"
		}
		sources = append(sources, core.Source{
			URL:          hlsURL,
			Type:         srcType,
			Quality:      "auto",
			Subtitles:    stream.Subtitles,
			Verification: "proxy",
		})
		if intro == nil && stream.Intro != nil {
			intro = &core.SkipTimestamp{Start: stream.Intro.Start, End: stream.Intro.End}
		}
		if outro == nil && stream.Outro != nil {
			outro = &core.SkipTimestamp{Start: stream.Outro.Start, End: stream.Outro.End}
		}
	}
	if len(sources) == 0 {
		return nil, nil
	}
	var downloads []core.DownloadLink
	for _, dl := range payload.Downloads {
		u := strings.TrimSpace(dl.URL)
		if u == "" {
			continue
		}
		downloads = append(downloads, core.DownloadLink{URL: u, Label: strings.TrimSpace(dl.Label)})
	}
	headers := map[string]string{}
	if len(payload.Headers) > 0 {
		for k, v := range payload.Headers {
			headers[k] = v
		}
	}
	if headers["Referer"] == "" {
		for _, s := range payload.Streams {
			if s.Referer != "" {
				headers["Referer"] = s.Referer
				break
			}
		}
	}
	if headers["Referer"] == "" {
		headers["Referer"] = "https://megaplay.buzz/"
	}
	return &SourceResult{Sources: sources, Headers: headers, Downloads: downloads, Intro: intro, Outro: outro}, nil
}

// resolveShow finds the AnikotoTV show slug and ID from an AniList ID.
// Checks the static mapping first, then searches anikototv.to dynamically.
func (p *AnikotoProvider) resolveShow(ctx context.Context, anilistID string) (slug string, showID string, err error) {
	// Fast path: check static mapping
	if entry := GetAnikotoMapping(anilistID); entry != nil {
		p.log.Info().Str("anilistId", anilistID).Str("slug", entry.Slug).Str("showId", entry.ShowID).Msg("anikoto: resolved from mapping")
		return entry.Slug, entry.ShowID, nil
	}

	// Slow path: fetch AniList title, search anikototv.to, verify on watch page
	title, err := p.fetchAniListTitle(ctx, anilistID)
	if err != nil {
		return "", "", fmt.Errorf("anilist title fetch failed: %w", err)
	}

	// Search anikototv.to — extract data-tip (show ID) and watch href
	searchURL := fmt.Sprintf("%s/search?keyword=%s", anikotoBase, url.QueryEscape(title))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, searchURL, nil)
	if err != nil {
		return "", "", err
	}
	p.setAjaxHeaders(req)
	req.Header.Del("X-Requested-With")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("search request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return "", "", err
	}
	html := string(body)

	// Each search result has: <div class="ani poster tip" data-tip="SHOW_ID"> <a href="...watch/SLUG/ep-1">
	// Extract pairs of (data-tip, watch-slug) from adjacent elements.
	entryRe := regexp.MustCompile(`data-tip="(\d+)"[^<]*<a[^>]*href="(?:https?://anikototv\.to)?/watch/([^"]+)"`)
	entries := entryRe.FindAllStringSubmatch(html, -1)
	if len(entries) == 0 {
		// Fallback: extract data-tip IDs and watch slugs separately
		tipRe := regexp.MustCompile(`data-tip="(\d+)"`)
		slugRe := regexp.MustCompile(`href="(?:https?://anikototv\.to)?/watch/([^"]+/ep-\d+)"`)
		tips := tipRe.FindAllStringSubmatch(html, -1)
		slugs := slugRe.FindAllStringSubmatch(html, -1)
		for i := 0; i < len(tips) && i < len(slugs); i++ {
			entries = append(entries, []string{"", tips[i][1], slugs[i][1]})
		}
	}

	// For each candidate, verify the AniList ID on the watch page
	anilistPattern := regexp.MustCompile(`anilist\.co/file/anilistcdn/media/anime/(?:banner|poster)/` + anilistID + `-`)
	seen := make(map[string]bool)
	for _, entry := range entries {
		if len(entry) < 3 {
			continue
		}
		tID := entry[1]
		slugCandidate := entry[2]
		// Strip /ep-N suffix to get the base slug
		baseSlug := regexp.MustCompile(`/ep-\d+$`).ReplaceAllString(slugCandidate, "")
		if seen[baseSlug] {
			continue
		}
		seen[baseSlug] = true

		// Fetch the watch page and verify AniList ID
		pageURL := fmt.Sprintf("%s/watch/%s", anikotoBase, slugCandidate)
		pageHTML, err := p.fetchPage(ctx, pageURL)
		if err != nil {
			continue
		}
		if anilistPattern.MatchString(pageHTML) {
			showIDFromPage := extractShowID(pageHTML)
			if showIDFromPage == "" {
				showIDFromPage = tID
			}
			p.log.Info().Str("slug", baseSlug).Str("showId", showIDFromPage).Msg("anikoto: resolved from search")
			// Cache the mapping for future lookups
			AddAnikotoMapping(anilistID, AnikotoMapping{
				ShowID: showIDFromPage,
				Slug:   baseSlug,
				Title:  title,
			})
			return baseSlug, showIDFromPage, nil
		}
	}

	return "", "", fmt.Errorf("no matching show found for anilistId=%s", anilistID)
}

// extractShowID pulls the data-id from the watch-main div.
func extractShowID(html string) string {
	re := regexp.MustCompile(`id="watch-main"[^>]*data-id="(\d+)"`)
	m := re.FindStringSubmatch(html)
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}

// fetchAniListTitle queries AniList GraphQL for the English or romaji title.
func (p *AnikotoProvider) fetchAniListTitle(ctx context.Context, anilistID string) (string, error) {
	query := `{"query":"{ Media(id:` + anilistID + `,type:ANIME){title{english romaji}} }"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.anilist.co",
		strings.NewReader(query))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
	if err != nil {
		return "", err
	}

	var result struct {
		Data struct {
			Media struct {
				Title struct {
					English *string `json:"english"`
					Romaji  *string `json:"romaji"`
				} `json:"title"`
			} `json:"Media"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	if result.Data.Media.Title.English != nil && *result.Data.Media.Title.English != "" {
		return *result.Data.Media.Title.English, nil
	}
	if result.Data.Media.Title.Romaji != nil && *result.Data.Media.Title.Romaji != "" {
		return *result.Data.Media.Title.Romaji, nil
	}
	return "", fmt.Errorf("no title found for anilistId=%s", anilistID)
}

// anikotoEpisode represents an episode entry from the HTML.
type anikotoEpisode struct {
	slug    string
	dataIDs string
	number  int
}

// fetchEpisodeDataIDs fetches the episode list and returns the data-ids for the target episode.
func (p *AnikotoProvider) fetchEpisodeDataIDs(ctx context.Context, showID string, episode int) (string, error) {
	// POST with empty style/vrf — the site requires this body
	episodeURL := fmt.Sprintf("%s/ajax/episode/list/%s", anikotoBase, showID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, episodeURL, strings.NewReader("style=&vrf="))
	if err != nil {
		return "", err
	}
	p.setAjaxHeaders(req)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("episode list request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return "", err
	}

	var result struct {
		Status int    `json:"status"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	if result.Status != 200 {
		return "", fmt.Errorf("episode list returned status %d", result.Status)
	}

	// Parse episode links: <a ... data-slug="6" data-ids="..." ...>
	// Match by data-slug (episode number as string)
	re := regexp.MustCompile(`<a[^>]*?data-slug="(\d+)"[^>]*?data-ids="([^"]+)"`)
	matches := re.FindAllStringSubmatch(result.Result, -1)

	episodeStr := strconv.Itoa(episode)
	for _, m := range matches {
		if len(m) >= 3 && m[1] == episodeStr {
			return m[2], nil
		}
	}

	// Fallback: try data-num attribute
	re2 := regexp.MustCompile(`<a[^>]*?data-num="(\d+)"[^>]*?data-ids="([^"]+)"`)
	matches2 := re2.FindAllStringSubmatch(result.Result, -1)
	for _, m := range matches2 {
		if len(m) >= 3 && m[1] == episodeStr {
			return m[2], nil
		}
	}

	return "", fmt.Errorf("episode %d not found in list", episode)
}

// anikotoServerEntry represents a server from the API.
type anikotoServerEntry struct {
	linkID     string
	svID       string
	name       string
	serverType string
}

// fetchServers fetches the server list for a given data-ids and language.
func (p *AnikotoProvider) fetchServers(ctx context.Context, dataIDs string, lang string) ([]anikotoServerEntry, error) {
	serverURL := fmt.Sprintf("%s/ajax/server/list?servers=%s", anikotoBase, url.QueryEscape(dataIDs))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL, nil)
	if err != nil {
		return nil, err
	}
	p.setAjaxHeaders(req)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("server list request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil, err
	}

	var result struct {
		Status int    `json:"status"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if result.Status != 200 {
		return nil, fmt.Errorf("server list returned status %d", result.Status)
	}

	// Parse server HTML
	// Pattern: <div class="type" data-type="sub"><label>...</label><ul>
	//   <li data-link-id="..." data-sv-id="..." ...>ServerName</li>
	// </ul></div>

	var entries []anikotoServerEntry

	// Find type containers
	typeRe := regexp.MustCompile(`<div[^>]*?class="type"[^>]*?data-type="([^"]+)"`)
	typeMatches := typeRe.FindAllStringSubmatch(result.Result, -1)

	for _, typeMatch := range typeMatches {
		serverType := typeMatch[1]

		// Filter by requested language
		if lang != "" && serverType != lang {
			continue
		}

		// Find servers within this type section
		// Extract the section between this type div and the next
		typeStart := strings.Index(result.Result, typeMatch[0])
		if typeStart == -1 {
			continue
		}

		liRe := regexp.MustCompile(`<li[^>]*?data-sv-id="([^"]+)"[^>]*?data-link-id="([^"]+)"[^>]*?>([^<]*(?:<b>[^<]*</b>[^<]*)?)</li>`)
		liMatches := liRe.FindAllStringSubmatch(result.Result[typeStart:], -1)

		for _, liMatch := range liMatches {
			if len(liMatch) >= 4 {
				name := strings.TrimSpace(liMatch[3])
				name = regexp.MustCompile(`<[^>]+>`).ReplaceAllString(name, "")
				name = strings.TrimSpace(name)

				entries = append(entries, anikotoServerEntry{
					linkID:     liMatch[2],
					svID:       liMatch[1],
					name:       name,
					serverType: serverType,
				})
			}
		}
	}

	return entries, nil
}

// fetchVideoURL gets the actual video iframe URL for a server link.
func (p *AnikotoProvider) fetchVideoURL(ctx context.Context, linkID string) (string, map[string][]float64, error) {
	serverURL := fmt.Sprintf("%s/ajax/server?get=%s", anikotoBase, url.QueryEscape(linkID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL, nil)
	if err != nil {
		return "", nil, err
	}
	p.setAjaxHeaders(req)

	resp, err := p.client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("video URL request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return "", nil, err
	}

	var result struct {
		Status int `json:"status"`
		Result struct {
			URL      string               `json:"url"`
			SkipData map[string][]float64 `json:"skip_data"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", nil, err
	}
	if result.Status != 200 {
		return "", nil, fmt.Errorf("video URL returned status %d", result.Status)
	}

	return result.Result.URL, result.Result.SkipData, nil
}

// setAjaxHeaders sets the standard headers for anikoto AJAX requests.
func (p *AnikotoProvider) setAjaxHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("Referer", anikotoBase+"/")
}

// fetchPage does a simple GET with browser UA and returns the response body as string.
func (p *AnikotoProvider) fetchPage(ctx context.Context, pageURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Referer", anikotoBase+"/")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return "", err
	}
	return string(body), nil
}
