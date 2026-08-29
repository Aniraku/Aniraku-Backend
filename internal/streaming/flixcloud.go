package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// FlixCloudProvider scrapes AnimeX watch pages to extract FlixCloud embed
// URLs. Two servers are offered per episode:
//   - Yuta → flixcloud.cc/e/{id}?v=1
//   - Syota → flixcloud.cc/e/{id}?v=2
//
// Language is auto-detected from what AnimeX has for the given episode.
// If the anime slug can't be resolved, FlixCloud silently yields nothing
// so Miruro handles it instead.
type FlixCloudProvider struct {
	client *http.Client
	log    zerolog.Logger
}

func NewFlixCloudProvider(log zerolog.Logger) *FlixCloudProvider {
	return &FlixCloudProvider{
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		log: log,
	}
}

func (p *FlixCloudProvider) Name() string { return "flixcloud" }

func (p *FlixCloudProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("flixcloud search not implemented")
}

func (p *FlixCloudProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("flixcloud episode listing not implemented")
}

// FindEpisodeSource scrapes the AnimeX watch page for the given anilist ID
// and episode, extracts FlixCloud access_ids, and returns embed sources.
// Returns nil (not an error) when the anime can't be found on AnimeX —
// callers should fall back to other providers.
func (p *FlixCloudProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	anilistID := providerID

	// Resolve the AnimeX slug for this AniList ID.
	slug, err := p.resolveSlug(ctx, anilistID)
	if err != nil {
		p.log.Debug().Err(err).Str("anilistId", anilistID).Msg("flixcloud: slug not found, skipping")
		return nil, nil // silent skip — not an error
	}

	// Scrape the watch page for FlixCloud access_ids.
	accessIDs, detectedLang, err := p.scrapeAccessID(ctx, slug, episode)
	if err != nil {
		p.log.Debug().Err(err).Str("slug", slug).Int("episode", episode).Msg("flixcloud: access_id not found")
		return nil, nil // silent skip
	}

	// "dual" means both sub and dub audio tracks — available for any request.
	if detectedLang != "dual" && lang != "" && detectedLang != lang {
		p.log.Debug().
			Str("requested", lang).
			Str("detected", detectedLang).
			Str("slug", slug).
			Int("episode", episode).
			Msg("flixcloud: language mismatch, skipping")
		return nil, nil
	}

	p.log.Info().
		Str("anilistId", anilistID).
		Str("slug", slug).
		Int("episode", episode).
		Strs("accessIds", accessIDs).
		Str("detectedLang", detectedLang).
		Msg("flixcloud: resolved")

	// Map each access_id to a server: first → Yuta, second → Syota, rest → Syota.
	serverNames := []string{"Yuta", "Syota"}
	var sources []core.Source
	for i, id := range accessIDs {
		name := "Syota"
		if i < len(serverNames) {
			name = serverNames[i]
		}
		_ = name // name used only for logging context
		sources = append(sources, core.Source{
			URL:          fmt.Sprintf("https://flixcloud.cc/e/%s", id),
			Type:         "embed",
			Quality:      "auto",
			Verification: "embed",
		})
	}

	return &SourceResult{
		Sources: sources,
		Headers: map[string]string{
			"Referer": "https://animex.one/",
		},
	}, nil
}

// resolveSlug builds the AnimeX watch-page slug from the AniList title.
// AnimeX URL format: /watch/{title-slug}-{anilistId}-episode-{ep}
func (p *FlixCloudProvider) resolveSlug(ctx context.Context, anilistID string) (string, error) {
	title, err := p.fetchAniListTitle(ctx, anilistID)
	if err != nil {
		return "", fmt.Errorf("anilist title fetch failed: %w", err)
	}

	slug := slugify(title) + "-" + anilistID
	return slug, nil
}

// fetchAniListTitle queries AniList GraphQL for the English or romaji title.
func (p *FlixCloudProvider) fetchAniListTitle(ctx context.Context, anilistID string) (string, error) {
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

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}

	var result struct {
		Data struct {
			Media struct {
				Title struct {
					English *string `json:"english"`
					Romaji *string `json:"romaji"`
				} `json:"title"`
			} `json:"Media"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	t := result.Data.Media.Title
	if t.English != nil && *t.English != "" {
		return *t.English, nil
	}
	if t.Romaji != nil && *t.Romaji != "" {
		return *t.Romaji, nil
	}
	return "", fmt.Errorf("no title found for anilist %s", anilistID)
}

// slugify converts a title into an AnimeX-compatible URL slug.
func slugify(title string) string {
	lower := strings.ToLower(title)
	var b strings.Builder
	prevHyphen := false
	for _, r := range lower {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevHyphen = false
		} else if r == ' ' || r == '-' || r == ':' || r == '\'' || r == '.' || r == ',' || r == '&' || r == '(' || r == ')' || r == '/' {
			if !prevHyphen && b.Len() > 0 {
				b.WriteRune('-')
				prevHyphen = true
			}
		} else {
			b.WriteRune(r)
			prevHyphen = false
		}
	}
	s := strings.TrimRight(b.String(), "-")
	s = strings.ReplaceAll(s, "--", "-")
	return s
}

// scrapeAccessID fetches the AnimeX watch page and extracts ALL FlixCloud
// access_ids from the embedded SvelteKit SSR data. It also detects the
// language (sub/dub/dual) based on the page content.
func (p *FlixCloudProvider) scrapeAccessID(ctx context.Context, slug string, episode int) (accessIDs []string, lang string, err error) {
	watchURL := fmt.Sprintf("https://animex.one/watch/%s-episode-%d", slug, episode)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, watchURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("animeX returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil, "", err
	}
	html := string(body)

	// Extract ALL access_ids from the SvelteKit SSR resolve data.
	pat := regexp.MustCompile(`access_id\s*:\s*"([a-zA-Z0-9]+)"`)
	matches := pat.FindAllStringSubmatch(html, -1)
	if len(matches) == 0 {
		return nil, "", fmt.Errorf("access_id not found in watch page")
	}
	for _, m := range matches {
		accessIDs = append(accessIDs, m[1])
	}

	// Detect language: AnimeX uses "sub", "dub", or "dual" in the audio field.
	// "dual" means the source has both sub and dub audio tracks.
	// Note: SvelteKit SSR uses unquoted keys (audio:"dual" not "audio":"dual").
	lang = "sub"
	lowerHTML := strings.ToLower(html)
	if strings.Contains(lowerHTML, `audio:"dub"`) {
		lang = "dub"
	} else if strings.Contains(lowerHTML, `audio:"dual"`) {
		lang = "dual"
	}

	return accessIDs, lang, nil
}
