package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

type SenshiProvider struct {
	baseURL string
	client  *http.Client
	log     zerolog.Logger
}

func NewSenshiProvider(log zerolog.Logger, httpClient *http.Client) *SenshiProvider {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &SenshiProvider{
		baseURL: "https://senshi.live",
		client:  httpClient,
		log:     log,
	}
}

func (p *SenshiProvider) SetClient(client *http.Client) {
	if client != nil {
		p.client = client
	}
}

func (p *SenshiProvider) Name() string { return "senshi" }

type senshiSearchResponse struct {
	Data []struct {
		ID           json.RawMessage `json:"id"`
		PublicID     string          `json:"public_id"`
		Title        string          `json:"title"`
		TitleEnglish string          `json:"title_english"`
		Type         string          `json:"type"`
	} `json:"data"`
}

type senshiEpisode struct {
	ID        int     `json:"id"`
	EpID      int     `json:"ep_id"`
	MalID     int     `json:"mal_id"`
	EpTitle   string  `json:"ep_title"`
	EpFiller  bool    `json:"ep_filler"`
	EpRecap   bool    `json:"ep_recap"`
}

type senshiSource struct {
	URL     string `json:"url"`
	Server2 string `json:"server2"`
	Status  string `json:"status"`
}

func (p *SenshiProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	body, _ := json.Marshal(map[string]any{
		"searchTerm": title,
		"page":       1,
		"limit":      20,
	})

	req, err := http.NewRequestWithContext(ctx, "POST", p.baseURL+"/anime/filter", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Referer", p.baseURL)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var payload senshiSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	// Build results and score them for relevance
	type scoredResult struct {
		result SearchResult
		score  int
	}

	scored := make([]scoredResult, 0, len(payload.Data))
	titleLower := strings.ToLower(title)

	for _, item := range payload.Data {
		itemTitle := item.TitleEnglish
		if itemTitle == "" {
			itemTitle = item.Title
		}

		var numericID string
		if err := json.Unmarshal(item.ID, &numericID); err != nil {
			var num float64
			if err2 := json.Unmarshal(item.ID, &num); err2 == nil {
				numericID = fmt.Sprintf("%.0f", num)
			}
		}

		itemTitleLower := strings.ToLower(itemTitle)
		score := 0

		// Heavily prefer TV type over movies/specials
		if item.Type == "TV" {
			score += 200
		} else if item.Type == "OVA" {
			score += 50
		}
		// Movies and specials get no bonus (score stays at base)

		// Exact match gets highest score
		if itemTitleLower == titleLower {
			score = 100
		} else if strings.HasPrefix(itemTitleLower, titleLower) {
			// Starts with the search title (e.g., "Bleach" matches "Bleach: Thousand-Year Blood War")
			score = 50
		} else if strings.Contains(itemTitleLower, titleLower) {
			// Contains the search title
			score = 30
		}

		// Penalize sequels/prequels
		sequelMarkers := []string{"part 2", "part 3", "part 4", "season 2", "season 3", "season 4",
			"the separation", "the conflict", "the calamity", "blood war",
			"shippuden", " gt", " super", " returns", " next"}
		for _, marker := range sequelMarkers {
			if strings.Contains(itemTitleLower, marker) {
				score -= 30
			}
		}

		// Bonus for exact length match (prefer original series)
		if len(itemTitleLower) == len(titleLower) {
			score += 20
		}

		// Prefer results with fewer words (original series usually shorter)
		itemWords := len(strings.Fields(itemTitleLower))
		searchWords := len(strings.Fields(titleLower))
		if itemWords <= searchWords+1 {
			score += 10
		}

		// Prefer original TV series over specials/OVAs
		scored = append(scored, scoredResult{
			result: SearchResult{
				ID:    numericID,
				Title: itemTitle,
			},
			score: score,
		})
	}

	// Sort by score (highest first)
	for i := 0; i < len(scored); i++ {
		for j := i + 1; j < len(scored); j++ {
			if scored[j].score > scored[i].score {
				scored[i], scored[j] = scored[j], scored[i]
			}
		}
	}

	results := make([]SearchResult, 0, len(scored))
	for _, s := range scored {
		results = append(results, s.result)
	}

	return results, nil
}

func (p *SenshiProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", p.baseURL+"/episodes/"+url.PathEscape(providerID), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Referer", p.baseURL)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var episodes []senshiEpisode
	if err := json.Unmarshal(body, &episodes); err != nil {
		return nil, err
	}

	result := make([]Episode, 0, len(episodes))
	for _, ep := range episodes {
		result = append(result, Episode{
			Number: ep.EpID,
			Title:  ep.EpTitle,
			Filler: ep.EpFiller,
			Recap:  ep.EpRecap,
		})
	}

	return result, nil
}

func (p *SenshiProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	// First get episode list to find the ep_id and intro/outro timestamps
	episodes, err := p.FindEpisodes(ctx, providerID)
	if err != nil {
		return nil, err
	}

	if episode < 1 || episode > len(episodes) {
		return nil, fmt.Errorf("episode %d not found", episode)
	}

	// Get the MAL ID from the first search result (we need it for source lookup)
	// For now, use the providerID as a fallback
	epID := fmt.Sprintf("%d", episode)

	// Get source URLs
	sourceURL := fmt.Sprintf("%s/episode-embeds/%s/%s", p.baseURL, url.PathEscape(providerID), epID)
	req, err := http.NewRequestWithContext(ctx, "GET", sourceURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Referer", p.baseURL)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var sources []senshiSource
	if err := json.NewDecoder(resp.Body).Decode(&sources); err != nil {
		return nil, err
	}

	// Filter by language
	var filtered []senshiSource
	for _, s := range sources {
		if lang == "dub" {
			if strings.EqualFold(s.Status, "Dub") {
				filtered = append(filtered, s)
			}
		} else if lang == "sub" {
			if strings.EqualFold(s.Status, "HardSub") || strings.EqualFold(s.Status, "Sub") {
				filtered = append(filtered, s)
			}
		}
	}
	// Only fallback to all sources if Sub was requested and no HardSub found
	if len(filtered) == 0 && lang != "dub" {
		filtered = sources
	}
	if len(filtered) == 0 {
		return nil, fmt.Errorf("no %s source available for this episode", strings.ToUpper(lang))
	}

	// Build core.Source list
	coreSources := make([]core.Source, 0, len(filtered))
	for _, s := range filtered {
		u := s.URL
		if u == "" {
			u = s.Server2
		}
		if u == "" {
			continue
		}

		streamType := "hls"
		if strings.Contains(u, ".mp4") {
			streamType = "mp4"
		}

		coreSources = append(coreSources, core.Source{
			URL:     u,
			Type:    streamType,
			Quality: "auto",
		})
	}

	if len(coreSources) == 0 {
		return nil, fmt.Errorf("no video sources found")
	}

	return &SourceResult{
		Sources: coreSources,
		Headers: map[string]string{
			"Referer":    p.baseURL,
			"User-Agent": browserUA,
		},
	}, nil
}
