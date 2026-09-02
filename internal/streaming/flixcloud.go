package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// FlixCloudProvider fetches embed URLs from the Reanime API and returns them
// as embed sources for the frontend's embedded player.
// Servers are named by access ID order: Yuta (1st), Syota (2nd), Mike (3rd+).
type FlixCloudProvider struct {
	client      *http.Client
	log         zerolog.Logger
	reanimeBase string
	flixBase    string
}

func NewFlixCloudProvider(log zerolog.Logger) *FlixCloudProvider {
	reanimeBase := strings.TrimRight(os.Getenv("ANIRAKU_REANIME_BASE"), "/")
	if reanimeBase == "" {
		reanimeBase = "https://reanime.to"
	}
	flixBase := strings.TrimRight(os.Getenv("ANIRAKU_FLIXCLOUD_BASE"), "/")
	if flixBase == "" {
		flixBase = "https://flixcloud.cc"
	}
	return &FlixCloudProvider{
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		log:         log,
		reanimeBase: reanimeBase,
		flixBase:    flixBase,
	}
}

func (p *FlixCloudProvider) Name() string { return "flixcloud" }

func (p *FlixCloudProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("flixcloud search not implemented")
}

func (p *FlixCloudProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("flixcloud episode listing not implemented")
}

type reanimeServer struct {
	ID         string `json:"$id"`
	ServerName string `json:"serverName"`
	DataLink   string `json:"dataLink"`
	DataType   string `json:"dataType"`
}

type reanimeResponse struct {
	Success bool            `json:"success"`
	Servers []reanimeServer `json:"servers"`
}

func (p *FlixCloudProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	reanimeURL := fmt.Sprintf("%s/api/flix/%s/%d", p.reanimeBase, providerID, episode)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reanimeURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := p.client.Do(req)
	if err != nil {
		p.log.Debug().Err(err).Str("anilistId", providerID).Int("episode", episode).Msg("flixcloud: reanime request failed")
		return nil, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.log.Debug().Int("status", resp.StatusCode).Str("anilistId", providerID).Msg("flixcloud: reanime returned non-200")
		return nil, nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	var apiResp reanimeResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		p.log.Debug().Err(err).Str("anilistId", providerID).Msg("flixcloud: reanime parse failed")
		return nil, nil
	}

	if !apiResp.Success || len(apiResp.Servers) == 0 {
		p.log.Debug().Str("anilistId", providerID).Msg("flixcloud: no servers from reanime")
		return nil, nil
	}

	type accessEntry struct {
		id     string
		subURL string
		dubURL string
	}
	seen := make(map[string]*accessEntry)
	var order []string

	for _, s := range apiResp.Servers {
		parts := strings.SplitN(s.ID, "-", 3)
		accessID := s.ID
		if len(parts) >= 3 {
			accessID = parts[1]
		}

		if lang != "" && lang != "dual" && s.DataType != lang {
			continue
		}

		embedURL := s.DataLink
		if embedURL == "" {
			embedURL = fmt.Sprintf("%s/e/%s?v=2", p.flixBase, accessID)
		}

		entry, exists := seen[accessID]
		if !exists {
			entry = &accessEntry{id: accessID}
			seen[accessID] = entry
			order = append(order, accessID)
		}

		switch s.DataType {
		case "sub":
			if entry.subURL == "" {
				entry.subURL = embedURL
			}
		case "dub":
			if entry.dubURL == "" {
				entry.dubURL = embedURL
			}
		}
	}

	if len(order) == 0 {
		return nil, nil
	}

	serverNames := []string{"Yuta", "Syota", "Mike"}
	var sources []core.Source

	for i, accessID := range order {
		entry := seen[accessID]

		name := "Mike"
		if i < len(serverNames) {
			name = serverNames[i]
		}

		p.log.Info().
			Str("anilistId", providerID).
			Int("episode", episode).
			Str("accessId", accessID).
			Str("serverName", name).
			Msg("flixcloud: resolved")

		if entry.subURL != "" {
			sources = append(sources, core.Source{
				URL:          entry.subURL,
				Type:         "embed",
				Quality:      "auto",
				Verification: "embed",
			})
		}

		if entry.dubURL != "" {
			sources = append(sources, core.Source{
				URL:          entry.dubURL,
				Type:         "embed",
				Quality:      "auto",
				Verification: "embed",
			})
		}
	}

	if len(sources) == 0 {
		return nil, nil
	}

	return &SourceResult{
		Sources: sources,
		Headers: map[string]string{},
	}, nil
}
