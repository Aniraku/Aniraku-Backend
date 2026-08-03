package streaming

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

type contextKey string

const refreshKey contextKey = "refresh_cache"

func WithRefresh(ctx context.Context) context.Context {
	return context.WithValue(ctx, refreshKey, true)
}

func IsRefresh(ctx context.Context) bool {
	v, _ := ctx.Value(refreshKey).(bool)
	return v
}

type Manager struct {
	log       zerolog.Logger
	providers []Provider
	client    *http.Client
}

type Provider interface {
	Name() string
	Search(ctx context.Context, title string) ([]SearchResult, error)
	FindEpisodes(ctx context.Context, providerID string) ([]Episode, error)
	FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error)
}

type SearchResult struct {
	ID    string
	Title string
}

type Episode struct {
	Number int
	Title  string
	Filler bool
	Recap  bool
}

type SourceResult struct {
	Sources []core.Source
	Headers map[string]string
}

func NewManager(log zerolog.Logger, miruroAPIBase string, httpClient *http.Client) *Manager {
	m := &Manager{
		log:    log,
		client: httpClient,
	}
	m.providers = []Provider{
		NewMiruroProvider(log, miruroAPIBase),
	}
	return m
}

func (m *Manager) GetSources(ctx context.Context, title string, episode int, lang, quality string) (*core.StreamResult, error) {
	return m.GetSourcesForProvider(ctx, title, episode, "", lang, quality, 0)
}

func (m *Manager) GetSourcesForProvider(ctx context.Context, title string, episode int, provider, lang, quality string, animeID int) (*core.StreamResult, error) {
	// Miruro-only: test all sub-providers, return working ones as servers
	result, err := m.tryMiruro(ctx, animeID, episode, lang, quality)
	if err == nil && result != nil && len(result.Sources) > 0 {
		return result, nil
	}
	return nil, err
}

// FindAllServers tests ALL Miruro sub-providers for the requested lang, returns every working one as a server.
func (m *Manager) FindAllServers(ctx context.Context, animeID int, episode int, lang string) []core.Server {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return nil
	}

	if lang == "" {
		lang = "sub"
	}

	anilistID := fmt.Sprintf("%d", animeID)
	serverMap := miruro.FindAllSources(ctx, anilistID, episode, lang)

	var servers []core.Server
	for name, sr := range serverMap {
		servers = append(servers, core.Server{
			Name:     name,
			Provider: "miruro",
			Lang:     lang,
			Sources:  sr.Sources,
			Headers:  sr.Headers,
		})
	}

	return servers
}

func (m *Manager) tryMiruro(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return nil, fmt.Errorf("miruro provider not available")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying miruro")

	// FindEpisodeSource uses the anilist ID string
	anilistID := fmt.Sprintf("%d", animeID)
	source, err := miruro.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("miruro failed: %w", err)
	}

	if source == nil || len(source.Sources) == 0 {
		return nil, fmt.Errorf("miruro returned no sources")
	}

	return m.applyQualityFilter(source, quality), nil
}

func (m *Manager) applyQualityFilter(result *SourceResult, quality string) *core.StreamResult {
	sources := result.Sources
	if quality != "auto" && quality != "" {
		filtered := filterByQuality(sources, quality)
		if len(filtered) > 0 {
			sources = filtered
		}
	}

	return &core.StreamResult{
		Sources: sources,
		Headers: result.Headers,
	}
}

func (m *Manager) HasAnimeDub(ctx context.Context, animeID int) bool {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return false
	}
	return miruro.HasDub(ctx, fmt.Sprintf("%d", animeID))
}

func (m *Manager) GetEpisodeThumbnails(ctx context.Context, animeID int) map[int]string {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return nil
	}
	return miruro.GetEpisodeThumbnails(ctx, fmt.Sprintf("%d", animeID))
}

func (m *Manager) GetEpisodeTitles(ctx context.Context, animeID int) map[int]string {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return nil
	}
	return miruro.GetEpisodeTitles(ctx, fmt.Sprintf("%d", animeID))
}

func (m *Manager) HasAnime(ctx context.Context, animeID int) bool {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return false
	}
	return miruro.HasAnime(ctx, fmt.Sprintf("%d", animeID))
}

func filterByQuality(sources []core.Source, quality string) []core.Source {
	var filtered []core.Source
	q := strings.ToLower(quality)
	for _, s := range sources {
		if strings.Contains(strings.ToLower(s.Quality), q) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
