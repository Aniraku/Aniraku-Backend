package streaming

import (
	"context"
	"fmt"
	"net/http"
	"sort"
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
	// LearnHost, when set, is forwarded to providers so that hosts vouched
	// for by the trusted provider chain (verified, reachable source URLs)
	// are fed to the media-proxy allowlist as they surface.
	LearnHost func(host string)
}

// SetHostLearner registers the callback that receives provider-vouched hosts.
func (m *Manager) SetHostLearner(fn func(host string)) {
	m.LearnHost = fn
	if miruro, ok := m.providers[0].(*MiruroProvider); ok {
		miruro.SetHostLearner(fn)
	}
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

func NewManager(log zerolog.Logger, miruroAPIBase, zenAPIBase string, httpClient *http.Client) *Manager {
	m := &Manager{
		log:    log,
		client: httpClient,
	}
	m.providers = []Provider{
		NewMiruroProvider(log, miruroAPIBase),
		NewZenProvider(log, zenAPIBase, httpClient),
	}
	return m
}

func (m *Manager) GetSources(ctx context.Context, title string, episode int, lang, quality string) (*core.StreamResult, error) {
	return m.GetSourcesForProvider(ctx, title, episode, "", lang, quality, 0, 0)
}

// GetSourcesForProvider resolves sources for the requested provider/lang.
// Fallback chain: Miruro sub-providers first; when they yield nothing, the
// ZEN API embed player (title + malID are used to match the anime there).
// An explicit provider == "zen" (user picked a "zein" server) resolves the
// ZEN API player first and only falls back to Miruro when Zen has nothing.
func (m *Manager) GetSourcesForProvider(ctx context.Context, title string, episode int, provider, lang, quality string, animeID, malID int) (*core.StreamResult, error) {
	zen, _ := m.providers[1].(*ZenProvider)
	if provider == "zen" {
		zenResult, zenErr := zen.Resolve(ctx, animeID, title, malID, episode, lang)
		if zenErr == nil && zenResult != nil && len(zenResult.Sources) > 0 {
			return m.applyQualityFilter(zenResult, quality), nil
		}
		result, err := m.tryMiruro(ctx, animeID, episode, provider, lang, quality)
		if err == nil && result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		if zenErr != nil {
			return nil, zenErr
		}
		return nil, err
	}

	result, err := m.tryMiruro(ctx, animeID, episode, provider, lang, quality)
	if err == nil && result != nil && len(result.Sources) > 0 {
		return result, nil
	}

	if zen == nil {
		return nil, err
	}
	zenResult, zenErr := zen.Resolve(ctx, animeID, title, malID, episode, lang)
	if zenErr == nil && zenResult != nil && len(zenResult.Sources) > 0 {
		m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("miruro empty, falling back to zen")
		return m.applyQualityFilter(zenResult, quality), nil
	}
	if err != nil {
		return nil, err
	}
	return nil, zenErr
}

// zenServerLimit is the cap of "zein" servers shown per lang.
const zenServerLimit = 2

// FindAllServers lists up to 4 selectable servers per lang: 2 Miruro servers
// (quality selection preserved) + up to 2 "zein" servers from the ZEN API.
// When Zen has nothing for the anime/lang, up to 4 working Miruro servers are
// listed instead. Miruro servers are sorted for deterministic selection.
func (m *Manager) FindAllServers(ctx context.Context, animeID int, episode int, lang, title string, malID int) []core.Server {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return nil
	}

	if lang == "" {
		lang = "sub"
	}

	anilistID := fmt.Sprintf("%d", animeID)
	serverMap := miruro.FindAllSources(ctx, anilistID, episode, lang)

	var miruroServers []core.Server
	for name, sr := range serverMap {
		miruroServers = append(miruroServers, core.Server{
			Name:     name,
			Provider: "miruro",
			Lang:     lang,
			Sources:  sr.Sources,
			Headers:  sr.Headers,
		})
	}
	sort.Slice(miruroServers, func(i, j int) bool {
		return miruroServers[i].Name < miruroServers[j].Name
	})

	var zenServers []core.Server
	if zen, ok := m.providers[1].(*ZenProvider); ok {
		zenResult, zenErr := zen.Resolve(ctx, animeID, title, malID, episode, lang)
		if zenErr == nil && zenResult != nil && len(zenResult.Sources) > 0 {
			for i, src := range zenResult.Sources {
				if i >= zenServerLimit {
					break
				}
				name := "zein"
				if i > 0 {
					name = fmt.Sprintf("zein %d", i+1)
				}
				zenServers = append(zenServers, core.Server{
					Name:     name,
					Provider: "zen",
					Lang:     lang,
					Sources:  []core.Source{src},
				})
			}
		}
	}

	miruroLimit := 2
	if len(zenServers) == 0 {
		miruroLimit = 4
	}
	if len(miruroServers) > miruroLimit {
		miruroServers = miruroServers[:miruroLimit]
	}

	return append(miruroServers, zenServers...)
}

func (m *Manager) tryMiruro(ctx context.Context, animeID int, episode int, provider, lang, quality string) (*core.StreamResult, error) {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return nil, fmt.Errorf("miruro provider not available")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Str("provider", provider).Msg("trying miruro")

	// FindEpisodeSource uses the anilist ID string
	anilistID := fmt.Sprintf("%d", animeID)
	source, err := miruro.findEpisodeSource(ctx, anilistID, episode, lang, provider)
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

// ProbePlayable reports whether the anime has at least one actually playable
// Miruro stream (source fetched and reachability-verified), plus the total
// sub/dub episode counts advertised in the catalog. Used by the hentai
// streamability filter so titles without playable streams never surface.
func (m *Manager) ProbePlayable(ctx context.Context, animeID int) (subCount, dubCount int, playable bool) {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return 0, 0, false
	}
	return miruro.ProbePlayable(ctx, fmt.Sprintf("%d", animeID))
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
