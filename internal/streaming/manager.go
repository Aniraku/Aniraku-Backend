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

// SetMediaProbe registers the playback-path gate (the media proxy) so
// providers only serve sources that would actually play through it.
func (m *Manager) SetMediaProbe(probe MediaProbe) {
	if miruro, ok := m.providers[0].(*MiruroProvider); ok {
		miruro.SetMediaProbe(probe)
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
	// Intro/Outro are Miruro-provided skip segments, passed through to the
	// client so it can offer manual skip buttons.
	Intro *core.SkipTimestamp
	Outro *core.SkipTimestamp
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
	return m.GetSourcesForProvider(ctx, episode, "", lang, quality, 0)
}

// GetSourcesForProvider resolves sources for the requested provider/lang.
func (m *Manager) GetSourcesForProvider(ctx context.Context, episode int, provider, lang, quality string, animeID int) (*core.StreamResult, error) {
	result, err := m.tryMiruro(ctx, animeID, episode, provider, lang, quality)
	if err == nil && result != nil && len(result.Sources) > 0 {
		return result, nil
	}
	return nil, err
}

// FindAllServers lists every selectable Miruro server per lang (quality
// selection preserved). Ordering is deterministic: best playback verdict
// first (proxy > direct > embed > dead), then provider order. No server is
// hidden — datacenter verdicts are hints, not filters.
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

	// Deterministic provider order: best verdict first, then known-provider
	// order, then unknown providers in API order.
	idxOf := func(name string) int {
		for i, n := range miruroAllProviders {
			if n == name {
				return i
			}
		}
		return len(miruroAllProviders)
	}

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
	sort.SliceStable(miruroServers, func(i, j int) bool {
		bi := serverVerdictRank(miruroServers[i])
		bj := serverVerdictRank(miruroServers[j])
		if bi != bj {
			return bi > bj
		}
		return idxOf(miruroServers[i].Name) < idxOf(miruroServers[j].Name)
	})

	return miruroServers
}

// serverVerdictRank maps a server's best per-source verification tag to a
// comparable rank (proxy > direct > embed > dead).
func serverVerdictRank(s core.Server) int {
	best := 0
	for _, src := range s.Sources {
		switch src.Verification {
		case "proxy":
			return 3
		case "direct":
			if best < 2 {
				best = 2
			}
		case "embed":
			if best < 1 {
				best = 1
			}
		}
	}
	return best
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
		Intro:   result.Intro,
		Outro:   result.Outro,
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

// GetEpisodeFlags returns filler/recap flags per episode number.
func (m *Manager) GetEpisodeFlags(ctx context.Context, animeID int) (map[int]bool, map[int]bool) {
	miruro, ok := m.providers[0].(*MiruroProvider)
	if !ok {
		return nil, nil
	}
	return miruro.GetEpisodeFlags(ctx, fmt.Sprintf("%d", animeID))
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
