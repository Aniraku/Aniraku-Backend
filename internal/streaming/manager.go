package streaming

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

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

// PlaybackVerdict ranks how a source can reach a player. Verdicts are soft
// ordering hints, never filters: a "dead" datacenter verdict does not mean
// the source is dead for a residential browser.
type PlaybackVerdict int

const (
	VerdictDead PlaybackVerdict = iota
	VerdictEmbed
	VerdictDirect
	VerdictProxy
)

func (v PlaybackVerdict) String() string {
	switch v {
	case VerdictProxy:
		return "proxy"
	case VerdictDirect:
		return "direct"
	case VerdictEmbed:
		return "embed"
	}
	return "dead"
}

type Manager struct {
	log       zerolog.Logger
	providers []Provider
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
	Sources   []core.Source
	Headers   map[string]string
	Downloads []core.DownloadLink
	// Intro/Outro are provider skip segments, passed through to the client
	// so it can offer manual skip buttons.
	Intro *core.SkipTimestamp
	Outro *core.SkipTimestamp
}

// NewManager builds the provider set. Anikoto is primary (fully in-process:
// show resolve -> episode data-ids -> servers -> embed decrypt -> verified
// m3u8); FlixCloud is the fallback for embed playback.
func NewManager(log zerolog.Logger) *Manager {
	return &Manager{
		log: log,
		providers: []Provider{
			NewAnikotoProvider(log),
			NewFlixCloudProvider(log),
		},
	}
}

// GetSources resolves sources using the default provider order (anikoto,
// then flixcloud).
func (m *Manager) GetSources(ctx context.Context, title string, episode int, lang, quality string) (*core.StreamResult, error) {
	return m.GetSourcesForProvider(ctx, episode, "", lang, quality, 0)
}

// GetSourcesForProvider resolves sources for the requested provider/lang.
func (m *Manager) GetSourcesForProvider(ctx context.Context, episode int, provider, lang, quality string, animeID int) (*core.StreamResult, error) {
	return m.GetSourcesForProviderWithSlug(ctx, episode, provider, lang, quality, animeID, "")
}

// GetSourcesForProviderWithSlug is the frontend-aware streaming entry point.
// slug is accepted for API compatibility; the Anikoto resolver maps AniList
// IDs itself, so the slug is unused today.
func (m *Manager) GetSourcesForProviderWithSlug(ctx context.Context, episode int, provider, lang, quality string, animeID int, slug string) (*core.StreamResult, error) {
	switch provider {
	case "anikoto":
		result, err := m.tryAnikoto(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("anikoto: no sources for this episode")
	case "flixcloud":
		result, err := m.tryFlixCloudWithSlug(ctx, animeID, episode, lang, quality, slug)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("flixcloud: no sources for this episode")
	case "miruro", "hop", "bonk", "bee", "moo", "ally", "pewe", "kiwi", "mimi":
		return nil, fmt.Errorf("provider %q removed - use anikoto or flixcloud", provider)
	}
	var lastErr error
	// Anikoto direct first, FlixCloud embed fallback.
	candidates := []func() (*core.StreamResult, error){
		func() (*core.StreamResult, error) { return m.tryAnikoto(ctx, animeID, episode, lang, quality) },
		func() (*core.StreamResult, error) { return m.tryFlixCloudWithSlug(ctx, animeID, episode, lang, quality, slug) },
	}
	for _, try := range candidates {
		res, err := try()
		if err == nil && res != nil && len(res.Sources) > 0 {
			return res, nil
		}
		if err != nil {
			lastErr = err
		} else if lastErr == nil {
			lastErr = fmt.Errorf("no sources from provider")
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no streaming source found for this episode")
}

// containsHentai returns true if the genre list contains the Hentai genre.
func containsHentai(genres []string) bool {
	for _, g := range genres {
		if strings.EqualFold(g, "hentai") {
			return true
		}
	}
	return false
}

// FindAllServers lists every selectable server per lang (quality selection
// preserved). Ordering is deterministic: best playback verdict first (proxy >
// direct > embed > dead), then provider order. No server is hidden — dead
// providers simply contribute nothing.
// genres is used to skip providers that should not serve certain content
// (e.g. Anikoto is skipped for Hentai titles).
func (m *Manager) FindAllServers(ctx context.Context, animeID int, episode int, lang string, genres []string) []core.Server {
	if lang == "" {
		lang = "sub"
	}

	// All providers run AT ONCE (fan-out), then merge in fixed provider
	// order and rank by playback verdict.
	anilistID := fmt.Sprintf("%d", animeID)
	var akServers, fcServers []core.Server
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if !containsHentai(genres) {
			akServers = m.collectAnikotoServers(ctx, anilistID, episode, lang)
		}
	}()
	go func() {
		defer wg.Done()
		fcServers = m.collectFlixServers(ctx, anilistID, episode, lang)
	}()
	wg.Wait()

	allServers := append(akServers, fcServers...)
	sort.SliceStable(allServers, func(i, j int) bool {
		return serverVerdictRank(allServers[i]) > serverVerdictRank(allServers[j])
	})

	return allServers
}

// collectFlixServers maps FlixCloud sources to Yuta/Syota/Mike servers.
func (m *Manager) collectFlixServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		fc, ok := prov.(*FlixCloudProvider)
		if !ok {
			continue
		}
		sr, err := fc.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil || sr == nil || len(sr.Sources) == 0 {
			continue // silent skip
		}
		serverNames := []string{"Yuta", "Syota", "Mike"}
		for i, src := range sr.Sources {
			name := "Mike"
			if i < len(serverNames) {
				name = serverNames[i]
			}
			out = append(out, core.Server{
				Name:     name,
				Provider: "flixcloud",
				Lang:     lang,
				Sources:  []core.Source{src},
				Headers:  sr.Headers,
			})
		}
	}
	return out
}

// collectAnikotoServers maps Anikoto sources to Niko + Momo servers.
func (m *Manager) collectAnikotoServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		ak, ok := prov.(*AnikotoProvider)
		if !ok {
			continue
		}
		sr, err := ak.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil || sr == nil || len(sr.Sources) == 0 {
			continue
		}
		serverNames := anikotoServers
		for i, src := range sr.Sources {
			name := serverNames[len(serverNames)-1]
			if i < len(serverNames) {
				name = serverNames[i]
			}
			out = append(out, core.Server{
				Name:      name,
				Provider:  "anikoto",
				Lang:      lang,
				Sources:   []core.Source{src},
				Headers:   sr.Headers,
				Downloads: sr.Downloads,
			})
		}
	}
	return out
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

func (m *Manager) getFlixCloudProvider() *FlixCloudProvider {
	for _, p := range m.providers {
		if fc, ok := p.(*FlixCloudProvider); ok {
			return fc
		}
	}
	return nil
}

func (m *Manager) tryFlixCloudWithSlug(ctx context.Context, animeID int, episode int, lang, quality, slug string) (*core.StreamResult, error) {
	fc := m.getFlixCloudProvider()
	if fc == nil {
		return nil, fmt.Errorf("flixcloud provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying flixcloud")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := fc.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("flixcloud failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}

	return m.applyQualityFilter(source, quality), nil
}

func (m *Manager) getAnikotoProvider() *AnikotoProvider {
	for _, p := range m.providers {
		if ak, ok := p.(*AnikotoProvider); ok {
			return ak
		}
	}
	return nil
}

func (m *Manager) tryAnikoto(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	ak := m.getAnikotoProvider()
	if ak == nil {
		return nil, fmt.Errorf("anikoto provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying anikoto")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := ak.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("anikoto failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}

	return m.applyQualityFilter(source, quality), nil
}

func (m *Manager) applyQualityFilter(result *SourceResult, quality string) *core.StreamResult {
	qualities := sourceQualities(result.Sources)
	sources := result.Sources
	if quality != "auto" && quality != "" {
		filtered := filterByQuality(sources, quality)
		if len(filtered) > 0 {
			sources = filtered
		}
	}

	return &core.StreamResult{
		Sources:   sources,
		Headers:   result.Headers,
		Qualities: qualities,
		Downloads: result.Downloads,
		Intro:     result.Intro,
		Outro:     result.Outro,
	}
}

// sourceQualities reports only provider-returned labels. Clients must never
// invent adaptive renditions: an Auto HLS source without explicit variants
// remains Auto because the native Expo player does not expose a writable level
// selector.
func sourceQualities(sources []core.Source) []string {
	seen := make(map[string]bool)
	qualities := make([]string, 0, len(sources))
	for _, source := range sources {
		quality := strings.TrimSpace(source.Quality)
		if quality == "" || seen[strings.ToLower(quality)] {
			continue
		}
		seen[strings.ToLower(quality)] = true
		qualities = append(qualities, quality)
	}
	return qualities
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
