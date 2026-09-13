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
	// LearnHost, when set, receives hosts the provider chain itself verified
	// (probed stream manifests, subtitle tracks, download links) so the HTTP
	// layer can feed the media-proxy CDN allowlist and provider CDN rotation
	// never 403s at the gate.
	LearnHost func(host string)
}

// SetHostLearner registers the callback that receives provider-verified hosts.
func (m *Manager) SetHostLearner(fn func(host string)) {
	m.LearnHost = fn
	for _, p := range m.providers {
		if ak, ok := p.(*AnikotoProvider); ok {
			ak.SetHostLearner(fn)
		}
		if zk, ok := p.(*ZokoProvider); ok {
			zk.SetHostLearner(fn)
		}
		if ax, ok := p.(*AnimeXProvider); ok {
			ax.SetHostLearner(fn)
		}
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
	Sources    []core.Source
	Headers    map[string]string
	Downloads  []core.DownloadLink
	ServerName string
	// Intro/Outro are provider skip segments, passed through to the client
	// so it can offer manual skip buttons.
	Intro *core.SkipTimestamp
	Outro *core.SkipTimestamp
}

// NewManager builds the provider set. Anikoto is primary (fully in-process:
// show resolve -> episode data-ids -> servers -> embed decrypt -> verified
// m3u8); AnimeX (plyr API, XOR-decoded direct URLs) is second; Zoko
// (ZokoAnime, AniList-keyed) is third; FlixCloud is the fallback for embed
// playback.
func NewManager(log zerolog.Logger) *Manager {
	return &Manager{
		log: log,
		providers: []Provider{
			NewAnikotoProvider(log),
			NewAnimeXProvider(log, ""),
			NewZokoProvider(log),
			NewFlixCloudProvider(log),
		},
	}
}

// GetSources resolves sources using the default provider order (anikoto,
// zoko, then flixcloud).
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
	case "animex", "yuki", "neko", "zuna", "sora":
		result, err := m.tryAnimeX(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("animex: no sources for this episode")
	case "zoko", "zokoanime":
		// Frontend sends "zoko"; accept "zokoanime" as an alias. Backend
		// hits ZokoAnime (/stream/ani/{anilistId}/{ep}/{sub|dub}).
		result, err := m.tryZoko(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("zoko: no sources for this episode")
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
		return nil, fmt.Errorf("provider %q removed - use anikoto, zoko or flixcloud", provider)
	}
	var lastErr error
	// Anikoto direct first, AnimeX (plyr API) second, Zoko (AniList-keyed)
	// third, FlixCloud embed fallback.
	candidates := []func() (*core.StreamResult, error){
		func() (*core.StreamResult, error) { return m.tryAnikoto(ctx, animeID, episode, lang, quality) },
		func() (*core.StreamResult, error) { return m.tryAnimeX(ctx, animeID, episode, lang, quality) },
		func() (*core.StreamResult, error) { return m.tryZoko(ctx, animeID, episode, lang, quality) },
		func() (*core.StreamResult, error) {
			return m.tryFlixCloudWithSlug(ctx, animeID, episode, lang, quality, slug)
		},
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
	var akServers, axServers, zkServers, fcServers []core.Server
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		if !containsHentai(genres) {
			akServers = m.collectAnikotoServers(ctx, anilistID, episode, lang)
		}
	}()
	go func() {
		defer wg.Done()
		if !containsHentai(genres) {
			axServers = m.collectAnimeXServers(ctx, anilistID, episode, lang)
		}
	}()
	go func() {
		defer wg.Done()
		if !containsHentai(genres) {
			zkServers = m.collectZokoServers(ctx, anilistID, episode, lang)
		}
	}()
	go func() {
		defer wg.Done()
		fcServers = m.collectFlixServers(ctx, anilistID, episode, lang)
	}()
	wg.Wait()

	// Merged downloads for Zoko: Zoko's own link plus the already-fetched
	// Anikoto download links together — falling back to one downloads-only
	// Anikoto lookup when Anikoto streams were all CDN-blocked (no servers,
	// but downloads may still exist). No extra network call in the common
	// case — akServers was fetched in parallel above.
	zkServers = mergeZokoDownloads(ctx, m, zkServers, akServers, anilistID, episode, lang)

	allServers := append(append(append(akServers, axServers...), zkServers...), fcServers...)
	sort.SliceStable(allServers, func(i, j int) bool {
		return serverVerdictRank(allServers[i]) > serverVerdictRank(allServers[j])
	})

	return allServers
}

// appendNamedServers maps a provider's source list onto display names
// (Niko/Momo, Yuta/Syota/Mike, ...). When there are more sources than names,
// extras collide on the last name — on a collision the source with the
// richer subtitle track list wins, the other is dropped, so a display name
// never appears twice in the server list.
func appendNamedServers(out []core.Server, serverNames []string, provider, lang string, sr *SourceResult) []core.Server {
	nameIndex := make(map[string]int, len(serverNames))
	for i, src := range sr.Sources {
		name := serverNames[len(serverNames)-1]
		if i < len(serverNames) {
			name = serverNames[i]
		}
		if idx, ok := nameIndex[name]; ok {
			if len(src.Subtitles) > len(out[idx].Sources[0].Subtitles) {
				out[idx].Sources = []core.Source{src}
			}
			continue
		}
		nameIndex[name] = len(out)
		out = append(out, core.Server{
			Name:      name,
			Provider:  provider,
			Lang:      lang,
			Sources:   []core.Source{src},
			Headers:   sr.Headers,
			Downloads: sr.Downloads,
		})
	}
	return out
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
		out = appendNamedServers(out, []string{"Yuta", "Syota", "Mike"}, "flixcloud", lang, sr)
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
		out = appendNamedServers(out, anikotoServers[:], "anikoto", lang, sr)
	}
	return out
}

// collectZokoServers maps ZokoAnime sources to the single "Zoko" server.
// The provider stays probe-verified + proxied (never direct): ZokoProvider
// drops CDN-blocked manifests before they can surface here.
func (m *Manager) collectZokoServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		zk, ok := prov.(*ZokoProvider)
		if !ok {
			continue
		}
		sr, err := zk.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil || sr == nil || len(sr.Sources) == 0 {
			continue // silent skip: CDN-blocked or missing episode
		}
		out = appendNamedServers(out, []string{zokoServerName}, "zoko", lang, sr)
	}
	return out
}

// mergeZokoDownloads merges the Anikoto download links into the Zoko
// servers alongside Zoko's own link (deduped by URL, Zoko's own first).
// Stream sources are never mixed — only Downloads. Falls back to a
// downloads-only Anikoto lookup when the parallel Anikoto fetch produced no
// servers (e.g. every stream CDN-blocked).
func mergeZokoDownloads(ctx context.Context, m *Manager, zkServers, akServers []core.Server, anilistID string, episode int, lang string) []core.Server {
	if len(zkServers) == 0 {
		return zkServers
	}
	var links []core.DownloadLink
	for _, s := range akServers {
		links = mergeDownloadLinks(links, s.Downloads)
	}
	if len(links) == 0 {
		if ak := m.getAnikotoProvider(); ak != nil {
			links = ak.FetchDownloadLinks(ctx, anilistID, episode, lang)
		}
	}
	for i := range zkServers {
		zkServers[i].Downloads = mergeDownloadLinks(zkServers[i].Downloads, links)
	}
	return zkServers
}

// mergeDownloadLinks dedupes download links by URL, keeping primary order.
func mergeDownloadLinks(primary, fallback []core.DownloadLink) []core.DownloadLink {
	seen := make(map[string]bool, len(primary)+len(fallback))
	out := make([]core.DownloadLink, 0, len(primary)+len(fallback))
	for _, d := range primary {
		if d.URL == "" || seen[d.URL] {
			continue
		}
		seen[d.URL] = true
		out = append(out, d)
	}
	for _, d := range fallback {
		if d.URL == "" || seen[d.URL] {
			continue
		}
		seen[d.URL] = true
		out = append(out, d)
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

func (m *Manager) getZokoProvider() *ZokoProvider {
	for _, p := range m.providers {
		if zk, ok := p.(*ZokoProvider); ok {
			return zk
		}
	}
	return nil
}

func (m *Manager) getAnimeXProvider() *AnimeXProvider {
	for _, p := range m.providers {
		if ax, ok := p.(*AnimeXProvider); ok {
			return ax
		}
	}
	return nil
}

// tryAnimeX resolves an AnimeX stream (plyr API with XOR-decoded direct URLs).
func (m *Manager) tryAnimeX(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	ax := m.getAnimeXProvider()
	if ax == nil {
		return nil, fmt.Errorf("animex provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying animex")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := ax.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("animex failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}

	return m.applyQualityFilter(source, quality), nil
}

// collectAnimeXServers maps AnimeX sources to AnimeX-1 / AnimeX-2 servers.
func (m *Manager) collectAnimeXServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		ax, ok := prov.(*AnimeXProvider)
		if !ok {
			continue
		}
		sr, err := ax.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil || sr == nil || len(sr.Sources) == 0 {
			continue // silent skip
		}
		name := sr.ServerName
		if name == "" {
			name = "AnimeX"
		}
		out = appendNamedServers(out, []string{name}, "animex", lang, sr)
	}
	return out
}

// tryZoko resolves a ZokoAnime stream (frontend "zoko" -> backend ZokoAnime
// /stream/ani/{anilistId}/{ep}/{sub|dub}). The stream itself always comes
// from ZokoAnime, probe-verified + proxied — never Anikoto direct. Downloads
// are merged: Zoko's own link plus the Anikoto download links together.
func (m *Manager) tryZoko(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	zk := m.getZokoProvider()
	if zk == nil {
		return nil, fmt.Errorf("zoko provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying zoko")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := zk.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("zoko failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}

	if ak := m.getAnikotoProvider(); ak != nil {
		source.Downloads = mergeDownloadLinks(source.Downloads, ak.FetchDownloadLinks(ctx, anilistID, episode, lang))
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
