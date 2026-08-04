package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// All seven Miruro sub-providers. No priority ordering — all are tested equally.
var miruroAllProviders = []string{"bee", "bonk", "ally", "moo", "pewe", "kiwi", "hop"}

// subBadProviders are known to fail for sub (444 or filtered to 0)
var subBadProviders = map[string]bool{"kiwi": true, "hop": true}

// dubBadProviders are known to fail for dub (404/429)
var dubBadProviders = map[string]bool{"ally": true, "kiwi": true, "pewe": true, "hop": true}

type MiruroProvider struct {
	apiBase string
	client  *http.Client
	log     zerolog.Logger
}

func NewMiruroProvider(log zerolog.Logger, apiBase string) *MiruroProvider {
	if apiBase == "" {
		apiBase = "https://miruro-api-v3.onrender.com"
	}
	return &MiruroProvider{
		apiBase: apiBase,
		client:  &http.Client{Timeout: 60 * time.Second},
		log:     log,
	}
}

func (p *MiruroProvider) Name() string { return "miruro" }

// --- Miruro API response types ---

type miruroEpisodesResponse struct {
	Mappings  miruroMappings                `json:"mappings"`
	Providers map[string]miruroProviderData `json:"providers"`
}

type miruroMappings struct {
	AnilistID int    `json:"aniId"`
	MalID     int    `json:"malId"`
	Title     string `json:"title"`
}

type miruroProviderData struct {
	Episodes miruroProviderEpisodes `json:"episodes"`
}

type miruroProviderEpisodes struct {
	Sub []miruroEpisode `json:"sub"`
	Dub []miruroEpisode `json:"dub"`
}

type miruroEpisode struct {
	ID       string  `json:"id"`
	Number   float64 `json:"number"`
	Title    string  `json:"title"`
	Image    string  `json:"image"`
	AirDate  string  `json:"airDate"`
	Duration float64 `json:"duration"`
	Filler   bool    `json:"filler"`
	URL      string  `json:"url"`
}

type miruroSourceResponse struct {
	Streams   []miruroStream   `json:"streams"`
	Subtitles []miruroSubtitle `json:"subtitles"`
	Intro     *miruroTimestamp `json:"intro"`
	Outro     *miruroTimestamp `json:"outro"`
}

type miruroStream struct {
	URL      string `json:"url"`
	Type     string `json:"type"`
	Quality  string `json:"quality"`
	Audio    string `json:"audio"`
	Referer  string `json:"referer"`
	Server   string `json:"server"`
	IsActive bool   `json:"isActive"`
}

type miruroSubtitle struct {
	URL   string `json:"url"`
	Label string `json:"label"`
	Lang  string `json:"lang"`
	Kind  string `json:"kind"`
}

type miruroTimestamp struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

// --- cached episode data per anime ---

type miruroAnimeCache struct {
	providers map[string]miruroProviderData
	fetchedAt time.Time
}

var (
	miruroCache      = map[string]*miruroAnimeCache{}
	miruroCacheMu    sync.RWMutex
	miruroSourceCache = map[string]*miruroSourceCacheEntry{}
	miruroSourceMu   sync.RWMutex
)

// --- cached stream sources per (anilistID, episode, lang) ---
type miruroSourceCacheEntry struct {
	result    *SourceResult
	fetchedAt time.Time
}

func cacheKey(anilistID string) string { return anilistID }
func sourceCacheKey(aid string, ep int, lang string) string {
	return aid + ":" + strconv.Itoa(ep) + ":" + lang
}

func (p *MiruroProvider) fetchEpisodes(ctx context.Context, anilistID string) (*miruroEpisodesResponse, error) {
	key := cacheKey(anilistID)
	miruroCacheMu.RLock()
	if c, ok := miruroCache[key]; ok && time.Since(c.fetchedAt) < 5*time.Minute {
		miruroCacheMu.RUnlock()
		return &miruroEpisodesResponse{Providers: c.providers}, nil
	}
	miruroCacheMu.RUnlock()

	req, err := http.NewRequestWithContext(ctx, "GET", p.apiBase+"/episodes/"+anilistID, nil)
	if err != nil {
		return nil, err
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("miruro episodes request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("miruro episodes returned %d: %s", resp.StatusCode, string(body))
	}

	var data miruroEpisodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode miruro episodes: %w", err)
	}

	miruroCacheMu.Lock()
	miruroCache[key] = &miruroAnimeCache{
		providers: data.Providers,
		fetchedAt: time.Now(),
	}
	miruroCacheMu.Unlock()

	return &data, nil
}

// bestProvider picks the first provider that has episodes for the given lang (no priority ordering).
func (p *MiruroProvider) bestProvider(providers map[string]miruroProviderData, lang string) (string, *miruroProviderData, []miruroEpisode) {
	for _, name := range miruroAllProviders {
		prov, ok := providers[name]
		if !ok {
			continue
		}
		var eps []miruroEpisode
		if lang == "dub" {
			eps = prov.Episodes.Dub
		} else {
			eps = prov.Episodes.Sub
		}
		if len(eps) > 0 {
			return name, &prov, eps
		}
	}
	return p.fallbackProvider(providers, lang)
}

func (p *MiruroProvider) fallbackProvider(providers map[string]miruroProviderData, lang string) (string, *miruroProviderData, []miruroEpisode) {
	for name, prov := range providers {
		var eps []miruroEpisode
		if lang == "dub" {
			eps = prov.Episodes.Dub
		} else {
			eps = prov.Episodes.Sub
		}
		if len(eps) > 0 {
			return name, &prov, eps
		}
	}
	return "", nil, nil
}

func (p *MiruroProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	// Miruro is searched by AniList ID, not title. Return empty.
	return nil, nil
}

func (p *MiruroProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	data, err := p.fetchEpisodes(ctx, providerID)
	if err != nil {
		return nil, err
	}

	_, _, eps := p.bestProvider(data.Providers, "sub")
	if eps == nil {
		_, _, eps = p.bestProvider(data.Providers, "dub")
	}
	if eps == nil {
		return nil, fmt.Errorf("no episodes found for %s", providerID)
	}

	result := make([]Episode, 0, len(eps))
	for _, e := range eps {
		result = append(result, Episode{
			Number: int(e.Number),
			Title:  e.Title,
			Filler: e.Filler,
		})
	}
	return result, nil
}

type miruroCandidate struct {
	name     string
	prov     *miruroProviderData
	episodes []miruroEpisode
	priority int
}

// sortedCandidates returns all providers that have episodes for lang.
// Filters out known-bad providers for sub and dub.
func (p *MiruroProvider) sortedCandidates(providers map[string]miruroProviderData, lang string) []miruroCandidate {
	var candidates []miruroCandidate
	for _, name := range miruroAllProviders {
		if lang == "sub" && subBadProviders[name] {
			continue
		}
		if lang == "dub" && dubBadProviders[name] {
			continue
		}
		prov, ok := providers[name]
		if !ok {
			continue
		}
		var eps []miruroEpisode
		if lang == "dub" {
			eps = prov.Episodes.Dub
		} else {
			eps = prov.Episodes.Sub
		}
		if len(eps) == 0 {
			continue
		}
		candidates = append(candidates, miruroCandidate{name: name, prov: &prov, episodes: eps})
	}

	// Fallback: any provider with episodes for this lang (in case not in miruroAllProviders)
	if len(candidates) == 0 {
		for name, prov := range providers {
			if lang == "sub" && subBadProviders[name] {
				continue
			}
			if lang == "dub" && dubBadProviders[name] {
				continue
			}
			var eps []miruroEpisode
			if lang == "dub" {
				eps = prov.Episodes.Dub
			} else {
				eps = prov.Episodes.Sub
			}
			if len(eps) > 0 {
				candidates = append(candidates, miruroCandidate{name: name, prov: &prov, episodes: eps})
			}
		}
	}
	return candidates
}

func (p *MiruroProvider) fetchSource(ctx context.Context, episodeID string) (*miruroSourceResponse, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	streamURL := fmt.Sprintf("%s/%s", p.apiBase, episodeID)
	req, err := http.NewRequestWithContext(fetchCtx, "GET", streamURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("miruro source request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("miruro source returned %d: %s", resp.StatusCode, string(body))
	}

	var sourceResp miruroSourceResponse
	if err := json.NewDecoder(resp.Body).Decode(&sourceResp); err != nil {
		return nil, fmt.Errorf("failed to decode miruro source: %w", err)
	}
	return &sourceResp, nil
}

// verifySourceURL rejects URLs from CDNs known to return 403/502 or expired tokens.
func (p *MiruroProvider) verifySourceURL(ctx context.Context, url string) error {
	lower := strings.ToLower(url)
	if strings.Contains(lower, "mp4upload") ||
		strings.Contains(lower, "uns.bio") {
		return fmt.Errorf("unreliable source domain: %s", lower)
	}
	return nil
}

// testSourceReachability probes the upstream CDN with a short-range GET to verify it's reachable
// from this server's IP range. CDNs that block datacenter IPs return 502 or connection refused.
func testSourceReachability(ctx context.Context, url string, headers map[string]string, client *http.Client) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Range", "bytes=0-511")

	probeCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	resp, err := client.Do(req.WithContext(probeCtx))
	if err != nil {
		return fmt.Errorf("source unreachable: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode == 502 || resp.StatusCode == 403 {
		return fmt.Errorf("source blocked by CDN (HTTP %d)", resp.StatusCode)
	}
	return nil
}

func (p *MiruroProvider) buildSourceResult(sourceResp *miruroSourceResponse) *SourceResult {
	var subtitles []core.Subtitle
	for _, sub := range sourceResp.Subtitles {
		if sub.URL == "" {
			continue // ponytail: skip empty subtitle URLs to avoid proxy 400s
		}
		subtitles = append(subtitles, core.Subtitle{
			URL:   sub.URL,
			Lang:  sub.Lang,
			Label: sub.Label,
		})
	}

	var coreSources []core.Source
	for _, s := range sourceResp.Streams {
		if s.Type == "embed" {
			continue
		}
		if s.URL == "" {
			continue
		}
		lower := strings.ToLower(s.URL)
		if strings.Contains(lower, "mp4upload") ||
			strings.Contains(lower, "uns.bio") {
			continue
		}
		streamType := "hls"
		if strings.Contains(s.URL, ".mp4") {
			streamType = "mp4"
		}
		coreSources = append(coreSources, core.Source{
			URL:       s.URL,
			Type:      streamType,
			Quality:   s.Quality,
			Subtitles: subtitles,
		})
	}

	qualityOrder := map[string]int{"1080p": 0, "720p": 1, "480p": 2, "360p": 3, "auto": 4}
	sort.Slice(coreSources, func(i, j int) bool {
		oi, oki := qualityOrder[coreSources[i].Quality]
		oj, okj := qualityOrder[coreSources[j].Quality]
		if !oki {
			oi = 5
		}
		if !okj {
			oj = 5
		}
		return oi < oj
	})

	headers := map[string]string{
		"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}
	if len(sourceResp.Streams) > 0 && sourceResp.Streams[0].Referer != "" {
		headers["Referer"] = sourceResp.Streams[0].Referer
	}

	return &SourceResult{
		Sources: coreSources,
		Headers: headers,
	}
}

func (p *MiruroProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	data, err := p.fetchEpisodes(ctx, providerID)
	if err != nil {
		return nil, err
	}

	candidates := p.sortedCandidates(data.Providers, lang)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no %s provider available for %s", lang, providerID)
	}

	var lastErr error
	var embedURL string

	// Stage 1: Fetch sources from ALL providers sequentially (fast Miruro API calls)
	type verified struct {
		name   string
		result *SourceResult
	}
	var verifiedSet []verified

	for _, c := range candidates {
		var episodeID string
		var episodeURL string
		for _, e := range c.episodes {
			if int(e.Number) == episode {
				episodeID = e.ID
				episodeURL = e.URL
				break
			}
		}
		if episodeID == "" {
			continue
		}

		// Check cache first
		sk := sourceCacheKey(providerID+":"+c.name, episode, lang)
		miruroSourceMu.RLock()
		if cached, ok := miruroSourceCache[sk]; ok && time.Since(cached.fetchedAt) < 10*time.Minute && !IsRefresh(ctx) {
			miruroSourceMu.RUnlock()
			verifiedSet = append(verifiedSet, verified{name: c.name, result: cached.result})
			continue
		}
		miruroSourceMu.RUnlock()

		sourceResp, err := p.fetchSource(ctx, episodeID)
		if err != nil {
			p.log.Warn().Err(err).Str("provider", c.name).Str("lang", lang).Int("episode", episode).Msg("miruro provider failed, trying next")
			lastErr = err
			if embedURL == "" && episodeURL != "" {
				embedURL = episodeURL
			}
			continue
		}

		result := p.buildSourceResult(sourceResp)

		if len(result.Sources) == 0 {
			p.log.Warn().Str("provider", c.name).Msg("miruro source filtered to 0 sources, skipping")
			lastErr = fmt.Errorf("all sources filtered for provider %s", c.name)
			continue
		}

		if err := p.verifySourceURL(ctx, result.Sources[0].URL); err != nil {
			p.log.Warn().Err(err).Str("provider", c.name).Msg("miruro source domain blocked, skipping")
			lastErr = err
			continue
		}

		verifiedSet = append(verifiedSet, verified{name: c.name, result: result})
	}

	if len(verifiedSet) == 0 {
		return p.miruroFallback(embedURL, lastErr, episode, lang, providerID)
	}

	// Stage 2: Test ALL verified CDN URLs IN PARALLEL — first one that responds wins
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultCh := make(chan *SourceResult, len(verifiedSet))
	var wg sync.WaitGroup

	for _, v := range verifiedSet {
		wg.Add(1)
		v := v
		go func() {
			defer wg.Done()
			if err := testSourceReachability(ctx, v.result.Sources[0].URL, v.result.Headers, p.client); err == nil {
				select {
				case resultCh <- v.result:
				case <-ctx.Done():
				}
			}
		}()
	}

	// Close resultCh when all goroutines finish (all failed)
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Wait for first successful result or all failures
	select {
	case result := <-resultCh:
		// Cache the result under the provider name that won
		for _, v := range verifiedSet {
			if v.result == result {
				sk := sourceCacheKey(providerID+":"+v.name, episode, lang)
				miruroSourceMu.Lock()
				miruroSourceCache[sk] = &miruroSourceCacheEntry{result: result, fetchedAt: time.Now()}
				miruroSourceMu.Unlock()
				break
			}
		}
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// FindAllSources tests ALL Miruro sub-providers for the given episode and lang.
// Returns a map of provider name → SourceResult for every provider that passed reachability.
func (p *MiruroProvider) FindAllSources(ctx context.Context, providerID string, episode int, lang string) map[string]*SourceResult {
	data, err := p.fetchEpisodes(ctx, providerID)
	if err != nil {
		p.log.Warn().Err(err).Str("anilistId", providerID).Msg("failed to fetch episodes for FindAllSources")
		return nil
	}

	candidates := p.sortedCandidates(data.Providers, lang)
	if len(candidates) == 0 {
		return nil
	}

	type candidateResult struct {
		name   string
		result *SourceResult
	}

	// Fetch sources from ALL candidates in parallel
	var mu sync.Mutex
	var wg sync.WaitGroup
	var verified []candidateResult

	for _, c := range candidates {
		var episodeID string
		for _, e := range c.episodes {
			if int(e.Number) == episode {
				episodeID = e.ID
				break
			}
		}
		if episodeID == "" {
			continue
		}

		wg.Add(1)
		c := c
		go func() {
			defer wg.Done()

			// Check cache first
			sk := sourceCacheKey(providerID+":"+c.name, episode, lang)
			miruroSourceMu.RLock()
			if cached, ok := miruroSourceCache[sk]; ok && time.Since(cached.fetchedAt) < 10*time.Minute && !IsRefresh(ctx) {
				miruroSourceMu.RUnlock()
				mu.Lock()
				verified = append(verified, candidateResult{name: c.name, result: cached.result})
				mu.Unlock()
				return
			}
			miruroSourceMu.RUnlock()

			// ponytail: fetchSource already uses its own timeout context
			sourceResp, err := p.fetchSource(ctx, episodeID)
			if err != nil {
				p.log.Warn().Err(err).Str("provider", c.name).Msg("FindAllSources: fetchSource failed")
				return
			}

			result := p.buildSourceResult(sourceResp)
			if len(result.Sources) == 0 {
				return
			}

			if err := p.verifySourceURL(ctx, result.Sources[0].URL); err != nil {
				p.log.Warn().Err(err).Str("provider", c.name).Msg("FindAllSources: domain blocked")
				return
			}

			mu.Lock()
			verified = append(verified, candidateResult{name: c.name, result: result})
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(verified) == 0 {
		return nil
	}

	// Test reachability of ALL verified sources in parallel
	type serverResult struct {
		name   string
		result *SourceResult
	}

	var resultsMu sync.Mutex
	var resultsWg sync.WaitGroup
	results := []serverResult{}

	for _, v := range verified {
		resultsWg.Add(1)
		v := v
		go func() {
			defer resultsWg.Done()
			if err := testSourceReachability(ctx, v.result.Sources[0].URL, v.result.Headers, p.client); err == nil {
				// Cache the result
				sk := sourceCacheKey(providerID+":"+v.name, episode, lang)
				miruroSourceMu.Lock()
				miruroSourceCache[sk] = &miruroSourceCacheEntry{result: v.result, fetchedAt: time.Now()}
				miruroSourceMu.Unlock()

				resultsMu.Lock()
				results = append(results, serverResult{name: v.name, result: v.result})
				resultsMu.Unlock()
			}
		}()
	}
	resultsWg.Wait()

	// Build map of working providers
	serverMap := make(map[string]*SourceResult, len(results))
	for _, r := range results {
		serverMap[r.name] = r.result
	}

	p.log.Info().Int("total", len(candidates)).Int("working", len(serverMap)).Str("lang", lang).Int("episode", episode).Msg("FindAllSources completed")
	return serverMap
}

// ProbePlayable reports whether this anime has at least one actually playable
// Miruro stream (source fetched and reachability-verified), plus the total
// sub/dub episode counts advertised in the catalog. Cheap when the source
// cache is warm; bounded by the caller's context when it is not.
func (p *MiruroProvider) ProbePlayable(ctx context.Context, anilistID string) (subCount, dubCount int, playable bool) {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		p.log.Warn().Err(err).Str("anilistId", anilistID).Msg("probe: episodes fetch failed")
		return 0, 0, false
	}

	var firstSub, firstDub float64
	for _, prov := range data.Providers {
		for _, e := range prov.Episodes.Sub {
			if int(e.Number) > subCount {
				subCount = int(e.Number)
			}
			if firstSub == 0 || e.Number < firstSub {
				firstSub = e.Number
			}
		}
		for _, e := range prov.Episodes.Dub {
			if int(e.Number) > dubCount {
				dubCount = int(e.Number)
			}
			if firstDub == 0 || e.Number < firstDub {
				firstDub = e.Number
			}
		}
	}

	if subCount > 0 && len(p.FindAllSources(ctx, anilistID, int(firstSub), "sub")) > 0 {
		return subCount, dubCount, true
	}
	if dubCount > 0 && len(p.FindAllSources(ctx, anilistID, int(firstDub), "dub")) > 0 {
		return subCount, dubCount, true
	}
	return subCount, dubCount, false
}

func (p *MiruroProvider) miruroFallback(embedURL string, lastErr error, episode int, lang, providerID string) (*SourceResult, error) {
	if embedURL != "" {
		p.log.Warn().Str("url", embedURL).Str("lang", lang).Int("episode", episode).Msg("miruro pipe blocked, returning direct provider URL as embed")
		return &SourceResult{
			Sources: []core.Source{{
				URL:  embedURL,
				Type: "embed",
			}},
			Headers: map[string]string{
				"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
			},
		}, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("all miruro providers failed for ep %d %s: %w", episode, lang, lastErr)
	}
	return nil, fmt.Errorf("episode %d not found in any %s provider for %s", episode, lang, providerID)
}

// HasAnime checks if ANY provider has episodes (sub or dub) for this anime.
func (p *MiruroProvider) HasAnime(ctx context.Context, anilistID string) bool {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return false
	}
	for _, prov := range data.Providers {
		if len(prov.Episodes.Sub) > 0 || len(prov.Episodes.Dub) > 0 {
			return true
		}
	}
	return false
}

// HasDub checks if ANY provider has dub episodes for this anime.
func (p *MiruroProvider) HasDub(ctx context.Context, anilistID string) bool {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return false
	}
	for _, prov := range data.Providers {
		if len(prov.Episodes.Dub) > 0 {
			return true
		}
	}
	return false
}

// GetEpisodeThumbnails returns episode number → thumbnail URL mapping from the best provider.
func (p *MiruroProvider) GetEpisodeThumbnails(ctx context.Context, anilistID string) map[int]string {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return nil
	}

	_, _, eps := p.bestProvider(data.Providers, "sub")
	if eps == nil {
		_, _, eps = p.bestProvider(data.Providers, "dub")
	}
	if eps == nil {
		return nil
	}

	thumbs := make(map[int]string, len(eps))
	for _, e := range eps {
		if e.Image != "" {
			thumbs[int(e.Number)] = e.Image
		}
	}
	return thumbs
}

// GetEpisodeTitles returns episode number → title mapping from the best provider.
func (p *MiruroProvider) GetEpisodeTitles(ctx context.Context, anilistID string) map[int]string {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return nil
	}

	_, _, eps := p.bestProvider(data.Providers, "sub")
	if eps == nil {
		_, _, eps = p.bestProvider(data.Providers, "dub")
	}
	if eps == nil {
		return nil
	}

	titles := make(map[int]string, len(eps))
	for _, e := range eps {
		if e.Title != "" {
			titles[int(e.Number)] = e.Title
		}
	}
	return titles
}

// GetProviders returns the list of available provider names for an anime.
func (p *MiruroProvider) GetProviders(ctx context.Context, anilistID string) []string {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return nil
	}

	var available []string
	for _, name := range miruroAllProviders {
		if _, ok := data.Providers[name]; ok {
			available = append(available, name)
		}
	}
	return available
}

// GetEpisodeCount returns the number of episodes for a given lang from the best provider.
func (p *MiruroProvider) GetEpisodeCount(ctx context.Context, anilistID string, lang string) int {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return 0
	}
	_, _, eps := p.bestProvider(data.Providers, lang)
	return len(eps)
}
