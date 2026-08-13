package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// Active Miruro sub-providers. No priority ordering — all are tested equally.
var miruroAllProviders = []string{"bonk", "ally", "moo", "pewe", "kiwi"}

// Removed providers (bee/vidtube, hop/kaa.lt): their streams are embed-only
// player pages that refuse framing or demand site sessions, so they can never
// play reliably. They are excluded everywhere — never listed, never probed,
// never picked as fallback.
var miruroRemovedProviders = map[string]bool{
	"bee": true,
	"hop": true,
	"moo": true,
}

// PlaybackVerdict ranks how a source can reach a player. Verdicts are soft
// signals used for ordering and metadata only — they never filter providers,
// because CDNs routinely serve datacenter and residential IPs differently.
type PlaybackVerdict int

const (
	VerdictDead   PlaybackVerdict = iota // unplayable via any path we know of
	VerdictEmbed                         // works only as an iframe/embed player
	VerdictDirect                        // reachable with a plain browser-like client
	VerdictProxy                         // verified playable through the media proxy
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

// MediaProbe validates how a source can reach a player. The proxy path uses
// uTLS browser fingerprints, never follows redirects, filters headers and
// sends Accept-Encoding: identity — CDNs routinely treat that traffic
// differently from a plain Go client (a naive Range probe passes for hosts
// that then 403/404 the real playback path). A "direct" verdict means the
// source is reachable with a plain browser-like client instead.
type MediaProbe interface {
	// ProbePlayback reports how the source would play: VerdictProxy if the
	// media-proxy path works, VerdictDirect if only a plain browser-like
	// client works, VerdictDead otherwise. For HLS the manifest must fetch
	// 200 and parse as a playlist whose first media playlist also loads;
	// for MP4 a ranged GET must return a media body. Redirects, HTML error
	// pages, 403/404/502 are unplayable.
	ProbePlayback(ctx context.Context, srcType, rawURL string, headers map[string]string) PlaybackVerdict
}

type MiruroProvider struct {
	apiBase string
	client  *http.Client
	log     zerolog.Logger
	// learnHost, when set, is called with hosts that the trusted provider
	// vouches for (verified, reachable source URLs). The HTTP layer
	// registers it to feed the media-proxy CDN allowlist so rotated CDN
	// hostnames are allowed the moment they surface instead of 403ing.
	learnHost func(host string)
	// probe, when set, is the authoritative playability gate (the media
	// proxy's transport). Without it (unit tests) a plain reachability
	// check is used instead.
	probe MediaProbe
	// health tracks consecutive failures per Miruro sub-provider so that
	// CDN-blocked providers are skipped automatically instead of being
	// re-probed on every request.
	health   map[string]*providerHealth
	healthMu sync.Mutex
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

// SetHostLearner registers the callback that receives provider-vouched hosts.
func (p *MiruroProvider) SetHostLearner(fn func(host string)) {
	p.learnHost = fn
}

// SetMediaProbe registers the playback-path gate used to decide whether a
// source is served to players. Without one, sources pass a plain
// reachability check instead.
func (p *MiruroProvider) SetMediaProbe(probe MediaProbe) {
	p.probe = probe
}

// verdictResult ranks every source in a result through the registered probe
// (or a plain reachability check when no probe is registered) and returns a
// shallow copy with Verification tags filled in, plus the provider's best
// verdict. Individually dead media URLs are removed before the result is
// returned. This is important when one provider response contains a mixture
// of a playable quality and an expired/blocked CDN URL: returning the dead URL
// would still let the browser mount it during quality selection or fallback.
func (p *MiruroProvider) verdictResult(ctx context.Context, result *SourceResult) (*SourceResult, PlaybackVerdict) {
	if result == nil || len(result.Sources) == 0 {
		return nil, VerdictDead
	}
	best := VerdictDead
	sources := make([]core.Source, 0, len(result.Sources))
	for _, src := range result.Sources {
		// Reject known dead CDN ranges before spending probe time on them.
		if err := p.verifySourceURL(ctx, src.URL); err != nil {
			continue
		}

		var v PlaybackVerdict
		if src.Type == "embed" {
			v = VerdictEmbed
		} else if p.probe != nil {
			v = p.probe.ProbePlayback(ctx, src.Type, src.URL, result.Headers)
		} else if testSourceReachability(ctx, src.URL, result.Headers, p.client) == nil {
			v = VerdictDirect
		}
		if v == VerdictDead {
			continue
		}

		src.Verification = v.String()
		sources = append(sources, src)
		if v > best {
			best = v
		}
	}
	if len(sources) == 0 {
		return nil, VerdictDead
	}

	// Order sources by verdict (proxy > direct > embed) so Sources[0] is
	// always the most reliable path; ties keep the original quality ordering.
	sort.SliceStable(sources, func(i, j int) bool {
		vi := playbackVerdictRank(sources[i].Verification)
		vj := playbackVerdictRank(sources[j].Verification)
		return vi > vj
	})
	return &SourceResult{Sources: sources, Headers: result.Headers, Intro: result.Intro, Outro: result.Outro}, best
}

// playbackVerdictRank maps a verification tag to a comparable rank.
func playbackVerdictRank(verification string) int {
	switch verification {
	case "proxy":
		return 3
	case "direct":
		return 2
	case "embed":
		return 1
	}
	return 0
}

// providerHealth tracks consecutive failures for a Miruro sub-provider.
// A provider is auto-blocked after providerFailureThreshold consecutive
// pipe failures (Miruro API 444s/5xx/transport errors) and is retried once
// after providerCoolOff, so transient outages recover on their own. CDN
// playback verdicts never count — datacenter reachability differs from the
// player's and must never hide a provider from the server list.
type providerHealth struct {
	consecutiveFailures int
	lastFailure         time.Time
	mu                  sync.Mutex
}

const (
	providerFailureThreshold = 3
	providerCoolOff          = 15 * time.Minute
)

func (ph *providerHealth) recordFailure() {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	ph.consecutiveFailures++
	ph.lastFailure = time.Now()
}

func (ph *providerHealth) recordSuccess() {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	ph.consecutiveFailures = 0
}

func (ph *providerHealth) isBlocked() bool {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	if ph.consecutiveFailures < providerFailureThreshold {
		return false
	}
	return time.Since(ph.lastFailure) < providerCoolOff
}

func (p *MiruroProvider) healthFor(name string) *providerHealth {
	p.healthMu.Lock()
	defer p.healthMu.Unlock()
	if p.health == nil {
		p.health = map[string]*providerHealth{}
	}
	h, ok := p.health[name]
	if !ok {
		h = &providerHealth{}
		p.health[name] = h
	}
	return h
}

// providerBlocked reports whether a sub-provider is currently auto-blocked.
func (p *MiruroProvider) providerBlocked(name string) bool {
	h := p.healthFor(name)
	return h != nil && h.isBlocked()
}

func (p *MiruroProvider) recordProviderFailure(name string) {
	if name != "" {
		p.healthFor(name).recordFailure()
	}
}

func (p *MiruroProvider) recordProviderSuccess(name string) {
	if name != "" {
		p.healthFor(name).recordSuccess()
	}
}

// learnResultHosts feeds every host referenced by a verified, reachable
// source result to the allowlist learner. Only call this for results that
// passed verifySourceURL and testSourceReachability — the provider chain
// plus our own probes are the trust boundary that vouches for the host.
func (p *MiruroProvider) learnResultHosts(result *SourceResult) {
	if p.learnHost == nil || result == nil {
		return
	}
	seen := map[string]bool{}
	learn := func(raw string) {
		u, err := url.Parse(raw)
		if err != nil {
			return
		}
		host := strings.ToLower(u.Hostname())
		if host == "" || seen[host] {
			return
		}
		seen[host] = true
		p.learnHost(host)
	}
	for _, s := range result.Sources {
		learn(s.URL)
		for _, sub := range s.Subtitles {
			learn(sub.URL)
		}
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
	// Aniskip holds per-episode skip segments (op/ed/recap) sourced from
	// the AniSkip dataset — the Miruro source endpoint does not return
	// intro/outro itself, so this is where Skip Intro/Credits data lives.
	Aniskip []miruroAniskip `json:"aniskip"`
}

type miruroAniskip struct {
	Episode int     `json:"episode"`
	Type    string  `json:"type"`
	Start   float64 `json:"start"`
	End     float64 `json:"end"`
	Votes   int     `json:"votes"`
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
	miruroCache       = map[string]*miruroAnimeCache{}
	miruroCacheMu     sync.RWMutex
	miruroSourceCache = map[string]*miruroSourceCacheEntry{}
	miruroSourceMu    sync.RWMutex
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
	if c, ok := miruroCache[key]; ok && time.Since(c.fetchedAt) < 5*time.Minute && !IsRefresh(ctx) {
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
		if miruroRemovedProviders[name] {
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
		if len(eps) > 0 {
			return name, &prov, eps
		}
	}
	return p.fallbackProvider(providers, lang)
}

func (p *MiruroProvider) fallbackProvider(providers map[string]miruroProviderData, lang string) (string, *miruroProviderData, []miruroEpisode) {
	for name, prov := range providers {
		if miruroRemovedProviders[name] {
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

// sortedCandidates returns all providers that have episodes for lang:
// the known providers in canonical order first, then any additional
// providers Miruro advertises (e.g. ANIMEDUNYA), in API order. Known-bad
// providers are NOT pre-skipped: verdicts and the per-provider auto-block
// decide what actually works, so every provider gets a fair shot.
func (p *MiruroProvider) sortedCandidates(providers map[string]miruroProviderData, lang string) []miruroCandidate {
	var candidates []miruroCandidate
	seen := map[string]bool{}
	for _, name := range miruroAllProviders {
		if miruroRemovedProviders[name] {
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
		seen[name] = true
		candidates = append(candidates, miruroCandidate{name: name, prov: &prov, episodes: eps})
	}

	// Any provider with episodes for this lang that is not in the known
	// list, in API order (unknown names must surface, not be dropped).
	for name, prov := range providers {
		if seen[name] || miruroRemovedProviders[name] {
			continue
		}
		var eps []miruroEpisode
		if lang == "dub" {
			eps = prov.Episodes.Dub
		} else {
			eps = prov.Episodes.Sub
		}
		if len(eps) > 0 {
			seen[name] = true
			candidates = append(candidates, miruroCandidate{name: name, prov: &prov, episodes: eps})
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

// verifySourceURL only rejects hosts known to block datacenter ranges.
func (p *MiruroProvider) verifySourceURL(ctx context.Context, rawURL string) error {
	lower := strings.ToLower(rawURL)
	// 203.188.166.234 is the CDN returning the repeated 502s seen in the
	// browser. Block the observed /24 because the provider rotates addresses
	// within the same unreliable edge range.
	if strings.Contains(lower, "uns.bio") ||
		strings.Contains(lower, "203.188.166.") ||
		strings.Contains(lower, "185.237.106.79") {
		return fmt.Errorf("unreliable source domain: %s", lower)
	}
	return nil
}

// testSourceReachability probes the upstream CDN with a short-range GET to verify it's reachable
// from this server's IP range. CDNs that block datacenter IPs return 502 or connection refused,
// and expired tokenized URLs (miruro reuses CDN tokens for months) return 401 — both must be
// treated as dead so the player never mounts against a stream that cannot load.
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

	if resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 404 || resp.StatusCode == 502 {
		return fmt.Errorf("source blocked by CDN (HTTP %d)", resp.StatusCode)
	}
	return nil
}

func (p *MiruroProvider) buildSourceResult(sourceResp *miruroSourceResponse, aniskip []miruroAniskip, episode int) *SourceResult {
	// AniSkip timestamps are authoritative when present; fall back to any
	// intro/outro the source API itself provides.
	intro, outro := skipFromAniskip(aniskip, episode)
	if intro == nil {
		intro = skipTimestamp(sourceResp.Intro)
	}
	if outro == nil {
		outro = skipTimestamp(sourceResp.Outro)
	}
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
		if s.URL == "" {
			continue
		}
		streamType := s.Type
		if streamType != "mp4" && streamType != "hls" && streamType != "embed" {
			streamType = "hls"
		}
		if streamType == "hls" && strings.Contains(s.URL, ".mp4") {
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
		Intro:   intro,
		Outro:   outro,
	}
}

// skipFromAniskip picks the best intro ("op") and outro ("ed") segment for
// an episode from Miruro's mappings.aniskip. Multiple providers report the
// same segment; positive-voted entries win over mixed/negative ones, then
// votes break ties. Degenerate zero-length segments are dropped.
func skipFromAniskip(entries []miruroAniskip, episode int) (intro, outro *core.SkipTimestamp) {
	var bestIntro, bestOutro *miruroAniskip
	better := func(candidate, current *miruroAniskip) bool {
		if current == nil {
			return true
		}
		if (candidate.Votes >= 0) != (current.Votes >= 0) {
			return candidate.Votes >= 0
		}
		return candidate.Votes > current.Votes
	}
	for i := range entries {
		e := &entries[i]
		// Zero-start segments are bogus (they'd mark the whole episode as
		// skip); the Miruro source path already rejects start < 0.
		if e.Episode != episode || e.End <= e.Start || e.Start <= 0 {
			continue
		}
		switch e.Type {
		case "op":
			if better(e, bestIntro) {
				bestIntro = e
			}
		case "ed":
			if better(e, bestOutro) {
				bestOutro = e
			}
		}
	}
	conv := func(e *miruroAniskip) *core.SkipTimestamp {
		if e == nil {
			return nil
		}
		return &core.SkipTimestamp{Start: e.Start, End: e.End}
	}
	return conv(bestIntro), conv(bestOutro)
}

// skipTimestamp converts a Miruro intro/outro segment to the public model,
// ignoring degenerate zero-length segments.
func skipTimestamp(t *miruroTimestamp) *core.SkipTimestamp {
	if t == nil || t.End <= t.Start || t.Start < 0 {
		return nil
	}
	return &core.SkipTimestamp{Start: t.Start, End: t.End}
}

func (p *MiruroProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	return p.findEpisodeSource(ctx, providerID, episode, lang, "")
}

// isPipeFailure reports whether the Miruro API itself refused the pipe
// (444/5xx/transport). Only these count toward provider auto-blocking —
// CDN playback verdicts never block a provider.
func isPipeFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "miruro source request failed") {
		return true
	}
	return strings.Contains(msg, "returned 444") || strings.Contains(msg, "returned 5")
}

// defaultMiruroHeaders are the headers sent with every source result.
func defaultMiruroHeaders() map[string]string {
	return map[string]string{
		"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	}
}

// embedFromPage builds a last-resort embed source from the provider's
// episode page (used when the Miruro pipe is blocked and the API returns no
// streams). The embed player can attempt it as an iframe.
func embedFromPage(episodeURL string) *SourceResult {
	return &SourceResult{
		Sources: []core.Source{{
			URL:          episodeURL,
			Type:         "embed",
			Quality:      "auto",
			Verification: "embed",
		}},
		Headers: defaultMiruroHeaders(),
	}
}

func (p *MiruroProvider) findEpisodeSource(ctx context.Context, providerID string, episode int, lang, preferred string) (*SourceResult, error) {
	data, err := p.fetchEpisodes(ctx, providerID)
	if err != nil {
		return nil, err
	}

	candidates := p.sortedCandidates(data.Providers, lang)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no %s provider available for %s", lang, providerID)
	}

	// When the caller asks for a specific server, try it first (it is likely
	// cached from FindAllServers) and fall back to the rest if it is blocked.
	if preferred != "" {
		for i := range candidates {
			if candidates[i].name == preferred {
				preferredCandidate := candidates[i]
				candidates = append(candidates[:i], candidates[i+1:]...)
				candidates = append([]miruroCandidate{preferredCandidate}, candidates...)
				break
			}
		}
	}

	var lastErr error

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

		if p.providerBlocked(c.name) {
			p.log.Debug().Str("provider", c.name).Str("lang", lang).Msg("provider auto-blocked, skipping")
			continue
		}

		sourceResp, err := p.fetchSource(ctx, episodeID)
		if err != nil {
			p.log.Warn().Err(err).Str("provider", c.name).Str("lang", lang).Int("episode", episode).Msg("miruro provider failed, trying next")
			if isPipeFailure(err) {
				p.recordProviderFailure(c.name)
			}
			lastErr = err
			// Pipe blocked: keep the provider's episode page as an embed
			// option so the second (iframe) player can still try it.
			if episodeURL != "" {
				verifiedSet = append(verifiedSet, verified{name: c.name, result: embedFromPage(episodeURL)})
			}
			continue
		}

		result := p.buildSourceResult(sourceResp, data.Mappings.Aniskip, episode)

		if len(result.Sources) == 0 {
			// API returned no usable streams — offer the episode page embed.
			if episodeURL != "" {
				verifiedSet = append(verifiedSet, verified{name: c.name, result: embedFromPage(episodeURL)})
			}
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
		return p.miruroFallback("", lastErr, episode, lang, providerID)
	}

	// Stage 2: Rank ALL verified results IN PARALLEL through the real
	// playback path (the media proxy transport, then a plain browser-like
	// client). Verdicts are soft: every provider is kept, and the winner is
	// the one with the best path (proxy > direct > embed), falling back to
	// the preferred provider if verdicts tie. When everything is dead the
	// preferred provider still wins — CDNs serve different clients
	// differently, so the player's own fallback chain is the final judge.
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	type verdict struct {
		name   string
		result *SourceResult
		best   PlaybackVerdict
	}
	var passing []verdict
	var passMu sync.Mutex
	var wg sync.WaitGroup

	for _, v := range verifiedSet {
		wg.Add(1)
		v := v
		go func() {
			defer wg.Done()
			ranked, best := p.verdictResult(ctx, v.result)
			if ranked == nil {
				return
			}
			// Never return or cache a media-only result whose every source
			// failed the real proxy/direct playback probe. Keeping it here
			// caused the browser to mount the same dead CDN URL repeatedly.
			if best == VerdictDead {
				p.log.Debug().Str("provider", v.name).Msg("dropping dead-only source result")
				return
			}
			passMu.Lock()
			passing = append(passing, verdict{name: v.name, result: ranked, best: best})
			passMu.Unlock()
		}()
	}
	wg.Wait()

	if len(passing) == 0 {
		return p.miruroFallback("", lastErr, episode, lang, providerID)
	}

	// Deterministic winner: when the caller named a provider, that provider
	// wins if it has any sources at all — its verdict is a soft hint, and the
	// browser's own fallback chain (proxy → direct → next server) is the
	// final judge. Without a named provider, the best verdict wins, with
	// provider order breaking ties.
	idxOf := func(name string) int {
		for i, n := range miruroAllProviders {
			if n == name {
				return i
			}
		}
		return len(miruroAllProviders)
	}
	if preferred != "" {
		for _, v := range passing {
			if v.name == preferred {
				winner := v
				// vouch for the winning source's hosts and cache it under
				// the provider name that won (best-effort allowance).
				p.learnResultHosts(winner.result)
				if winner.best > VerdictDead {
					p.recordProviderSuccess(winner.name)
				}
				sk := sourceCacheKey(providerID+":"+winner.name, episode, lang)
				miruroSourceMu.Lock()
				miruroSourceCache[sk] = &miruroSourceCacheEntry{result: winner.result, fetchedAt: time.Now()}
				miruroSourceMu.Unlock()
				return winner.result, nil
			}
		}
	}
	sort.SliceStable(passing, func(i, j int) bool {
		if passing[i].best != passing[j].best {
			return passing[i].best > passing[j].best
		}
		if passing[i].name == preferred && passing[j].name != preferred {
			return true
		}
		if passing[j].name == preferred && passing[i].name != preferred {
			return false
		}
		return idxOf(passing[i].name) < idxOf(passing[j].name)
	})
	winner := passing[0]

	// Vouch for the winning source's hosts and cache it under the provider
	// name that won (a best-effort allowance; dead-but-listable providers
	// never block).
	p.learnResultHosts(winner.result)
	if winner.best > VerdictDead {
		p.recordProviderSuccess(winner.name)
	}
	sk := sourceCacheKey(providerID+":"+winner.name, episode, lang)
	miruroSourceMu.Lock()
	miruroSourceCache[sk] = &miruroSourceCacheEntry{result: winner.result, fetchedAt: time.Now()}
	miruroSourceMu.Unlock()

	return winner.result, nil
}

type serverResult struct {
	name   string
	result *SourceResult
}

// bestVerdict returns the highest per-source verdict stored in a ranked
// result (read from the Verification tags written by verdictResult).
func (r *SourceResult) bestVerdict() PlaybackVerdict {
	if r == nil {
		return VerdictDead
	}
	best := VerdictDead
	for _, s := range r.Sources {
		switch s.Verification {
		case "proxy":
			return VerdictProxy
		case "direct":
			if best < VerdictDirect {
				best = VerdictDirect
			}
		case "embed":
			if best < VerdictEmbed {
				best = VerdictEmbed
			}
		}
	}
	return best
}

// FindAllSources tests ALL Miruro sub-providers for the given episode and
// lang. Returns a map of provider name → SourceResult for every provider
// that has any source (stream or embed). Verdicts are soft ordering hints
// ("proxy"/"direct"/"embed"/"dead" per source) — nothing is dropped, because
// datacenter reachability is not the player's reachability.
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

			if p.providerBlocked(c.name) {
				p.log.Debug().Str("provider", c.name).Str("lang", lang).Msg("FindAllSources: provider auto-blocked, skipping")
				return
			}

			// ponytail: fetchSource already uses its own timeout context
			sourceResp, err := p.fetchSource(ctx, episodeID)
			if err != nil {
				p.log.Warn().Err(err).Str("provider", c.name).Msg("FindAllSources: fetchSource failed")
				if isPipeFailure(err) {
					p.recordProviderFailure(c.name)
				}
				// Pipe blocked — keep the episode page as an embed option.
				if episodeURL != "" {
					mu.Lock()
					verified = append(verified, candidateResult{name: c.name, result: embedFromPage(episodeURL)})
					mu.Unlock()
				}
				return
			}

			result := p.buildSourceResult(sourceResp, data.Mappings.Aniskip, episode)
			if len(result.Sources) == 0 {
				if episodeURL != "" {
					mu.Lock()
					verified = append(verified, candidateResult{name: c.name, result: embedFromPage(episodeURL)})
					mu.Unlock()
				}
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

	// Rank ALL verified results in parallel through the real playback path.
	// Nothing is dropped: every provider with sources is offered, ordered by
	// verdict so the client can try the most reliable path first.
	serverMap := make(map[string]*SourceResult, len(verified))
	var resultsMu sync.Mutex
	var resultsWg sync.WaitGroup
	results := make([]serverResult, 0, len(verified))

	for _, v := range verified {
		resultsWg.Add(1)
		v := v
		go func() {
			defer resultsWg.Done()
			ranked, best := p.verdictResult(ctx, v.result)
			if ranked == nil {
				return
			}
			// Do not expose or cache a provider whose media URLs all failed
			// the exact proxy/direct playback probe. Embed results still pass
			// because they carry VerdictEmbed rather than VerdictDead.
			if best == VerdictDead {
				p.log.Debug().Str("provider", v.name).Msg("FindAllSources: dropping dead-only source result")
				return
			}
			if best > VerdictDead {
				p.recordProviderSuccess(v.name)
			}
			// Vouch for the source's hosts so the media proxy accepts them
			// without a static allowlist entry (best-effort).
			p.learnResultHosts(ranked)
			// Cache the result under the provider name
			sk := sourceCacheKey(providerID+":"+v.name, episode, lang)
			miruroSourceMu.Lock()
			miruroSourceCache[sk] = &miruroSourceCacheEntry{result: ranked, fetchedAt: time.Now()}
			miruroSourceMu.Unlock()

			resultsMu.Lock()
			results = append(results, serverResult{name: v.name, result: ranked})
			resultsMu.Unlock()
		}()
	}
	resultsWg.Wait()

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
	sort.SliceStable(results, func(i, j int) bool {
		bi, bj := results[i].result.bestVerdict(), results[j].result.bestVerdict()
		if bi != bj {
			return bi > bj
		}
		return idxOf(results[i].name) < idxOf(results[j].name)
	})
	for _, r := range results {
		serverMap[r.name] = r.result
	}

	p.log.Info().Int("total", len(candidates)).Int("listed", len(serverMap)).Str("lang", lang).Int("episode", episode).Msg("FindAllSources completed")
	return serverMap
}

// hasPlayableMediaSource reports whether any provider result contains a
// source the in-app player can actually play. Embed-only results (episode-
// page iframes) can't drive the player, so a probe must never call an anime
// "playable" on the strength of embeds alone — that would surface hentai
// titles (or any anime) in listings that then fail to play.
func hasPlayableMediaSource(serverMap map[string]*SourceResult) bool {
	for _, sr := range serverMap {
		if sr == nil {
			continue
		}
		for _, s := range sr.Sources {
			if s.Type == "hls" || s.Type == "mp4" {
				return true
			}
		}
	}
	return false
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

	if subCount > 0 && hasPlayableMediaSource(p.FindAllSources(ctx, anilistID, int(firstSub), "sub")) {
		return subCount, dubCount, true
	}
	if dubCount > 0 && hasPlayableMediaSource(p.FindAllSources(ctx, anilistID, int(firstDub), "dub")) {
		return subCount, dubCount, true
	}
	return subCount, dubCount, false
}

func (p *MiruroProvider) miruroFallback(embedURL string, lastErr error, episode int, lang, providerID string) (*SourceResult, error) {
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

// buildEpisodeMaps returns thumbnails and titles from ally (most complete
// episode metadata), filling missing fields from other providers.
func (p *MiruroProvider) buildEpisodeMaps(data *miruroEpisodesResponse) (map[int]string, map[int]string) {
	// Ally has the most complete episode titles and thumbnails
	ally, ok := data.Providers["ally"]
	var bestEps []miruroEpisode
	if ok {
		if len(ally.Episodes.Sub) > 0 {
			bestEps = ally.Episodes.Sub
		} else if len(ally.Episodes.Dub) > 0 {
			bestEps = ally.Episodes.Dub
		}
	}
	if bestEps == nil {
		_, _, bestEps = p.bestProvider(data.Providers, "sub")
		if bestEps == nil {
			_, _, bestEps = p.bestProvider(data.Providers, "dub")
		}
	}
	if bestEps == nil {
		return nil, nil
	}

	thumbs := make(map[int]string, len(bestEps))
	titles := make(map[int]string, len(bestEps))
	for _, e := range bestEps {
		n := int(e.Number)
		if e.Image != "" {
			thumbs[n] = e.Image
		}
		if e.Title != "" {
			titles[n] = e.Title
		}
	}

	// Fill missing from other providers
	for _, pdata := range data.Providers {
		for _, extra := range [][]miruroEpisode{pdata.Episodes.Sub, pdata.Episodes.Dub} {
			for _, e := range extra {
				n := int(e.Number)
				if thumbs[n] == "" && e.Image != "" {
					thumbs[n] = e.Image
				}
				if titles[n] == "" && e.Title != "" {
					titles[n] = e.Title
				}
			}
		}
	}
	return thumbs, titles
}

// GetEpisodeThumbnails returns episode number → thumbnail URL mapping.
func (p *MiruroProvider) GetEpisodeThumbnails(ctx context.Context, anilistID string) map[int]string {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return nil
	}
	thumbs, _ := p.buildEpisodeMaps(data)
	if len(thumbs) == 0 {
		return nil
	}
	return thumbs
}

// GetEpisodeTitles returns episode number → title mapping.
func (p *MiruroProvider) GetEpisodeTitles(ctx context.Context, anilistID string) map[int]string {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return nil
	}
	_, titles := p.buildEpisodeMaps(data)
	if len(titles) == 0 {
		return nil
	}
	return titles
}

// GetEpisodeFlags returns episode number → (filler, recap) flags, taken from
// the best sub provider (dub fallback), with gaps filled from other providers.
func (p *MiruroProvider) GetEpisodeFlags(ctx context.Context, anilistID string) (filler, recap map[int]bool) {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return nil, nil
	}

	var bestEps []miruroEpisode
	// Prefer the same providers the episode list is built from.
	if prov, ok := data.Providers["ally"]; ok && len(prov.Episodes.Sub) > 0 {
		bestEps = prov.Episodes.Sub
	} else if prov, ok := data.Providers["ally"]; ok && len(prov.Episodes.Dub) > 0 {
		bestEps = prov.Episodes.Dub
	}
	if bestEps == nil {
		_, _, bestEps = p.bestProvider(data.Providers, "sub")
		if bestEps == nil {
			_, _, bestEps = p.bestProvider(data.Providers, "dub")
		}
	}
	if bestEps == nil {
		return nil, nil
	}

	filler = make(map[int]bool, len(bestEps))
	recap = make(map[int]bool, len(bestEps))
	mark := func(e miruroEpisode) {
		n := int(e.Number)
		if e.Filler {
			filler[n] = true
		}
		// Miruro marks recap episodes via a title marker when the flag
		// itself is absent — catch both spellings.
		title := strings.ToLower(e.Title)
		if strings.Contains(title, "recap") || strings.Contains(title, "(recap)") {
			recap[n] = true
		}
	}
	for _, e := range bestEps {
		mark(e)
	}
	// Fill missing from other providers
	for _, pdata := range data.Providers {
		for _, extra := range [][]miruroEpisode{pdata.Episodes.Sub, pdata.Episodes.Dub} {
			for _, e := range extra {
				if !filler[int(e.Number)] && !recap[int(e.Number)] {
					mark(e)
				}
			}
		}
	}
	return filler, recap
}

// GetProviders returns the list of available provider names for an anime
// (known providers in canonical order, then any unknown ones Miruro lists).
func (p *MiruroProvider) GetProviders(ctx context.Context, anilistID string) []string {
	data, err := p.fetchEpisodes(ctx, anilistID)
	if err != nil {
		return nil
	}

	seen := map[string]bool{}
	var available []string
	for _, name := range miruroAllProviders {
		if miruroRemovedProviders[name] {
			continue
		}
		if _, ok := data.Providers[name]; ok {
			seen[name] = true
			available = append(available, name)
		}
	}
	for name := range data.Providers {
		if !seen[name] && !miruroRemovedProviders[name] {
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
