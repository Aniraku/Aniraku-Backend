package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

const (
	defaultZenAPIBase = "https://api.anizen.tr"
	zenLookupTTL      = 30 * time.Minute
	zenDeadTTL        = 60 * time.Minute
)

// ZenProvider resolves anime via the ZenAPI (api.anizen.tr). Miruro's
// per-provider health machinery is reused: after consecutive API failures the
// provider is auto-blocked for providerCoolOff, so an expired/dead ZenAPI
// removes itself from the chain and recovers on its own. Per-anime dead
// entries are cached so expired anime (e.g. Miruro-dead titles) are never
// re-probed within zenDeadTTL.
//
// ZenAPI exposes its own player (https://api.anizen.tr/player/<encrypted>),
// which is Cloudflare-JS-challenged for direct fetches but embeds fine in an
// iframe (no X-Frame-Options, CORS `*`). Sources are therefore returned as
// Type "embed" — the frontend swaps ArtPlayer for ZenAPI's player.
type ZenProvider struct {
	log     zerolog.Logger
	apiBase string
	client  *http.Client

	healthMu sync.Mutex
	health   map[string]*providerHealth

	infoMu sync.Mutex
	info   map[int]*zenAnimeInfo

	deadMu sync.Mutex
	dead   map[string]time.Time
}

// zenAnimeInfo is the resolved catalog entry for one AniList ID.
type zenAnimeInfo struct {
	malID     int
	fetchedAt time.Time
	episodes  map[int]zenEpisodeInfo
}

// zenEpisodeInfo holds the per-language embed player URLs for one episode.
type zenEpisodeInfo struct {
	hasSub bool
	hasDub bool
	sub    []string
	dub    []string
}

// ZenAPI JSON shapes (verified live):
//
//	GET /api/search?keyword=...  -> results.data: [{id (slug), title, tvInfo:{sub,dub}}]
//	GET /api/info?id=<slug>      -> results.data: {numericAnimeId, anilistId, malId,
//	                                 episodes:{episodes:[{episode_no, hasSub, hasDub,
//	                                 streams:{sub:[{embed}], dub:[{embed}]}}]}}
type zenSearchResponse struct {
	Results zenSearchResults `json:"results"`
}

type zenSearchResults struct {
	Data []zenSearchItem `json:"data"`
}

type zenSearchItem struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type zenInfoResponse struct {
	Results zenInfoResults `json:"results"`
}

type zenInfoResults struct {
	Data zenInfoData `json:"data"`
}

type zenInfoData struct {
	NumericAnimeID string           `json:"numericAnimeId"`
	AnilistID      *int             `json:"anilistId"`
	MalID          int              `json:"malId"`
	Episodes       zenEpisodesBlock `json:"episodes"`
}

type zenEpisodesBlock struct {
	Episodes []zenEpisode `json:"episodes"`
}

type zenEpisode struct {
	EpisodeNo int             `json:"episode_no"`
	HasSub    bool            `json:"hasSub"`
	HasDub    bool            `json:"hasDub"`
	Streams   zenStreamGroups `json:"streams"`
}

type zenStreamGroups struct {
	Sub []zenStream `json:"sub"`
	Dub []zenStream `json:"dub"`
}

type zenStream struct {
	Embed string `json:"embed"`
}

func NewZenProvider(log zerolog.Logger, apiBase string, client *http.Client) *ZenProvider {
	if apiBase == "" {
		apiBase = defaultZenAPIBase
	}
	return &ZenProvider{
		log:     log,
		apiBase: strings.TrimRight(apiBase, "/"),
		client:  client,
		health:  map[string]*providerHealth{},
		info:    map[int]*zenAnimeInfo{},
		dead:    map[string]time.Time{},
	}
}

func (p *ZenProvider) Name() string { return "zen" }

// Search satisfies the Provider interface; ZenAPI resolution flows through
// Resolve (search + malId matching) rather than these primitives.
func (p *ZenProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	slugs, err := p.search(ctx, title)
	if err != nil {
		return nil, err
	}
	results := make([]SearchResult, 0, len(slugs))
	for _, s := range slugs {
		results = append(results, SearchResult{ID: s, Title: s})
	}
	return results, nil
}

// FindEpisodes is unsupported for ZenAPI; Resolve looks the anime up on demand.
func (p *ZenProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("zen: FindEpisodes not supported")
}

// FindEpisodeSource resolves an embed source by AniList ID without title
// context; the manager calls Resolve with title/malID for matching.
func (p *ZenProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(providerID)
	if err != nil {
		return nil, fmt.Errorf("zen: invalid anime id %q", providerID)
	}
	return p.Resolve(ctx, id, "", 0, episode, lang)
}

func (p *ZenProvider) healthFor(name string) *providerHealth {
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

func (p *ZenProvider) blocked() bool {
	h := p.healthFor("zen")
	return h.isBlocked()
}

func (p *ZenProvider) recordFailure() {
	p.healthFor("zen").recordFailure()
}

func (p *ZenProvider) recordSuccess() {
	p.healthFor("zen").recordSuccess()
}

func (p *ZenProvider) markDead(key string) {
	p.deadMu.Lock()
	defer p.deadMu.Unlock()
	if p.dead == nil {
		p.dead = map[string]time.Time{}
	}
	p.dead[key] = time.Now().Add(zenDeadTTL)
}

func (p *ZenProvider) isDead(key string) bool {
	p.deadMu.Lock()
	defer p.deadMu.Unlock()
	until, ok := p.dead[key]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(p.dead, key)
		return false
	}
	return true
}

func (p *ZenProvider) cachedInfo(anilistID int) *zenAnimeInfo {
	p.infoMu.Lock()
	defer p.infoMu.Unlock()
	info, ok := p.info[anilistID]
	if !ok || time.Since(info.fetchedAt) > zenLookupTTL {
		return nil
	}
	return info
}

func (p *ZenProvider) storeInfo(anilistID int, info *zenAnimeInfo) {
	p.infoMu.Lock()
	defer p.infoMu.Unlock()
	if p.info == nil {
		p.info = map[int]*zenAnimeInfo{}
	}
	info.fetchedAt = time.Now()
	p.info[anilistID] = info
}

func (p *ZenProvider) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.apiBase+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("zen: %s returned %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// search returns the top candidate slugs for a title keyword.
func (p *ZenProvider) search(ctx context.Context, keyword string) ([]string, error) {
	var resp zenSearchResponse
	if err := p.get(ctx, "/api/search?keyword="+url.QueryEscape(keyword), &resp); err != nil {
		return nil, err
	}
	var slugs []string
	for _, item := range resp.Results.Data {
		if item.ID != "" {
			slugs = append(slugs, item.ID)
		}
	}
	return slugs, nil
}

// fetchInfo resolves the ZenAPI catalog entry for an AniList ID: search by
// title, then match info by malId (anilistId fallback; ZenAPI usually leaves
// it null). The result is cached per AniList ID for zenLookupTTL.
func (p *ZenProvider) fetchInfo(ctx context.Context, anilistID int, title string, malID int) (*zenAnimeInfo, error) {
	if info := p.cachedInfo(anilistID); info != nil {
		return info, nil
	}
	if p.isDead(fmt.Sprintf("a:%d", anilistID)) {
		return nil, fmt.Errorf("zen: not available for this anime")
	}
	if title == "" {
		return nil, fmt.Errorf("zen: no title for matching")
	}

	slugs, err := p.search(ctx, title)
	if err != nil {
		p.recordFailure()
		return nil, fmt.Errorf("zen search failed: %w", err)
	}
	if len(slugs) == 0 {
		p.markDead(fmt.Sprintf("a:%d", anilistID))
		return nil, fmt.Errorf("zen: not available for this anime")
	}

	var matched *zenAnimeInfo
	for _, slug := range slugs {
		var resp zenInfoResponse
		if err := p.get(ctx, "/api/info?id="+url.QueryEscape(slug), &resp); err != nil {
			p.recordFailure()
			return nil, fmt.Errorf("zen info failed: %w", err)
		}
		data := resp.Results.Data
		info := &zenAnimeInfo{
			malID:    data.MalID,
			episodes: map[int]zenEpisodeInfo{},
		}
		if data.MalID > 0 && data.MalID == malID {
			matched = info
		} else if data.AnilistID != nil && *data.AnilistID == anilistID {
			matched = info
		}
		if matched == nil {
			continue
		}
		for _, ep := range data.Episodes.Episodes {
			ze := zenEpisodeInfo{hasSub: ep.HasSub, hasDub: ep.HasDub}
			for _, s := range ep.Streams.Sub {
				if s.Embed != "" {
					ze.sub = append(ze.sub, s.Embed)
				}
			}
			for _, s := range ep.Streams.Dub {
				if s.Embed != "" {
					ze.dub = append(ze.dub, s.Embed)
				}
			}
			matched.episodes[ep.EpisodeNo] = ze
		}
		break
	}

	if matched == nil {
		p.markDead(fmt.Sprintf("a:%d", anilistID))
		return nil, fmt.Errorf("zen: not available for this anime")
	}

	p.storeInfo(anilistID, matched)
	p.recordSuccess()
	return matched, nil
}

// Resolve returns a ZenAPI embed source for the requested episode/lang, or a
// "not available" error when the anime or episode has no stream there.
func (p *ZenProvider) Resolve(ctx context.Context, anilistID int, title string, malID int, episode int, lang string) (*SourceResult, error) {
	if p.blocked() {
		return nil, fmt.Errorf("zen: provider blocked")
	}
	if lang == "" {
		lang = "sub"
	}

	deadKey := fmt.Sprintf("e:%d:%d:%s", anilistID, episode, lang)
	if p.isDead(deadKey) {
		return nil, fmt.Errorf("zen: not available for this anime")
	}

	info, err := p.fetchInfo(ctx, anilistID, title, malID)
	if err != nil {
		return nil, err
	}

	ep, ok := info.episodes[episode]
	has := ok && ((lang == "dub" && ep.hasDub) || (lang == "sub" && ep.hasSub))
	embeds := ep.sub
	if lang == "dub" {
		embeds = ep.dub
	}
	if !has || len(embeds) == 0 {
		p.markDead(deadKey)
		return nil, fmt.Errorf("zen: not available for this anime")
	}

	seen := map[string]bool{}
	sources := make([]core.Source, 0, len(embeds))
	for _, e := range embeds {
		if seen[e] {
			continue
		}
		seen[e] = true
		sources = append(sources, core.Source{
			URL:     e,
			Type:    "embed",
			Quality: "auto",
		})
	}

	return &SourceResult{Sources: sources}, nil
}
