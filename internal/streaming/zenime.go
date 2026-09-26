package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// isEmbedPage reports whether a source URL is a player page rather than
// fetchable media: query-level dt_embed flags and embed/watch/player path
// segments. Matching is structural (parsed URL), never substring over the
// whole string — signed query tokens can contain arbitrary text.
// Anything else takes the HLS probe path, where the fetched content (not
// the URL shape) decides playability. This matters: gateway URLs like
// stream.animeparadise.moe/m3u8?url=<token> carry no media extension but
// serve real playlists.
func isEmbedPage(rawURL string) bool {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return true
	}
	if u.Query().Get("dt_embed") != "" {
		return true
	}
	for _, seg := range strings.Split(strings.ToLower(u.Path), "/") {
		switch seg {
		case "embed", "watch", "player":
			return true
		}
	}
	return false
}

// ZenimeProvider resolves streams through the arms-manga-api backend (the
// API behind the Zenime frontend): AniList-keyed info lookups plus
// per-provider watch calls. No catalog search, no slug guessing — episode
// IDs come straight from the provider's own episode list.
//
// Sub-providers (slots): xanime -> Miru for regular titles; hentaimama ->
// Miru for hentai. animeparadise (Robin) was dropped deliberately: its
// library is far smaller than xanime's, and its arms watch endpoint is a
// measured 19s tail (Vercel scraping upstream) that cost every /servers
// round for a slot almost nobody picks. Megaplay/reanime-flavored sources
// are likewise NOT wired: they duplicate the backends already serving
// Niko (megaplay.buzz) and Yuta (flixcloud.cc).
type ZenimeProvider struct {
	client    *http.Client
	log       zerolog.Logger
	apiBase   string
	learnHost func(host string)

	infoMu sync.Mutex
	info   map[string]*zenimeInfoEntry

	// watchMu guards the watch-result cache (see zenimeWatchTTL).
	watchMu sync.Mutex
	watch   map[string]*zenimeWatchEntry
}

// zenimeSubProvider binds an arms provider ID to its server slot.
type zenimeSubProvider struct {
	provider string
	slot     string
}

var zenimeNormalProviders = []zenimeSubProvider{
	{provider: "xanime", slot: "Miru"},
}

var zenimeHentaiProviders = []zenimeSubProvider{
	{provider: "hentaimama", slot: "Miru"},
}

// zenimeInfoTTL bounds info caching: episode lists only grow when new
// episodes air, so hours-old data is fine — and info responses run ~500KB,
// which must never refetch per request.
const zenimeInfoTTL = 6 * time.Hour

// maxZenimeInfoEntries caps the info cache: one entry per title+provider
// stays small, the cap only stops unbounded growth over process lifetime.
const maxZenimeInfoEntries = 300

// zenimeEmptyTTL is the effective cache life of an empty episode list —
// long enough to avoid hammering a title the provider genuinely lacks,
// short enough to recover within minutes when the endpoint was flapping.
const zenimeEmptyTTL = 2 * time.Minute

type zenimeInfoEntry struct {
	episodes []zenimeEpisode
	fetched  time.Time
}

// zenimeWatchTTL bounds watch-result caching. The arms watch endpoint
// costs an upstream round plus segment probes every call — and before
// animeparadise was dropped it was the /servers fan-out's measured tail
// (~19s per call on Vercel, vs xanime's 0.5s). A few minutes of reuse
// collapses repeat rounds into cache hits; the embedded source URLs carry
// their own validity anyway (a stale one 403s at play time, exactly like
// the minute-old server-list snapshot it rides on).
const zenimeWatchTTL = 3 * time.Minute

// maxZenimeWatchEntries caps the watch cache (one entry per episode +
// provider + lang — bounded by catalog size, not by requests).
const maxZenimeWatchEntries = 500

type zenimeWatchEntry struct {
	// res may be nil: a completed resolution with nothing playable is
	// cached too, so a slow watch that found no passable candidate is not
	// re-paid every round. Errors are never cached (flaps retry fresh).
	res     *SourceResult
	fetched time.Time
}

type zenimeEpisode struct {
	id     string
	number int
}

// zenimeUA is the browser UA for arms/probe fetches.
const zenimeUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

func NewZenimeProvider(log zerolog.Logger) *ZenimeProvider {
	base := strings.TrimRight(os.Getenv("ANIRAKU_ARMS_BASE"), "/")
	if base == "" {
		base = "https://arms-manga-api.vercel.app"
	}
	return &ZenimeProvider{
		client:  &http.Client{Timeout: 30 * time.Second, Transport: netguard.NewTransport()},
		log:     log,
		apiBase: base,
		info:    make(map[string]*zenimeInfoEntry),
		watch:   make(map[string]*zenimeWatchEntry),
	}
}

func (p *ZenimeProvider) Name() string { return "zenime" }

func (p *ZenimeProvider) SetHostLearner(fn func(host string)) {
	p.learnHost = fn
}

func (p *ZenimeProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *ZenimeProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("zenime search not implemented")
}

func (p *ZenimeProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("zenime episode listing not implemented")
}

// FindEpisodeSource resolves regular titles (xanime -> Miru). HLS only —
// embeds never list for SFW.
func (p *ZenimeProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	return p.findSources(ctx, anilistID, episode, lang, zenimeNormalProviders, false)
}

// FindHentaiEpisodeSource resolves hentai titles (hentaimama -> Miru).
// HLS first; embed player pages allowed as fallback (NSFW only).
func (p *ZenimeProvider) FindHentaiEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	return p.findSources(ctx, anilistID, episode, lang, zenimeHentaiProviders, true)
}

func (p *ZenimeProvider) findSources(ctx context.Context, anilistID string, episode int, lang string, subs []zenimeSubProvider, allowEmbed bool) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}
	// Arms run CONCURRENTLY: measured in sequence they were the /servers
	// fan-out's slowest collector (6-30s, each arm mostly waiting on the
	// arms info/watch fetches + segment probes). Results are collected
	// per-index and assembled in subs order below so slot naming and
	// server ordering stay deterministic.
	results := make([]*SourceResult, len(subs))
	var wg sync.WaitGroup
	for i, sp := range subs {
		i, sp := i, sp
		wg.Add(1)
		go func() {
			defer wg.Done()
			epID, ok := p.findEpisodeID(ctx, anilistID, episode, sp.provider)
			if !ok {
				return
			}
			sr, err := p.resolveWatch(ctx, epID, sp.provider, lang, allowEmbed)
			if err != nil {
				p.log.Info().Err(err).Str("anilistId", anilistID).Str("provider", sp.provider).Msg("zenime: watch failed")
				return
			}
			results[i] = sr
		}()
	}
	wg.Wait()

	var outSources []core.Source
	var outNames []string
	var headers map[string]string
	for i, sp := range subs {
		sr := results[i]
		if sr == nil || len(sr.Sources) == 0 {
			continue
		}
		outSources = append(outSources, sr.Sources...)
		for range sr.Sources {
			outNames = append(outNames, sp.slot)
		}
		if headers == nil {
			headers = sr.Headers
		}
	}
	if len(outSources) == 0 {
		return nil, nil
	}
	return &SourceResult{
		Sources:     outSources,
		Headers:     headers,
		ServerNames: outNames,
	}, nil
}

// findEpisodeID maps (title, episode number) to the provider's episode ID
// via the cached info response. Exact number match only — never guess.
func (p *ZenimeProvider) findEpisodeID(ctx context.Context, anilistID string, episode int, provider string) (string, bool) {
	eps, err := p.fetchInfo(ctx, anilistID, provider)
	if err != nil {
		p.log.Info().Err(err).Str("anilistId", anilistID).Str("provider", provider).Msg("zenime: info failed")
		return "", false
	}
	for _, e := range eps {
		if e.number == episode && e.id != "" {
			return e.id, true
		}
	}
	return "", false
}

// fetchInfo returns the provider episode list, cached per title+provider.
// One immediate retry on failure: the arms info endpoint flaps per title
// (observed: HTTP 500s alternating with full responses minutes apart), and
// a retry converts most flaps into hits.
func (p *ZenimeProvider) fetchInfo(ctx context.Context, anilistID, provider string) ([]zenimeEpisode, error) {
	eps, err := p.fetchInfoOnce(ctx, anilistID, provider)
	if err == nil {
		return eps, nil
	}
	return p.fetchInfoOnce(ctx, anilistID, provider)
}

func (p *ZenimeProvider) fetchInfoOnce(ctx context.Context, anilistID, provider string) ([]zenimeEpisode, error) {
	key := anilistID + "\x00" + provider
	p.infoMu.Lock()
	if e, ok := p.info[key]; ok && time.Since(e.fetched) < zenimeInfoTTL {
		eps := e.episodes
		p.infoMu.Unlock()
		return eps, nil
	}
	p.infoMu.Unlock()

	u := fmt.Sprintf("%s/meta/anilist/info/%s?provider=%s",
		p.apiBase, url.PathEscape(anilistID), url.QueryEscape(provider))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("arms info returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var raw struct {
		Episodes []struct {
			ID     string `json:"id"`
			Number int    `json:"number"`
		} `json:"episodes"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	eps := make([]zenimeEpisode, 0, len(raw.Episodes))
	for _, e := range raw.Episodes {
		if e.ID != "" {
			eps = append(eps, zenimeEpisode{id: e.ID, number: e.Number})
		}
	}
	p.infoMu.Lock()
	if len(p.info) >= maxZenimeInfoEntries {
		var oldestKey string
		var oldest time.Time
		first := true
		for k, e := range p.info {
			if first || e.fetched.Before(oldest) {
				oldestKey, oldest, first = k, e.fetched, false
			}
		}
		if oldestKey != "" {
			delete(p.info, oldestKey)
		}
	}
	// Empty lists are cached briefly, never for the full TTL: the arms
	// info endpoint flaps per title (observed: HTTP 500s alternating with
	// 200-empty and full responses), and a cached empty poisons the slot
	// for hours while the upstream recovers in minutes. Genuinely absent
	// titles re-probe cheaply every few minutes instead.
	fetched := time.Now()
	if len(eps) == 0 {
		p.log.Info().Str("anilistId", anilistID).Str("provider", provider).Msg("zenime: no episodes listed")
		fetched = fetched.Add(-(zenimeInfoTTL - zenimeEmptyTTL))
	}
	p.info[key] = &zenimeInfoEntry{episodes: eps, fetched: fetched}
	p.infoMu.Unlock()
	return eps, nil
}

// resolveWatch fetches sources for one provider episode ID with the
// result cached for zenimeWatchTTL (hits AND completed empties — errors
// always bypass so flaps retry fresh). This skips both the upstream watch
// call and the segment probes on repeat rounds.
func (p *ZenimeProvider) resolveWatch(ctx context.Context, episodeID, provider, lang string, allowEmbed bool) (*SourceResult, error) {
	key := fmt.Sprintf("%s\x00%s\x00%s\x00%t", episodeID, provider, lang, allowEmbed)
	p.watchMu.Lock()
	if e, ok := p.watch[key]; ok && time.Since(e.fetched) < zenimeWatchTTL {
		p.watchMu.Unlock()
		return e.res, nil
	}
	p.watchMu.Unlock()

	sr, err := p.resolveWatchUncached(ctx, episodeID, provider, lang, allowEmbed)
	if err != nil {
		return sr, err
	}

	p.watchMu.Lock()
	if len(p.watch) >= maxZenimeWatchEntries {
		now := time.Now()
		for k, e := range p.watch {
			if now.Sub(e.fetched) >= zenimeWatchTTL {
				delete(p.watch, k)
			}
		}
		for k := range p.watch {
			if len(p.watch) < maxZenimeWatchEntries {
				break
			}
			delete(p.watch, k)
		}
	}
	p.watch[key] = &zenimeWatchEntry{res: sr, fetched: time.Now()}
	p.watchMu.Unlock()
	return sr, nil
}

// resolveWatchUncached is the network body: it selects the dub/sub variant
// by lang (falling back to whatever exists), and keeps only sources whose
// segments serve from this egress (lenient verdict — definitive blocks
// only, so unknown-host quirks never regress a listing).
// HLS first always; embed pages ship as type:"embed" only when allowEmbed
// (hentai providers serve player pages, regular ones must resolve to HLS).
func (p *ZenimeProvider) resolveWatchUncached(ctx context.Context, episodeID, provider, lang string, allowEmbed bool) (*SourceResult, error) {
	u := fmt.Sprintf("%s/meta/anilist/watch?episodeId=%s&provider=%s",
		p.apiBase, url.QueryEscape(episodeID), url.QueryEscape(provider))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("arms watch returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var raw struct {
		Servers []struct {
			Name string `json:"name"`
			URL  string `json:"url"`
			Type string `json:"type"`
		} `json:"servers"`
		Sources []struct {
			URL     string `json:"url"`
			Quality string `json:"quality"`
			IsM3U8  bool   `json:"isM3U8"`
		} `json:"sources"`
		Subtitles []struct {
			URL   string `json:"url"`
			Lang  string `json:"lang"`
			Label string `json:"label"`
		} `json:"subtitles"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	referer := raw.Headers["Referer"]

	// Dub/sub selection across the typed server variants, falling back to
	// the default sources when no variant matches the requested lang.
	wantDub := lang == "dub"
	type pick struct {
		url     string
		quality string
	}
	var cands []pick
	for _, s := range raw.Servers {
		if s.URL == "" {
			continue
		}
		isDub := strings.Contains(strings.ToLower(s.Type+" "+s.Name), "dub")
		if wantDub != isDub {
			continue
		}
		cands = append(cands, pick{url: s.URL, quality: s.Name})
	}
	if len(cands) == 0 {
		for _, s := range raw.Sources {
			if s.URL == "" {
				continue
			}
			q := s.Quality
			if q == "" {
				q = "auto"
			}
			cands = append(cands, pick{url: s.URL, quality: q})
		}
	}
	var subs []core.Subtitle
	for _, t := range raw.Subtitles {
		if strings.TrimSpace(t.URL) == "" {
			continue
		}
		p.learnURLHost(t.URL)
		label := strings.TrimSpace(t.Label)
		if label == "" {
			label = strings.TrimSpace(t.Lang)
		}
		langCode := mapSubtitleLang(label)
		if strings.TrimSpace(t.Lang) != "" {
			langCode = strings.TrimSpace(t.Lang)
		}
		subs = append(subs, core.Subtitle{URL: t.URL, Lang: langCode, Label: label})
	}
	for _, c := range cands {
		p.learnURLHost(c.url)
		// Embed pages ship as type:"embed", NSFW only: the allowEmbed
		// gate keeps them out of regular titles entirely.
		if isEmbedPage(c.url) {
			if !allowEmbed {
				continue
			}
			quality := c.quality
			if quality == "" {
				quality = "auto"
			}
			return &SourceResult{
				Sources: []core.Source{{
					URL:          c.url,
					Type:         "embed",
					Quality:      quality,
					Subtitles:    subs,
					Verification: "embed",
				}},
				Headers: map[string]string{"Referer": referer},
			}, nil
		}
		// Segment-depth honesty (lenient): a reachable manifest whose
		// segments are egress-blocked only produces a spinning player.
		// Definitive blocks drop the candidate; ambiguous payloads keep
		// it — same rule as the AnimeX sub-providers.
		if !probeSegmentsLenient(ctx, p.client, c.url, referer, zenimeUA) {
			p.log.Info().Str("provider", provider).Str("url", c.url).Msg("zenime: segments blocked from this egress, dropping candidate")
			continue
		}
		quality := c.quality
		if quality == "" {
			quality = "auto"
		}
		return &SourceResult{
			Sources: []core.Source{{
				URL:          c.url,
				Type:         "hls",
				Quality:      quality,
				Subtitles:    subs,
				Verification: "proxy",
			}},
			Headers: map[string]string{"Referer": referer},
		}, nil
	}
	return nil, nil
}
