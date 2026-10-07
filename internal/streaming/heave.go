package streaming

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// Heave resolves animeheaven.me — a plain-PHP catalog with a cookie-gated
// player, verified end-to-end from VPS egress 2026-10-07 with plain HTTP:
//
//	fastsearch.php?s=<title>        -> result anchors + fastname titles
//	anime.php?<id>                  -> episode anchors (md5 key + number)
//	gate.php (Cookie: key=<md5>)    -> <video><source> mp4 mirror list
//
// No Cloudflare challenge, no captcha, no signed calls, no JS runtime:
// three GETs per resolve. The CDN (ck/ct/rx.animeheaven.me) serves byte
// ranges to this egress without a Referer (206 verified).
//
// The site is single-track — its own tagline is "Dubbed Anime Schedule"
// and no sub/dub marker appears anywhere in its markup — so the same file
// serves both lanes and Heave is listed for sub and dub alike, like kaa's
// dual-audio manifests.

const (
	heaveDefaultBase = "https://animeheaven.me"
	heaveServerName  = "Heave"
	heaveAnilistURL  = "https://graphql.aniraku.tech"
	heaveCacheTTL    = 10 * time.Minute
)

var (
	// Show results: <a href='/anime.php?ID'> ... <div class='fastname'>Title
	heaveFastRe = regexp.MustCompile(`(?s)<a[^>]*href=['"]/anime\.php\?([A-Za-z0-9]+)['"][^>]*>.*?class=['"]fastname['"][^>]*>([^<]+)`)
	// Episode rows: <a id ="<md5>" onclick='gatea( "<md5>")' ...> ... <div class=' watch2 bc '>71
	heaveEpisodeRe = regexp.MustCompile(`(?s)id ?= ?"([0-9a-f]{32})"[^>]*onclick=.gatea.{0,600}?watch2[^>]*>\s*(\d+)`)
	// Gate player: <source src='https://ck.animeheaven.me/video.mp4?...' type='video/mp4'
	heaveSourceRe = regexp.MustCompile(`source src=['"](https?://[^'"]+)['"]`)
	// Anivault's placeholder rule: gate.php may answer with an interstitial
	// player instead of the episode — never expose it as a stream.
	heavePlaceholderRe = regexp.MustCompile(`(?i)watch\s+episode\s+on\s+animeheaven`)
)

type heaveShowEntry struct {
	id      string
	expires time.Time
}

type heaveEpisodeEntry struct {
	keys    map[int]string
	expires time.Time
}

type heaveResolvedEntry struct {
	sr      *SourceResult
	expires time.Time
}

type HeaveProvider struct {
	log        zerolog.Logger
	client     *http.Client
	base       string
	anilistURL string
	learnHost  func(string)

	mu       sync.Mutex
	shows    map[string]*heaveShowEntry    // anilistID -> animeheaven show id
	episodes map[string]*heaveEpisodeEntry // show id -> episode number -> gate key
	resolved map[string]*heaveResolvedEntry
}

func NewHeaveProvider(log zerolog.Logger, base, anilistURL string) *HeaveProvider {
	if strings.TrimSpace(base) == "" {
		base = heaveDefaultBase
	}
	if strings.TrimSpace(anilistURL) == "" {
		anilistURL = heaveAnilistURL
	}
	return &HeaveProvider{
		log:        log,
		client:     &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		base:       strings.TrimRight(base, "/"),
		anilistURL: anilistURL,
		shows:      make(map[string]*heaveShowEntry),
		episodes:   make(map[string]*heaveEpisodeEntry),
		resolved:   make(map[string]*heaveResolvedEntry),
	}
}

func (p *HeaveProvider) Name() string { return "heave" }

func (p *HeaveProvider) SetHostLearner(fn func(string)) { p.learnHost = fn }

func (p *HeaveProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *HeaveProvider) get(ctx context.Context, rawURL, referer string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("heave: HTTP %d for %s", resp.StatusCode, rawURL)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (p *HeaveProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	rawURL := p.base + "/fastsearch.php?s=" + url.QueryEscape(title)
	body, err := p.get(ctx, rawURL, p.base+"/")
	if err != nil {
		return nil, err
	}
	var out []SearchResult
	seen := make(map[string]bool)
	for _, m := range heaveFastRe.FindAllStringSubmatch(body, -1) {
		id, name := m[1], strings.TrimSpace(m[2])
		if id == "" || name == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, SearchResult{ID: id, Title: name})
	}
	return out, nil
}

// FindEpisodes returns the episode-key map of one animeheaven show. The
// providerID is the animeheaven show id (the fastsearch anchor id).
func (p *HeaveProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	keys, err := p.episodeKeys(ctx, providerID)
	if err != nil {
		return nil, err
	}
	nums := make([]int, 0, len(keys))
	for n := range keys {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	out := make([]Episode, 0, len(nums))
	for _, n := range nums {
		out = append(out, Episode{Number: n, Title: fmt.Sprintf("Episode %d", n)})
	}
	return out, nil
}

func (p *HeaveProvider) episodeKeys(ctx context.Context, showID string) (map[int]string, error) {
	p.mu.Lock()
	if e, ok := p.episodes[showID]; ok && time.Now().Before(e.expires) {
		p.mu.Unlock()
		return e.keys, nil
	}
	p.mu.Unlock()

	body, err := p.get(ctx, p.base+"/anime.php?"+showID, p.base+"/")
	if err != nil {
		return nil, err
	}
	keys := make(map[int]string)
	for _, m := range heaveEpisodeRe.FindAllStringSubmatch(body, -1) {
		key, numStr := m[1], m[2]
		var n int
		if _, err := fmt.Sscanf(numStr, "%d", &n); err != nil || n <= 0 {
			continue
		}
		if _, dup := keys[n]; !dup {
			keys[n] = key
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("heave: no episodes on show page %s", showID)
	}

	p.mu.Lock()
	p.episodes[showID] = &heaveEpisodeEntry{keys: keys, expires: time.Now().Add(heaveCacheTTL)}
	p.mu.Unlock()
	return keys, nil
}

// FindEpisodeSource resolves one episode to the gate.php MP4 mirror list.
// The site carries a single audio track per episode (dub-leaning), so lang
// only gates the API contract — both lanes receive the same file, matching
// the kaa dual-audio precedent.
func (p *HeaveProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}

	showID, err := p.findShow(ctx, providerID)
	if err != nil {
		return nil, err
	}

	cacheKey := showID + ":" + fmt.Sprintf("%d:%s", episode, langKey)
	p.mu.Lock()
	if e, ok := p.resolved[cacheKey]; ok && time.Now().Before(e.expires) {
		p.mu.Unlock()
		return cloneSourceResult(e.sr), nil
	}
	p.mu.Unlock()

	keys, err := p.episodeKeys(ctx, showID)
	if err != nil {
		return nil, err
	}
	key, ok := keys[episode]
	if !ok {
		p.log.Info().Str("showId", showID).Int("episode", episode).
			Msg("heave: episode not in show page key list")
		return nil, nil
	}

	showReferer := p.base + "/anime.php?" + showID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/gate.php", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Cookie", "key="+key)
	req.Header.Set("Referer", showReferer)
	req.Header.Set("Origin", p.base)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("heave: gate HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
	if err != nil {
		return nil, err
	}
	page := string(body)
	if heavePlaceholderRe.MatchString(page) {
		p.log.Info().Str("showId", showID).Int("episode", episode).
			Msg("heave: gate returned the placeholder player, dropping")
		return nil, nil
	}

	var mp4s []string
	seen := make(map[string]bool)
	for _, m := range heaveSourceRe.FindAllStringSubmatch(page, -1) {
		src := strings.TrimSpace(m[1])
		if !strings.HasPrefix(src, "http") || seen[src] {
			continue
		}
		seen[src] = true
		if strings.Contains(strings.ToLower(src), ".mp4") {
			mp4s = append(mp4s, src)
		}
	}
	if len(mp4s) == 0 {
		p.log.Info().Str("showId", showID).Int("episode", episode).
			Msg("heave: gate returned no mp4 source")
		return nil, nil
	}

	// Honesty before listing: probe every mirror from this egress (range
	// request + magic bytes) and ship only the ones that answer — a gate
	// can list a CDN edge that is already down.
	var playable []string
	for _, u := range mp4s {
		if probeMediaFileLenient(ctx, p.client, u, showReferer, browserUA) {
			playable = append(playable, u)
		} else {
			p.log.Info().Str("url", u).Msg("heave: mp4 probe failed, dropping mirror")
		}
	}
	if len(playable) == 0 {
		p.log.Info().Str("showId", showID).Int("episode", episode).
			Msg("heave: no mirror survived probing")
		return nil, nil
	}

	sources := make([]core.Source, 0, len(playable))
	for _, u := range playable {
		p.learnURLHost(u)
		sources = append(sources, core.Source{
			URL:          u,
			Type:         "mp4",
			Quality:      "auto",
			Verification: "direct",
		})
	}

	sr := &SourceResult{
		Sources:    sources,
		Headers:    map[string]string{},
		ServerName: heaveServerName,
	}

	p.mu.Lock()
	p.resolved[cacheKey] = &heaveResolvedEntry{sr: cloneSourceResult(sr), expires: time.Now().Add(heaveCacheTTL)}
	p.mu.Unlock()

	p.log.Info().Str("showId", showID).Int("episode", episode).Str("lang", langKey).
		Int("mirrors", len(sources)).Msg("heave: resolved")
	return sr, nil
}

// findShow maps an AniList id to an animeheaven show id via AniList titles
// + fastsearch. The result is cached; a negative result is not.
func (p *HeaveProvider) findShow(ctx context.Context, anilistID string) (string, error) {
	p.mu.Lock()
	if e, ok := p.shows[anilistID]; ok && time.Now().Before(e.expires) {
		p.mu.Unlock()
		return e.id, nil
	}
	p.mu.Unlock()

	titles, err := fetchAnilistTitles(ctx, p.client, p.anilistURL, anilistID)
	if err != nil {
		return "", fmt.Errorf("heave: %w", err)
	}
	if len(titles) == 0 {
		return "", fmt.Errorf("heave: anilist %s has no titles", anilistID)
	}

	// Query with the strongest title first, then the next distinct one —
	// fastsearch is substring-ish, so an exact-title match over the result
	// set is what decides (the One Piece fastsearch lists "One Piece
	// Heroines" first, the main show second).
	queries := make([]string, 0, 2)
	for _, t := range titles {
		if len([]rune(t)) < 3 {
			continue
		}
		dup := false
		for _, q := range queries {
			if strings.EqualFold(q, t) {
				dup = true
				break
			}
		}
		if !dup {
			queries = append(queries, t)
		}
		if len(queries) == 2 {
			break
		}
	}

	var lastErr error
	for _, q := range queries {
		results, err := p.Search(ctx, q)
		if err != nil {
			lastErr = err
			continue
		}
		if best, ok := pickProviderTitle(results, titles); ok {
			p.mu.Lock()
			p.shows[anilistID] = &heaveShowEntry{id: best.ID, expires: time.Now().Add(heaveCacheTTL)}
			p.mu.Unlock()
			p.log.Info().Str("anilistId", anilistID).Str("showId", best.ID).
				Str("matched", best.Title).Msg("heave: show resolved")
			return best.ID, nil
		}
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("heave: no show match for anilist %s", anilistID)
}
