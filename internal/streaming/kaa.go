package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// KaaProvider resolves streams through kaa.lt (AniList-keyed search +
// episode listings, krussdomi embedded players, HLS masters).
//
// Verified live 2026-09-30 from the production egress:
//   - kaa.lt search / episodes / watch pages: 200, no datacenter block.
//   - krussdomi master + quality playlists: 200 (direct and via media proxy).
//   - krussdomi segment bytes REQUIRE `Origin: https://krussdomi.com`
//     (Referer alone 403s, every UA 404/403s without Origin). The provider
//     therefore always ships Referer+Origin in result Headers so the media
//     proxy forwards them (applyProxyQueryHeaders passes both through).
//
// DUAL-LANGUAGE RULE (operator-verified): one decrypted krussdomi manifest
// serves both sub (ja-JP) and dub (en-US) — Naruto ep1 resolves to manifest
// 64d7164244c6d04c12f3fdbb under both langs. The resolve cache below is
// keyed WITHOUT lang, so the second lang is always a cache hit and the
// upstream is never fetched per-lang.
const (
	kaaAPIBase     = "https://kaa.lt"
	kaaKrussOrigin = "https://krussdomi.com"
	kaaKrussRef    = "https://krussdomi.com/"

	kaaSlugTTL           = 24 * time.Hour
	kaaResolveTTL        = 10 * time.Minute
	maxKaaSlugEntries    = 500
	maxKaaResolveEntries = 500
)

var (
	kaaPlayerRe = regexp.MustCompile(`\{\s*name:\s*"([^"]+)"\s*,\s*shortName:\s*"([^"]+)"\s*,\s*src:\s*"([^"]+)"\s*\}`)
	kaaCatRe    = regexp.MustCompile(`https?://[a-zA-Z0-9.\-]+/cat-player/player\?[^"'\\s]+`)
	kaaM3U8Re   = regexp.MustCompile(`https?://[^\s"'<>\\&]+\.m3u8[^\s"'<>\\&]*`)
	kaaVTTRe    = regexp.MustCompile(`https?://[^\s"'<>\\&]+\.vtt[^\s"'<>\\&]*`)
)

type KaaProvider struct {
	log        zerolog.Logger
	client     *http.Client
	kaaBase    string
	anilistURL string
	learnHost  func(host string)

	mu       sync.Mutex
	slugs    map[int]*kaaSlugEntry
	resolved map[kaaResolveKey]*kaaResolvedEntry
}

type kaaSlugEntry struct {
	slug    string
	title   string
	fetched time.Time
}

type kaaResolveKey struct {
	slug    string
	episode int
}

type kaaResolvedEntry struct {
	result  *SourceResult
	fetched time.Time
}

type kaaSearchHit struct {
	Slug      string `json:"slug"`
	Title     string `json:"title"`
	Year      int    `json:"year"`
	Type      string `json:"type"`
	StartDate string `json:"start_date"`
}

type kaaEpisode struct {
	Number int    `json:"episode_number"`
	Slug   string `json:"slug"`
	Title  string `json:"title"`
}

func NewKaaProvider(log zerolog.Logger, kaaBase, anilistURL string) *KaaProvider {
	if kaaBase == "" {
		kaaBase = kaaAPIBase
	}
	if anilistURL == "" {
		anilistURL = "https://graphql.anilist.co"
	}
	return &KaaProvider{
		log:        log,
		client:     &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		kaaBase:    strings.TrimRight(kaaBase, "/"),
		anilistURL: anilistURL,
		slugs:      make(map[int]*kaaSlugEntry),
		resolved:   make(map[kaaResolveKey]*kaaResolvedEntry),
	}
}

func (p *KaaProvider) Name() string { return "kaa" }

func (p *KaaProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *KaaProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *KaaProvider) kaaHeaders(referer string) map[string]string {
	return map[string]string{
		"User-Agent": browserUA,
		"Referer":    referer,
	}
}

func (p *KaaProvider) doJSON(ctx context.Context, method, rawURL string, body any, headers map[string]string, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kaa: %s %s -> HTTP %d", method, rawURL, resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}

func (p *KaaProvider) doText(ctx context.Context, rawURL string, headers map[string]string, limit int64) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("kaa: GET %s -> HTTP %d", rawURL, resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Search maps a title to kaa.lt show slugs.
func (p *KaaProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	var hits []kaaSearchHit
	if err := p.doJSON(ctx, http.MethodPost, p.kaaBase+"/api/search",
		map[string]string{"query": title}, p.kaaHeaders(p.kaaBase+"/"), &hits); err != nil {
		return nil, err
	}
	out := make([]SearchResult, 0, len(hits))
	for _, h := range hits {
		if h.Slug == "" {
			continue
		}
		out = append(out, SearchResult{ID: h.Slug, Title: h.Title})
	}
	return out, nil
}

// FindEpisodes lists episodes for a kaa.lt show slug across all pages.
func (p *KaaProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	eps, err := p.listEpisodes(ctx, providerID, "ja-JP", 0)
	if err != nil {
		return nil, err
	}
	out := make([]Episode, 0, len(eps))
	for _, e := range eps {
		out = append(out, Episode{Number: e.Number, Title: e.Title})
	}
	return out, nil
}

func (p *KaaProvider) listEpisodes(ctx context.Context, slug, lang string, firstEp int) ([]kaaEpisode, error) {
	var first struct {
		Result []kaaEpisode `json:"result"`
		Pages  []struct {
			Eps []int `json:"eps"`
		} `json:"pages"`
	}
	u := fmt.Sprintf("%s/api/show/%s/episodes?ep=%d&lang=%s", p.kaaBase, slug, firstEp, lang)
	if err := p.doJSON(ctx, http.MethodGet, u, nil, p.kaaHeaders(p.kaaBase+"/"), &first); err != nil {
		return nil, err
	}
	eps := append([]kaaEpisode(nil), first.Result...)
	seen := map[int]bool{}
	for _, e := range eps {
		seen[e.Number] = true
	}
	for _, page := range first.Pages {
		if len(page.Eps) == 0 {
			continue
		}
		// Page 1 is already in hand (first.Result above) — only fetch
		// pages whose leading episode we have not seen.
		if seen[page.Eps[0]] {
			continue
		}
		pu := fmt.Sprintf("%s/api/show/%s/episodes?ep=%d&lang=%s", p.kaaBase, slug, page.Eps[0], lang)
		var pd struct {
			Result []kaaEpisode `json:"result"`
		}
		if err := p.doJSON(ctx, http.MethodGet, pu, nil, p.kaaHeaders(p.kaaBase+"/"), &pd); err != nil {
			return nil, err
		}
		for _, e := range pd.Result {
			if !seen[e.Number] {
				eps = append(eps, e)
				seen[e.Number] = true
			}
		}
	}
	sort.Slice(eps, func(i, j int) bool { return eps[i].Number < eps[j].Number })
	return eps, nil
}

// FindEpisodeSource resolves one episode. The result is cached WITHOUT lang
// (dual-language rule): a dub request reuses the sub resolve and vice versa.
func (p *KaaProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("kaa: bad anilist id %q", anilistID)
	}
	langs := []string{kaaLangParam(lang), kaaLangParam(otherKaaLang(lang))}

	slug, _, err := p.resolveSlug(ctx, id)
	if err != nil {
		return nil, err
	}
	if got := p.loadResolved(slug, episode); got != nil {
		return got, nil
	}
	var lastErr error
	for _, kl := range langs {
		sr, err := p.resolveEpisode(ctx, id, slug, episode, kl, lang)
		if err == nil && sr != nil && len(sr.Sources) > 0 {
			p.storeResolved(slug, episode, sr)
			return sr, nil
		}
		if err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("kaa: no sources for episode %d", episode)
}

func kaaLangParam(lang string) string {
	if strings.EqualFold(lang, "dub") {
		return "en-US"
	}
	return "ja-JP"
}

func otherKaaLang(lang string) string {
	if strings.EqualFold(lang, "dub") {
		return "sub"
	}
	return "dub"
}

func (p *KaaProvider) loadResolved(slug string, episode int) *SourceResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[kaaResolveKey{slug: slug, episode: episode}]
	if !ok || time.Since(e.fetched) > kaaResolveTTL {
		return nil
	}
	return e.result
}

func (p *KaaProvider) storeResolved(slug string, episode int, sr *SourceResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.resolved {
		if time.Since(e.fetched) > kaaResolveTTL {
			delete(p.resolved, k)
		}
	}
	if len(p.resolved) >= maxKaaResolveEntries {
		oldest := kaaResolveKey{}
		var oldestAt time.Time
		first := true
		for k, e := range p.resolved {
			if first || e.fetched.Before(oldestAt) {
				oldest, oldestAt, first = k, e.fetched, false
			}
		}
		delete(p.resolved, oldest)
	}
	p.resolved[kaaResolveKey{slug: slug, episode: episode}] = &kaaResolvedEntry{result: sr, fetched: now}
}

// resolveSlug maps an AniList ID to a kaa.lt slug (24h cache).
func (p *KaaProvider) resolveSlug(ctx context.Context, id int) (string, string, error) {
	p.mu.Lock()
	if e, ok := p.slugs[id]; ok && time.Since(e.fetched) < kaaSlugTTL {
		slug, title := e.slug, e.title
		p.mu.Unlock()
		return slug, title, nil
	}
	p.mu.Unlock()

	titles, year, err := p.anilistTitles(ctx, id)
	if err != nil {
		return "", "", err
	}
	var hits []kaaSearchHit
	for _, t := range titles {
		var hs []kaaSearchHit
		if err := p.doJSON(ctx, http.MethodPost, p.kaaBase+"/api/search",
			map[string]string{"query": t}, p.kaaHeaders(p.kaaBase+"/"), &hs); err != nil {
			continue
		}
		if len(hs) > 0 {
			hits = hs
			break
		}
	}
	if len(hits) == 0 {
		return "", "", fmt.Errorf("kaa: no show match for anilist %d", id)
	}
	best := hits[0]
	bestScore := -1
	for _, h := range hits {
		if s := kaaScoreHit(h, titles, year); s > bestScore {
			best, bestScore = h, s
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, e := range p.slugs {
		if time.Since(e.fetched) > kaaSlugTTL {
			delete(p.slugs, k)
		}
	}
	if len(p.slugs) >= maxKaaSlugEntries {
		for k := range p.slugs {
			delete(p.slugs, k)
			break
		}
	}
	p.slugs[id] = &kaaSlugEntry{slug: best.Slug, title: best.Title, fetched: time.Now()}
	return best.Slug, best.Title, nil
}

func kaaScoreHit(h kaaSearchHit, titles []string, year int) int {
	s := 0
	if year != 0 && h.Year == year {
		s += 10
	}
	if strings.EqualFold(h.Type, "tv") {
		s += 2
	}
	tl := strings.ToLower(h.Title)
	for _, t := range titles {
		t = strings.ToLower(strings.TrimSpace(t))
		if t != "" && (strings.Contains(tl, t) || strings.Contains(t, tl)) {
			s += 5
			break
		}
	}
	if year != 0 && strings.HasPrefix(h.StartDate, strconv.Itoa(year)) {
		s += 3
	}
	return s
}

func (p *KaaProvider) anilistTitles(ctx context.Context, id int) ([]string, int, error) {
	gql := `query($id:Int){Media(id:$id,type:ANIME){title{romaji english}startDate{year}}}`
	var out struct {
		Data struct {
			Media struct {
				Title struct {
					Romaji  string `json:"romaji"`
					English string `json:"english"`
				} `json:"title"`
				StartDate struct {
					Year int `json:"year"`
				} `json:"startDate"`
			} `json:"Media"`
		} `json:"data"`
	}
	if err := p.doJSON(ctx, http.MethodPost, p.anilistURL,
		map[string]any{"query": gql, "variables": map[string]any{"id": id}},
		map[string]string{"Accept": "application/json",
			"User-Agent": browserUA}, &out); err != nil {
		return nil, 0, fmt.Errorf("kaa: anilist titles: %w", err)
	}
	var titles []string
	if t := strings.TrimSpace(out.Data.Media.Title.English); t != "" {
		titles = append(titles, t)
	}
	if t := strings.TrimSpace(out.Data.Media.Title.Romaji); t != "" {
		titles = append(titles, t)
	}
	if len(titles) == 0 {
		return nil, 0, fmt.Errorf("kaa: anilist %d has no titles", id)
	}
	return titles, out.Data.Media.StartDate.Year, nil
}

type kaaPlayer struct {
	name  string
	short string
	src   string
}

func (p *KaaProvider) resolveEpisode(ctx context.Context, id int, slug string, episode int, kaaLang, reqLang string) (*SourceResult, error) {
	eps, err := p.listEpisodes(ctx, slug, kaaLang, episode)
	if err != nil {
		return nil, err
	}
	var match *kaaEpisode
	for i := range eps {
		if eps[i].Number == episode {
			match = &eps[i]
			break
		}
	}
	if match == nil || match.Slug == "" {
		return nil, fmt.Errorf("kaa: episode %d not listed (%s)", episode, kaaLang)
	}
	watchURL := fmt.Sprintf("%s/%s/ep-%d-%s", p.kaaBase, slug, episode, match.Slug)

	raw, err := p.doText(ctx, watchURL, p.kaaHeaders(p.kaaBase+"/"), 1<<20)
	if err != nil {
		return nil, err
	}
	players := kaaExtractPlayers(raw)
	if len(players) == 0 {
		return nil, fmt.Errorf("kaa: no embedded players on %s", watchURL)
	}
	var lastErr error
	for _, pl := range players {
		// DASH-only arms carry no m3u8 — skip without an upstream call.
		if strings.Contains(strings.ToLower(pl.src), "type=dash") {
			continue
		}
		sr, err := p.resolvePlayer(ctx, pl, watchURL, reqLang)
		if err == nil && sr != nil && len(sr.Sources) > 0 {
			p.log.Info().Int("animeId", id).Int("episode", episode).
				Str("lang", reqLang).Str("player", pl.name).Msg("kaa resolved")
			return sr, nil
		}
		if err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("kaa: no playable player for episode %d", episode)
}

func kaaExtractPlayers(raw string) []kaaPlayer {
	txt := strings.ReplaceAll(strings.ReplaceAll(raw, "\\u002F", "/"), "\\/", "/")
	var out []kaaPlayer
	for _, m := range kaaPlayerRe.FindAllStringSubmatch(txt, -1) {
		if len(m) != 4 || strings.TrimSpace(m[3]) == "" {
			continue
		}
		out = append(out, kaaPlayer{name: m[1], short: m[2], src: m[3]})
	}
	if len(out) == 0 {
		for _, u := range kaaCatRe.FindAllString(txt, -1) {
			out = append(out, kaaPlayer{name: "unknown", short: "?", src: u})
		}
	}
	return out
}

func kaaUnescape(s string) string {
	s = html.UnescapeString(s)
	s = strings.ReplaceAll(s, "\\u002F", "/")
	return strings.ReplaceAll(s, "\\/", "/")
}

func (p *KaaProvider) resolvePlayer(ctx context.Context, pl kaaPlayer, watchURL, reqLang string) (*SourceResult, error) {
	raw, err := p.doText(ctx, pl.src, p.kaaHeaders(watchURL), 1<<20)
	if err != nil {
		return nil, err
	}
	txt := kaaUnescape(raw)
	masters := kaaM3U8Re.FindAllString(txt, -1)
	if len(masters) == 0 {
		return nil, fmt.Errorf("kaa: player %q has no m3u8", pl.name)
	}
	headers := map[string]string{
		"Referer": kaaKrussRef,
		"Origin":  kaaKrussOrigin,
	}
	var lastErr error
	for _, master := range masters {
		variants, ok := p.probeKaaMaster(ctx, master)
		if !ok {
			lastErr = fmt.Errorf("kaa: master probe failed")
			continue
		}
		sr := &SourceResult{Headers: headers}
		subs := kaaVTTRe.FindAllString(txt, -1)
		for _, v := range variants {
			p.learnURLHost(v.url)
			src := core.Source{
				URL:          v.url,
				Type:         "hls",
				Quality:      v.quality,
				Verification: "proxy",
			}
			for _, s := range subs {
				p.learnURLHost(s)
				src.Subtitles = append(src.Subtitles, core.Subtitle{URL: s, Lang: reqLang, Label: reqLang})
			}
			sr.Sources = append(sr.Sources, src)
			sr.ServerNames = append(sr.ServerNames, pl.name)
		}
		if len(sr.Sources) == 0 {
			lastErr = fmt.Errorf("kaa: no variants in master")
			continue
		}
		p.learnURLHost(master)
		return sr, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("kaa: player %q unplayable", pl.name)
}

type kaaVariant struct {
	url       string
	quality   string
	bandwidth int
}

// probeKaaMaster verifies master -> first media -> first segment with the
// Origin header krussdomi segments require, warming the VOD playback cache
// like every other provider probe. Returns the master's quality variants.
func (p *KaaProvider) probeKaaMaster(ctx context.Context, master string) ([]kaaVariant, bool) {
	fetch := func(rawURL string, limit int64, timeout time.Duration) ([]byte, bool) {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(cctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, false
		}
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Referer", kaaKrussRef)
		req.Header.Set("Origin", kaaKrussOrigin)
		resp, err := p.client.Do(req)
		if err != nil {
			return nil, false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, false
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, limit))
		if err != nil {
			return nil, false
		}
		return b, true
	}
	mhead, ok := fetch(master, 65536, 10*time.Second)
	if !ok || !strings.Contains(string(mhead), "#EXTM3U") {
		return nil, false
	}
	VODCacheSet(master, mhead)
	variants := kaaParseVariants(string(mhead), master)
	if len(variants) == 0 {
		return nil, false
	}
	media := variants[0].url
	mbody, ok := fetch(media, 262144, 10*time.Second)
	if !ok || !strings.Contains(string(mbody), "#EXTM3U") {
		return nil, false
	}
	VODCacheSet(media, mbody)
	seg := firstPlaylistURL(string(mbody), media)
	if seg == "" {
		return nil, false
	}
	shead, ok := fetch(seg, 8192, 12*time.Second)
	if !ok || !segmentBytesPlayable(shead) {
		return nil, false
	}
	return variants, true
}

// kaaParseVariants lists every quality rendition in a master playlist,
// resolving relative and protocol-relative URIs like a player would.
func kaaParseVariants(body, masterURL string) []kaaVariant {
	base, err := url.Parse(masterURL)
	if err != nil {
		return nil
	}
	lines := strings.Split(body, "\n")
	var out []kaaVariant
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "#EXT-X-STREAM-INF") || i+1 >= len(lines) {
			continue
		}
		uri := strings.TrimSpace(lines[i+1])
		if uri == "" || strings.HasPrefix(uri, "#") {
			continue
		}
		abs := uri
		if ref, err := url.Parse(uri); err == nil {
			abs = base.ResolveReference(ref).String()
		}
		bw := 0
		if m := regexp.MustCompile(`BANDWIDTH=(\d+)`).FindStringSubmatch(line); len(m) == 2 {
			bw, _ = strconv.Atoi(m[1])
		}
		quality := "auto"
		if m := regexp.MustCompile(`RESOLUTION=\d+x(\d+)`).FindStringSubmatch(line); len(m) == 2 {
			quality = m[1] + "p"
		}
		out = append(out, kaaVariant{url: abs, quality: quality, bandwidth: bw})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].bandwidth > out[j].bandwidth })
	return out
}
