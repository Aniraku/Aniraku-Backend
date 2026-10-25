package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
// Verified live against the production egress:
//   - kaa.lt search / episodes / watch pages: 200, no datacenter block.
//   - krussdomi master + quality playlists: 200 (direct and via media proxy).
//   - krussdomi segment bytes require `Origin: https://krussdomi.com`
//     (Referer alone 403s, every UA 404/403s without Origin). The provider
//     therefore always ships Referer+Origin in result Headers so the media
//     proxy forwards them (applyProxyQueryHeaders passes both through).
//
// Language is strict per-lang: each lang reads its own listing, sub
// resolves ja-JP pages, dub resolves en-US pages, never the other. When
// the dub page has no players, no dub server is listed even if sub
// resolves (observed: Noblesse ep1 has a ja-JP CatStream embed but the
// en-US page carries zero embeds). The resolve cache is keyed WITH lang
// so a cached sub result can never leak into a dub listing or vice
// versa; dub sources still carry the SUB page's subtitle files (see
// Manager.withDubSubtitles). Dual-language shows can list two slugs for
// the same episode: the ja-JP listing carries players while the en-US
// listing carries none.
//
// Audio: sources are always MASTER playlists, never quality variants.
// Variant playlists carry zero #EXT-X-MEDIA lines (verified), so a player
// loading a variant directly gets whatever audio is muxed/default
// (observed: lang=sub playing dub on anilist 130298). The master carries
// both AUDIO renditions, and the media proxy strips the wrong one per
// the request's al=sub/dub parameter (see stripAudioRenditions); the
// player then ABRs natively across the master's variants with the
// correct track.
//
// Server names: kaa servers are named by position (nico, robin, D'Luff,
// then the same crew scheme), never raw player names.
const (
	kaaAPIBase     = "https://kaa.lt"
	kaaKrussOrigin = "https://krussdomi.com"
	kaaKrussRef    = "https://krussdomi.com/"

	kaaSlugTTL           = 24 * time.Hour
	kaaResolveTTL        = 10 * time.Minute
	maxKaaSlugEntries    = 500
	maxKaaResolveEntries = 500

	// kaaListingBudget: per-call ceiling for the episode-listing fetch.
	// The same listing page measured 0.9s, 6.2s, then 11.2s across draws,
	// so kaa.lt's loading is wildly variable, and a draw slower than 6s
	// can never complete the resolve under the collector budget (watch
	// page + players still need ~4s). Capping the draw there converts a
	// guaranteed wall-burn into a fast fail; any round that could have
	// succeeded inside the budget (listing + watch + players all within
	// it) is unaffected.
	kaaListingBudget = 6 * time.Second
)

var (
	kaaPlayerRe = regexp.MustCompile(`\{\s*name:\s*"([^"]+)"\s*,\s*shortName:\s*"([^"]+)"\s*,\s*src:\s*"([^"]+)"\s*\}`)
	kaaCatRe    = regexp.MustCompile(`https?://[a-zA-Z0-9.\-]+/cat-player/player\?[^"'\\s]+`)
	// Master playlists: absolute AND protocol-relative (CatStream embeds
	// "//bl.krussdomi.com/.../master.m3u8", the reference scraper matches
	// both via (?:https?:)?// and normalizes with _fix_url).
	kaaM3U8Re = regexp.MustCompile(`(?:https?:)?//[^\s"'<>\\&]+\.m3u8[^\s"'<>\\&]*`)
	// Subtitle files: .vtt AND .srt (CatStream ships srt only), absolute
	// or protocol-relative. Thumbnail tracks (preview-*.vtt) are filtered
	// by kaaFindSubtitles, not by this pattern.
	kaaSubRe = regexp.MustCompile(`(?:https?:)?//[^\s"'<>\\&]+\.(?:vtt|srt)[^\s"'<>\\&]*`)
)

type KaaProvider struct {
	log        zerolog.Logger
	client     *http.Client
	kaaBase    string
	anilistURL string
	learnHost  func(host string)

	mu       sync.Mutex
	slugs    map[int]*kaaSlugEntry
	bgWarm   map[string]bool
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
	lang    string // strict per-lang: "sub" or "dub"
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
	Number kaaEpNum `json:"episode_number"`
	Slug   string   `json:"slug"`
	Title  string   `json:"title"`
}

// kaaEpNum tolerates fractional episode numbers (observed 1004.5 on long
// runners, 14.5 specials) as JSON numbers or strings. Integer requests
// match only whole values; fractional entries are unaddressable through
// the int-episode API and never match.
type kaaEpNum float64

func (n *kaaEpNum) UnmarshalJSON(b []byte) error {
	var f float64
	if err := json.Unmarshal(b, &f); err == nil {
		*n = kaaEpNum(f)
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return err
	}
	*n = kaaEpNum(f)
	return nil
}

func (n kaaEpNum) matches(ep int) bool { return float64(n) == float64(ep) }

func (n kaaEpNum) key() string { return strconv.FormatFloat(float64(n), 'f', -1, 64) }

func NewKaaProvider(log zerolog.Logger, kaaBase, anilistURL string) *KaaProvider {
	if kaaBase == "" {
		kaaBase = kaaAPIBase
	}
	if anilistURL == "" {
		anilistURL = core.AnilistGraphQLEndpoint()
	}
	return &KaaProvider{
		log: log,
		// DisableKeepAlives: from this egress a reused connection can go
		// permanently silent (krussdomi; fresh-connection probes to the
		// same hosts answered in under a second while the Go client sat
		// out its full budget on a keep-alive). kaa fetches are few per
		// round, so per-request dials cost ~30ms and buy reliable reads.
		client:     kaaFreshConnClient(),
		kaaBase:    strings.TrimRight(kaaBase, "/"),
		anilistURL: anilistURL,
		slugs:      make(map[int]*kaaSlugEntry),
		bgWarm:     make(map[string]bool),
		resolved:   make(map[kaaResolveKey]*kaaResolvedEntry),
	}
}

// bgRetryResolve launches at most one background resolve per (slug, ep,
// lang) after a failed round. The collector budget bounds what a round
// may attempt, not what the provider may attempt in total: kaa.lt listing
// draws range 0.9s to 11s for the same page, and krussdomi can blackhole
// keep-alives, so a draw that went badly gets no second chance inside its
// budget. This retry runs detached with a fresh 20s budget and fills the
// 10-minute resolve cache the moment the upstream cooperates; every later
// round then serves the resolve from the cache at full strength, so kaa
// does not keep vanishing because one draw was slow. In-flight retries
// dedupe per key; the leak is bounded (one per failing key, 20s cap).
func (p *KaaProvider) bgRetryResolve(anilistID int, slug string, episode int, langKey, reqLang string) {
	key := fmt.Sprintf("%s|%d|%s", slug, episode, langKey)
	p.mu.Lock()
	if p.bgWarm[key] {
		p.mu.Unlock()
		return
	}
	p.bgWarm[key] = true
	p.mu.Unlock()

	go func() {
		defer func() {
			p.mu.Lock()
			delete(p.bgWarm, key)
			p.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		sr, err := p.resolveEpisode(ctx, anilistID, slug, episode, kaaLangParam(reqLang), reqLang)
		if err != nil || sr == nil || len(sr.Sources) == 0 {
			p.log.Info().Str("slug", slug).Int("episode", episode).Str("lang", langKey).
				Err(err).Msg("kaa: background retry failed")
			return
		}
		p.storeResolved(slug, episode, langKey, sr)
		p.log.Info().Str("slug", slug).Int("episode", episode).Str("lang", langKey).
			Int("players", len(sr.Sources)).Msg("kaa: background retry cached")
	}()
}

// kaaFreshConnClient builds the guarded client with connection reuse off.
func kaaFreshConnClient() *http.Client {
	tr := netguard.NewTransport().Clone()
	tr.DisableKeepAlives = true
	return &http.Client{Timeout: 45 * time.Second, Transport: tr}
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
	seenEp := map[int]bool{}
	for _, e := range eps {
		if f := float64(e.Number); f != float64(int(f)) || seenEp[int(f)] {
			continue // fractional entries are unaddressable via the API
		} else {
			seenEp[int(f)] = true
		}
		out = append(out, Episode{Number: int(float64(e.Number)), Title: e.Title})
	}
	return out, nil
}

// kaaPageMark is one pagination marker from an anchored listing page:
// kaa.lt flags every OTHER page by its leading episode number.
type kaaPageMark struct {
	// Page markers are usually ints but kaa.lt sometimes emits
	// fractional episode numbers (observed 14.5 for specials),
	// json.Number keeps the unmarshal from killing the resolve.
	Eps []json.Number `json:"eps"`
}

// fetchEpisodePage fetches ONE anchored page of a show's episode listing.
func (p *KaaProvider) fetchEpisodePage(ctx context.Context, slug, lang, anchor string) ([]kaaEpisode, []kaaPageMark, error) {
	var page struct {
		Result []kaaEpisode  `json:"result"`
		Pages  []kaaPageMark `json:"pages"`
	}
	u := fmt.Sprintf("%s/api/show/%s/episodes?ep=%s&lang=%s", p.kaaBase, slug, anchor, lang)
	lsCtx, cancel := context.WithTimeout(ctx, kaaListingBudget)
	defer cancel()
	if err := p.doJSON(lsCtx, http.MethodGet, u, nil, p.kaaHeaders(p.kaaBase+"/"), &page); err != nil {
		return nil, nil, err
	}
	return page.Result, page.Pages, nil
}

func (p *KaaProvider) listEpisodes(ctx context.Context, slug, lang string, firstEp int) ([]kaaEpisode, error) {
	firstResult, pages, err := p.fetchEpisodePage(ctx, slug, lang, strconv.Itoa(firstEp))
	if err != nil {
		return nil, err
	}
	eps := append([]kaaEpisode(nil), firstResult...)
	seen := map[string]bool{}
	for _, e := range eps {
		seen[e.Number.key()] = true
	}
	for _, page := range pages {
		if len(page.Eps) == 0 {
			continue
		}
		// Page 1 is already in hand (first.Result above), only fetch
		// pages whose leading episode we have not seen.
		if seen[page.Eps[0].String()] {
			continue
		}
		pu := fmt.Sprintf("%s/api/show/%s/episodes?ep=%s&lang=%s", p.kaaBase, slug, page.Eps[0].String(), lang)
		var pd struct {
			Result []kaaEpisode `json:"result"`
		}
		if err := p.doJSON(ctx, http.MethodGet, pu, nil, p.kaaHeaders(p.kaaBase+"/"), &pd); err != nil {
			return nil, err
		}
		for _, e := range pd.Result {
			if k := e.Number.key(); !seen[k] {
				eps = append(eps, e)
				seen[k] = true
			}
		}
	}
	sort.Slice(eps, func(i, j int) bool { return eps[i].Number < eps[j].Number })
	return eps, nil
}

// FindEpisodeSource resolves one episode for EXACTLY the requested lang
// (strict per-lang rule): a sub request reads the ja-JP listing, a dub
// request the en-US listing, and there is no cross-lang fallback, when the
// dub page has no players, no dub server is listed even if sub resolves.
// The resolve cache is keyed WITH lang for the same reason: a cached sub
// result must never leak into a dub listing or vice versa.
//
// Dub sources still carry the ja-JP (sub) page's subtitle files, merged
// in by Manager.withDubSubtitles. That is a targeted subtitle fetch by
// URL, not a manifest share.
func (p *KaaProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("kaa: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}

	slug, _, err := p.resolveSlug(ctx, id)
	if err != nil {
		return nil, err
	}
	if got := p.loadResolved(slug, episode, langKey); got != nil {
		return got, nil
	}
	sr, err := p.resolveEpisode(ctx, id, slug, episode, kaaLangParam(lang), lang)
	if err != nil {
		// The miss schedules one background resolve: any moment the
		// upstreams cooperate fills the resolve cache, and rounds after
		// that serve kaa fully, a slow draw never benches it for a TTL.
		p.bgRetryResolve(id, slug, episode, langKey, lang)
		return nil, err
	}
	if sr == nil || len(sr.Sources) == 0 {
		p.bgRetryResolve(id, slug, episode, langKey, lang)
		return nil, fmt.Errorf("kaa: no sources for episode %d (%s)", episode, langKey)
	}
	p.storeResolved(slug, episode, langKey, sr)
	return sr, nil
}

// kaaLangParam returns kaa.lt's listing-language code for a request lang.
func kaaLangParam(lang string) string {
	if strings.EqualFold(lang, "dub") {
		return "en-US"
	}
	return "ja-JP"
}

// deadlineShapedErr reports whether an upstream failure was a
// budget/deadline cut rather than a plain network error: deadline-shaped
// failures must not be retried on the same context (the retry cannot find
// the missing time).
func deadlineShapedErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func (p *KaaProvider) loadResolved(slug string, episode int, lang string) *SourceResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[kaaResolveKey{slug: slug, episode: episode, lang: lang}]
	if !ok || time.Since(e.fetched) > kaaResolveTTL {
		return nil
	}
	// Deep copy: callers (quality filter, subtitle merge, proxy wrap) must
	// never mutate the cached entry through the shared backing array.
	return cloneSourceResult(e.result)
}

func (p *KaaProvider) storeResolved(slug string, episode int, lang string, sr *SourceResult) {
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
	p.resolved[kaaResolveKey{slug: slug, episode: episode, lang: lang}] = &kaaResolvedEntry{result: cloneSourceResult(sr), fetched: now}
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

// kaaServerNames names kaa servers by position.
func kaaServerName(i int) string {
	if i >= 0 && i < len(kaaServerNames) {
		return kaaServerNames[i]
	}
	return fmt.Sprintf("kaa-%d", i+1)
}

var kaaServerNames = []string{
	"Nico", "robin", "D'Luff",
	"zoro", "sanji", "nami", "usopp", "chopper", "franky", "brook", "jimbei",
}

func (p *KaaProvider) resolveEpisode(ctx context.Context, id int, slug string, episode int, kaaLang, reqLang string) (*SourceResult, error) {
	var match *kaaEpisode
	// Fast path: the anchored page usually carries the requested episode
	// itself. Long runners paginate deep (One Piece = 1180 listed eps) and
	// listEpisodes' full sequential walk takes 10-19s, over the 10s
	// collector budget, which cut every One Piece round even with episode 1
	// one hop away (One Piece rounds once lost all Kaa servers this way). Only the
	// rare not-on-page case falls back to the full walk.
	//
	// A deadline-shaped failure is NOT retried: the listing budget (6s)
	// already consumed most of the collector budget, and kaa.lt's flaky
	// draws resolve by round, not by instant retry, retrying here just
	// burned the wall (measured: 6s fail + fresh 6s walk = the 10s cut).
	if firstPage, _, fpErr := p.fetchEpisodePage(ctx, slug, kaaLang, strconv.Itoa(episode)); fpErr == nil {
		for i := range firstPage {
			if firstPage[i].Number.matches(episode) && firstPage[i].Slug != "" {
				match = &firstPage[i]
				break
			}
		}
	} else if !deadlineShapedErr(fpErr) {
		eps, err := p.listEpisodes(ctx, slug, kaaLang, episode)
		if err != nil {
			return nil, err
		}
		for i := range eps {
			if eps[i].Number.matches(episode) {
				match = &eps[i]
				break
			}
		}
	}
	if match == nil || match.Slug == "" {
		return nil, fmt.Errorf("kaa: episode %d not listed (%s)", episode, kaaLang)
	}
	watchURL := fmt.Sprintf("%s/%s/ep-%d-%s", p.kaaBase, slug, episode, match.Slug)

	raw, watchErr := p.doText(ctx, watchURL, p.kaaHeaders(p.kaaBase+"/"), 1<<20)
	players := kaaExtractPlayers(raw)
	if watchErr != nil || len(players) == 0 {
		// Episode slugs rotate on kaa.lt. A cached listing can therefore
		// return a valid 200 watch page with no players while the fresh
		// listing points at the playable slug. Redraw the exact language
		// listing once before declaring the provider unavailable.
		if freshPage, _, freshErr := p.fetchEpisodePage(ctx, slug, kaaLang, strconv.Itoa(episode)); freshErr == nil {
			for i := range freshPage {
				if freshPage[i].Number.matches(episode) && freshPage[i].Slug != "" && freshPage[i].Slug != match.Slug {
					match = &freshPage[i]
					watchURL = fmt.Sprintf("%s/%s/ep-%d-%s", p.kaaBase, slug, episode, match.Slug)
					raw, watchErr = p.doText(ctx, watchURL, p.kaaHeaders(p.kaaBase+"/"), 1<<20)
					players = kaaExtractPlayers(raw)
					break
				}
			}
		}
	}
	if watchErr != nil {
		return nil, watchErr
	}
	if len(players) == 0 {
		return nil, fmt.Errorf("kaa: no embedded players on %s", watchURL)
	}
	// Every playable player becomes its own server (named by position),
	// first-win would hide working mirrors. Players are independent
	// upstream embeds: resolving them sequentially let one tarpitted host
	// stall the rest (observed 30s+ kaa times), so up to 3 resolve
	// concurrently, each under its own deadline. Assembly stays in listed
	// order so server names never shift.
	type hit struct {
		master string
		subs   []core.Subtitle
		ok     bool
	}
	hits := make([]hit, len(players))
	var pWg sync.WaitGroup
	pSem := make(chan struct{}, 3)
	for i, pl := range players {
		// DASH-only arms carry no m3u8, skip without an upstream call.
		if strings.Contains(strings.ToLower(pl.src), "type=dash") {
			continue
		}
		if ctx.Err() != nil {
			break
		}
		pWg.Add(1)
		go func(i int, pl kaaPlayer) {
			defer pWg.Done()
			select {
			case pSem <- struct{}{}:
				defer func() { <-pSem }()
			case <-ctx.Done():
				return
			}
			plCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			master, subs, err := p.resolvePlayerMaster(plCtx, pl, watchURL)
			if err != nil {
				p.log.Warn().Str("player", pl.name).Str("src", pl.src).Err(err).
					Msg("kaa: player resolve failed, skipping")
				return
			}
			hits[i] = hit{master: master, subs: subs, ok: true}
		}(i, pl)
	}
	pWg.Wait()
	// Collector-budget cut mid-fan-out does NOT discard what resolved:
	// players are independent, and completed ones hold complete streams
	// (master + subtitles + headers). The expired-budget scenario, one player slower than the wall dropped the other two
	// playable ones, must never erase usable sources. Dead-ctx failed
	// players are simply absent from hits.
	var playable []hit
	for _, h := range hits {
		if h.ok {
			playable = append(playable, h)
		}
	}
	if len(playable) == 0 {
		return nil, fmt.Errorf("kaa: no playable player for episode %d", episode)
	}
	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", reqLang).Int("players", len(playable)).Msg("kaa resolved")

	headers := map[string]string{
		"Referer": kaaKrussRef,
		"Origin":  kaaKrussOrigin,
	}
	// Subtitles are the resolving page's own player-page files. Dub
	// sources carry the SUB page's files; that pairing is enforced
	// centrally by Manager.withDubSubtitles, which matches per upstream
	// URL, an in-provider fetch here would only duplicate that work.
	sr := &SourceResult{Headers: headers}
	for i, h := range playable {
		subs := h.subs
		p.learnURLHost(h.master)
		sr.Sources = append(sr.Sources, core.Source{
			URL:          h.master,
			Type:         "hls",
			Quality:      "auto",
			Verification: "proxy",
			Subtitles:    subs,
		})
		sr.ServerNames = append(sr.ServerNames, kaaServerName(i))
	}
	return sr, nil
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

// kaaSubtitleTrack labels one subtitle file via the shared resolver.
// Krussdomi encodes the language in the filename (309567_en.srt,
// 60596_th.srt, 278072_zh-Hans.srt); filename-coded files resolve from
// that. Bare-hash .vtt files appear only through the player island's
// metadata (see kaaParseSubtitleMeta), which carries the real language,
// so this fallback keeps the en hint only for a page that loses it.
func kaaSubtitleTrack(rawURL string) core.Subtitle {
	return buildSubtitle(rawURL, "", "", "en")
}

// kaaSubtitleMeta is one subtitle entry decoded from the player island's
// props block: file URL, language code, native display name.
type kaaSubtitleMeta struct {
	src  string
	lang string
	name string
}

// kaaIslandPropsRe captures one props attribute from a cat-player page.
// The attribute is HTML-escaped JSON, so its quotes are &quot; escapes
// and the surrounding " characters stay attribute bounds.
var kaaIslandPropsRe = regexp.MustCompile(`props="([^"]+)"`)

// kaaParseSubtitleMeta pulls the subtitle entries out of the player
// page's island props attributes. It returns tracks in page order,
// deduped by URL; an empty result means the page carries no island
// metadata and the caller falls back to the bare-URL scan.
//
// The cat-player page embeds its playback config as an Astro island: a
// props attribute holding a JSON blob in Astro's tagged form, plain
// values wrapped as [0, value] and arrays as [1, elements] (objects keep
// their shape with tagged field values). The subtitles entry carries the
// real metadata that a URL-only scan loses: language code (two-letter
// en/th/vi on some players, three-letter eng/ger/spa on others) and the
// native display name; src is required, format is optional and where
// present must be srt or vtt. Thumbnails live in a separate key and
// never show up here.
func kaaParseSubtitleMeta(raw string) []kaaSubtitleMeta {
	var out []kaaSubtitleMeta
	seen := map[string]bool{}
	for _, m := range kaaIslandPropsRe.FindAllStringSubmatch(raw, -1) {
		var blob any
		if err := json.Unmarshal([]byte(html.UnescapeString(m[1])), &blob); err != nil {
			continue
		}
		obj, ok := kaaDecodeAstro(blob).(map[string]any)
		if !ok {
			continue
		}
		entries, ok := obj["subtitles"].([]any)
		if !ok {
			continue
		}
		for _, e := range entries {
			track, ok := e.(map[string]any)
			if !ok {
				continue
			}
			if format, ok := track["format"].(string); ok && format != "srt" && format != "vtt" {
				continue
			}
			src, _ := track["src"].(string)
			if src == "" {
				continue
			}
			src = kaaFixURL(src)
			if seen[src] {
				continue
			}
			seen[src] = true
			lang, _ := track["language"].(string)
			name, _ := track["name"].(string)
			out = append(out, kaaSubtitleMeta{src: src, lang: lang, name: name})
		}
	}
	return out
}

// kaaDecodeAstro unwraps Astro's serialization tags: [0, v] is a plain
// value, [1, elems] an array, anything else keeps its JSON shape with
// every field or element decoded again.
func kaaDecodeAstro(v any) any {
	switch t := v.(type) {
	case []any:
		if len(t) == 2 {
			tag, isTag := t[0].(float64)
			if isTag && tag == 0 {
				return kaaDecodeAstro(t[1])
			}
			if isTag && tag == 1 {
				elems, _ := t[1].([]any)
				out := make([]any, len(elems))
				for i, e := range elems {
					out[i] = kaaDecodeAstro(e)
				}
				return out
			}
		}
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = kaaDecodeAstro(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = kaaDecodeAstro(e)
		}
		return out
	default:
		return v
	}
}

// kaaFixURL mirrors the reference scraper's _fix_url: krussdomi embeds
// broken triple-slash URLs (https:///subbl...) and protocol-relative
// masters (//bl.krussdomi.com/.../master.m3u8).
func kaaFixURL(u string) string {
	u = strings.ReplaceAll(u, "https:///", "https://")
	u = strings.ReplaceAll(u, "http:///", "http://")
	if strings.HasPrefix(u, "//") {
		u = "https:" + u
	}
	return u
}

// kaaFindMasters extracts unique normalized master playlist URLs.
func kaaFindMasters(txt string) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range kaaM3U8Re.FindAllString(txt, -1) {
		if u := kaaFixURL(m); !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	return out
}

// kaaFindSubtitles extracts unique normalized subtitle file URLs (.vtt and
// .srt, kept raw). Thumbnail tracks
// (preview-*.vtt, served under the player's "thumbnails" key) are not
// subtitles and are dropped.
func kaaFindSubtitles(txt string) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range kaaSubRe.FindAllString(txt, -1) {
		u := kaaFixURL(m)
		if strings.Contains(strings.ToLower(u), "preview") {
			continue
		}
		if !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	return out
}

// resolvePlayerMaster verifies one embedded player and returns its master
// playlist URL plus the player page's subtitle tracks.
func (p *KaaProvider) resolvePlayerMaster(ctx context.Context, pl kaaPlayer, watchURL string) (string, []core.Subtitle, error) {
	raw, err := p.doText(ctx, pl.src, p.kaaHeaders(watchURL), 1<<20)
	if err != nil {
		return "", nil, err
	}
	txt := kaaUnescape(raw)
	masters := kaaFindMasters(txt)
	if len(masters) == 0 {
		return "", nil, fmt.Errorf("kaa: player %q has no m3u8", pl.name)
	}
	subs := p.playerSubtitles(raw, txt)
	var lastErr error
	for _, master := range masters {
		if _, ok := p.probeKaaMaster(ctx, master); !ok {
			lastErr = fmt.Errorf("kaa: master probe failed")
			continue
		}
		return master, subs, nil
	}
	if lastErr != nil {
		return "", nil, lastErr
	}
	return "", nil, fmt.Errorf("kaa: player %q unplayable", pl.name)
}

// playerSubtitles builds the server's subtitle list. Primary source is
// the island metadata (real language per track); when a page ships none
// of it, the bare-URL scan still finds .vtt/.srt files and resolves
// theirs from the filename code, falling back to the English hint.
func (p *KaaProvider) playerSubtitles(raw, txt string) []core.Subtitle {
	var out []core.Subtitle
	for _, m := range kaaParseSubtitleMeta(raw) {
		p.learnURLHost(m.src)
		out = append(out, buildSubtitle(m.src, m.lang, m.name, ""))
	}
	if len(out) > 0 {
		return out
	}
	for _, u := range kaaFindSubtitles(txt) {
		p.learnURLHost(u)
		out = append(out, kaaSubtitleTrack(u))
	}
	return out
}

type kaaVariant struct {
	url       string
	quality   string
	bandwidth int
}

// probeKaaMaster verifies master -> first media -> first segment with the
// Origin header krussdomi segments require, warming the VOD playback cache
// like every other provider probe. Returns the master's quality variants.
// kaaProbeFetchTimeout caps each hop at 6s: healthy krussdomi reads are
// well under 1s, and a hung hop must not consume the whole collector
// budget, the budget cuts everything anyway, but per-hop caps let the
// NEXT player's surviving probe use the freed time.
const kaaProbeFetchTimeout = 6 * time.Second

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
	mhead, ok := fetch(master, 65536, kaaProbeFetchTimeout)
	if !ok || !strings.Contains(string(mhead), "#EXTM3U") {
		return nil, false
	}
	VODCacheSet(master, mhead)
	variants := kaaParseVariants(string(mhead), master)
	if len(variants) == 0 {
		return nil, false
	}
	media := variants[0].url
	mbody, ok := fetch(media, 262144, kaaProbeFetchTimeout)
	if !ok || !strings.Contains(string(mbody), "#EXTM3U") {
		return nil, false
	}
	VODCacheSet(media, mbody)
	seg := firstPlaylistURL(string(mbody), media)
	if seg == "" {
		return nil, false
	}
	shead, ok := fetch(seg, 8192, kaaProbeFetchTimeout)
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
