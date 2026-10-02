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
// LANGUAGE RULE (operator, strict per-lang): each lang reads its own
// listing — sub resolves ja-JP pages, dub resolves en-US pages, never the
// other. When the dub page has no players, no dub server is listed even if
// sub resolves (observed: Noblesse ep1 has a ja-JP CatStream embed but the
// en-US page carries zero embeds). The resolve cache is keyed WITH lang so
// a cached sub result can never leak into a dub listing or vice versa.
// Dub sources still carry the SUB page's subtitle files (subtitle rule).
//
// Observed live 2026-10-01: ja-JP ep1 slug 67fd53 (players) vs en-US ep1
// slug ae903e (no embeds) for the same show.
//
// AUDIO RULE: sources are always MASTER playlists, never quality variants.
// Variant playlists carry zero #EXT-X-MEDIA lines (verified), so a player
// loading a variant directly gets whatever audio is muxed/default (observed:
// lang=sub playing dub on anilist 130298). The master carries both AUDIO
// renditions, and the media proxy strips the wrong one per the request's
// al=sub/dub parameter (see stripAudioRenditions) — the player then ABRs
// natively across the master's variants with the correct track.
//
// SERVER NAMES (operator): kaa servers are named by position — nico, robin,
// D'Luff, then the same crew scheme — never raw player names.
//
// SUBTITLE RULE (operator): Sora (animex) dub sources carry the Nico
// subtitle files, enforced centrally by Manager.withDubSubtitles — every
// other provider's dub keeps its default subtitles.
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
	// Master playlists: absolute AND protocol-relative (CatStream embeds
	// "//bl.krussdomi.com/.../master.m3u8" — the reference scraper matches
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

func (n kaaEpNum) equals(ep int) bool { return float64(n) == float64(ep) }

func (n kaaEpNum) key() string { return strconv.FormatFloat(float64(n), 'f', -1, 64) }

func NewKaaProvider(log zerolog.Logger, kaaBase, anilistURL string) *KaaProvider {
	if kaaBase == "" {
		kaaBase = kaaAPIBase
	}
	if anilistURL == "" {
		anilistURL = "https://graphql.aniraku.tech"
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

func (p *KaaProvider) listEpisodes(ctx context.Context, slug, lang string, firstEp int) ([]kaaEpisode, error) {
	var first struct {
		Result []kaaEpisode `json:"result"`
		Pages  []struct {
			// Page markers are usually ints but kaa.lt sometimes emits
			// fractional episode numbers (observed 14.5 for specials) —
			// json.Number keeps the unmarshal from killing the resolve.
			Eps []json.Number `json:"eps"`
		} `json:"pages"`
	}
	u := fmt.Sprintf("%s/api/show/%s/episodes?ep=%d&lang=%s", p.kaaBase, slug, firstEp, lang)
	if err := p.doJSON(ctx, http.MethodGet, u, nil, p.kaaHeaders(p.kaaBase+"/"), &first); err != nil {
		return nil, err
	}
	eps := append([]kaaEpisode(nil), first.Result...)
	seen := map[string]bool{}
	for _, e := range eps {
		seen[e.Number.key()] = true
	}
	for _, page := range first.Pages {
		if len(page.Eps) == 0 {
			continue
		}
		// Page 1 is already in hand (first.Result above) — only fetch
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
// request the en-US listing, and there is no cross-lang fallback — when the
// dub page has no players, no dub server is listed even if sub resolves.
// The resolve cache is keyed WITH lang for the same reason: a cached sub
// result must never leak into a dub listing or vice versa.
//
// Dub sources still carry the SUB (ja-JP) page's subtitle files (operator
// subtitle rule), enforced centrally by withDubSubtitles — that is a
// targeted subtitle fetch, not a manifest share.
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
		return nil, err
	}
	if sr == nil || len(sr.Sources) == 0 {
		return nil, fmt.Errorf("kaa: no sources for episode %d (%s)", episode, langKey)
	}
	p.storeResolved(slug, episode, langKey, sr)
	return sr, nil
}

func kaaLangParam(lang string) string {
	if strings.EqualFold(lang, "dub") {
		return "en-US"
	}
	return "ja-JP"
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

// kaaServerNames names kaa servers by position (operator scheme).
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
	eps, err := p.listEpisodes(ctx, slug, kaaLang, episode)
	if err != nil {
		return nil, err
	}
	var match *kaaEpisode
	for i := range eps {
		if eps[i].Number.equals(episode) {
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
	// Every playable player becomes its own server (named by position) —
	// first-win would hide working mirrors.
	type hit struct {
		master string
		vtts   []string
	}
	var hits []hit
	var lastErr error
	for _, pl := range players {
		// DASH-only arms carry no m3u8 — skip without an upstream call.
		if strings.Contains(strings.ToLower(pl.src), "type=dash") {
			continue
		}
		master, vtts, err := p.resolvePlayerMaster(ctx, pl, watchURL)
		if err != nil {
			lastErr = err
			continue
		}
		hits = append(hits, hit{master: master, vtts: vtts})
	}
	if len(hits) == 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("kaa: no playable player for episode %d", episode)
	}
	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", reqLang).Int("players", len(hits)).Msg("kaa resolved")

	headers := map[string]string{
		"Referer": kaaKrussRef,
		"Origin":  kaaKrussOrigin,
	}
	// Subtitles are the resolving page's own player-page files. The
	// operator rule (dub sources carry the SUB page's files) is enforced
	// centrally by Manager.withDubSubtitles, which matches per upstream
	// URL — an in-provider fetch here would only duplicate that work.
	sr := &SourceResult{Headers: headers}
	for i, h := range hits {
		var subs []core.Subtitle
		for _, s := range h.vtts {
			p.learnURLHost(s)
			subs = append(subs, kaaSubtitleTrack(s, reqLang))
		}
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

// kaaSubNameRe extracts the language code from a subtitle filename
// (309567_en.srt, 60596_th.srt, 278072_zh-Hans.srt).
var kaaSubNameRe = regexp.MustCompile(`[_-]([A-Za-z]{2,8}(?:-[A-Za-z0-9]+)?)\.(?:srt|vtt)(?:[?#]|$)`)

// kaaLangNames maps filename codes to display labels (English-name style
// like the other providers' labels).
var kaaLangNames = map[string]string{
	"en": "English", "eng": "English",
	"ar": "Arabic", "ara": "Arabic",
	"th": "Thai", "tha": "Thai",
	"vi": "Vietnamese", "vie": "Vietnamese",
	"id": "Indonesian", "ind": "Indonesian",
	"ms": "Malay", "may": "Malay",
	"zh": "Chinese", "zh-hans": "Chinese", "zh-hant": "Chinese Traditional",
	"fr": "French", "fre": "French", "fra": "French",
	"de": "German", "ger": "German", "deu": "German",
	"it": "Italian", "ita": "Italian",
	"pt": "Portuguese", "por": "Portuguese",
	"ru": "Russian", "rus": "Russian",
	"es": "Spanish", "spa": "Spanish",
	"hi": "Hindi", "hin": "Hindi",
	"ja": "Japanese", "jpn": "Japanese",
	"ko": "Korean", "kor": "Korean",
	"nl": "Dutch", "nld": "Dutch",
	"tr": "Turkish", "tur": "Turkish",
	"pl": "Polish", "pol": "Polish",
	"uk": "Ukrainian", "ukr": "Ukrainian",
	"ro": "Romanian", "ron": "Romanian",
	"hu": "Hungarian", "hun": "Hungarian",
	"cs": "Czech", "ces": "Czech",
	"el": "Greek", "ell": "Greek",
	"he": "Hebrew", "heb": "Hebrew",
	"sv": "Swedish", "swe": "Swedish",
	"da": "Danish", "dan": "Danish",
	"fi": "Finnish", "fin": "Finnish",
	"no": "Norwegian", "nor": "Norwegian",
}

// kaaSubtitleTrack labels one subtitle file with its real language,
// parsed from the filename code (309567_en.srt, 60596_th.srt,
// 278072_zh-Hans.srt). Files without a known code (bare-hash .vtt)
// keep the request lang as a fallback tag.
func kaaSubtitleTrack(rawURL, fallback string) core.Subtitle {
	base := rawURL
	if i := strings.IndexAny(base, "?#"); i >= 0 {
		base = base[:i]
	}
	if m := kaaSubNameRe.FindStringSubmatch(base); m != nil {
		if name, ok := kaaLangNames[strings.ToLower(m[1])]; ok {
			return core.Subtitle{URL: rawURL, Lang: strings.ToLower(m[1]), Label: name}
		}
	}
	return core.Subtitle{URL: rawURL, Lang: fallback, Label: fallback}
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
// .srt, raw as the operator rule requires). Thumbnail tracks
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
// playlist URL plus the player page's subtitle links.
func (p *KaaProvider) resolvePlayerMaster(ctx context.Context, pl kaaPlayer, watchURL string) (string, []string, error) {
	raw, err := p.doText(ctx, pl.src, p.kaaHeaders(watchURL), 1<<20)
	if err != nil {
		return "", nil, err
	}
	txt := kaaUnescape(raw)
	masters := kaaFindMasters(txt)
	if len(masters) == 0 {
		return "", nil, fmt.Errorf("kaa: player %q has no m3u8", pl.name)
	}
	vtts := kaaFindSubtitles(txt)
	var lastErr error
	for _, master := range masters {
		if _, ok := p.probeKaaMaster(ctx, master); !ok {
			lastErr = fmt.Errorf("kaa: master probe failed")
			continue
		}
		return master, vtts, nil
	}
	if lastErr != nil {
		return "", nil, lastErr
	}
	return "", nil, fmt.Errorf("kaa: player %q unplayable", pl.name)
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
