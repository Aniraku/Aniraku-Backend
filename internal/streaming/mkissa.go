package streaming

// Mkissa (mkissa.to) provider — direct m3u8/mp4 sources, no embeds.
//
// Pipeline (mirrors /home/ichigoat/mkissa_scraper.py + mkissa_engine/):
//  1. AniList ID -> titles (own AniList lookup, now graphql.aniraku.tech).
//  2. Search api.mkissa.net (plain GraphQL, no auth): edges carry aniListId,
//     so the mapping is an exact ID match; title scoring is the fallback.
//     The episode list rides along (availableEpisodesDetail, string numbers
//     that may be fractional like kaa's 14.5).
//  3. Episode sources via the engine (third_party/mkissa-engine,
//     run_sources.mjs): it owns the signed-query crypto (lane key, AES-GCM)
//     that pure Go cannot re-derive from the obfuscated site JS. Go spawns
//     `bun run_sources.mjs` (node fallback) once per cache miss with the
//     request context — retries/lane refresh stay inside the engine.
//  4. Keep only direct m3u8/mp4 (extractedUrl); pure embeds are skipped
//     (operator: no embed method). Each source kind gets a stable cute
//     server name (Chuu/Mua/Kissy + spares) — 1-3 live at runtime.
//  5. Honesty probe per source (master->media->segment, VOD cache warm)
//     like every other provider; mp4 verified with a byte-range GET.
//
// Strict per-lang like kaa: sub reads translationType sub, dub reads dub;
// a missing dub episode list means no dub server. Hentai needs no gate:
// search sends allowAdult:false, so adult titles never match.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
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

const (
	mkissaAPIBase = "https://api.mkissa.net/api"
	mkissaReferer = "https://mkissa.to/"
	mkissaOrigin  = "https://mkissa.to"

	mkissaSlugTTL           = 24 * time.Hour
	mkissaResolveTTL        = 30 * time.Minute
	maxMkissaSlugEntries    = 500
	maxMkissaResolveEntries = 500
	mkissaEngineTimeout     = 40 * time.Second
)

// mkissaServerNames maps engine source kinds to stable cute server names.
// Only kinds that extract to direct m3u8/mp4 are listed (1-3 live at
// runtime); pure-embed kinds never surface, so they need no names.
var mkissaServerNames = map[string]string{
	"default": "Chuu",   // wixmp direct m3u8
	"uv-mp4":  "Mua",    // allanime clock HLS
	"mp4":     "Kissy",  // mp4upload mp4
	"ss-hls":  "Smooch", // streamsb
	"sl-mp4":  "Peck",   // streamlare
	"ok":      "Xoxo",   // ok.ru
}

type MkissaProvider struct {
	log        zerolog.Logger
	client     *http.Client
	apiBase    string
	anilistURL string
	learnHost  func(host string)

	// engineBin/engineScript override the bun+script auto-resolve
	// (tests point these at a stub).
	engineBin    string
	engineScript string

	mu       sync.Mutex
	slugs    map[string]*mkissaSlugEntry
	resolved map[mkissaResolveKey]*mkissaResolvedEntry
	daemon   *mkissaDaemon
}

type mkissaSlugEntry struct {
	showID  string
	title   string
	fetched time.Time
}

type mkissaResolveKey struct {
	showID  string
	episode int
	lang    string // strict per-lang: "sub" or "dub"
}

type mkissaResolvedEntry struct {
	result  *SourceResult
	fetched time.Time
}

type mkissaEdge struct {
	ID       string           `json:"_id"`
	Name     string           `json:"name"`
	English  string           `json:"englishName"`
	Native   string           `json:"nativeName"`
	Episodes any              `json:"availableEpisodes"`
	Detail   map[string][]any `json:"availableEpisodesDetail"`
	AniList  any              `json:"aniListId"`
}

func NewMkissaProvider(log zerolog.Logger) *MkissaProvider {
	p := &MkissaProvider{
		log:        log,
		client:     &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		apiBase:    mkissaAPIBase,
		anilistURL: "https://graphql.aniraku.tech",
		slugs:      make(map[string]*mkissaSlugEntry),
		resolved:   make(map[mkissaResolveKey]*mkissaResolvedEntry),
	}
	p.daemon = newMkissaDaemon(log, p)
	return p
}

func (p *MkissaProvider) Name() string { return "mkissa" }

func (p *MkissaProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *MkissaProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *MkissaProvider) mkissaHeaders() map[string]string {
	return map[string]string{
		"User-Agent": browserUA,
		"Referer":    mkissaReferer,
		"Origin":     mkissaOrigin,
	}
}

func (p *MkissaProvider) doJSON(ctx context.Context, payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBase, bytes.NewReader(b))
	if err != nil {
		return err
	}
	for k, v := range p.mkissaHeaders() {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mkissa: search POST -> HTTP %d", resp.StatusCode)
	}
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(rb, out)
}

// ---------------------------------------------------------------- search

const mkissaSearchQuery = `query($search:SearchInput $limit:Int $page:Int $translationType:VaildTranslationTypeEnumType $countryOrigin:VaildCountryOriginEnumType){shows(search:$search limit:$limit page:$page translationType:$translationType countryOrigin:$countryOrigin){edges{_id name englishName nativeName slugTime availableEpisodes availableEpisodesDetail aniListId __typename}}}`

func (p *MkissaProvider) searchShows(ctx context.Context, query, mode string) ([]mkissaEdge, error) {
	var out struct {
		Data struct {
			Shows struct {
				Edges []mkissaEdge `json:"edges"`
			} `json:"shows"`
		} `json:"data"`
	}
	err := p.doJSON(ctx, map[string]any{
		"query": mkissaSearchQuery,
		"variables": map[string]any{
			"search":          map[string]any{"allowAdult": false, "allowUnknown": false, "query": query},
			"limit":           40,
			"page":            1,
			"translationType": mode,
			"countryOrigin":   "ALL",
		},
	}, &out)
	if err != nil {
		return nil, err
	}
	return out.Data.Shows.Edges, nil
}

func mkissaAniListID(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strconv.Itoa(int(t))
	case json.Number:
		return strings.TrimSpace(t.String())
	}
	return ""
}

func mkissaEpString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		if strings.TrimSpace(t) == "" {
			return "", false
		}
		return strings.TrimSpace(t), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	case json.Number:
		return strings.TrimSpace(t.String()), true
	}
	return "", false
}

func (p *MkissaProvider) anilistTitles(ctx context.Context, id int) ([]string, error) {
	gql := `query($id:Int){Media(id:$id,type:ANIME){title{romaji english}}}`
	var out struct {
		Data struct {
			Media struct {
				Title struct {
					Romaji  string `json:"romaji"`
					English string `json:"english"`
				} `json:"title"`
			} `json:"Media"`
		} `json:"data"`
	}
	body, _ := json.Marshal(map[string]any{"query": gql, "variables": map[string]any{"id": id}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.anilistURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", browserUA)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mkissa: anilist titles -> HTTP %d", resp.StatusCode)
	}
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, err
	}
	var titles []string
	if t := strings.TrimSpace(out.Data.Media.Title.English); t != "" {
		titles = append(titles, t)
	}
	if t := strings.TrimSpace(out.Data.Media.Title.Romaji); t != "" {
		titles = append(titles, t)
	}
	if len(titles) == 0 {
		return nil, fmt.Errorf("mkissa: anilist %d has no titles", id)
	}
	return titles, nil
}

// mkissaScoreHit ports the scraper's title scoring (exact > substring >
// token overlap, entries with episodes preferred).
func mkissaScoreHit(e mkissaEdge, titles []string) float64 {
	text := strings.ToLower(strings.Join([]string{e.Name, e.English, e.Native}, " "))
	var score float64
	for _, t := range titles {
		if t == "" {
			continue
		}
		tl := strings.ToLower(t)
		switch {
		case tl == strings.ToLower(e.English) || tl == strings.ToLower(e.Name):
			score = max(score, 100)
		case strings.Contains(text, tl):
			score = max(score, 80)
		default:
			tt := map[string]bool{}
			for _, w := range strings.FieldsFunc(tl, func(r rune) bool {
				return !(r == '_' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z'))
			}) {
				tt[w] = true
			}
			ct := map[string]bool{}
			for _, w := range strings.FieldsFunc(text, func(r rune) bool {
				return !(r == '_' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z'))
			}) {
				ct[w] = true
			}
			if len(tt) > 0 && len(ct) > 0 {
				var overlap float64
				for w := range tt {
					if ct[w] {
						overlap++
					}
				}
				score = max(score, overlap/float64(len(tt))*60)
			}
		}
	}
	for _, eps := range e.Detail {
		if len(eps) > 0 {
			score += 5
			break
		}
	}
	return score
}

// resolveShow maps an AniList ID to a mkissa showId (24h cache). Exact
// aniListId match first; title scoring fallback (mirrors the scraper).
func (p *MkissaProvider) resolveShow(ctx context.Context, id int, langKey string) (string, string, error) {
	slugKey := strconv.Itoa(id) + "/" + langKey
	p.mu.Lock()
	if e, ok := p.slugs[slugKey]; ok && time.Since(e.fetched) < mkissaSlugTTL {
		showID, title := e.showID, e.title
		p.mu.Unlock()
		return showID, title, nil
	}
	p.mu.Unlock()

	titles, err := p.anilistTitles(ctx, id)
	if err != nil {
		return "", "", err
	}
	want := strconv.Itoa(id)
	var best *mkissaEdge
	bestScore := -1.0
	for _, t := range titles {
		edges, err := p.searchShows(ctx, t, langKey)
		if err != nil || len(edges) == 0 {
			continue
		}
		for i := range edges {
			if mkissaAniListID(edges[i].AniList) == want {
				e := edges[i]
				best = &e
				bestScore = 1000
				break
			}
		}
		if bestScore == 1000 {
			break
		}
		var local *mkissaEdge
		localScore := -1.0
		for i := range edges {
			if s := mkissaScoreHit(edges[i], titles); s > localScore {
				e := edges[i]
				local, localScore = &e, s
			}
		}
		if local != nil && localScore > bestScore {
			best, bestScore = local, localScore
		}
		if best != nil {
			break
		}
	}
	if best == nil {
		return "", "", fmt.Errorf("mkissa: no show match for anilist %d", id)
	}
	title := best.English
	if title == "" {
		title = best.Name
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.slugs {
		if time.Since(e.fetched) > mkissaSlugTTL {
			delete(p.slugs, k)
		}
	}
	if len(p.slugs) >= maxMkissaSlugEntries {
		for k := range p.slugs {
			delete(p.slugs, k)
			break
		}
	}
	p.slugs[slugKey] = &mkissaSlugEntry{showID: best.ID, title: title, fetched: now}
	return best.ID, title, nil
}

// episodeStrings returns the raw episode strings for one audio track,
// sorted numerically (fractional strings like "1061.5" preserved).
func episodeStrings(e mkissaEdge, langKey string) []string {
	raw := e.Detail[langKey]
	if len(raw) == 0 && langKey == "dub" {
		return nil
	}
	var out []string
	for _, v := range raw {
		if s, ok := mkissaEpString(v); ok {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		fi, _ := strconv.ParseFloat(out[i], 64)
		fj, _ := strconv.ParseFloat(out[j], 64)
		return fi < fj
	})
	return out
}

// ---------------------------------------------------------------- episodes

// Search maps a title to mkissa showIds (debug/tooling parity).
func (p *MkissaProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	edges, err := p.searchShows(ctx, title, "sub")
	if err != nil {
		return nil, err
	}
	var out []SearchResult
	for _, e := range edges {
		t := e.English
		if t == "" {
			t = e.Name
		}
		out = append(out, SearchResult{ID: e.ID, Title: t})
	}
	return out, nil
}

// FindEpisodes lists integer episodes for a mkissa showId.
func (p *MkissaProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	edges, err := p.searchShows(ctx, providerID, "sub")
	if err != nil {
		return nil, err
	}
	seen := map[int]bool{}
	var out []Episode
	for _, e := range edges {
		for _, s := range episodeStrings(e, "sub") {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil || f != float64(int(f)) {
				continue
			}
			if !seen[int(f)] {
				seen[int(f)] = true
				out = append(out, Episode{Number: int(f)})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out, nil
}

// Engine spawn politeness: api.mkissa.net throttles per egress IP ("try
// again in N seconds"), a bucket shared by ALL our traffic. Concurrent
// collectors must never stampede it, so whole engine runs are serialized
// globally with a gap between them; rate-limit errors get one extra
// Go-side retry after N+1s (the engine already retries 3x internally with
// shorter pauses). Under deep concurrency later runs fail clean on the
// fan-out deadline and the other providers cover.
var (
	mkissaEngineMu     sync.Mutex
	mkissaEngineFreeAt time.Time
	mkissaEngineGap    = 1500 * time.Millisecond
	mkissaRateRe       = regexp.MustCompile(`try again in (\d+) seconds?`)
	mkissaRateExtra    = 1000 * time.Millisecond
	mkissaRateRetries  = 1
)

// Throttle circuit breaker: the signed-call bucket is per egress IP,
// shared by all traffic. Two consecutive throttle-class failures trip a
// cooldown that skips engine spawns entirely (resolve cache still serves);
// one success resets it. Without this the fan-out hammers a dry bucket
// forever and the provider never recovers.
var mkissaBreaker = &mkissaThrottleBreaker{cooldown: 20 * time.Minute, tripAfter: 2}

type mkissaThrottleBreaker struct {
	mu          sync.Mutex
	cooldown    time.Duration
	tripAfter   int
	consecutive int
	blockedUpTo time.Time
}

func (b *mkissaThrottleBreaker) blocked() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Now().Before(b.blockedUpTo)
}

func (b *mkissaThrottleBreaker) record(success, throttled bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case success:
		b.consecutive = 0
		b.blockedUpTo = time.Time{}
	case throttled:
		b.consecutive++
		if b.consecutive >= b.tripAfter {
			b.blockedUpTo = time.Now().Add(b.cooldown)
		}
	default:
		b.consecutive = 0
	}
}

// mkissaThrottleErr reports whether an engine error is throttle-class
// (rate message or captcha challenge — both mean "back off this IP").
func mkissaThrottleErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if mkissaRateRe.MatchString(msg) {
		return true
	}
	return strings.Contains(msg, "NEED_CAPTCHA")
}

// ---------------------------------------------------------------- engine

type mkissaEngineSource struct {
	Name         string `json:"name"`
	URL          string `json:"url"`
	ExtractedURL string `json:"extractedUrl"`
	Type         string `json:"type"`
	Priority     int    `json:"priority"`
}

type mkissaEngineOutput struct {
	ID      uint64 `json:"id"`
	ShowID  string `json:"showId"`
	Audio   string `json:"audio"`
	Results []struct {
		Episode string               `json:"episode"`
		Sources []mkissaEngineSource `json:"sources"`
		Error   string               `json:"error"`
	} `json:"results"`
	Error string `json:"error"`
	Code  any    `json:"code"`
}

// daemonCommand resolves the persistent runner: explicit override
// (tests), then MKISSA_ENGINE env, then the vendored daemon script with
// bun (node fallback).
func (p *MkissaProvider) daemonCommand() (string, string, error) {
	if p.engineBin != "" {
		return p.engineBin, p.engineScript, nil
	}
	var script string
	if env := strings.TrimSpace(os.Getenv("MKISSA_ENGINE")); env != "" {
		script = env
	} else {
		for _, c := range []string{
			"/app/third_party/mkissa-engine/mkissa_daemon.mjs",
			"third_party/mkissa-engine/mkissa_daemon.mjs",
		} {
			if st, err := os.Stat(c); err == nil && !st.IsDir() {
				script = c
				break
			}
		}
	}
	if script == "" {
		return "", "", fmt.Errorf("mkissa: engine script not found")
	}
	if bin, err := exec.LookPath("bun"); err == nil {
		return bin, script, nil
	}
	if bin, err := exec.LookPath("node"); err == nil {
		return bin, script, nil
	}
	return "", "", fmt.Errorf("mkissa: neither bun nor node on PATH")
}

// runEngine sends one request to the persistent daemon (plus the Go-side
// rate-limit retry). Whole runs stay serialized globally with a gap, and
// the lane key persists daemon-side across requests.
func (p *MkissaProvider) runEngine(ctx context.Context, showID, audio, epStr string) ([]mkissaEngineSource, error) {
	// Breaker first: never spend fan-out time (or bucket) while cooling.
	if mkissaBreaker.blocked() {
		return nil, fmt.Errorf("mkissa: throttled cooldown")
	}
	mkissaEngineMu.Lock()
	defer func() {
		mkissaEngineFreeAt = time.Now().Add(mkissaEngineGap)
		mkissaEngineMu.Unlock()
	}()
	if wait := time.Until(mkissaEngineFreeAt); wait > 0 {
		mkissaEngineMu.Unlock()
		select {
		case <-ctx.Done():
			mkissaEngineMu.Lock()
			return nil, ctx.Err()
		case <-time.After(wait):
			mkissaEngineMu.Lock()
		}
	}
	var lastErr error
	for attempt := 0; attempt <= mkissaRateRetries; attempt++ {
		srcs, err := p.daemon.Call(ctx, showID, audio, epStr)
		if err == nil {
			mkissaBreaker.record(true, false)
			return srcs, nil
		}
		lastErr = err
		throttled := mkissaThrottleErr(err)
		mkissaBreaker.record(false, throttled)
		if throttled && mkissaBreaker.blocked() {
			return nil, err
		}
		secs := 0
		if m := mkissaRateRe.FindStringSubmatch(err.Error()); len(m) == 2 {
			secs, _ = strconv.Atoi(m[1])
		}
		if secs <= 0 || attempt == mkissaRateRetries {
			return nil, err
		}
		p.log.Info().Str("showId", showID).Int("waitS", secs).Msg("mkissa: rate-limited, retrying after pause")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(secs)*time.Second + mkissaRateExtra):
		}
	}
	return nil, lastErr
}

// ---------------------------------------------------------------- resolve

func (p *MkissaProvider) loadResolved(showID string, episode int, lang string) *SourceResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[mkissaResolveKey{showID: showID, episode: episode, lang: lang}]
	if !ok || time.Since(e.fetched) > mkissaResolveTTL {
		return nil
	}
	// Deep copy: callers must never mutate the cached entry.
	return cloneSourceResult(e.result)
}

func (p *MkissaProvider) storeResolved(showID string, episode int, lang string, sr *SourceResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.resolved {
		if time.Since(e.fetched) > mkissaResolveTTL {
			delete(p.resolved, k)
		}
	}
	if len(p.resolved) >= maxMkissaResolveEntries {
		oldest := mkissaResolveKey{}
		var oldestAt time.Time
		first := true
		for k, e := range p.resolved {
			if first || e.fetched.Before(oldestAt) {
				oldest, oldestAt, first = k, e.fetched, false
			}
		}
		delete(p.resolved, oldest)
	}
	p.resolved[mkissaResolveKey{showID: showID, episode: episode, lang: lang}] = &mkissaResolvedEntry{result: cloneSourceResult(sr), fetched: now}
}

// FindEpisodeSource resolves one episode for exactly the requested lang
// (strict per-lang like kaa): a dub request with no dub episode entry
// fails instead of falling back to sub.
func (p *MkissaProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("mkissa: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	showID, showTitle, err := p.resolveShow(ctx, id, langKey)
	if err != nil {
		return nil, err
	}
	if got := p.loadResolved(showID, episode, langKey); got != nil {
		return got, nil
	}
	epStr, err := p.matchEpisode(ctx, showID, showTitle, langKey, episode)
	if err != nil {
		return nil, err
	}
	rawSources, err := p.runEngine(ctx, showID, langKey, epStr)
	if err != nil {
		return nil, err
	}
	sr := &SourceResult{Headers: map[string]string{"Referer": mkissaReferer}}
	for _, s := range rawSources {
		name, ok := mkissaServerNames[strings.ToLower(strings.TrimSpace(s.Name))]
		if !ok {
			p.log.Debug().Str("kind", s.Name).Msg("mkissa: unnamed source kind skipped")
			continue
		}
		u := strings.TrimSpace(s.ExtractedURL)
		if u == "" {
			continue // pure embed (operator: no embed method)
		}
		var typ string
		switch {
		case strings.Contains(u, ".m3u8"):
			typ = "hls"
		case strings.Contains(u, ".mp4"):
			typ = "mp4"
		default:
			continue // not a direct stream URL
		}
		if typ == "hls" {
			if _, ok := p.probeMkissaMaster(ctx, u); !ok {
				continue
			}
		} else if !p.probeMkissaMP4(ctx, u) {
			continue
		}
		p.learnURLHost(u)
		sr.Sources = append(sr.Sources, core.Source{
			URL:          u,
			Type:         typ,
			Quality:      "auto",
			Verification: "proxy",
		})
		sr.ServerNames = append(sr.ServerNames, name)
	}
	if len(sr.Sources) == 0 {
		return nil, fmt.Errorf("mkissa: no playable direct source for episode %d (%s)", episode, langKey)
	}
	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", lang).Int("sources", len(sr.Sources)).Msg("mkissa resolved")
	p.storeResolved(showID, episode, langKey, sr)
	return sr, nil
}

// matchEpisode verifies the episode is listed for this audio track and
// returns its exact episode string (fractional entries never match an int
// request, mirroring the engine's exact-string lookup). It re-searches by
// the resolved show title and picks the ID-matching edge: the API has no
// direct show-by-ID lookup, and a raw showId text query returns nothing.
func (p *MkissaProvider) matchEpisode(ctx context.Context, showID, showTitle, langKey string, episode int) (string, error) {
	edges, err := p.searchShows(ctx, showTitle, langKey)
	if err != nil {
		return "", err
	}
	for i := range edges {
		if edges[i].ID != showID {
			continue
		}
		for _, s := range episodeStrings(edges[i], langKey) {
			if f, err := strconv.ParseFloat(s, 64); err == nil && f == float64(episode) {
				return s, nil
			}
		}
		return "", fmt.Errorf("mkissa: episode %d not listed (%s)", episode, langKey)
	}
	return "", fmt.Errorf("mkissa: show %s not found", showID)
}

// probeMkissaMaster verifies master -> first media -> first segment and
// warms the VOD playback cache like every other provider probe.
func (p *MkissaProvider) probeMkissaMaster(ctx context.Context, master string) (string, bool) {
	fetch := func(rawURL string, limit int64, timeout time.Duration) ([]byte, bool) {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(cctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, false
		}
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Referer", mkissaReferer)
		resp, err := p.client.Do(req)
		if err != nil {
			return nil, false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, false
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, limit))
		if err != nil || len(b) == 0 {
			return nil, false
		}
		return b, true
	}
	mhead, ok := fetch(master, 65536, 10*time.Second)
	if !ok || !strings.Contains(string(mhead), "#EXTM3U") {
		return "", false
	}
	VODCacheSet(master, mhead)
	variants := kaaParseVariants(string(mhead), master)
	if len(variants) == 0 {
		return "", false
	}
	media := variants[0].url
	mbody, ok := fetch(media, 262144, 10*time.Second)
	if !ok || !strings.Contains(string(mbody), "#EXTM3U") {
		return "", false
	}
	VODCacheSet(media, mbody)
	seg := firstPlaylistURL(string(mbody), media)
	if seg == "" {
		return "", false
	}
	shead, ok := fetch(seg, 8192, 12*time.Second)
	if !ok || !segmentBytesPlayable(shead) {
		return "", false
	}
	return master, true
}

// probeMkissaMP4 verifies a direct mp4 with a byte-range GET.
func (p *MkissaProvider) probeMkissaMP4(ctx context.Context, rawURL string) bool {
	cctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", mkissaReferer)
	req.Header.Set("Range", "bytes=0-2047")
	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return false
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return err == nil && len(b) > 0
}
