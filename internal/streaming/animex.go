package streaming

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

const (
	animexXORKey   = "10b06cdc1ca48c9fb0b94af97cc040cf"
	animexCDNBase  = "https://cdnx.aniwatchtv.site"
	animexPlayerUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
	animexPlyrBase = "https://plyr.animex.one"
	// animexStaleTTL bounds serving last-good sub-provider results when a
	// fresh resolve fails (timeout, API 404 flaps, challenge storms,
	// rotated blocked edges): the URLs involved are stable for hours, so a
	// minutes-old entry plays fine and keeps the server listed instead of
	// flickering it in and out.
	animexStaleTTL = 10 * time.Minute
	// maxAnimeXStaleEntries caps the stale-result cache: one entry per
	// title/episode/lang/sub-provider stays tiny, the cap only stops
	// unbounded growth over a multi-year process lifetime.
	maxAnimeXStaleEntries = 2000
)

// animexProviderTimeout bounds one sub-provider resolve (API + probe).
// Slow-but-working providers (yuki API runs observed past 10s) must still
// return; only true hangs get cut. A var so tests can shrink it.
var animexProviderTimeout = 20 * time.Second

// animexAPIHedgeDelay is when a slow sources-API attempt gets a parallel
// twin (first to finish wins). Normally the API answers in 0.3-1.8s and
// the hedge never fires; a hung request used to eat the whole provider
// budget (observed: yuki held all 20s and cost the Mochi slot on a cold
// cache). Hedging recovers a flap in ~8s while a merely slow attempt
// still returns by itself — nothing that works today can regress.
const animexAPIHedgeDelay = 8 * time.Second

// animexPlyrTimeout bounds waiting on the plyr page (slug + provider
// list). It has its own internal retry, and the fallback path (anilistID
// + static provider list) lists every core slot anyway — so a hung plyr
// page must not stall the whole collector.
const animexPlyrTimeout = 10 * time.Second

// animexProviders is the fallback provider order used when the plyr page
// cannot be parsed. Priority matches the plyr subProviders list, minus
// blocked providers (see animexBlockedProviders).
var animexProviders = []string{"beep", "yuki", "neko", "sora"}

// animexBlockedProviders lists sub-providers that must never be offered,
// regardless of what the plyr page lists. loli ("Anzu") serves image-segment
// playlists (numbered .jpg payloads) — it does not play, so it is hard-
// excluded before any network call. Everything else is resolved and gated
// only by the manifest reachability probe: no content sniffing in the drop
// path, because providers like yuki (Mochi) cloak probe requests with image
// payloads while serving real video to players.
var animexBlockedProviders = map[string]bool{
	"loli": true,
}

// filterBlockedProviders drops blocked sub-provider IDs from a plyr list.
func filterBlockedProviders(in []string) []string {
	out := make([]string, 0, len(in))
	for _, id := range in {
		if !animexBlockedProviders[id] {
			out = append(out, id)
		}
	}
	return out
}

// animexProviderNames maps provider IDs to human-readable server names.
var animexProviderNames = map[string]string{
	"yuki": "Mochi",
	"neko": "Chibi",
	"zuna": "Kira",
	"sora": "Sora",
	"uwu":  "Koharu",
	"beep": "Lumi",
	"loli": "Anzu",
}

// animexProviderDefaultReferer maps provider IDs to their default referer.
// Trailing slashes matter: the megaplay CDN 403s slashless origin referers
// (see normalizeOriginReferer) — the slash form is what real browsers send.
var animexProviderDefaultReferer = map[string]string{
	"yuki": "https://megaplay.buzz/",
	"sora": "https://krussdomi.com/",
	"uwu":  "https://kwik.cx/",
	"beep": "",
	"neko": "",
	"zuna": "",
}

// animexChallengeError marks a Cloudflare challenge/block response from the
// AnimeX API (pp.animex.one). It is transient: re-establishing the plyr
// clearance session and retrying usually gets through.
type animexChallengeError struct {
	status int
}

func (e *animexChallengeError) Error() string {
	return fmt.Sprintf("animex api returned HTTP %d (cloudflare challenge)", e.status)
}

// animexAPIResponse is the JSON shape returned by the AnimeX sources API.
type animexAPIResponse struct {
	Sources []struct {
		URL     string `json:"url"`
		Quality string `json:"quality"`
		Type    string `json:"type"`
	} `json:"sources"`
	Tracks []struct {
		URL     string `json:"url"`
		Lang    string `json:"lang"`
		Label   string `json:"label"`
		Default bool   `json:"default"`
		Kind    string `json:"kind"`
	} `json:"tracks"`
	Chapters []struct {
		Title string  `json:"title"`
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	} `json:"chapters"`
	Headers map[string]string `json:"headers"`
}

// AnimeXProvider resolves streams from the AnimeX plyr player API.
// It fetches the plyr page first to establish Cloudflare clearance, then uses
// those cookies to call the sources API and decode XOR+base64url proxy URLs
// into direct m3u8 and subtitle URLs.
type AnimeXProvider struct {
	client    *http.Client
	log       zerolog.Logger
	apiBase   string
	learnHost func(host string)

	sessionMu  sync.Mutex
	session    *animexSession
	sessionTTL time.Duration

	// Last-good sub-provider results for stale serving (see animexStaleTTL).
	staleMu sync.Mutex
	stale   map[string]*animexStaleEntry
}

// animexStaleEntry is one sub-provider's last good result.
type animexStaleEntry struct {
	res       *SourceResult
	fetchedAt time.Time
}

// animexSession holds a Cloudflare clearance session for the AnimeX API.
type animexSession struct {
	cookies   []*http.Cookie
	createdAt time.Time
}

func (p *AnimeXProvider) SetHostLearner(fn func(host string)) {
	p.learnHost = fn
}

func (p *AnimeXProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func NewAnimeXProvider(log zerolog.Logger, apiBase string) *AnimeXProvider {
	if apiBase == "" {
		apiBase = "https://pp.animex.one"
	}

	jar, _ := cookiejar.New(nil)
	return &AnimeXProvider{
		client: &http.Client{
			Timeout:   45 * time.Second,
			Transport: netguard.NewTransport(),
			Jar:       jar,
		},
		log:        log,
		apiBase:    strings.TrimRight(apiBase, "/"),
		sessionTTL: 5 * time.Minute,
		stale:      make(map[string]*animexStaleEntry),
	}
}

func (p *AnimeXProvider) Name() string { return "animex" }

func (p *AnimeXProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("animex search not implemented")
}

func (p *AnimeXProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("animex episode listing not implemented")
}

// ensureSession fetches the plyr page to establish Cloudflare clearance cookies.
// The cookies are cached for sessionTTL to avoid repeated challenges.
func (p *AnimeXProvider) ensureSession(ctx context.Context) error {
	p.sessionMu.Lock()
	defer p.sessionMu.Unlock()

	if p.session != nil && time.Since(p.session.createdAt) < p.sessionTTL {
		return nil
	}

	plyrURL := animexPlyrBase + "/e/1/1"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, plyrURL, nil)
	if err != nil {
		return fmt.Errorf("animex plyr request: %w", err)
	}
	req.Header.Set("User-Agent", animexPlayerUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("animex plyr fetch: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("animex plyr returned HTTP %d", resp.StatusCode)
	}

	cookies := p.client.Jar.Cookies(req.URL)
	if len(cookies) == 0 {
		p.log.Debug().Msg("animex: no cookies from plyr page, proceeding without session")
	}

	p.session = &animexSession{
		cookies:   cookies,
		createdAt: time.Now(),
	}
	p.log.Debug().Int("cookies", len(cookies)).Msg("animex: session established")
	return nil
}

// invalidateSession drops the cached Cloudflare clearance so the next
// ensureSession call re-establishes it.
func (p *AnimeXProvider) invalidateSession() {
	p.sessionMu.Lock()
	p.session = nil
	p.sessionMu.Unlock()
}

// FindEpisodeSource resolves the first available AnimeX stream (plyr API,
// XOR-decoded direct m3u8 + subtitles). Contract: exactly one SourceResult —
// used by the fast /stream fallback path.
//
// The AnimeX API expects a slug ID (e.g. "bleach-thousand-year-blood-war-the-calamity-ts6ov"),
// NOT the numeric AniList ID. The plyr page is fetched to extract the correct
// slug from the embedded SvelteKit data.
func (p *AnimeXProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	results, lastErr := p.resolveAllProviders(ctx, anilistID, episode, lang)
	for _, r := range results {
		if r != nil && len(r.Sources) > 0 {
			return r, nil
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("animex: no sources found for any provider")
}

// FindAllEpisodeSources resolves EVERY sub-provider the plyr page lists for
// the episode (beep/yuki/neko/sora/loli/...), not just the first one that
// answers. Each returned SourceResult carries its own provider-specific
// Referer/User-Agent headers, so callers must map results to servers (one
// server per result) rather than merging sources under a single header set.
// Results are ordered by plyr priority. Used by the /servers fan-out.
func (p *AnimeXProvider) FindAllEpisodeSources(ctx context.Context, anilistID string, episode int, lang string) ([]*SourceResult, error) {
	return p.resolveAllProviders(ctx, anilistID, episode, lang)
}

// resolveAllProviders is the shared resolver behind FindEpisodeSource and
// FindAllEpisodeSources.
//
// The plyr page gives the authoritative per-language provider list — episodes
// that only exist on a provider missing from our static list (e.g. loli) are
// exactly the ones that otherwise "don't show up". Providers resolve with
// bounded concurrency (3): wide enough to stay well inside the fan-out
// budget, narrow enough not to provoke Cloudflare rate limiting on the shared
// clearance session. A Cloudflare challenge on the API (intermittent 403
// HTML) is transient: one session refresh, then each challenged provider is
// retried once with fresh clearance.
func (p *AnimeXProvider) resolveAllProviders(ctx context.Context, anilistID string, episode int, lang string) ([]*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}

	// Fetch the plyr page (slug + provider list) and establish the
	// Cloudflare clearance session concurrently — independent requests
	// (the jar is goroutine-safe), saving ~300-600ms off every call.
	// One short-backoff retry on the plyr page: it intermittently serves
	// the Cloudflare challenge.
	type plyrRes struct {
		slug      string
		providers []string
		err       error
	}
	plyrCh := make(chan plyrRes, 1)
	go func() {
		slug, providers, err := p.fetchPlyrData(ctx, anilistID, episode, lang)
		if err != nil {
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				plyrCh <- plyrRes{err: ctx.Err()}
				return
			}
			slug, providers, err = p.fetchPlyrData(ctx, anilistID, episode, lang)
		}
		plyrCh <- plyrRes{slug: slug, providers: providers, err: err}
	}()
	if sessErr := p.ensureSession(ctx); sessErr != nil {
		p.log.Debug().Err(sessErr).Msg("animex: session establishment failed, trying anyway")
	}
	var pr plyrRes
	select {
	case pr = <-plyrCh:
	case <-time.After(animexPlyrTimeout):
		pr = plyrRes{err: fmt.Errorf("plyr page exceeded %s", animexPlyrTimeout)}
	case <-ctx.Done():
		pr = plyrRes{err: ctx.Err()}
	}
	slug, providers, err := pr.slug, pr.providers, pr.err
	if err != nil {
		p.log.Debug().Err(err).Msg("animex: failed to read plyr page, using anilistId and static providers")
		slug = anilistID
		providers = animexProviders
	}
	// The plyr list is authoritative, but known-bad sub-providers (loli /
	// Anzu) are excluded no matter what it lists.
	providers = filterBlockedProviders(providers)
	p.log.Debug().Str("anilistId", anilistID).Str("slug", slug).Strs("providers", providers).Msg("animex: resolved plyr data")

	results := make([]*SourceResult, len(providers))
	errs := make([]error, len(providers))
	var refreshOnce sync.Once
	sem := make(chan struct{}, 3) // bounded concurrency
	var wg sync.WaitGroup
	for i, providerID := range providers {
		wg.Add(1)
		go func(i int, providerID string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			attempt := func() (*SourceResult, error) {
				res, err := p.resolveProvider(ctx, slug, episode, lang, providerID)
				if err != nil {
					var challenge *animexChallengeError
					if errors.As(err, &challenge) {
						refreshOnce.Do(func() {
							p.log.Info().Msg("animex: cloudflare challenge, refreshing session")
							p.invalidateSession()
							if sessErr := p.ensureSession(ctx); sessErr != nil {
								p.log.Debug().Err(sessErr).Msg("animex: session refresh failed")
							}
						})
						res, err = p.resolveProvider(ctx, slug, episode, lang, providerID)
					}
				}
				return res, err
			}
			start := time.Now()
			res, err := attempt()
			// Fast failure (not a hang): likely a transient edge flap —
			// one immediate retry converts most into hits. Slow failures
			// are left alone to avoid piling retries onto hangs, and
			// legitimate empties (no error) are never retried.
			if shouldRetryProvider(err, time.Since(start)) {
				if res2, err2 := attempt(); err2 == nil && res2 != nil && len(res2.Sources) > 0 {
					res, err = res2, nil
				}
			}
			staleKey := animexStaleKey(anilistID, episode, lang, providerID)
			if res != nil && len(res.Sources) > 0 {
				p.storeStale(staleKey, res)
			} else if err != nil {
				// Fresh resolve failed: serve the last good result while
				// it is fresh (URLs are stable for hours) instead of
				// flickering a working server out of the list. Expired
				// entries refuse themselves inside loadStale.
				if stale := p.loadStale(staleKey); stale != nil {
					p.log.Info().Str("provider", providerID).Msg("animex: serving stale result after failure")
					res, err = stale, nil
				}
			}
			if err != nil {
				p.log.Info().Err(err).Str("provider", providerID).Msg("animex: provider failed")
			}
			results[i], errs[i] = res, err
		}(i, providerID)
	}
	wg.Wait()

	var lastErr error
	for _, e := range errs {
		if e != nil {
			lastErr = errors.Join(lastErr, e)
		}
	}
	return results, lastErr
}

// animexStaleKey identifies one cached sub-provider result.
func animexStaleKey(anilistID string, episode int, lang, providerID string) string {
	return anilistID + "/" + strconv.Itoa(episode) + "/" + lang + "/" + providerID
}

// storeStale remembers a good result (deep-copied — callers keep using
// theirs, and future readers must never observe a mutated entry).
func (p *AnimeXProvider) storeStale(key string, sr *SourceResult) {
	p.staleMu.Lock()
	defer p.staleMu.Unlock()
	if len(p.stale) >= maxAnimeXStaleEntries {
		now := time.Now()
		var oldestKey string
		var oldest time.Time
		first := true
		for k, e := range p.stale {
			if now.Sub(e.fetchedAt) > animexStaleTTL {
				delete(p.stale, k)
				continue
			}
			if first || e.fetchedAt.Before(oldest) {
				oldestKey, oldest, first = k, e.fetchedAt, false
			}
		}
		if len(p.stale) >= maxAnimeXStaleEntries && oldestKey != "" {
			delete(p.stale, oldestKey)
		}
	}
	p.stale[key] = &animexStaleEntry{res: cloneSourceResult(sr), fetchedAt: time.Now()}
}

// loadStale returns the stored result when it is still fresh, deleting and
// refusing expired entries.
func (p *AnimeXProvider) loadStale(key string) *SourceResult {
	p.staleMu.Lock()
	defer p.staleMu.Unlock()
	e, ok := p.stale[key]
	if !ok {
		return nil
	}
	if time.Since(e.fetchedAt) > animexStaleTTL {
		delete(p.stale, key)
		return nil
	}
	return e.res
}

// cloneSourceResult deep-copies the mutable parts of a result so the stale
// cache never shares slices or maps with live results.
func cloneSourceResult(sr *SourceResult) *SourceResult {
	if sr == nil {
		return nil
	}
	out := &SourceResult{
		ServerName: sr.ServerName,
		Headers:    make(map[string]string, len(sr.Headers)),
		Sources:    make([]core.Source, 0, len(sr.Sources)),
		Downloads:  make([]core.DownloadLink, 0, len(sr.Downloads)),
	}
	for k, v := range sr.Headers {
		out.Headers[k] = v
	}
	for _, s := range sr.Sources {
		cs := s
		if s.Subtitles != nil {
			cs.Subtitles = append([]core.Subtitle(nil), s.Subtitles...)
		}
		out.Sources = append(out.Sources, cs)
	}
	out.Downloads = append(out.Downloads, sr.Downloads...)
	if sr.ServerNames != nil {
		out.ServerNames = append([]string(nil), sr.ServerNames...)
	}
	if sr.Intro != nil {
		c := *sr.Intro
		out.Intro = &c
	}
	if sr.Outro != nil {
		c := *sr.Outro
		out.Outro = &c
	}
	return out
}

// shouldRetryProvider reports whether a failed sub-provider resolve is
// worth one immediate retry: real errors that failed fast (transient edge
// flaps — the usual reason a working server like Mochi misses one list).
// Slow failures (hangs, long challenge storms) and legitimate empties (no
// error, nothing listed) are never retried.
func shouldRetryProvider(err error, elapsed time.Duration) bool {
	const fastFailure = 3 * time.Second
	return err != nil && elapsed < fastFailure
}

// fetchPlyrData fetches the AnimeX plyr page once and extracts BOTH the show
// slug and the per-language provider list from the embedded SvelteKit data:
//
//	servers:{subProviders:[{id:"beep",default:true,...},...],dubProviders:[...]},
//	id:"bleach-...-ts6ov",anilistId:185874,...
//
// The provider list is authoritative — the site only offers what it lists
// here, so querying exactly these IDs (in listed order) is what makes every
// available episode show up.
func (p *AnimeXProvider) fetchPlyrData(ctx context.Context, anilistID string, episode int, lang string) (string, []string, error) {
	plyrURL := fmt.Sprintf("%s/e/%s/%d", animexPlyrBase, anilistID, episode)
	p.log.Debug().Str("url", plyrURL).Msg("animex: fetching plyr page")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, plyrURL, nil)
	if err != nil {
		return "", nil, fmt.Errorf("plyr request: %w", err)
	}
	req.Header.Set("User-Agent", animexPlayerUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("plyr fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("plyr returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if err != nil {
		return "", nil, fmt.Errorf("plyr read: %w", err)
	}
	html := string(body)

	slug, err := extractPlyrSlug(html)
	if err != nil {
		return "", nil, err
	}
	providers := extractPlyrProviders(html, lang)
	if len(providers) == 0 {
		providers = animexProviders
	}
	return slug, providers, nil
}

// extractPlyrProviders parses the subProviders/dubProviders ID list for the
// requested language out of the SvelteKit page data.
func extractPlyrProviders(html, lang string) []string {
	key := `subProviders:`
	if lang == "dub" {
		key = `dubProviders:`
	}
	start := strings.Index(html, key)
	if start < 0 {
		return nil
	}
	region := html[start:]
	if end := strings.Index(region, "],"); end >= 0 {
		region = region[:end]
	}
	var out []string
	for _, m := range regexp.MustCompile(`id:"([a-z0-9]+)"`).FindAllStringSubmatch(region, -1) {
		out = append(out, m[1])
	}
	return out
}

// extractPlyrSlug parses the SvelteKit embedded page data from the plyr HTML
// to extract the show slug. The data is embedded in a script like:
//
//	...,id:"bleach-thousand-year-blood-war-the-calamity-ts6ov",anilistId:185874,...
//
// Strategy: find `anilistId:` first, then look backwards for the nearest `id:"`
// that belongs to the same data block (not a provider id).
func extractPlyrSlug(html string) (string, error) {
	anilistPos := strings.Index(html, `anilistId:`)
	if anilistPos < 0 {
		return "", fmt.Errorf("no anilistId field found in plyr page")
	}

	searchRegion := html[:anilistPos]

	var lastShowID string
	idx := 0
	for {
		pos := strings.Index(searchRegion[idx:], `id:"`)
		if pos < 0 {
			break
		}
		pos += idx

		endQuote := strings.Index(searchRegion[pos+4:], `"`)
		if endQuote < 0 {
			idx = pos + 4
			continue
		}
		candidate := searchRegion[pos+4 : pos+4+endQuote]
		if candidate == "" {
			idx = pos + 4 + endQuote
			continue
		}

		// Check in the FULL html what comes right after the closing quote.
		// Show-level: ,anilistId:  Provider-level: ,default: or },...
		absoluteQuoteEnd := pos + 4 + endQuote + 1
		if absoluteQuoteEnd+1 < len(html) {
			after := html[absoluteQuoteEnd:]
			if strings.HasPrefix(after, ",anilistId:") {
				lastShowID = candidate
			}
		}

		idx = pos + 4 + endQuote
	}

	if lastShowID == "" {
		return "", fmt.Errorf("could not extract show slug from plyr page")
	}
	return lastShowID, nil
}

// resolveProvider fetches sources from a specific AnimeX provider and decodes
// the proxy URLs to direct m3u8/subtitle URLs.
func (p *AnimeXProvider) resolveProvider(ctx context.Context, anilistID string, episode int, lang string, providerID string) (*SourceResult, error) {
	// Bound a single sub-provider: the shared client allows 45s, and one
	// hanging CDN edge must not hold a semaphore slot (and the tail) for
	// that long. Healthy providers answer in 0.3-1.8s; the budget covers
	// slow-but-working API runs plus the probe round (the API stage is
	// further hedged at animexAPIHedgeDelay).
	ctx, cancel := context.WithTimeout(ctx, animexProviderTimeout)
	defer cancel()
	// 1. Call the AnimeX API with browser-like headers — hedged: if the
	// first attempt has not answered within animexAPIHedgeDelay, a second
	// one runs in parallel and the first to finish wins (see the const's
	// comment). Each attempt builds its own request.
	apiURL := fmt.Sprintf("%s/rest/api/sources?id=%s&epNum=%d&type=%s&providerId=%s",
		p.apiBase, url.PathEscape(anilistID), episode, lang, url.PathEscape(providerID))
	p.log.Debug().Str("url", apiURL).Msg("animex: calling API")

	type apiFetch struct {
		resp animexAPIResponse
		err  error
	}
	fetch := func() apiFetch {
		var out apiFetch
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			out.err = fmt.Errorf("animex api request: %w", err)
			return out
		}
		// Mirror browser request headers exactly
		req.Header.Set("User-Agent", animexPlayerUA)
		req.Header.Set("Accept", "application/json, text/plain, */*")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Referer", animexPlyrBase+"/")
		req.Header.Set("Origin", animexPlyrBase)
		req.Header.Set("Sec-Fetch-Dest", "empty")
		req.Header.Set("Sec-Fetch-Mode", "cors")
		req.Header.Set("Sec-Fetch-Site", "cross-site")
		req.Header.Set("Sec-Ch-Ua", `"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"`)
		req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
		req.Header.Set("Sec-Ch-Ua-Platform", `"Windows"`)

		resp, err := p.client.Do(req)
		if err != nil {
			out.err = fmt.Errorf("animex api fetch: %w", err)
			return out
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusForbidden {
			// Cloudflare challenge/block — transient, retryable via a fresh
			// plyr session.
			out.err = &animexChallengeError{status: resp.StatusCode}
			return out
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			out.err = fmt.Errorf("animex api returned HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
			return out
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 4*1024*1024)).Decode(&out.resp); err != nil {
			out.resp = animexAPIResponse{}
			out.err = fmt.Errorf("animex api decode: %w", err)
		}
		return out
	}

	first := make(chan apiFetch, 1)
	go func() { first <- fetch() }()
	var got apiFetch
	select {
	case got = <-first:
	case <-time.After(animexAPIHedgeDelay):
		second := make(chan apiFetch, 1)
		go func() { second <- fetch() }()
		select {
		case got = <-first:
		case got = <-second:
		}
	}
	if got.err != nil {
		return nil, got.err
	}
	apiResp := got.resp

	if len(apiResp.Sources) == 0 {
		return nil, nil
	}

	// 2. Probe + tag. The API returns direct CDN URLs gated behind per-
	// provider Referer/User-Agent headers (returned in apiResp.Headers).
	// Browsers cannot attach those headers themselves, so playback always
	// goes through the Aniraku media proxy: every source ships with
	// Verification "proxy" and the provider headers attached to the result.
	// A failed probe is not fatal — the proxy egress and residential clients
	// can reach CDNs that block this server.
	referer := apiResp.Headers["Referer"]
	if referer == "" {
		referer = animexProviderDefaultReferer[providerID]
	}
	userAgent := apiResp.Headers["User-Agent"]

	var sources []core.Source
	// effectiveUA is the User-Agent used for probes AND shipped in the
	// result headers: a CDN that gates by UA class (see iosSafariUA) must
	// be played with the exact UA the probe cleared, or the media proxy
	// would 403 on the very segments we just verified. It starts as the
	// provider's configured UA and may swap to iOS Safari once.
	effectiveUA := userAgent
	adoptedIOS := false
	for _, src := range apiResp.Sources {
		directURL := src.URL

		// Decode CDN proxy URLs (/uwu/) to get the actual origin m3u8 URL.
		// The CDN proxy is only needed for browser playback (CORS). Our server
		// can fetch the origin directly, bypassing CDN IP/Referer checks.
		if decoded, _, _ := DecodeAnimeXProxyURL(src.URL); decoded != src.URL {
			directURL = decoded
			p.log.Debug().Str("origin", directURL).Msg("animex: decoded CDN proxy to origin URL")
		}

		// Apply domain rewrites (vivibebe→hawk, etc.)
		// Only apply global rewrites — never re-encode into CDN proxy.
		for _, rw := range animexGlobalRewrites {
			directURL = rw(directURL)
		}

		// Learn the host for CDN allowlist
		p.learnURLHost(directURL) // The media proxy shares this server's egress: a manifest the probe
		// cannot reach would 403 through the proxy too, so it is dropped
		// instead of surfacing a server that can only produce 502s. The
		// same honesty rule extends one level deeper: a reachable manifest
		// whose segments are egress-blocked only produces a spinning
		// player. NOTE: a 403 is not automatically an egress block —
		// Sora's bl1.* segment layer 403s every desktop/Android UA while
		// serving iPhone Safari, so a failed pair is retried with the iOS
		// UA before condemning the source.
		//
		// The segment verdict is deliberately lenient — definitive blocks
		// only (unreachable, non-2xx, HTML error pages). Ambiguous payloads
		// (image cloaks some CDNs serve probes while playing video fine)
		// keep the server listed: worst case is today's behavior, never a
		// regression on a working stream.
		headOK, _ := p.probeHLSHead(ctx, directURL, referer, effectiveUA)
		segOK := headOK && p.probeFirstSegment(ctx, directURL, referer, effectiveUA)
		if !headOK || !segOK {
			// One atomic retry of the WHOLE pair with iOS Safari (only
			// once per result): if it clears, that UA becomes the
			// playback UA too.
			if effectiveUA != iosSafariUA {
				if hOK, _ := p.probeHLSHead(ctx, directURL, referer, iosSafariUA); hOK &&
					p.probeFirstSegment(ctx, directURL, referer, iosSafariUA) {
					effectiveUA = iosSafariUA
					adoptedIOS = true
					p.log.Info().Str("provider", providerID).Str("url", directURL).Msg("animex: CDN gates by UA class, adopting iOS Safari UA")
				}
			}
			if effectiveUA != iosSafariUA {
				if !headOK {
					p.log.Info().Str("provider", providerID).Str("url", directURL).Msg("animex: manifest blocked from this egress, dropping source")
				} else {
					p.log.Info().Str("provider", providerID).Str("url", directURL).Msg("animex: segments blocked from this egress, dropping source")
				}
				continue
			}
		}

		// Build subtitle tracks from the API response
		var subs []core.Subtitle
		for _, track := range apiResp.Tracks {
			subURL := track.URL
			// Decode CDN proxy URLs for subtitles too
			if decoded, _, _ := DecodeAnimeXProxyURL(track.URL); decoded != track.URL {
				subURL = decoded
			}
			for _, rw := range animexGlobalRewrites {
				subURL = rw(subURL)
			}
			// Upstream subtitle URLs are sometimes malformed
			// (https:///subbl.krussdomi.com/... with an empty host):
			// repair or drop so broken tracks never list.
			clean, ok := sanitizeSubtitleURL(subURL)
			if !ok {
				p.log.Debug().Str("provider", providerID).Str("url", track.URL).Msg("animex: dropping malformed subtitle URL")
				continue
			}
			subURL = clean
			p.learnURLHost(subURL)
			langCode := track.Lang
			if langCode == "" {
				langCode = mapSubtitleLang(track.Label)
			}
			subs = append(subs, core.Subtitle{
				URL:   subURL,
				Lang:  langCode,
				Label: track.Label,
			})
		}

		// Determine source type from URL
		srcType := "hls"
		if strings.Contains(directURL, ".mp4") || strings.Contains(directURL, ".m4v") {
			srcType = "mp4"
		} else if strings.Contains(directURL, ".mpd") {
			srcType = "dash"
		}

		sources = append(sources, core.Source{
			URL:          directURL,
			Type:         srcType,
			Quality:      src.Quality,
			Subtitles:    subs,
			Verification: "proxy",
		})
	}

	// 3. Extract chapters (intro/outro)
	var intro, outro *core.SkipTimestamp
	for _, ch := range apiResp.Chapters {
		switch strings.ToLower(ch.Title) {
		case "intro":
			intro = &core.SkipTimestamp{Start: ch.Start, End: ch.End}
		case "outro":
			outro = &core.SkipTimestamp{Start: ch.Start, End: ch.End}
		}
	}

	serverName := animexProviderNames[providerID]
	if serverName == "" {
		serverName = "Hana"
	}
	p.log.Info().Str("provider", serverName).Str("lang", lang).Int("sources", len(sources)).Msg("animex: resolved")

	// Attach the provider headers (Referer, and User-Agent for providers like
	// sora whose CDN gates by UA class) so the media proxy replays them.
	headers := map[string]string{}
	for k, v := range apiResp.Headers {
		headers[k] = v
	}
	if headers["Referer"] == "" && referer != "" {
		headers["Referer"] = referer
	}
	if adoptedIOS {
		// The probe cleared the segments with the iOS UA — playback must
		// send the same one (bl1.* 403s desktop/Android UAs outright).
		headers["User-Agent"] = iosSafariUA
	} else if headers["User-Agent"] == "" {
		headers["User-Agent"] = animexPlayerUA
	}

	return &SourceResult{
		Sources:    sources,
		Headers:    headers,
		ServerName: serverName,
		Intro:      intro,
		Outro:      outro,
	}, nil
}

// ---------------------------------------------------------------------------
// AnimeX proxy URL codec — XOR + base64url encoding/decoding
// ---------------------------------------------------------------------------

// EncodeAnimeXProxyURL encodes a raw URL + referer into the CDN proxy format.
// Format: cdnx.aniwatchtv.site/uwu/{base64url(xor(payload, key))}
func EncodeAnimeXProxyURL(originalURL, referer, userAgent string) string {
	payload := []byte(originalURL)
	payload = append(payload, 0)
	payload = append(payload, []byte(referer)...)
	if userAgent != "" {
		payload = append(payload, 0)
		payload = append(payload, []byte(userAgent)...)
	}

	key := []byte(animexXORKey)
	for i := range payload {
		payload[i] ^= key[i%len(key)]
	}

	b64 := base64.RawURLEncoding.EncodeToString(payload)
	return animexCDNBase + "/uwu/" + b64
}

// DecodeAnimeXProxyURL decodes a CDN proxy URL back to the original URL,
// referer, and user-agent. Returns the raw URL unchanged if it is not a proxy URL.
func DecodeAnimeXProxyURL(proxyURL string) (originalURL, referer, userAgent string) {
	if !strings.Contains(proxyURL, "/uwu/") {
		return proxyURL, "", ""
	}

	token := proxyURL[strings.Index(proxyURL, "/uwu/")+5:]
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		// Try standard base64 with padding restoration
		stdToken := strings.ReplaceAll(token, "-", "+")
		stdToken = strings.ReplaceAll(stdToken, "_", "/")
		if pad := len(stdToken) % 4; pad != 0 {
			stdToken += strings.Repeat("=", 4-pad)
		}
		payload, err = base64.StdEncoding.DecodeString(stdToken)
		if err != nil {
			return proxyURL, "", ""
		}
	}

	key := []byte(animexXORKey)
	for i := range payload {
		payload[i] ^= key[i%len(key)]
	}

	// Split on null bytes: url \0 referer \0 [user-agent]
	parts := strings.Split(string(payload), "\x00")
	if len(parts) < 1 {
		return proxyURL, "", ""
	}
	originalURL = parts[0]
	if len(parts) >= 2 {
		referer = parts[1]
	}
	if len(parts) >= 3 {
		userAgent = parts[2]
	}
	return originalURL, referer, userAgent
}

// ---------------------------------------------------------------------------
// Manifest probe
// ---------------------------------------------------------------------------

// fetchHead GETs up to limit bytes of a URL with provider referer/UA.
func (p *AnimeXProvider) fetchHead(ctx context.Context, rawURL, referer, userAgent string, limit int64) ([]byte, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, false
	}
	ua := userAgent
	if ua == "" {
		ua = animexPlayerUA
	}
	req.Header.Set("User-Agent", ua)
	if referer != "" {
		// Normalized: slashless origin referers 403 on the megaplay CDN
		// (same trap that hid Niko) — yuki's fallback is exactly
		// "https://megaplay.buzz" without the trailing slash.
		req.Header.Set("Referer", normalizeOriginReferer(referer))
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", limit-1))
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, false
	}
	head, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, false
	}
	return head, true
}

// probeHLSHead verifies a manifest URL answers from this egress with a real
// HLS playlist. It intentionally inspects the manifest only — no segment
// fetching, no content classification: probe requests are not always served
// the same bytes players get (yuki/Mochi's CDN answers probes with image
// payloads while the stream plays fine), so deeper verdicts misfire.
func (p *AnimeXProvider) probeHLSHead(ctx context.Context, manifestURL, referer, userAgent string) (bool, []byte) {
	head, ok := p.fetchHead(ctx, manifestURL, referer, userAgent, 8192)
	if !ok {
		return false, nil
	}
	return strings.Contains(string(head), "#EXTM3U"), head
}

// sanitizeSubtitleURL repairs malformed subtitle URLs from upstream APIs
// (observed: https:///subbl.krussdomi.com/... with an empty host — the API
// drops a slash) by promoting the first path segment to the host, and
// rejects URLs that are still unusable so broken tracks never list.
// Relative URLs are rejected outright: without a host they can never play.
func sanitizeSubtitleURL(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || (!strings.Contains(s, "://") && !strings.HasPrefix(s, "//")) {
		return "", false
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", false
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	if u.Host == "" && strings.HasPrefix(u.Path, "/") {
		rest := strings.TrimPrefix(u.Path, "/")
		if i := strings.Index(rest, "/"); i > 0 {
			u.Host = rest[:i]
			u.Path = rest[i:]
		} else if rest != "" {
			u.Host = rest
			u.Path = ""
		}
	}
	if u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", false
	}
	return u.String(), true
}

// playlist is fetchable from this egress (definitive blocks only —
// unreachable, non-2xx, HTML error pages — so ambiguous payloads never
// regress a working stream). Shared shape with the Anikoto segment probe;
// the verdict here is intentionally the lenient half (no magic-byte
// requirement) because sub-providers have no edge alternatives to fall
// back to.
func (p *AnimeXProvider) probeFirstSegment(ctx context.Context, masterURL, referer, userAgent string) bool {
	return probeSegmentsLenient(ctx, p.client, masterURL, referer, userAgent)
}

// ---------------------------------------------------------------------------
// Per-provider URL rewrites — mirrors the client-side Xr() / Jr[] / Zr[] logic
// ---------------------------------------------------------------------------

// animexGlobalRewrites are applied to all provider URLs before provider-specific
// transforms. Mirrors the client-side Zr[] array.
var animexGlobalRewrites = []func(string) string{
	// vivibebe → hawk CDN rewrite
	func(u string) string {
		return strings.Replace(u, "https://vivibebe.site/public/stream/", "https://hawk.aniwatchtv.site/media/", 1)
	},
	// NOTE: there is deliberately NO playeng r2 → bd CDN rewrite, although
	// the client-side Zr[] array has one. bd.aniwatchtv.site's Cloudflare
	// blocks datacenter egress (403 on every header combination, verified
	// live), while playeng.animeapps.top serves this server fine with its
	// own referer (200 + EXTM3U, verified live via the media proxy). Since
	// all playback flows through the proxy, the native URL is used as-is.
}

// animexSkipRefererWrap is the set of providers whose URLs should NOT be
// wrapped via the CDN proxy even if they have a Referer header.
var animexSkipRefererWrap = map[string]bool{
	"vee":  true,
	"neko": true,
	"loli": true,
}

// animexApplyRewrites applies the global and per-provider URL transforms
// exactly as the client-side Xr() function does.
// If the URL is already a CDN proxy URL (/uwu/), it is returned as-is to
// prevent double-encoding.
func animexApplyRewrites(rawURL, providerID string, headers map[string]string) string {
	u := rawURL

	// If already a CDN proxy URL, don't re-encode
	if strings.Contains(u, "/uwu/") {
		return u
	}

	// 1. Global rewrites
	for _, rw := range animexGlobalRewrites {
		u = rw(u)
	}

	// 2. Provider-specific transforms
	pid := strings.ToLower(providerID)
	switch pid {
	case "sora":
		// Wrap through CDN proxy with krussdomi referer
		if ref := animexProviderDefaultReferer["sora"]; ref != "" {
			u = EncodeAnimeXProxyURL(u, ref, "")
		}
	case "yuki":
		// Wrap through CDN proxy with referer from API headers or megaplay default
		ref := headers["Referer"]
		if ref == "" {
			ref = animexProviderDefaultReferer["yuki"]
		}
		ua := headers["User-Agent"]
		u = EncodeAnimeXProxyURL(u, ref, ua)
	case "uwu":
		// Wrap through CDN proxy with kwik.cx referer, vault CDN
		u = EncodeAnimeXProxyURL(u, "https://kwik.cx/", "")
	case "kiwi":
		u = EncodeAnimeXProxyURL(u, "https://anidb.app/", "")
	case "miku":
		u = EncodeAnimeXProxyURL(u, "https://allanime.uns.bio", "")
	case "beep":
		// Domain replacement for 24stream / aniwatchtv
		if strings.HasPrefix(u, "https://bd.24stream.xyz/media") ||
			strings.HasPrefix(u, "https://bd.aniwatchtv.site/media") {
			// keep as-is
		} else if strings.HasPrefix(u, "/") {
			u = "https://bd.aniwatchtv.site/media" + strings.Replace(u, "/r2", "", 1)
		} else {
			u = "https://bd.aniwatchtv.site/media" + animexPathOnly(u)
		}
	case "mochi":
		u = strings.Replace(u, "https://tools.fast4speed.rsvp", "https://mp4.24stream.xyz/storage", 1)
	case "vee", "loli", "neko":
		// pass-through
	default:
		// Fallback: if URL unchanged but Referer exists, wrap via CDN proxy
		if u == rawURL && !animexSkipRefererWrap[pid] {
			if ref := headers["Referer"]; ref != "" {
				u = EncodeAnimeXProxyURL(u, ref, headers["User-Agent"])
			}
		}
	}

	// 3. Upgrade http → https
	if strings.HasPrefix(u, "http://") {
		u = "https://" + u[7:]
	}

	return u
}

// animexPathOnly extracts the path + query from a URL string.
func animexPathOnly(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.RequestURI()
}

// truncate shortens a string to maxLen characters, appending "..." if truncated.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
