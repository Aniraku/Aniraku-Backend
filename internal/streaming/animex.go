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
)

// animexProviders is the fallback provider order used when the plyr page
// cannot be parsed. Priority matches the plyr subProviders list. Bad sources
// are filtered per-source by content sniffing, not per-provider.
var animexProviders = []string{"beep", "yuki", "neko", "sora", "loli"}

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
var animexProviderDefaultReferer = map[string]string{
	"yuki": "https://megaplay.buzz",
	"sora": "https://krussdomi.com",
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
			Timeout:   20 * time.Second,
			Transport: netguard.NewTransport(),
			Jar:       jar,
		},
		log:        log,
		apiBase:    strings.TrimRight(apiBase, "/"),
		sessionTTL: 5 * time.Minute,
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

	// Fetch the plyr page for the show slug + provider list. One
	// short-backoff retry: the plyr page intermittently serves the
	// Cloudflare challenge.
	slug, providers, err := p.fetchPlyrData(ctx, anilistID, episode, lang)
	if err != nil {
		time.Sleep(time.Second)
		slug, providers, err = p.fetchPlyrData(ctx, anilistID, episode, lang)
	}
	if err != nil {
		p.log.Debug().Err(err).Msg("animex: failed to read plyr page, using anilistId and static providers")
		slug = anilistID
		providers = animexProviders
	}
	p.log.Debug().Str("anilistId", anilistID).Str("slug", slug).Strs("providers", providers).Msg("animex: resolved plyr data")

	// Establish Cloudflare clearance session first.
	if err := p.ensureSession(ctx); err != nil {
		p.log.Debug().Err(err).Msg("animex: session establishment failed, trying anyway")
	}

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
			if err != nil {
				p.log.Debug().Err(err).Str("provider", providerID).Msg("animex: provider failed")
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
	// 1. Call the AnimeX API with browser-like headers
	apiURL := fmt.Sprintf("%s/rest/api/sources?id=%s&epNum=%d&type=%s&providerId=%s",
		p.apiBase, url.PathEscape(anilistID), episode, lang, url.PathEscape(providerID))
	p.log.Debug().Str("url", apiURL).Msg("animex: calling API")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("animex api request: %w", err)
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
		return nil, fmt.Errorf("animex api fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		// Cloudflare challenge/block — transient, retryable via a fresh
		// plyr session.
		return nil, &animexChallengeError{status: resp.StatusCode}
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("animex api returned HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var apiResp animexAPIResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4*1024*1024)).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("animex api decode: %w", err)
	}

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
	for _, src := range apiResp.Sources {
		directURL := src.URL

		// Decode CDN proxy URLs (/uwu/) to get the actual origin m3u8 URL.
		// The CDN proxy is only needed for browser playback (CORS). Our server
		// can fetch the origin directly, bypassing CDN IP/Referer checks.
		if decoded, _, _ := DecodeAnimeXProxyURL(src.URL); decoded != src.URL {
			directURL = decoded
			p.log.Debug().Str("origin", directURL).Msg("animex: decoded CDN proxy to origin URL")
		}

		// Apply domain rewrites (vivibebe→hawk, playeng→bd, etc.)
		// Only apply global rewrites — never re-encode into CDN proxy.
		for _, rw := range animexGlobalRewrites {
			directURL = rw(directURL)
		}

		// Learn the host for CDN allowlist
		p.learnURLHost(directURL)

		// The media proxy shares this server's egress: a manifest the probe
		// cannot reach would 403 through the proxy too, so it is dropped
		// instead of surfacing a server that can only produce 502s. The same
		// single fetch doubles as the image-segment guard: providers like
		// loli (Anzu) serve valid #EXTM3U playlists whose segments are
		// numbered images — reachable, "valid", and impossible to play.
		ok, head := p.probeHLSHead(ctx, directURL, referer, userAgent)
		if !ok {
			p.log.Info().Str("provider", providerID).Str("url", directURL).Msg("animex: manifest blocked from this egress, dropping source")
			continue
		}
		// Content-sniff the first segment. Extensions are meaningless here:
		// providers disguise real video as .jpg (hls.js sniffs magic bytes, so
		// it plays), while genuine image slideshows (Anzu/loli) also hide
		// behind valid #EXTM3U playlists. Drop on image data, and drop when
		// the segment is unreachable from this egress — animex sources ship
		// as Verification "proxy", so proxy egress == probe egress and an
		// unreachable segment can never play. Undecidable heads keep the
		// source (fail-open), mirroring the manifest probe.
		switch v := p.playlistSegmentVerdict(ctx, directURL, referer, userAgent, head, 0); v {
		case "image":
			p.log.Info().Str("provider", providerID).Str("url", directURL).Msg("animex: image-segment playlist detected (first segment is image data), dropping source")
			continue
		case "unreachable":
			p.log.Info().Str("provider", providerID).Str("url", directURL).Msg("animex: first segment unreachable from this egress, dropping source")
			continue
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
	// sora whose CDN expects a mobile UA) so the media proxy replays them.
	headers := map[string]string{}
	for k, v := range apiResp.Headers {
		headers[k] = v
	}
	if headers["Referer"] == "" && referer != "" {
		headers["Referer"] = referer
	}
	if headers["User-Agent"] == "" {
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
// Image-segment detection — content sniffing, not extension matching
// ---------------------------------------------------------------------------

// HLS players never trust file extensions: hls.js and friends sniff segment
// magic bytes, which is why playlists full of ".jpg"-named segments often
// contain perfectly playable video disguised to evade scrapers. The only
// reliable test is to fetch the first segment and classify its bytes.

// classifySegmentHead reports what kind of payload b (the first bytes of a
// media segment) most likely is: "image", "video", or "unknown".
func classifySegmentHead(b []byte) string {
	switch {
	case len(b) >= 3 && b[0] == 0xFF && b[1] == 0xD8 && b[2] == 0xFF: // JPEG
		return "image"
	case len(b) >= 4 && b[0] == 0x89 && b[1] == 'P' && b[2] == 'N' && b[3] == 'G': // PNG
		return "image"
	case len(b) >= 3 && b[0] == 'G' && b[1] == 'I' && b[2] == 'F': // GIF
		return "image"
	case len(b) >= 12 && string(b[:4]) == "RIFF" && string(b[8:12]) == "WEBP": // WebP
		return "image"
	}
	if len(b) >= 12 && (string(b[4:8]) == "ftyp" || string(b[4:8]) == "styp") {
		brand := strings.ToLower(string(b[8:12]))
		switch {
		case strings.HasPrefix(brand, "avif"), strings.HasPrefix(brand, "avis"),
			strings.HasPrefix(brand, "heic"), strings.HasPrefix(brand, "heim"),
			strings.HasPrefix(brand, "mif1"), strings.HasPrefix(brand, "msf1"):
			return "image" // HEIF/AVIF still-image containers
		default:
			return "video" // isom, mp42, dash, msdh, avc1, ...
		}
	}
	if len(b) >= 4 {
		switch string(b[:4]) {
		case "moof", "mdat", "moov", "styp":
			return "video" // fragmented / new-style MP4 boxes
		}
	}
	if len(b) >= 1 && b[0] == 0x47 { // MPEG-TS sync byte
		return "video"
	}
	return "unknown"
}

// firstPlaylistURI returns the first non-comment line of an m3u8 head —
// either a media segment (media playlist) or a variant playlist (master).
func firstPlaylistURI(head []byte) string {
	for _, line := range strings.Split(string(head), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		return line
	}
	return ""
}

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
		req.Header.Set("Referer", referer)
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

// playlistSegmentVerdict content-classifies what an HLS playlist actually
// serves, by reading its first real segment's bytes:
//
//	"image"       first segment is image data (JPEG/PNG/GIF/WebP/HEIF/AVIF)
//	"video"       first segment is MPEG-TS or MP4/fMP4 (possibly disguised)
//	"unreachable" manifest resolved but the segment cannot be fetched from
//	              this egress — playback through the media proxy cannot work
//	"undecidable" segment bytes classify as unknown, or a referenced playlist
//	              could not be resolved — callers keep the source
//
// Master playlists are followed one level deep into the first variant.
func (p *AnimeXProvider) playlistSegmentVerdict(ctx context.Context, manifestURL, referer, userAgent string, head []byte, depth int) string {
	uri := firstPlaylistURI(head)
	if uri == "" {
		return "undecidable"
	}
	// Master playlist: follow the first variant one level down.
	if strings.Contains(string(head), "#EXT-X-STREAM-INF") && depth == 0 {
		variant, ok := resolveReference(manifestURL, uri)
		if !ok {
			return "undecidable"
		}
		vhead, ok := p.fetchHead(ctx, variant, referer, userAgent, 8192)
		if !ok {
			return "undecidable"
		}
		return p.playlistSegmentVerdict(ctx, variant, referer, userAgent, vhead, depth+1)
	}
	segURL, ok := resolveReference(manifestURL, uri)
	if !ok {
		return "undecidable"
	}
	seg, ok := p.fetchHead(ctx, segURL, referer, userAgent, 512)
	if !ok {
		return "unreachable"
	}
	switch classifySegmentHead(seg) {
	case "image":
		return "image"
	case "video":
		return "video"
	default:
		return "undecidable"
	}
}

// resolveReference resolves a playlist URI reference against the manifest URL.
func resolveReference(manifestURL, ref string) (string, bool) {
	base, err := url.Parse(manifestURL)
	if err != nil {
		return "", false
	}
	r, err := url.Parse(ref)
	if err != nil {
		return "", false
	}
	return base.ResolveReference(r).String(), true
}

// probeHLSHead fetches a manifest and returns up to 8KB of its head
// alongside the reachability verdict. One fetch powers both the manifest
// probe ("does it look like HLS and answer from our egress?") and the
// image-segment guard ("is its first segment actually image data?").
func (p *AnimeXProvider) probeHLSHead(ctx context.Context, manifestURL, referer, userAgent string) (bool, []byte) {
	head, ok := p.fetchHead(ctx, manifestURL, referer, userAgent, 8192)
	if !ok {
		return false, nil
	}
	return strings.Contains(string(head), "#EXTM3U"), head
}

// ProbeHLS verifies a manifest URL serves a real HLS playlist. referer and
// userAgent mirror the provider headers the CDN expects; empty values are
// omitted.
func (p *AnimeXProvider) ProbeHLS(ctx context.Context, manifestURL, referer, userAgent string) bool {
	ok, _ := p.probeHLSHead(ctx, manifestURL, referer, userAgent)
	return ok
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
	// playeng r2 → bd CDN rewrite
	func(u string) string {
		if strings.HasPrefix(u, "https://playeng.animeapps.top/r2/") {
			rewritten := strings.Replace(u, "https://playeng.animeapps.top", "https://bd.aniwatchtv.site", 1)
			return strings.Replace(rewritten, "/r2", "", 1)
		}
		return u
	},
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
