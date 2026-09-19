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
// cannot be parsed. Priority matches the plyr subProviders list.
var animexProviders = []string{"beep", "yuki", "neko", "sora", "loli"}

// animexProviderNames maps provider IDs to human-readable server names.
var animexProviderNames = map[string]string{
	"yuki": "Nthing",
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

// FindEpisodeSource resolves an AnimeX episode stream by establishing a session,
// calling the plyr API, decoding XOR+base64url proxy URLs, and returning
// direct m3u8 + subtitles.
//
// The AnimeX API expects a slug ID (e.g. "bleach-thousand-year-blood-war-the-calamity-ts6ov"),
// NOT the numeric AniList ID. This method fetches the plyr page to extract the
// correct slug from the embedded SvelteKit data.
func (p *AnimeXProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}

	// Fetch the plyr page to get the show slug and the authoritative
	// per-language provider list (the plyr page is what the site itself
	// uses — episodes that only exist on a provider missing from our static
	// list, e.g. loli, are exactly the ones that "don't show up").
	slug, providers, err := p.fetchPlyrData(ctx, anilistID, episode, lang)
	if err != nil {
		// One short-backoff retry: the plyr page intermittently serves the
		// Cloudflare challenge.
		time.Sleep(time.Second)
		slug, providers, err = p.fetchPlyrData(ctx, anilistID, episode, lang)
	}
	if err != nil {
		p.log.Debug().Err(err).Msg("animex: failed to read plyr page, using anilistId and static providers")
		slug = anilistID
		providers = animexProviders
	}
	p.log.Debug().Str("anilistId", anilistID).Str("slug", slug).Strs("providers", providers).Msg("animex: resolved plyr data")

	// Establish Cloudflare clearance session first
	if err := p.ensureSession(ctx); err != nil {
		p.log.Debug().Err(err).Msg("animex: session establishment failed, trying anyway")
	}

	// Try each provider in order until one returns sources. A Cloudflare
	// challenge on the API (intermittent 403 HTML) is transient: invalidate
	// the cached plyr session and retry the same provider once with fresh
	// clearance. Sources the provider confirmed are never dropped later —
	// this loop is the only gate.
	var lastErr error
	sessionRetried := false
	for _, providerID := range providers {
		result, err := p.resolveProvider(ctx, slug, episode, lang, providerID)
		if err != nil {
			var challenge *animexChallengeError
			if errors.As(err, &challenge) && !sessionRetried {
				sessionRetried = true
				p.log.Info().Str("provider", providerID).Msg("animex: cloudflare challenge, refreshing session and retrying")
				p.invalidateSession()
				if sessErr := p.ensureSession(ctx); sessErr != nil {
					p.log.Debug().Err(sessErr).Msg("animex: session refresh failed")
				}
				result, err = p.resolveProvider(ctx, slug, episode, lang, providerID)
			}
		}
		if err != nil {
			lastErr = err
			p.log.Debug().Err(err).Str("provider", providerID).Msg("animex: provider failed")
			continue
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("animex: no sources found for any provider")
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
		// instead of surfacing a server that can only produce 502s.
		if !p.ProbeHLS(ctx, directURL, referer, userAgent) {
			p.log.Info().Str("provider", providerID).Str("url", directURL).Msg("animex: manifest blocked from this egress, dropping source")
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

// ProbeHLS verifies a manifest URL serves a real HLS playlist. referer and
// userAgent mirror the provider headers the CDN expects; empty values are
// omitted.
func (p *AnimeXProvider) ProbeHLS(ctx context.Context, manifestURL, referer, userAgent string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return false
	}
	ua := userAgent
	if ua == "" {
		ua = animexPlayerUA
	}
	req.Header.Set("User-Agent", ua)
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	head, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return err == nil && strings.Contains(string(head), "#EXTM3U")
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
