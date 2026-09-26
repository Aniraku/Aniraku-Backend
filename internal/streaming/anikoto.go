package streaming

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
)

const (
	anikotoBase             = "https://anikototv.to"
	anikotoEmbedURLTemplate = "https://anivexa-api-tu4a.onrender.com/watch/anikoto/%s/%s/anikoto-%d"
)

// anikotoServers maps API server names to display names.
// The first server from the API becomes Niko, second becomes Momo.
var anikotoServers = [2]string{"Niko", "Momo"}

// AnikotoProvider scrapes anikototv.to for streaming sources.
// Two servers per episode: Niko (1st) and Momo (2nd).
// Supports sub and dub via separate data-ids per language.
type AnikotoProvider struct {
	client *http.Client
	log    zerolog.Logger
	// learnHost, when set, is called with hosts this provider itself verified:
	// probed stream manifests, subtitle tracks, and download links. The HTTP
	// layer registers it to feed the media-proxy CDN allowlist so rotated CDN
	// hostnames are allowed the moment they surface instead of 403ing.
	learnHost func(host string)

	// Last-good merged results for stale serving: megaplay edges flap on
	// minute timescales while files stay stable for hours, so a
	// minutes-old result plays fine and keeps Niko/Momo listed through
	// dead windows instead of flickering out.
	staleMu sync.Mutex
	stale   map[string]*animexStaleEntry
}

// SetHostLearner registers the verified-host callback.
func (p *AnikotoProvider) SetHostLearner(fn func(host string)) {
	p.learnHost = fn
}

// learnURLHost parses raw and, if it is a usable http(s) URL, hands its host
// to the registered learner.
func (p *AnikotoProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" ||
		(u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func NewAnikotoProvider(log zerolog.Logger) *AnikotoProvider {
	// Session cookie jar is REQUIRED: ajax/server?get= rejects cookieless
	// requests. All chain calls share this client, hence one session. The
	// transport is SSRF-guarded: upstream-controlled embed URLs cannot steer
	// this server at private addresses.
	jar, _ := cookiejar.New(nil)
	return &AnikotoProvider{
		client: &http.Client{Timeout: 45 * time.Second, Jar: jar, Transport: netguard.NewTransport()},
		log:    log,
		stale:  make(map[string]*animexStaleEntry),
	}
}

func (p *AnikotoProvider) Name() string { return "anikoto" }

func (p *AnikotoProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("anikoto search not implemented")
}

func (p *AnikotoProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("anikoto episode listing not implemented")
}

// FindEpisodeSource resolves Anikoto streams via megaplay.buzz directly
// (AniList-keyed Niko slot first, MAL-keyed Momo slot second): page fetch
// -> MegaPlay decrypt -> verified m3u8. Same return contract (Quality
// "auto", Verification "proxy"); dub works through the same URL scheme.
func (p *AnikotoProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}
	key := providerID + "/" + strconv.Itoa(episode) + "/" + lang
	// Fresh serve: a successful resolve stays playable for minutes (file
	// URLs are stable, tokens outlive the window), so repeat lookups —
	// /servers fires on every player open — return instantly without
	// re-contacting megaplay. This also stops the probe bursts that make
	// Cloudflare rate-limit this egress into fake "blocked" verdicts.
	if fresh := p.loadAnikotoFresh(key); fresh != nil {
		return fresh, nil
	}
	sr, err := p.megaplayDirectKeys(ctx, providerID, p.megaplayMALID(ctx, providerID), episode, lang)
	if sr != nil && len(sr.Sources) > 0 {
		p.storeAnikotoStale(key, sr)
		return sr, err
	}
	// Fresh resolve failed: serve the last good result while fresh instead
	// of dropping Niko/Momo for a minutes-long edge/rate flap.
	if err != nil {
		if stale := p.loadAnikotoStale(key); stale != nil {
			p.log.Info().Str("anilistId", providerID).Int("episode", episode).Msg("anikoto: serving stale result after failure")
			return stale, nil
		}
	}
	return sr, err
}

// storeAnikotoStale remembers a good merged result (deep-copied).
func (p *AnikotoProvider) storeAnikotoStale(key string, sr *SourceResult) {
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

// loadAnikotoStale returns the stored result while fresh.
func (p *AnikotoProvider) loadAnikotoStale(key string) *SourceResult {
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

// anikotoFreshTTL is how long a successful resolve is served without
// re-contacting megaplay. Short enough that a rotated file/token cannot
// outstay its welcome for long, long enough that a burst of /servers
// calls collapses into one upstream resolve.
const anikotoFreshTTL = 5 * time.Minute

// loadAnikotoFresh returns the last successful result within
// anikotoFreshTTL. Expired or missing entries return nil (caller resolves
// fresh); the wider animexStaleTTL window still backs failures up via
// loadAnikotoStale.
func (p *AnikotoProvider) loadAnikotoFresh(key string) *SourceResult {
	p.staleMu.Lock()
	defer p.staleMu.Unlock()
	e, ok := p.stale[key]
	if !ok || time.Since(e.fetchedAt) > anikotoFreshTTL {
		return nil
	}
	return e.res
}

// megaplayBase is the megaplay host. A var (not const) so tests can point
// the key-resolve flow at a fake server.
var megaplayBase = "https://megaplay.buzz"

// megaplayDirectKeys resolves both megaplay.buzz keys for an episode — the
// AniList-keyed page (Niko slot) and, when malID > 0, the MAL-keyed page
// (Momo slot) — merging them into one result with per-source slot names.
// Serial, not parallel: if the AniList attempt proves the host is
// rate-limiting us, the MAL page (same limiter) is skipped instead of
// doubling down on a limiting host. A MAL result identical to the AniList
// file collapses to Niko only.
func (p *AnikotoProvider) megaplayDirectKeys(ctx context.Context, anilistID string, malID, episode int, lang string) (*SourceResult, error) {
	aniURL := fmt.Sprintf("%s/stream/ani/%s/%d/%s", megaplayBase, anilistID, episode, lang)
	aniRes, aniErr := p.megaplayDirectURL(ctx, aniURL, "ani="+anilistID)
	var malRes *SourceResult
	if malID > 0 && !isMegaPlayRateLimited(aniErr) {
		if aniErr != nil && isMegaPlayMissing(aniErr) {
			p.log.Info().Str("anilistId", anilistID).Msg("megaplayDirect: not carried ani-keyed, trying MAL key")
		}
		malURL := fmt.Sprintf("%s/stream/mal/%d/%d/%s", megaplayBase, malID, episode, lang)
		var malErr error
		malRes, malErr = p.megaplayDirectURL(ctx, malURL, fmt.Sprintf("mal=%d", malID))
		if malErr != nil {
			p.log.Info().Err(malErr).Str("anilistId", anilistID).Msg("megaplayDirect: MAL key failed")
		}
	}
	if aniRes == nil && malRes == nil {
		if aniErr != nil {
			return nil, fmt.Errorf("megaplay direct: %w", aniErr)
		}
		return nil, fmt.Errorf("megaplay direct: no sources")
	}
	out := &SourceResult{}
	var names []string
	if aniRes != nil {
		out.Sources = append(out.Sources, aniRes.Sources...)
		names = append(names, "Niko")
		out.Headers = aniRes.Headers
		out.Intro, out.Outro = aniRes.Intro, aniRes.Outro
	}
	if malRes != nil && len(malRes.Sources) > 0 {
		if len(out.Sources) > 0 && malRes.Sources[0].URL == out.Sources[0].URL {
			p.log.Info().Str("anilistId", anilistID).Msg("megaplayDirect: MAL resolved the same file, keeping Niko only")
		} else {
			out.Sources = append(out.Sources, malRes.Sources...)
			names = append(names, "Momo")
			if out.Headers == nil {
				out.Headers = malRes.Headers
			}
			if out.Intro == nil {
				out.Intro = malRes.Intro
			}
			if out.Outro == nil {
				out.Outro = malRes.Outro
			}
		}
	}
	out.ServerNames = names
	return out, nil
}

// megaplayMALID resolves the MAL ID for an AniList ID: AniZip mappings
// first, AniList's own idMal second (AniZip misses some titles, and this
// path exists precisely for titles missing from one index or another).
func (p *AnikotoProvider) megaplayMALID(ctx context.Context, anilistID string) int {
	id, err := strconv.Atoi(anilistID)
	if err != nil || id <= 0 {
		return 0
	}
	if malID := tmdb.FetchMalID(ctx, p.client, id); malID > 0 {
		return malID
	}
	return fetchAniListMALID(ctx, p.client, id)
}

// isMegaPlayMissing reports the "title not carried under this key" failure:
// the page answers 200 but carries no data-id, so the embed resolver fails
// with "embed file id not found". Anything else (decrypt failure, blocked
// CDN, rate limit) is a different problem the other key will not fix — same
// host, usually the same edges.
func isMegaPlayMissing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "embed file id not found")
}

// megaplayOrigin derives the API origin for a megaplay embed URL. All
// megaplay.buzz API routes (getSourcesNew, getSources) live at the host
// root — including for nested player paths like /videojs/stream/..., whose
// naive /stream/ split would point at a non-existent
// /videojs/stream/getSourcesNew (404 on every edge, misreported as
// blocked). Other hosts keep the legacy split behavior.
func megaplayOrigin(embedURL string) string {
	if u, e := url.Parse(embedURL); e == nil && u.Host != "" {
		if u.Host == "megaplay.buzz" {
			return u.Scheme + "://" + u.Host
		}
		if i := strings.Index(embedURL, "/stream/"); i != -1 {
			return embedURL[:i]
		}
		return u.Scheme + "://" + u.Host
	}
	return embedURL
}

// MAL-keyed) through the shared MegaPlay decrypt + probe chain and builds
// the SourceResult. This is the former megaplayDirect body, unchanged.
// megaplayDirectURL resolves one megaplay.buzz /stream page (ani- or
// MAL-keyed) through the shared MegaPlay decrypt + probe chain and builds
// the SourceResult.
func (p *AnikotoProvider) megaplayDirectURL(ctx context.Context, embedURL, label string) (*SourceResult, error) {
	p.log.Info().Str("url", embedURL).Str("key", label).Msg("megaplayDirect: trying")
	// Segment-depth probe, not master-depth: megaplay spreads masters and
	// segments across edges with different Cloudflare policies (observed:
	// nexabloom master 200 while quavex.top segments 403 on every header
	// combination). A reachable master with blocked segments only produces
	// a spinning player, so variants are selected by segment reachability.
	file, tracks, inTs, outTs, origin, pOK, err := resolveMegaPlayPlayable(ctx, p.client, embedURL, func(f, o string) bool {
		return p.probeHLSegments(ctx, f, o)
	})
	if err != nil {
		p.log.Info().Err(err).Str("url", embedURL).Msg("megaplayDirect: resolve failed")
		// Unwrapped: megaplayDirect matches on this error to decide
		// whether the MAL-keyed page is worth trying; it adds context.
		return nil, err
	}
	if file == "" {
		return nil, fmt.Errorf("embed returned no file")
	}
	// Every CDN edge probed blocked from this egress — the media proxy would
	// 403 too. Return an error so the caller falls through to other providers.
	if !pOK {
		p.log.Info().Str("file", file).Msg("megaplay direct: no playable CDN edge from this egress")
		return nil, fmt.Errorf("CDN blocked manifest")
	}
	p.log.Info().Str("file", file).Str("origin", origin).Msg("megaplayDirect: resolved")
	p.learnURLHost(file)
	var subs []core.Subtitle
	for _, t := range tracks {
		if strings.TrimSpace(t.URL) == "" {
			continue
		}
		p.learnURLHost(t.URL)
		subs = append(subs, core.Subtitle{
			URL:   t.URL,
			Lang:  mapSubtitleLang(t.Label),
			Label: t.Label,
		})
	}
	return &SourceResult{
		Sources: []core.Source{{
			URL:          file,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		}},
		Headers: map[string]string{"Referer": strings.TrimSuffix(origin, "/") + "/"},
		Intro:   inTs,
		Outro:   outTs,
	}, nil
}

// embedVariant tags which anikoto server-list slot an embed came from.
// HD-1's "?s=tcdn" variant can resolve through a different CDN edge per
// fetch, so it is treated as a distinct server (Niko/Momo) instead of being
// collapsed with the base embed.
func embedVariant(embedURL string) string {
	if strings.Contains(embedURL, "s=tcdn") {
		return "tcdn"
	}
	return "base"
}

// dedupeSourcesByURL collapses sources that resolved to the same file from
// the same embed variant, keeping the copy with the richer subtitle track
// list so the server list never shows the same stream twice. Different
// variants (base vs ?s=tcdn) stay distinct so both server slots fill.
func dedupeSourcesByURL(sources []core.Source, variants []string) []core.Source {
	best := make(map[string]int, len(sources))
	out := make([]core.Source, 0, len(sources))
	for i, s := range sources {
		variant := "base"
		if i < len(variants) {
			variant = variants[i]
		}
		key := s.URL + "|" + variant
		if idx, ok := best[key]; ok {
			if len(s.Subtitles) > len(out[idx].Subtitles) {
				out[idx] = s
			}
			continue
		}
		best[key] = len(out)
		out = append(out, s)
	}
	return out
}

// megaplayTrack is one subtitle/caption entry from /stream/getSources.
type megaplayTrack struct {
	URL   string
	Label string
}

// MegaPlay encrypts the legacy getSources payload with a static AES-256-CBC
// key/IV embedded in its own player bundle (lib/newclient.min.js, TRUST_AES_
// KEY/TRUST_AES_IV, zero-padded to the block size). The ciphertext is
// base64url, plaintext is {"file": "..."}.
const (
	megaPlayEncKey = "i?LMTAx0Q6,:}50U"
	megaPlayEncIv  = "W0;27ToaUpl_P%'c"
)

func decryptMegaPlayEnc(enc string) (string, error) {
	t := strings.NewReplacer("-", "+", "_", "/").Replace(enc)
	if pad := len(t) % 4; pad != 0 {
		t += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.StdEncoding.DecodeString(t)
	if err != nil {
		return "", err
	}
	if len(raw) < 16 || len(raw)%aes.BlockSize != 0 {
		return "", fmt.Errorf("enc payload length %d is not AES-CBC sized", len(raw))
	}
	key := []byte(megaPlayEncKey)
	key = append(key, make([]byte, 32-len(key))...)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	pt := make([]byte, len(raw))
	cipher.NewCBCDecrypter(block, []byte(megaPlayEncIv)).CryptBlocks(pt, raw)
	pad := int(pt[len(pt)-1])
	if pad > 0 && pad <= aes.BlockSize && pad <= len(pt) {
		pt = pt[:len(pt)-pad]
	}
	var out struct {
		File string `json:"file"`
	}
	if err := json.Unmarshal(pt, &out); err != nil {
		return "", err
	}
	if out.File == "" {
		return "", fmt.Errorf("decrypted enc has no file")
	}
	return out.File, nil
}

// resolveEmbed decrypts a MegaPlay-style embed URL to a direct file URL.
// Package-level so the OGFLix provider can reuse the exact same chain.
func (p *AnikotoProvider) resolveEmbed(ctx context.Context, embedURL string) (file string, tracks []megaplayTrack, intro, outro *core.SkipTimestamp, origin string, err error) {
	return resolveMegaPlayEmbed(ctx, p.client, embedURL)
}

// resolveMegaPlayEmbed is the shared MegaPlay decrypt chain: #aHR0c... base64
// embeds, data-id -> /stream/getSourcesNew (plain JSON), then legacy
// getSources + AES enc decrypt. Anivexa extractEmbedSource parity: spoofed
// Referer (hianimes.re) when fetching the embed page, then getSources API
// call for the HLS manifest.
func resolveMegaPlayEmbed(ctx context.Context, client *http.Client, embedURL string) (file string, tracks []megaplayTrack, intro, outro *core.SkipTimestamp, origin string, err error) {
	origin = embedURL
	origin = megaplayOrigin(embedURL)
	if i := strings.Index(embedURL, "#aHR0c"); i != -1 {
		if raw, e := base64.StdEncoding.DecodeString(embedURL[i+1:]); e == nil {
			if s := strings.TrimSpace(string(raw)); strings.Contains(s, ".m3u8") {
				return s, nil, nil, nil, origin, nil
			}
		}
	}
	// Anivexa parity: fetch embed page with spoofed Referer (hianimes.re)
	// and Chrome 124 UA — the embed servers validate referer and reject
	// requests that come from anikototv.to or the embed origin itself.
	embedReq, err := http.NewRequestWithContext(ctx, http.MethodGet, embedURL, nil)
	if err != nil {
		return "", nil, nil, nil, origin, err
	}
	embedReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	embedReq.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	embedReq.Header.Set("Accept-Language", "en-US,en;q=0.9")
	embedReq.Header.Set("Referer", "https://hianimes.re/")
	// Cloudflare-blocked egress retries once through the worker proxy.
	embedResp, err := fetchWithWorkerFallback(ctx, client, embedReq)
	if err != nil {
		return "", nil, nil, nil, origin, fmt.Errorf("embed page fetch failed: %w", err)
	}
	defer embedResp.Body.Close()
	if embedResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(embedResp.Body, 4096))
		return "", nil, nil, nil, origin, fmt.Errorf("embed page returned HTTP %d: %s", embedResp.StatusCode, string(body[:min(len(body), 200)]))
	}
	pageBytes, err := io.ReadAll(io.LimitReader(embedResp.Body, 512*1024))
	if err != nil {
		return "", nil, nil, nil, origin, err
	}
	page := string(pageBytes)
	m := regexp.MustCompile(`data-id="([^"]+)"`).FindStringSubmatch(page)
	if len(m) < 2 || m[1] == "" {
		return "", nil, nil, nil, origin, fmt.Errorf("embed file id not found")
	}
	// The player rewrites stream/getSources -> stream/getSourcesNew (plain
	// JSON); the legacy endpoint returns an AES-encrypted "enc" blob instead
	// of sources.file. Try New first, fall back to legacy + decrypt.
	// MegaPlay's own JS appends the embed's ?s= variant (tcdn/bcdn) to every
	// getSources call — the variant picks the CDN edge (tcdn -> megap.shiora
	// .site, bcdn/default -> fetch.nexabloom.top). Passing it through is what
	// makes tcdn embeds resolve to an edge that does not block datacenter
	// IPs.
	embedEdge := ""
	if eu, eErr := url.Parse(embedURL); eErr == nil {
		embedEdge = eu.Query().Get("s")
	}
	fetchSources := func(endpoint string) ([]byte, error) {
		srcURL := fmt.Sprintf("%s/%s?id=%s&id=%s",
			strings.TrimSuffix(origin, "/"), endpoint, url.QueryEscape(m[1]), url.QueryEscape(m[1]))
		if embedEdge != "" {
			srcURL += "&s=" + url.QueryEscape(embedEdge)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srcURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
		req.Header.Set("Referer", strings.TrimSuffix(origin, "/")+"/")
		resp, err := fetchWithWorkerFallback(ctx, client, req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	}
	var data struct {
		Sources struct {
			File string `json:"file"`
		} `json:"sources"`
		Tracks []struct {
			File  string `json:"file"`
			Label string `json:"label"`
		} `json:"tracks"`
		Intro *core.SkipTimestamp `json:"intro"`
		Outro *core.SkipTimestamp `json:"outro"`
		Enc   string              `json:"enc"`
	}
	if body, err := fetchSources("stream/getSourcesNew"); err == nil {
		json.Unmarshal(body, &data)
	}
	if data.Sources.File == "" {
		body, err := fetchSources("stream/getSources")
		if err != nil {
			return "", nil, nil, nil, origin, err
		}
		if err := json.Unmarshal(body, &data); err != nil {
			return "", nil, nil, nil, origin, err
		}
		if data.Sources.File == "" && data.Enc != "" {
			file, decErr := decryptMegaPlayEnc(data.Enc)
			if decErr != nil {
				return "", nil, nil, nil, origin, fmt.Errorf("embed enc decrypt failed: %w", decErr)
			}
			data.Sources.File = file
		}
	}
	if data.Sources.File == "" {
		return "", nil, nil, nil, origin, fmt.Errorf("embed returned no file")
	}
	for _, t := range data.Tracks {
		label := strings.TrimSpace(t.Label)
		if label == "" {
			label = "English"
		}
		tracks = append(tracks, megaplayTrack{URL: t.File, Label: label})
	}
	return data.Sources.File, tracks, data.Intro, data.Outro, origin, nil
}

// megaplayWorkerBase is a public CORS worker used as a fallback egress
// for megaplay.buzz fetches when Cloudflare blocks our datacenter IP
// (429/403/5xx + challenge statuses). The worker's Cloudflare egress
// passes where ours is refused (verified live: real embed pages + working
// getSourcesNew through it). Used ONLY as a fallback after a direct
// failure — never primary — so normal traffic never depends on
// third-party generosity. ANIRAKU_MEGAPLAY_WORKER overrides without a
// rebuild; empty disables the fallback.
var megaplayWorkerBase = megaplayWorkerBaseFromEnv()

func megaplayWorkerBaseFromEnv() string {
	if v := os.Getenv("ANIRAKU_MEGAPLAY_WORKER"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "https://cors-proxy.brentkennetha.workers.dev"
}

// needsWorkerRetry reports whether a failed fetch is worth one retry
// through the worker proxy: Cloudflare statuses (429/403 rate-limit and
// challenge pages, 5xx gateway/upstream errors) and transport errors.
// Anything else (including a cancelled context, and 200s whose bodies
// fail later parsing) is returned as-is.
func needsWorkerRetry(resp *http.Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return true
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusForbidden,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout,
		520, 521, 522, 523, 524:
		return true
	}
	return false
}

// fetchWithWorkerFallback performs req directly, retrying once through the
// CORS worker proxy when Cloudflare blocks our egress. On worker failure
// the ORIGINAL outcome is returned, so callers never see a worse error
// than they would have without the fallback.
func fetchWithWorkerFallback(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	resp, err := client.Do(req)
	if ctx.Err() != nil || !needsWorkerRetry(resp, err) || megaplayWorkerBase == "" {
		return resp, err
	}
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
	}
	wurl := megaplayWorkerBase + "/?url=" + url.QueryEscape(req.URL.String())
	wreq, werr := http.NewRequestWithContext(ctx, req.Method, wurl, nil)
	if werr != nil {
		return resp, err
	}
	for k, vv := range req.Header {
		for _, v := range vv {
			wreq.Header.Add(k, v)
		}
	}
	wresp, werr := client.Do(wreq)
	if werr != nil || wresp.StatusCode != http.StatusOK {
		if wresp != nil && wresp.Body != nil {
			io.Copy(io.Discard, io.LimitReader(wresp.Body, 4096))
			wresp.Body.Close()
		}
		return resp, err
	}
	return wresp, nil
}

// megaPlayEdgeVariants returns the s-param variants to try for an embed,
// with the embed's own variant first. MegaPlay's getSources picks the CDN
// edge server-side and the mapping rotates — the same embed can hand back a
// datacenter-blocked edge (bcdn/default -> fetch.nexabloom.top) one call and
// an open edge (tcdn -> shiora/akirax/mikora) the next.
func megaPlayEdgeVariants(embedURL string) []string {
	own := ""
	if eu, eErr := url.Parse(embedURL); eErr == nil {
		own = eu.Query().Get("s")
	}
	variants := []string{own}
	for _, v := range []string{"tcdn", "bcdn", ""} {
		dup := false
		for _, e := range variants {
			if e == v {
				dup = true
				break
			}
		}
		if !dup {
			variants = append(variants, v)
		}
	}
	return variants
}

// resolveMegaPlayPlayable resolves a MegaPlay embed across every CDN edge
// until one decrypts AND passes the probe callback, so a rotated blocked
// edge never demotes a server another edge would serve fine. probe may be
// nil to accept the first successful decrypt. probedOK reports whether the
// returned file passed the probe; when every edge decrypts but none probes
// clean, the first decrypt is returned with probedOK=false so the caller can
// still surface it (demoted) instead of dropping a provider-confirmed source.
func resolveMegaPlayPlayable(ctx context.Context, client *http.Client, embedURL string, probe func(file, origin string) bool) (file string, tracks []megaplayTrack, intro, outro *core.SkipTimestamp, origin string, probedOK bool, err error) {
	// Strip the s param so each variant can be requested explicitly.
	base := embedURL
	if eu, eErr := url.Parse(embedURL); eErr == nil && eu.Query().Get("s") != "" {
		q := eu.Query()
		q.Del("s")
		eu.RawQuery = q.Encode()
		base = eu.String()
	}
	join := "?"
	if strings.Contains(base, "?") {
		join = "&"
	}

	var (
		fbFile, fbOrigin string
		fbTracks         []megaplayTrack
		fbIn, fbOut      *core.SkipTimestamp
		firstDecErr      error
	)
	for _, v := range megaPlayEdgeVariants(embedURL) {
		u := base
		if v != "" {
			u = base + join + "s=" + url.QueryEscape(v)
		}
		f, tr, in, out, orig, dErr := resolveMegaPlayEmbed(ctx, client, u)
		if dErr != nil || f == "" {
			if firstDecErr == nil {
				firstDecErr = dErr
			}
			// All variants hit the same megaplay.buzz rate limiter from
			// the same egress: a 429 on one variant means the rest will
			// 429 too. Break instead of burning ~3 requests per
			// remaining variant on a verdict that's already known, and
			// record the rate limit (even over an earlier different
			// failure) so callers know the whole host is limiting us —
			// not that the title is missing under this key.
			if isMegaPlayRateLimited(dErr) {
				if firstDecErr == nil || !isMegaPlayRateLimited(firstDecErr) {
					firstDecErr = dErr
				}
				break
			}
			continue
		}
		if probe == nil || probe(f, orig) {
			return f, tr, in, out, orig, true, nil
		}
		if fbFile == "" {
			fbFile, fbTracks, fbIn, fbOut, fbOrigin = f, tr, in, out, orig
		}
	}
	if fbFile != "" {
		return fbFile, fbTracks, fbIn, fbOut, fbOrigin, false, nil
	}
	if firstDecErr != nil {
		return "", nil, nil, nil, "", false, firstDecErr
	}
	return "", nil, nil, nil, "", false, fmt.Errorf("megaplay: no sources on any edge")
}

// isMegaPlayRateLimited reports whether an embed-resolve error is the host
// rate-limiting this egress (HTTP 429). resolveMegaPlayEmbed surfaces the
// embed-page status verbatim ("embed page returned HTTP 429: ..."), so a
// substring match is precise — no other error in this chain carries a bare
// 429.
func isMegaPlayRateLimited(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "rate limit")
}

// probeHLSegments verifies a resolved master playlist is actually playable
// from this egress, end to end: master -> first media playlist -> first
// segment must all serve media bytes. See megaplayDirectURL for why
// master-depth is not enough. Relative segment/playlist URLs resolve
// against their parent, mirroring player behavior.
func (p *AnikotoProvider) probeHLSegments(ctx context.Context, fileURL, origin string) bool {
	return probeSegmentsStrict(ctx, p.client, fileURL, origin, "")
}

// firstPlaylistURL returns the first non-directive URL in a playlist,
// resolved against the playlist's own URL like a player resolves it.
func firstPlaylistURL(body, parent string) string {
	base, err := url.Parse(parent)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if ref, err := url.Parse(line); err == nil {
			return base.ResolveReference(ref).String()
		}
	}
	return ""
}

// segmentBytesPlayable judges raw segment bytes by magic, never by
// extension or host: MPEG-TS sync (0x47), ID3 timed-metadata prefix, fmp4
// boxes (ftyp/moof), or a nested playlist all pass. Cloudflare block pages
// (HTML) and tiny decoy payloads (1x1 PNG cloaks served instead of video)
// fail.
func segmentBytesPlayable(head []byte) bool {
	if len(head) == 0 {
		return false
	}
	sample := string(head)
	if len(sample) > 4096 {
		sample = sample[:4096]
	}
	if strings.Contains(strings.ToLower(sample), "<html") {
		return false
	}
	if head[0] == 0x47 {
		return true
	}
	if len(head) >= 3 && head[0] == 'I' && head[1] == 'D' && head[2] == '3' {
		return true
	}
	for _, magic := range []string{"ftyp", "moof", "#EXTM3U"} {
		if strings.Contains(sample, magic) {
			return true
		}
	}
	return false
}

// mapSubtitleLang maps a track label to a two-letter language code.
func mapSubtitleLang(label string) string {
	first := strings.ToLower(strings.Split(strings.TrimSpace(label), " ")[0])
	if len(first) == 2 {
		ok := true
		for _, r := range first {
			if r < 'a' || r > 'z' {
				ok = false
			}
		}
		if ok {
			return first
		}
	}
	switch first {
	case "english", "en":
		return "en"
	case "spanish":
		return "es"
	case "french":
		return "fr"
	case "german":
		return "de"
	case "portuguese":
		return "pt"
	case "arabic":
		return "ar"
	case "hindi":
		return "hi"
	default:
		return "en"
	}
}

// anilistMeta carries the titles Anivexa searches (english, romaji, synonyms).
type anilistMeta struct {
	english  string
	romaji   string
	synonyms []string
	episodes int
	format   string
}

func (m anilistMeta) keywords() []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range append([]string{m.english, m.romaji}, m.synonyms...) {
		if t = strings.TrimSpace(t); t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
		if len(out) >= 5 {
			break
		}
	}
	return out
}

// showModifiers are the title words that mark an OVA/movie/spin-off entry;
// a candidate carrying one the target title lacks is heavily penalized
// (Anivexa scoreCandidate parity).
var showModifiers = []string{
	"ova", "movie", "special", "specials", "tales", "journal", "part", "season", "kanwa", "spin-off", "spinoff", "theatre",
}

// normTitle lowercases and trims a title for fuzzy comparison.
func normTitle(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// absInt returns the absolute value of an integer.
func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// scoreShowCandidate is the Anivexa additive scorer, ported exactly: exact
// title matches dominate (+1000/+900/+800), partial matches add smaller
// bonuses, mismatched modifiers subtract heavily, and length difference is
// a mild tiebreaker. No threshold — the top score wins.
func scoreShowCandidate(cand titleCand, meta anilistMeta) float64 {
	score := 0.0
	candName := normTitle(cand.name)
	candJp := normTitle(cand.jp)
	candSlug := normTitle(cand.slug)
	normEn := normTitle(meta.english)
	normRom := normTitle(meta.romaji)

	if normEn != "" && candName == normEn {
		score += 1000
	}
	if normRom != "" && candName == normRom {
		score += 900
	}
	if normRom != "" && candJp == normRom {
		score += 800
	}

	targetText := strings.ToLower(meta.english + " " + meta.romaji + " " + strings.Join(meta.synonyms, " "))
	for _, mod := range showModifiers {
		candHas := strings.Contains(candName, mod) || strings.Contains(candSlug, mod)
		targetHas := strings.Contains(targetText, mod)
		if candHas && !targetHas {
			score -= 300
		}
	}

	titles := append([]string{meta.english, meta.romaji}, meta.synonyms...)
	for _, t := range titles {
		normT := normTitle(t)
		if len(normT) < 3 {
			continue
		}
		switch {
		case candName == normT:
			score += 200
		case strings.HasPrefix(candName, normT) || strings.HasPrefix(normT, candName):
			score += 80
		case strings.Contains(candName, normT) || strings.Contains(normT, candName):
			score += 40
		}
		if candJp != "" && candJp == normT {
			score += 100
		}
	}

	refLen := normEn
	if refLen == "" {
		refLen = normRom
	}
	score -= float64(absInt(len(candName)-len(refLen))) * 2
	return score
}

// filterLatinKeywords keeps at most max Latin-script queries, preserving
// order (English and romaji come first from keywords()). Non-Latin queries
// can never match a Latin-script catalog — verified live against
// anikototv.to — so they are dropped before any request is made. Shared by
// the Anikoto and OGFLix resolvers.
func filterLatinKeywords(keywords []string, max int) []string {
	out := make([]string, 0, len(keywords))
	seen := map[string]bool{}
	for _, k := range keywords {
		if len(out) >= max {
			break
		}
		if k = strings.TrimSpace(k); k != "" && !seen[k] && isLatinTitle(k) {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// isLatinTitle reports whether s is usable as an anikoto.tv filter query:
// the catalog is Latin-script, so a query containing CJK, Arabic, Hebrew,
// Thai or Indic scripts can never match and only wastes a request (plus
// retry sleeps during site flaps). Accented Latin, Cyrillic and Greek pass
// through conservatively and fetch exactly as today.
func isLatinTitle(s string) bool {
	if strings.TrimSpace(s) == "" {
		return false
	}
	for _, r := range s {
		if unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul,
			unicode.Arabic, unicode.Hebrew, unicode.Thai, unicode.Devanagari, unicode.Tamil,
			unicode.Telugu, unicode.Kannada, unicode.Malayalam, unicode.Bengali,
			unicode.Myanmar, unicode.Khmer, unicode.Lao) {
			return false
		}
	}
	return true
}

type titleCand struct {
	slug  string
	name  string
	jp    string
	score float64
}

// fetchAniListMetaFor is the shared AniList meta lookup. Primary source is
// AniList GraphQL; when AniList is down — it has recurring global outages
// (403 "temporarily disabled", 429 rate limits) — it falls back to AniZip,
// which mirrors the same mappings keyed by AniList ID.
func fetchAniListMetaFor(ctx context.Context, client *http.Client, anilistID string) (anilistMeta, error) {
	meta, err := fetchAniListMetaUpstream(ctx, client, anilistID)
	if err == nil {
		return meta, nil
	}
	id, idErr := strconv.Atoi(anilistID)
	if idErr != nil {
		return meta, fmt.Errorf("anilist meta failed (%v); anizip fallback skipped (invalid id)", err)
	}
	az, azErr := tmdb.FetchAniZipMediaMeta(ctx, client, id)
	if azErr != nil {
		return meta, fmt.Errorf("anilist meta failed (%v) and anizip fallback failed (%v)", err, azErr)
	}
	return anilistMeta{
		english:  az.English,
		romaji:   az.Romaji,
		synonyms: az.Synonyms,
		episodes: az.EpisodeCount,
	}, nil
}

// fetchAniListMetaUpstream is the direct AniList GraphQL lookup. It reports
// real upstream failures (HTTP status, GraphQL error payload) instead of
// misreporting them as "no title".
func fetchAniListMetaUpstream(ctx context.Context, client *http.Client, anilistID string) (anilistMeta, error) {
	var out anilistMeta
	query := `{"query":"{ Media(id:` + anilistID + `,type:ANIME){title{english romaji} synonyms episodes format} }"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.anilist.co",
		strings.NewReader(query))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024))
	if err != nil {
		return out, err
	}

	var result struct {
		Errors []struct {
			Message string `json:"message"`
			Status  int    `json:"status"`
		} `json:"errors"`
		Data struct {
			Media struct {
				Title struct {
					English *string `json:"english"`
					Romaji  *string `json:"romaji"`
				} `json:"title"`
				Synonyms []string `json:"synonyms"`
				Episodes *int     `json:"episodes"`
				Format   *string  `json:"format"`
			} `json:"Media"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return out, fmt.Errorf("anilist returned undecodable body (HTTP %d): %w", resp.StatusCode, err)
	}
	if len(result.Errors) > 0 {
		return out, fmt.Errorf("anilist graphql error (HTTP %d): %s", resp.StatusCode, result.Errors[0].Message)
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("anilist returned HTTP %d", resp.StatusCode)
	}

	if result.Data.Media.Title.English != nil {
		out.english = strings.TrimSpace(*result.Data.Media.Title.English)
	}
	if result.Data.Media.Title.Romaji != nil {
		out.romaji = strings.TrimSpace(*result.Data.Media.Title.Romaji)
	}
	for _, s := range result.Data.Media.Synonyms {
		if s = strings.TrimSpace(s); s != "" {
			out.synonyms = append(out.synonyms, s)
		}
	}
	if result.Data.Media.Episodes != nil {
		out.episodes = *result.Data.Media.Episodes
	}
	if result.Data.Media.Format != nil {
		out.format = *result.Data.Media.Format
	}
	if out.english == "" && out.romaji == "" {
		return out, fmt.Errorf("no title found for anilistId=%s", anilistID)
	}
	return out, nil
}
