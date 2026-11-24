package streaming

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/crypto/pbkdf2"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// FlixCloudProvider resolves the Reanime server list to DIRECT HLS streams.
// The old flow handed the frontend an embed page (flixcloud.cc/e/{id}?v=N);
// the reverse-engineered chain below reproduces what that page's player
// does, so FlixCloud plays like every other direct provider:
//
//  1. GET flixcloud.cc/e/{accessId}?v={n}        — SvelteKit SSR data island
//     carries obfuscation_seed, obfuscated_crypto_data (per-page field
//     names + AES-256-CBC blob) and w_payload (a ~350-byte randomized
//     WASM module).
//  2. Field names are derived by sha256-chaining the seed
//     (flixcloudFieldNames), locating the key frag 1/IV inside the crypto
//     blob and the playback-token + key frag 2 among the top-level fields.
//  3. GET /api/m3u8/{token}                      — SINGLE-USE token minted
//     at page render; answers {sha256(token+"vid")[0:10]: encrypted URL,
//     sha256(token+"key")[0:10]: key frag 3}.
//  4. The WASM module mixes frag1+frag2+frag3+seed-int (flixcloudMix) into
//     a 32-byte password; PBKDF2-HMAC-SHA256 (salt=seed, 1000 iters) →
//     XOR seed charwise → SHA-256 yields the AES-256 key; AES-CBC decrypt
//     of the blob is the master.m3u8 URL on fetch{8,9,10}.flixcloud.cc.
//  5. Every flixcloud m3u8 body is base64(XOR(plaintext, pk)) where pk is
//     the module's _c() output (static across pages). The proxy decodes
//     that wrapper (DecodeFlixcloudPlaylist) before rewriting.
//
// Servers are named by dataLink order: Yuta (v=1), Syota (v=2), Mike (v=3).
type FlixCloudProvider struct {
	client      *http.Client
	log         zerolog.Logger
	reanimeBase string
	flixBase    string

	// lastEmbedURL is the most recent live embed page (for the key
	// refresher); lastEmbedMu guards both it and the refresher installation.
	lastEmbedMu  sync.Mutex
	lastEmbedURL string

	// resolved caches direct master URLs per embed URL. The master URL has
	// no signature and its CDN copy is cached, so one decrypt serves many
	// sessions; the /api/m3u8 token behind it is single-use, which is why
	// resolution is cached rather than repeated per player request.
	resolvedMu sync.Mutex
	resolved   map[string]flixcloudResolve
}

const flixcloudResolveTTL = 45 * time.Minute

type flixcloudResolve struct {
	url       string
	subs      []core.Subtitle
	expiresAt time.Time
}

func NewFlixCloudProvider(log zerolog.Logger) *FlixCloudProvider {
	reanimeBase := strings.TrimRight(os.Getenv("ANIRAKU_REANIME_BASE"), "/")
	if reanimeBase == "" {
		reanimeBase = "https://reanime.to"
	}
	flixBase := strings.TrimRight(os.Getenv("ANIRAKU_FLIXCLOUD_BASE"), "/")
	if flixBase == "" {
		flixBase = "https://flixcloud.cc"
	}
	return &FlixCloudProvider{
		client: &http.Client{
			Timeout:   45 * time.Second,
			Transport: netguard.NewTransport(),
		},
		log:         log,
		reanimeBase: reanimeBase,
		flixBase:    flixBase,
		resolved:    map[string]flixcloudResolve{},
	}
}

func (p *FlixCloudProvider) Name() string { return "flixcloud" }

func (p *FlixCloudProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("flixcloud search not implemented")
}

func (p *FlixCloudProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("flixcloud episode listing not implemented")
}

// fetchReanime GETs the Reanime server list and returns the raw JSON body,
// or nil when it could not be obtained. A single retry with a short backoff
// absorbs transient failures, including the intermittent Cloudflare JS
// challenge this endpoint serves to datacenter egress ("Just a moment..."
// HTML). Every failure logs at Warn: this provider's skips used to be
// Debug-only, which made a fully-broken FlixCloud invisible in production.
func (p *FlixCloudProvider) fetchReanime(ctx context.Context, providerID string, episode int) []byte {
	reanimeURL := fmt.Sprintf("%s/api/flix/%s/%d", p.reanimeBase, providerID, episode)
	const attempts = 2
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(1500 * time.Millisecond):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reanimeURL, nil)
		if err != nil {
			p.log.Warn().Err(err).Str("anilistId", providerID).Msg("flixcloud: reanime request build failed")
			return nil
		}
		req.Header.Set("User-Agent", browserUA)

		resp, err := p.client.Do(req)
		if err != nil {
			p.log.Warn().Err(err).Str("anilistId", providerID).Int("episode", episode).
				Int("attempt", attempt).Msg("flixcloud: reanime request failed")
			continue
		}

		if resp.StatusCode != http.StatusOK {
			p.log.Warn().Int("status", resp.StatusCode).Str("anilistId", providerID).
				Int("attempt", attempt).Msg("flixcloud: reanime returned non-200")
			resp.Body.Close()
			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			p.log.Warn().Err(err).Str("anilistId", providerID).Int("attempt", attempt).
				Msg("flixcloud: reanime body read failed")
			continue
		}

		trimmed := strings.TrimSpace(string(body))
		if trimmed == "" || trimmed[0] != '{' {
			// Not JSON: the Cloudflare challenge page or an error interstitial.
			p.log.Warn().Str("anilistId", providerID).Int("attempt", attempt).
				Str("body", truncateForLog(trimmed, 160)).
				Msg("flixcloud: reanime returned non-JSON (challenge or interstitial?)")
			continue
		}
		return body
	}
	return nil
}

// truncateForLog keeps log lines bounded while still showing the part of an
// upstream response that identifies the failure (challenge title, error text).
func truncateForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

type reanimeServer struct {
	ID         string `json:"$id"`
	ServerName string `json:"serverName"`
	DataLink   string `json:"dataLink"`
	DataType   string `json:"dataType"`
}

type reanimeResponse struct {
	Success bool            `json:"success"`
	Servers []reanimeServer `json:"servers"`
}

// flixcloudPage fetches one embed page and extracts the fields the
// decryption chain needs straight from the SSR markup. The island's keys
// mix bare identifiers with quoted ones (and values contain colons, commas
// and hyphens), so every extraction is anchored on the literal field name
// rather than a whole-object parse — each field name is either fixed
// (obfuscation_seed, w_payload, obfuscated_crypto_data, subtitles) or
// derived from the seed before the lookup (flixcloudFieldNames).
type flixcloudPageData struct {
	seed       string
	wasmB64    string
	cryptoBlob string // the od_{...} object body (kf/ivf/metadata inside)
	kfField    string
	ivfField   string
	frag2B64   string
	token      string
	subs       []core.Subtitle
	embedID    string
}

var flixcloudDataIslandRe = regexp.MustCompile(`(?s)data:\{(.+?)\},uses:`)

func flixcloudIslandString(island, field string) string {
	// SvelteKit quotes keys that start with a digit (the hashed field names
	// do) and leaves letter-leading keys bare — allow both.
	re := regexp.MustCompile(`"?` + regexp.QuoteMeta(field) + `"?:\s*"([^"]*)"`)
	if m := re.FindStringSubmatch(island); m != nil {
		return m[1]
	}
	return ""
}

func (p *FlixCloudProvider) flixcloudPage(ctx context.Context, embedURL, referer string) (*flixcloudPageData, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, embedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", referer)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed page fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embed page status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, fmt.Errorf("embed page read: %w", err)
	}

	m := flixcloudDataIslandRe.FindSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("embed page has no SSR data island (challenge page?)")
	}
	island := string(m[1])

	seed := flixcloudIslandString(island, "obfuscation_seed")
	wasmB64 := flixcloudIslandString(island, "w_payload")
	if seed == "" || wasmB64 == "" {
		return nil, fmt.Errorf("data island missing obfuscation_seed/w_payload")
	}
	keyField, ivfField, _, _, _, tokenField, keyFrag2Field := flixcloudFieldNames(seed)

	// The crypto blob lives at obfuscated_crypto_data:{cd_..:{ad_..:[{od_..:{
	// ...body...}}]}}; the od body ends right before its metadata object's
	// closing brace chain, so anchor on the metadata tail.
	blobRe := regexp.MustCompile(`(?s)obfuscated_crypto_data:\{cd_[0-9a-f]{8}:\{ad_[0-9a-f]{8}:\[\{od_[0-9a-f]{8}:\{(.*)\}\}\]\}`)
	bm := blobRe.FindStringSubmatch(island)
	if bm == nil {
		return nil, fmt.Errorf("obfuscated_crypto_data blob not found")
	}

	page := &flixcloudPageData{
		seed:       seed,
		wasmB64:    wasmB64,
		cryptoBlob: bm[1],
		kfField:    keyField,
		ivfField:   ivfField,
		frag2B64:   flixcloudIslandString(island, keyFrag2Field),
		token:      flixcloudIslandString(island, tokenField),
		embedID:    embedURL,
	}
	if page.frag2B64 == "" || page.token == "" {
		return nil, fmt.Errorf("data island missing token/frag2 fields")
	}

	// Stylistic ASS/SRT subtitles ride along in the same island; surface the
	// English ones on the source so the app can offer them like ani.pm's.
	subRe := regexp.MustCompile(`\{url:"([^"]+)",language:"([^"]*)",format:"([^"]*)",default:(true|false)\}`)
	for _, sm := range subRe.FindAllStringSubmatch(island, -1) {
		u, lang, format := sm[1], sm[2], sm[3]
		if u == "" || lang == "" {
			continue
		}
		if !strings.Contains(strings.ToLower(lang), "english") {
			continue
		}
		label := lang
		if format == "ass" {
			label += " (styled)"
		}
		page.subs = append(page.subs, core.Subtitle{URL: u, Lang: "en", Label: label})
	}
	return page, nil
}

// flixcloudFieldNames reproduces the player's ar(): three sha256 iterations
// over seed+digit locate the crypto-blob field names, a second chain from
// the first hash locates key frag 2's top-level field, and the token field
// straddles both halves of the first hash.
func flixcloudFieldNames(seed string) (keyField, ivField, container, array, object, tokenField, keyFrag2Field string) {
	shaHex := func(s string) string {
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:])
	}
	e := seed
	for i := 0; i < 3; i++ {
		e = shaHex(e + strconv.Itoa(i))
	}
	a := e
	for i := 0; i < 3; i++ {
		a = shaHex(a + strconv.Itoa(i))
	}
	return "kf_" + e[8:16], "ivf_" + e[16:24],
		"cd_" + e[24:32], "ad_" + e[32:40], "od_" + e[40:48],
		e[48:64] + "_" + e[56:64],
		a[0:16] + "_" + a[16:24]
}

// flixcloudResolveEmbed runs the full decrypt chain for one embed URL and
// returns the direct master playlist URL plus the page's subtitle list.
func (p *FlixCloudProvider) flixcloudResolveEmbed(ctx context.Context, embedURL string) (string, []core.Subtitle, error) {
	page, err := p.flixcloudPage(ctx, embedURL, p.reanimeBase)
	if err != nil {
		return "", nil, err
	}
	master, subs, err := p.decryptEmbed(ctx, page)
	if err != nil {
		return "", nil, err
	}
	return master, subs, nil
}

// decryptEmbed performs steps 2-4 of the chain against an already-fetched
// page. It is separate from flixcloudResolveEmbed so a single-use token
// rejection (410 invalid_or_used_token) can re-run the whole flow on a
// FRESH page without re-fetching the first page's dead token.
func (p *FlixCloudProvider) decryptEmbed(ctx context.Context, page *flixcloudPageData) (string, []core.Subtitle, error) {
	frag1B64 := flixcloudIslandString(page.cryptoBlob, page.kfField)
	ivB64 := flixcloudIslandString(page.cryptoBlob, page.ivfField)
	if frag1B64 == "" || ivB64 == "" {
		return "", nil, fmt.Errorf("crypto blob missing kf/iv fields")
	}
	frag2B64, token := page.frag2B64, page.token

	// Single-use token: minted at page render, consumed by the first call.
	tokenResp := p.fetchPlaybackToken(ctx, token)
	if tokenResp == nil {
		return "", nil, fmt.Errorf("playback token fetch failed")
	}
	shaHex := func(s string) string {
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:])
	}
	ctB64, _ := tokenResp[shaHex(token + "vid")[0:10]].(string)
	frag3B64, _ := tokenResp[shaHex(token + "key")[0:10]].(string)
	if ctB64 == "" || frag3B64 == "" {
		return "", nil, fmt.Errorf("token response incomplete (vid/key fields)")
	}

	frag1, err := base64.StdEncoding.DecodeString(frag1B64)
	if err != nil {
		return "", nil, fmt.Errorf("frag1 decode: %w", err)
	}
	frag2, err := base64.StdEncoding.DecodeString(frag2B64)
	if err != nil {
		return "", nil, fmt.Errorf("frag2 decode: %w", err)
	}
	frag3, err := base64.StdEncoding.DecodeString(frag3B64)
	if err != nil {
		return "", nil, fmt.Errorf("frag3 decode: %w", err)
	}
	ct, err := base64.StdEncoding.DecodeString(ctB64)
	if err != nil {
		return "", nil, fmt.Errorf("url blob decode: %w", err)
	}
	iv, err := base64.StdEncoding.DecodeString(ivB64)
	if err != nil {
		return "", nil, fmt.Errorf("iv decode: %w", err)
	}
	wasmPayload, err := base64.StdEncoding.DecodeString(page.wasmB64)
	if err != nil {
		return "", nil, fmt.Errorf("w_payload decode: %w", err)
	}

	// k = first 8 seed chars as hex → the 32-bit seed integer _s stores.
	k64, err := strconv.ParseUint(page.seed[:8], 16, 64)
	if err != nil {
		return "", nil, fmt.Errorf("seed int parse: %w", err)
	}

	mod, err := flixcloudInstantiate(wasmPayload)
	if err != nil {
		return "", nil, fmt.Errorf("wasm instantiate: %w", err)
	}
	// Same instance also yields this render's playlist XOR key. The key is
	// per embed-page render (NOT a global build constant), so it is bound to
	// the master URL below: the proxy decodes that URL's body with exactly
	// this key even after the global candidate set has evicted it.
	var playlistKey []byte
	if pk, err := flixcloudPlaylistKey(mod); err == nil {
		playlistKey = pk
		flixcloudStorePlaylistKey(pk)
	}

	mixed, err := flixcloudMix(mod, frag1, frag2, frag3, uint32(k64))
	if err != nil {
		return "", nil, err
	}

	// PBKDF2(HMAC-SHA256, pw=mixed, salt=seed, 1000 iters, 32 bytes) →
	// XOR seed chars → SHA-256 = AES-256-CBC key (the player's exact chain).
	derived := pbkdf2.Key(mixed, []byte(page.seed), 1000, 32, sha256.New)
	xored := make([]byte, len(derived))
	copy(xored, derived)
	for i := range xored {
		xored[i] ^= page.seed[i%len(page.seed)]
	}
	aesKey := sha256.Sum256(xored)

	block, err := aes.NewCipher(aesKey[:])
	if err != nil {
		return "", nil, err
	}
	mode := cipher.NewCBCDecrypter(block, iv)
	if len(ct) == 0 || len(ct)%aes.BlockSize != 0 {
		return "", nil, fmt.Errorf("url blob not block-aligned (%d bytes)", len(ct))
	}
	plain := make([]byte, len(ct))
	mode.CryptBlocks(plain, ct)
	url := strings.TrimSpace(strings.TrimRight(string(plain), "\x00"))
	// PKCS7: the padding bytes repeat their length; the player only trims.
	if n := int(plain[len(plain)-1]); n > 0 && n <= aes.BlockSize {
		suffix := plain[len(plain)-n:]
		valid := true
		for _, b := range suffix {
			if int(b) != n {
				valid = false
				break
			}
		}
		if valid {
			url = strings.TrimSpace(string(plain[:len(plain)-n]))
		}
	}
	if !strings.Contains(url, "://") {
		return "", nil, fmt.Errorf("decrypted blob is not a URL (%d bytes)", len(plain))
	}
	if len(playlistKey) == 32 {
		flixcloudBindURLKey(url, playlistKey)
	}
	return url, page.subs, nil
}

func (p *FlixCloudProvider) fetchPlaybackToken(ctx context.Context, token string) map[string]any {
	tokenURL := p.flixBase + "/api/m3u8/" + token
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", p.flixBase+"/")

	resp, err := p.client.Do(req)
	if err != nil {
		p.log.Warn().Err(err).Msg("flixcloud: token request failed")
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		p.log.Warn().Str("body", truncateForLog(string(body), 120)).Msg("flixcloud: token response not JSON")
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		p.log.Warn().Int("status", resp.StatusCode).Str("body", truncateForLog(string(body), 120)).
			Msg("flixcloud: playback token rejected (single-use token consumed or expired?)")
		return nil
	}
	return parsed
}

// cachedResolve returns the cached master URL for an embed URL when fresh.
func (p *FlixCloudProvider) cachedResolve(embedURL string) *flixcloudResolve {
	p.resolvedMu.Lock()
	defer p.resolvedMu.Unlock()
	r, ok := p.resolved[embedURL]
	if !ok || time.Now().After(r.expiresAt) {
		return nil
	}
	return &r
}

func (p *FlixCloudProvider) storeResolve(embedURL string, r flixcloudResolve, ttl time.Duration) {
	p.resolvedMu.Lock()
	defer p.resolvedMu.Unlock()
	// Bound the map: embed URLs rotate per episode, old entries are dead.
	if len(p.resolved) > 512 {
		p.resolved = map[string]flixcloudResolve{}
	}
	r.expiresAt = time.Now().Add(ttl)
	p.resolved[embedURL] = r
}

// MediaURLExpiry reports the exp claim of a token= JWT riding on a media
// URL, when present. The proxy-link layer uses it to avoid sealing a link
// that outlives the upstream token it wraps.
func MediaURLExpiry(rawURL string) (time.Time, bool) {
	return flixcloudJWTExpiry(rawURL)
}

// flixcloudJWTExpiry reads the exp claim out of the IP-bound JWT that rides
// in the master URL's token= query. The JWT is opaque to us (HMAC-signed,
// not ours to verify) but its payload is readable, and its expiry governs
// how long the resolved master stays playable — flixcloud's own backend
// re-mints per token, so a fresh resolve always yields a fresh window.
// Signature-free masters (no token param) have no known expiry.
func flixcloudJWTExpiry(masterURL string) (time.Time, bool) {
	u, err := url.Parse(masterURL)
	if err != nil {
		return time.Time{}, false
	}
	tok := u.Query().Get("token")
	if tok == "" {
		return time.Time{}, false
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp <= 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Exp, 0), true
}

// FlixcloudKeyRefresher is the decode-failure self-heal hook: installed by
// the provider on its first resolve, it re-fetches a recent embed page,
// derives the CURRENT playlist key from that page's wasm data section, and
// adds it to the candidate set.
var FlixcloudKeyRefresher func(ctx context.Context) bool

var (
	flixcloudRefreshMu      sync.Mutex
	flixcloudRefreshLastTry time.Time
)

// FlixcloudRefreshKeys invokes the refresher, throttled to one attempt per
// 20s so a burst of failing requests cannot hammer the embed page.
func FlixcloudRefreshKeys(ctx context.Context) bool {
	flixcloudRefreshMu.Lock()
	if time.Since(flixcloudRefreshLastTry) < 20*time.Second {
		flixcloudRefreshMu.Unlock()
		return false
	}
	flixcloudRefreshLastTry = time.Now()
	fn := FlixcloudKeyRefresher
	flixcloudRefreshMu.Unlock()
	if fn == nil {
		return false
	}
	return fn(ctx)
}

// resolveDirect resolves one embed URL to a direct master playlist URL,
// retrying once on a fresh page when the single-use token was already gone.
func (p *FlixCloudProvider) resolveDirect(ctx context.Context, embedURL string) (string, []core.Subtitle, error) {
	// Token-bound playback must not ride stale caches: an explicit refresh
	// always re-resolves so a fresh single-use token (and a fresh JWT
	// window) is minted instead of reusing the previous one.
	if !IsRefresh(ctx) {
		if cached := p.cachedResolve(embedURL); cached != nil {
			return cached.url, cached.subs, nil
		}
	}
	master, subs, err := p.flixcloudResolveEmbed(ctx, embedURL)
	if err != nil {
		// A rejected token means the page render predates us by too long;
		// one fresh-page retry covers the window between our reanime call
		// and the token fetch only if the PAGE itself was stale (cache).
		p.log.Debug().Err(err).Str("embed", embedURL).Msg("flixcloud: first resolve failed, retrying fresh")
		master, subs, err = p.flixcloudResolveEmbed(ctx, embedURL)
		if err != nil {
			return "", nil, err
		}
	}
	// The master's JWT is the real lifetime: cache only until 15 minutes
	// before it lapses, never the fixed TTL past it. Refresh requests still
	// store (bounded the same way) so the next plain request is fast.
	ttl := flixcloudResolveTTL
	if exp, bounded := flixcloudJWTExpiry(master); bounded {
		if safe := time.Until(exp) - 15*time.Minute; safe < ttl {
			ttl = safe
		}
	}
	if ttl > 0 {
		p.storeResolve(embedURL, flixcloudResolve{url: master, subs: subs}, ttl)
	}
	return master, subs, nil
}

// rememberEmbed records a live embed URL for the key refresher (bounded,
// latest wins — any current page yields the current global playlist key).
func (p *FlixCloudProvider) rememberEmbed(ctx context.Context, embedURL string) {
	p.lastEmbedMu.Lock()
	p.lastEmbedURL = embedURL
	p.lastEmbedMu.Unlock()
	if FlixcloudKeyRefresher == nil {
		FlixcloudKeyRefresher = func(ctx context.Context) bool {
			p.lastEmbedMu.Lock()
			url := p.lastEmbedURL
			p.lastEmbedMu.Unlock()
			if url == "" {
				return false
			}
			page, err := p.flixcloudPage(ctx, url, p.reanimeBase)
			if err != nil {
				return false
			}
			wasm, err := base64.StdEncoding.DecodeString(page.wasmB64)
			if err != nil {
				return false
			}
			mod, err := flixcloudInstantiate(wasm)
			if err != nil {
				return false
			}
			pk, err := flixcloudPlaylistKey(mod)
			if err != nil {
				return false
			}
			flixcloudStorePlaylistKey(pk)
			return true
		}
	}
}

// FindEpisodeSource resolves every distinct flixcloud embed the Reanime
// server list offers (v=1/v=2/v=3) to direct HLS masters, concurrently.
func (p *FlixCloudProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	body := p.fetchReanime(ctx, providerID, episode)
	if body == nil {
		return nil, nil
	}

	var apiResp reanimeResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		p.log.Warn().Err(err).Str("anilistId", providerID).Int("episode", episode).
			Str("body", truncateForLog(string(body), 160)).
			Msg("flixcloud: reanime parse failed")
		return nil, nil
	}

	if !apiResp.Success || len(apiResp.Servers) == 0 {
		p.log.Warn().Str("anilistId", providerID).Int("episode", episode).
			Bool("success", apiResp.Success).Int("servers", len(apiResp.Servers)).
			Msg("flixcloud: no servers from reanime")
		return nil, nil
	}

	// One master playlist per (accessId, v) — reanime lists the same
	// dataLink twice for sub and dub because the master carries both audio
	// renditions; the `al` param strips the wrong one per request.
	type embedEntry struct {
		url  string
		name string
	}
	serverNames := []string{"Yuta", "Syota", "Mike"}
	seen := map[string]bool{}
	var entries []embedEntry
	for _, s := range apiResp.Servers {
		embedURL := s.DataLink
		if embedURL == "" {
			parts := strings.SplitN(s.ID, "-", 3)
			if len(parts) < 2 || parts[1] == "" {
				continue
			}
			embedURL = fmt.Sprintf("%s/e/%s?v=2", p.flixBase, parts[1])
		}
		if seen[embedURL] {
			continue
		}
		seen[embedURL] = true
		// Remember one live embed URL: the decode-failure refresher needs
		// ANY current page's wasm to recover the playlist key after a
		// flixcloud key rotation.
		p.rememberEmbed(ctx, embedURL)
		name := "FlixCloud"
		if len(entries) < len(serverNames) {
			name = serverNames[len(entries)]
		}
		entries = append(entries, embedEntry{url: embedURL, name: name})
	}
	if len(entries) == 0 {
		return nil, nil
	}

	// Resolve all embeds concurrently; each is page fetch + token fetch +
	// wasm mix + AES decrypt (~2-3 upstream round-trips), so serial
	// resolution of three servers would blow the collector budget.
	type resolvedEntry struct {
		name string
		url  string
		subs []core.Subtitle
		err  error
	}
	results := make([]resolvedEntry, len(entries))
	var wg sync.WaitGroup
	for i, entry := range entries {
		wg.Add(1)
		go func(idx int, e embedEntry) {
			defer wg.Done()
			url, subs, err := p.resolveDirect(ctx, e.url)
			results[idx] = resolvedEntry{name: e.name, url: url, subs: subs, err: err}
		}(i, entry)
	}
	wg.Wait()

	var sources []core.Source
	for i, res := range results {
		if res.err != nil {
			// Graceful degradation. The direct chain is reverse-engineered:
			// an upstream page-obfuscation change, a wasm key rotation we
			// cannot decode or an already-consumed single-use token breaks
			// it, and dropping the entry left the title with no FlixCloud
			// server at all. Ship the embed page instead — the iframe
			// player still works, which is exactly what FlixCloud served
			// before direct playback landed.
			p.log.Warn().Err(res.err).Str("anilistId", providerID).Int("episode", episode).
				Str("embed", entries[i].url).Msg("flixcloud: direct resolve failed, falling back to embed")
			sources = append(sources, flixcloudEmbedFallback(entries[i].url))
			continue
		}
		p.log.Info().
			Str("anilistId", providerID).
			Int("episode", episode).
			Str("serverName", res.name).
			Msg("flixcloud: resolved direct master")
		sources = append(sources, core.Source{
			URL:          res.url,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    res.subs,
			Verification: "direct",
		})
	}

	if len(sources) == 0 {
		return nil, nil
	}

	return &SourceResult{
		Sources: sources,
		Headers: map[string]string{
			"Referer":    p.flixBase + "/",
			"User-Agent": browserUA,
		},
	}, nil
}

// flixcloudEmbedFallback is the degraded source shape for a server whose
// direct chain could not be resolved: the embed page URL, played by the
// frontend's iframe player.
func flixcloudEmbedFallback(embedURL string) core.Source {
	return core.Source{
		URL:          embedURL,
		Type:         "embed",
		Quality:      "auto",
		Verification: "embed",
	}
}

// --- playlist obfuscation key -------------------------------------------

// flixcloudPKFallback is the playlist XOR key observed in the original
// sampled modules. It is no longer trusted as a global constant: the key is
// minted PER EMBED-PAGE RENDER (two titles resolved seconds apart carried
// 20ee… and 8286…), so a process that resolves many episodes accumulates
// generations quickly. The fallback stays first in the candidate set because
// older CDN cache nodes still serve bodies wrapped with it.
//
// The authoritative path is flixcloudURLKeys: every resolve binds its page's
// key to the master URL (and to every child playlist named by that master),
// so a decode never depends on what the global set happens to hold. Bodies
// whose URL binding is missing are still recoverable: flixcloudPlaylistOpenings
// are the stable plaintext openings of flixcloud playlists, and XORing the
// first 32 ciphertext bytes with one of them yields the full 32-byte key.
const flixcloudPKFallbackHex = "ad84671c539a5e919ae6932f604f2a16d26c93d5545e90f76518561453c2e744"

// flixcloudPKSetCap bounds the global generation cache. Per-page keys rotate
// constantly, so this is only a fast path for recently seen generations; the
// URL binding map and known-plaintext recovery cover everything else.
const flixcloudPKSetCap = 32

var (
	flixcloudPKMu       sync.RWMutex
	flixcloudPKOrder    []string
	flixcloudPKSet      = map[string][]byte{}
	flixcloudPKFallback = mustHex(flixcloudPKFallbackHex)

	// flixcloudURLKeys maps a query-stripped playlist URL to the key that
	// wraps its body. It is bounded FIFO: playlist URLs are unique per
	// render, so LRU bookkeeping buys nothing.
	flixcloudURLMu    sync.Mutex
	flixcloudURLKeys  = map[string][]byte{}
	flixcloudURLOrder []string
)

const flixcloudURLKeyCap = 4096

func init() {
	flixcloudPKSet[flixcloudPKFallbackHex] = flixcloudPKFallback
	flixcloudPKOrder = append(flixcloudPKOrder, flixcloudPKFallbackHex)
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// flixcloudPlaylistOpenings are the plaintext openings every flixcloud
// playlist generation has used. Each is at least 32 bytes, so the first 32
// ciphertext bytes XOR opening[:32] recover all 32 key bytes; the rest of the
// opening and the whole-body text check reject wrong candidates.
var flixcloudPlaylistOpenings = [][]byte{
	[]byte("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-MEDIA:TYPE=AUDIO"),
	[]byte("#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-MEDIA:TYPE=AUDIO"),
	[]byte("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-KEY:METHOD=AES-128"),
	[]byte("#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-KEY:METHOD=AES-128"),
	// Variant playlists observed WITHOUT a VERSION line: the key tag is the
	// first tag after the magic. Missing this opening made every such body
	// undecodable whenever the URL binding was absent (fresh process, stale
	// k-link, evicted key) — the "still wrapped after decode" failure.
	[]byte("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128"),
	[]byte("#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,"),
	[]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH="),
	[]byte("#EXTM3U\n#EXT-X-PLAYLIST-TYPE:VOD"),
	[]byte("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-STREAM-INF"),
	[]byte("#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-STREAM-INF"),
	[]byte("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD"),
	[]byte("#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-PLAYLIST-TYPE:VOD"),
	[]byte("#EXTM3U\n#EXT-X-TARGETDURATION:4\n"),
	[]byte("#EXTM3U\n#EXT-X-TARGETDURATION:6\n"),
	[]byte("#EXTM3U\n#EXT-X-TARGETDURATION:10"),
}

// flixcloudStorePlaylistKey adds a resolve-derived key to the candidate set.
func flixcloudStorePlaylistKey(pk []byte) {
	if len(pk) != 32 {
		return
	}
	id := hex.EncodeToString(pk)
	flixcloudPKMu.Lock()
	defer flixcloudPKMu.Unlock()
	if _, ok := flixcloudPKSet[id]; ok {
		return
	}
	// Bound the set: drop the OLDEST non-fallback entry. The fallback must
	// stay in the order forever — the old loop dropped index 0
	// unconditionally, so once enough distinct keys had accumulated the
	// fallback vanished and every body wrapped with it failed to decode
	// while the same bytes decoded fine in a fresh process.
	for len(flixcloudPKOrder) >= flixcloudPKSetCap {
		drop := -1
		for i, id := range flixcloudPKOrder {
			if id != flixcloudPKFallbackHex {
				drop = i
				break
			}
		}
		if drop < 0 {
			break // only the fallback left, nothing evictable
		}
		delete(flixcloudPKSet, flixcloudPKOrder[drop])
		flixcloudPKOrder = append(flixcloudPKOrder[:drop], flixcloudPKOrder[drop+1:]...)
	}
	// Fallback stays first so the common path needs one try.
	idx := 1
	if len(flixcloudPKOrder) < idx {
		idx = len(flixcloudPKOrder)
	}
	flixcloudPKOrder = append(flixcloudPKOrder, "")
	copy(flixcloudPKOrder[idx+1:], flixcloudPKOrder[idx:])
	flixcloudPKOrder[idx] = id
	flixcloudPKSet[id] = pk
}

// flixcloudPKSnapshot returns the current candidates, fallback first.
func flixcloudPKSnapshot() [][]byte {
	flixcloudPKMu.RLock()
	defer flixcloudPKMu.RUnlock()
	out := make([][]byte, 0, len(flixcloudPKOrder))
	for _, id := range flixcloudPKOrder {
		if pk, ok := flixcloudPKSet[id]; ok {
			out = append(out, pk)
		}
	}
	return out
}

// flixcloudURLKeyID normalizes a playlist URL for key lookup: the token
// query rotates per resolve while the path (and its body key) stays fixed.
func flixcloudURLKeyID(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" {
		return strings.TrimSpace(rawURL)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// flixcloudBindURLKey records the key that wraps rawURL's body.
func flixcloudBindURLKey(rawURL string, pk []byte) {
	if len(pk) != 32 || rawURL == "" {
		return
	}
	id := flixcloudURLKeyID(rawURL)
	key := append([]byte(nil), pk...)
	flixcloudURLMu.Lock()
	defer flixcloudURLMu.Unlock()
	if _, ok := flixcloudURLKeys[id]; ok {
		flixcloudURLKeys[id] = key
		return
	}
	for len(flixcloudURLOrder) >= flixcloudURLKeyCap {
		drop := flixcloudURLOrder[0]
		flixcloudURLOrder = flixcloudURLOrder[1:]
		delete(flixcloudURLKeys, drop)
	}
	flixcloudURLOrder = append(flixcloudURLOrder, id)
	flixcloudURLKeys[id] = key
}

// flixcloudURLKey returns the bound key for rawURL, or nil.
func flixcloudURLKey(rawURL string) []byte {
	id := flixcloudURLKeyID(rawURL)
	flixcloudURLMu.Lock()
	defer flixcloudURLMu.Unlock()
	if pk, ok := flixcloudURLKeys[id]; ok {
		return append([]byte(nil), pk...)
	}
	return nil
}

// flixcloudXORDecode applies the repeating 32-byte key.
func flixcloudXORDecode(decoded, pk []byte) []byte {
	out := make([]byte, len(decoded))
	for i := range decoded {
		out[i] = decoded[i] ^ pk[i%len(pk)]
	}
	return out
}

// flixcloudLooksPlain accepts a decoded body only when it is an HLS playlist
// and the whole body is text. A wrong key leaves high-bit noise even when
// the forced 32-byte opening happens to match.
func flixcloudLooksPlain(out []byte) bool {
	if !bytes.HasPrefix(out, []byte("#EXTM3U")) || !bytes.Contains(out, []byte("#EXT-X-")) {
		return false
	}
	text := 0
	for _, b := range out {
		if b == '\n' || b == '\r' || b == '\t' || (b >= 0x20 && b < 0x7f) {
			text++
		}
	}
	return text*100 >= len(out)*97
}

// flixcloudBase64Body decodes the wrapper. Both padded and unpadded standard
// base64 have been observed across CDN generations.
func flixcloudBase64Body(body []byte) ([]byte, bool) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil, false
	}
	if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil && len(decoded) >= 32 {
		return decoded, true
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(trimmed); err == nil && len(decoded) >= 32 {
		return decoded, true
	}
	return nil, false
}

// flixcloudDecode tries, in order: the URL-bound key, every recently seen
// generation, then key recovery from the playlist's known opening. It
// returns the plaintext and the key that produced it.
func flixcloudDecode(body, preferred []byte) ([]byte, []byte, bool) {
	if strings.HasPrefix(strings.TrimSpace(string(body)), "#EXTM3U") {
		return body, nil, false
	}
	decoded, ok := flixcloudBase64Body(body)
	if !ok {
		return body, nil, false
	}
	if len(preferred) == 32 {
		if out := flixcloudXORDecode(decoded, preferred); flixcloudLooksPlain(out) {
			return out, preferred, true
		}
	}
	for _, pk := range flixcloudPKSnapshot() {
		if len(pk) != 32 {
			continue
		}
		if out := flixcloudXORDecode(decoded, pk); flixcloudLooksPlain(out) {
			return out, pk, true
		}
	}
	for _, opening := range flixcloudPlaylistOpenings {
		if len(opening) < 32 || len(decoded) < 32 {
			continue
		}
		key := make([]byte, 32)
		for i := range key {
			key[i] = decoded[i] ^ opening[i]
		}
		out := flixcloudXORDecode(decoded, key)
		if bytes.HasPrefix(out, opening) && flixcloudLooksPlain(out) {
			return out, key, true
		}
	}
	return body, nil, false
}

var flixcloudURIAttrRe = regexp.MustCompile(`URI="([^"]+)"`)

// flixcloudBindPlaylistFamily binds the key to the playlist URL and to every
// child playlist it names. Variant bodies are wrapped with the same page key
// as their master, but live at a different URL, so this is what keeps the
// variant decode independent of the global candidate set.
func flixcloudBindPlaylistFamily(rawURL string, plain, pk []byte) {
	flixcloudBindURLKey(rawURL, pk)
	base, err := url.Parse(rawURL)
	if err != nil {
		return
	}
	bind := func(ref string) {
		ref = strings.TrimSpace(ref)
		if ref == "" || strings.HasPrefix(ref, "data:") {
			return
		}
		u, err := url.Parse(ref)
		if err != nil {
			return
		}
		abs := base.ResolveReference(u)
		if !strings.HasSuffix(strings.ToLower(abs.Path), ".m3u8") {
			return
		}
		flixcloudBindURLKey(abs.String(), pk)
	}
	for _, line := range strings.Split(string(plain), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			for _, m := range flixcloudURIAttrRe.FindAllStringSubmatch(line, -1) {
				bind(m[1])
			}
			continue
		}
		bind(line)
	}
}

// DecodeFlixcloudPlaylist unwraps the base64(XOR(plaintext, pk)) wrapper
// flixcloud puts around every m3u8 body. Plaintext passes through unchanged;
// on any decode failure the raw body is returned so the proxy's own error
// classification still handles it (an undecodable body can never be a valid
// playlist anyway).
func DecodeFlixcloudPlaylist(body []byte) []byte {
	out, _, _ := flixcloudDecode(body, nil)
	return out
}

// DecodeFlixcloudPlaylistForURL is the proxy entry point: it decodes with
// the key bound to this URL's family first, then falls back to the global
// set and known-plaintext recovery. On success it caches the key and binds
// every child playlist so the variant request decodes with the same key.
func DecodeFlixcloudPlaylistForURL(rawURL string, body []byte) []byte {
	out, pk, ok := flixcloudDecode(body, flixcloudURLKey(rawURL))
	if !ok || len(pk) != 32 {
		return out
	}
	flixcloudStorePlaylistKey(pk)
	flixcloudBindPlaylistFamily(rawURL, out, pk)
	return out
}
