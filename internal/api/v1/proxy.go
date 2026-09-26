package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/api/middleware"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/streaming"
)

func (h *Handlers) Stream(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var req core.StreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.AnimeID == 0 || req.Episode == 0 {
		h.respondError(w, http.StatusBadRequest, "animeId and episode are required")
		return
	}

	if req.Lang == "" {
		req.Lang = "sub"
	}
	if req.Quality == "" {
		req.Quality = "auto"
	}

	ctx := r.Context()
	if req.Refresh {
		ctx = streaming.WithRefresh(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	// AniList-keyed: the frontend sends AniList IDs and the Anikoto resolver
	// maps them directly. MAL IDs are normalized to AniList IDs at the search
	// boundary, never here.
	anilistID := req.AnimeID

	// Find best source for the requested provider/lang only
	result, err := h.stream.GetSourcesForProviderWithSlug(ctx, req.Episode, req.Provider, req.Lang, req.Quality, anilistID, req.Slug)

	if err != nil || result == nil || len(result.Sources) == 0 {
		h.log.Warn().Err(err).Int("animeId", req.AnimeID).Str("lang", req.Lang).Str("provider", req.Provider).Msg("streaming failed")
		if err != nil && (strings.Contains(err.Error(), "no source") || strings.Contains(err.Error(), "not found") ||
			strings.Contains(err.Error(), "no matching show") ||
			strings.Contains(err.Error(), "not available") || strings.Contains(err.Error(), "blocked") ||
			strings.Contains(err.Error(), "unreachable") || strings.Contains(err.Error(), "filtered")) {
			h.respondError(w, http.StatusNotFound, "no streaming source found")
			return
		}
		h.respondError(w, http.StatusBadGateway, "streaming source failed")
		return
	}

	proxyStreamResult(r, result, req.Lang)
	h.respondJSON(w, http.StatusOK, result)
}

// proxyStreamResult makes the API response safe for browsers. Providers return
// CDN URLs, but those URLs commonly reject browser CORS requests or require
// headers that only the server-side proxy can supply. Returning them directly
// bypasses /api/v1/proxy entirely, which is why the frontend can report a CDN
// 403 even while proxy requests succeed in the backend logs.
func proxyStreamResult(r *http.Request, result *core.StreamResult, lang string) {
	if result == nil {
		return
	}
	proxySources(r, result.Sources, result.Headers, lang)
}

func proxySources(r *http.Request, sources []core.Source, headers map[string]string, lang string) {
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return
	}
	headersParam := url.QueryEscape(string(headersJSON))
	proxyBase := requestPublicBaseURL(r)
	// Audio language for dual-audio masters: the proxy strips the wrong
	// AUDIO rendition at rewrite time (see stripAudioRenditions in
	// proxy_audio.go). Only a known lang qualifies.
	alParam := ""
	if lang == "sub" || lang == "dub" {
		alParam = "&al=" + lang
	}
	for i := range sources {
		source := &sources[i]
		if strings.ToLower(source.Type) != "hls" || source.URL == "" || strings.Contains(source.URL, "/api/v1/proxy?") {
			continue
		}
		source.URL = fmt.Sprintf("%s/api/v1/proxy?url=%s&headers=%s%s", proxyBase, url.QueryEscape(source.URL), headersParam, alParam)
		// Subtitle URLs are delivered RAW: the web client wraps them with its
		// own proxied() helper (which attaches headers + cache nonce). Wrapping
		// them here too produced double-encoded URLs that 403 at the gate.
		// Subtitle hosts are still vouched for the allowlist by the provider
		// (learnURLHost), so the proxy accepts them.
	}
}

func (h *Handlers) LegacyEpsrc(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	epStr := r.URL.Query().Get("ep")
	lang := r.URL.Query().Get("lang")
	if lang == "" {
		lang = "sub"
	}

	if idStr == "" || epStr == "" {
		h.respondError(w, http.StatusBadRequest, "id and ep are required")
		return
	}

	animeID, err := strconv.Atoi(idStr)
	episode, err2 := strconv.Atoi(epStr)
	if err != nil || err2 != nil || animeID <= 0 || episode <= 0 {
		h.respondError(w, http.StatusBadRequest, "invalid id or ep")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	// Legacy endpoint: delegate to the streaming manager (anikoto/flixcloud).
	result, err := h.stream.GetSourcesForProvider(ctx, episode, "", lang, "auto", animeID)
	if err != nil || result == nil || len(result.Sources) == 0 {
		h.respondError(w, http.StatusNotFound, "no streaming source found")
		return
	}

	proxyStreamResult(r, result, lang)
	h.respondJSON(w, http.StatusOK, result)
}

func (h *Handlers) GetServers(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("animeId")
	epStr := r.URL.Query().Get("episode")
	lang := r.URL.Query().Get("lang")
	genresStr := r.URL.Query().Get("genres")

	if idStr == "" || epStr == "" {
		h.respondError(w, http.StatusBadRequest, "animeId and episode are required")
		return
	}
	if lang == "" {
		lang = "sub"
	}

	animeID, err := strconv.Atoi(idStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid animeId")
		return
	}
	episode, err := strconv.Atoi(epStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid episode")
		return
	}

	// Parse genres from comma-separated query param (e.g. "Hentai" or "Action,Comedy").
	var genres []string
	if genresStr != "" {
		for _, g := range strings.Split(genresStr, ",") {
			g = strings.TrimSpace(g)
			if g != "" {
				genres = append(genres, g)
			}
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	if r.URL.Query().Get("refresh") == "1" || r.URL.Query().Get("refresh") == "true" {
		ctx = streaming.WithRefresh(ctx)
	}
	// AniList-keyed; IDs are normalized at the search boundary.
	anilistID := animeID

	servers := h.stream.FindAllServers(ctx, anilistID, episode, lang, genres)
	for i := range servers {
		proxySources(r, servers[i].Sources, servers[i].Headers, lang)
	}
	if servers == nil {
		servers = []core.Server{}
	}
	// The 90s fan-out above exceeds the server-wide 60s WriteTimeout. Without
	// lifting the write deadline, a slow provider chain gets the connection
	// killed mid-response and the client sees nothing after waiting it out.
	// The request context (90s) still bounds the total lifetime.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	h.respondJSON(w, http.StatusOK, servers)
}

// blockedProxyPorts are well-known non-HTTP service ports the media proxy
// refuses to dial. Everything else is allowed: CDNs legitimately serve video
// on other ports (observed: a1.mp4upload.com:183). The authoritative SSRF
// boundary stays in the dialer's resolved-IP control hook; the port list
// only stops accidental fetches of SSH/database/mail daemons.
var blockedProxyPorts = map[string]bool{
	"21": true, "22": true, "23": true, "25": true, "53": true,
	"110": true, "143": true, "389": true, "445": true, "465": true, "587": true,
	"993": true, "995": true, "1433": true, "1521": true, "2049": true,
	"2375": true, "3306": true, "3389": true, "5432": true, "5900": true,
	"6379": true, "9200": true, "11211": true, "27017": true,
}

// isSubtitleURL matches subtitle track URLs (extensions and known caption
// endpoints) so the proxy can label them parseable instead of downloadable.
func isSubtitleURL(pathLower string) bool {
	for _, m := range []string{".vtt", ".srt", "/captions", "/subtitle", "/subs/"} {
		if strings.Contains(pathLower, m) {
			return true
		}
	}
	return false
}

func (h *Handlers) Proxy(w http.ResponseWriter, r *http.Request) { // The media proxy must never be cached at the edge: edge caches store
	// response variants per URL. Responses already carry Vary: Origin (set
	// site-wide), so variants are keyed correctly. Only allowlisted origins
	// are echoed — reflecting an arbitrary Origin would let any website
	// read what the proxy fetches. Unknown/absent origins get '*': media
	// here is public (allowlisted CDNs only) and no proxy request is made
	// with credentials, so '*' never breaks playback.
	w.Header().Set("Cache-Control", "no-store, private")
	origin := r.Header.Get("Origin")
	switch {
	case middleware.IsAllowedOrigin(origin):
		w.Header().Set("Access-Control-Allow-Origin", origin)
	default:
		// Absent or unknown origin: media here is public (allowlisted CDNs
		// only) and the proxy is never called with credentials, so '*' is
		// safe and keeps playback working from any origin without letting
		// untrusted sites read credentialed responses.
		w.Header().Set("Access-Control-Allow-Origin", "*")
	}

	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		h.respondError(w, http.StatusBadRequest, "url parameter required")
		return
	}

	decodedURL, err := url.QueryUnescape(targetURL)
	if err != nil {
		decodedURL = targetURL
	}
	// Drop the cache-busting nonce (see rewriteHLSPlaylist) so the upstream
	// CDN never sees it.
	decodedURL = stripProxyNonce(decodedURL)

	// SSRF guard: cheap fast-fail on obviously bad hostnames. The dialer
	// Control hook (ssrfGuardControl) remains the authoritative boundary for
	// resolved-IP checks covering DNS rebinding, redirects, and alt encodings.
	parsed, err := url.Parse(decodedURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		h.respondError(w, http.StatusBadRequest, "invalid URL scheme")
		return
	}
	if port := parsed.Port(); port != "" && blockedProxyPorts[port] {
		h.respondError(w, http.StatusForbidden, "proxy target port not allowed")
		return
	}
	if host := strings.ToLower(parsed.Hostname()); isNuisanceProxyHost(host) {
		h.log.Warn().Str("proxy_host", host).Msg("proxy nuisance-ad host blocked")
		h.respondError(w, http.StatusForbidden, "proxy target blocked")
		return
	} else if validateProxyTarget(host) != nil {
		h.respondError(w, http.StatusForbidden, "proxy target not allowed")
		return
	} else if !isAllowedProxyHost(host) {
		// The dialer SSRF guard keeps private targets out; the CDN suffix
		// allowlist keeps the proxy from being a general public relay.
		h.log.Warn().Str("proxy_host", host).Msg("proxy host not on CDN allowlist")
		h.respondError(w, http.StatusForbidden, "proxy target not allowed")
		return
	}

	// Create request with proper headers
	req, err := http.NewRequestWithContext(r.Context(), "GET", decodedURL, nil)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid URL")
		return
	}

	// Set headers from query param
	headersJSON := r.URL.Query().Get("headers")
	applyProxyQueryHeaders(req, headersJSON)

	// Audio language (set by proxySources from the request's lang): dual-
	// audio masters get the wrong AUDIO rendition stripped during the HLS
	// rewrite so sub/dub can never play each other's track (see
	// stripAudioRenditions). Anything other than sub/dub disables the strip.
	al := r.URL.Query().Get("al")
	if al != "sub" && al != "dub" {
		al = ""
	}

	// Short-lived VOD playlist cache (see streaming.vod_cache.go). The
	// collector's honesty probe fetched this exact upstream body seconds
	// before the player asked for it, and the browser's prewarm fires HEADs
	// that would each cost another full upstream round-trip — burst traffic
	// that trips relay rate limits (krussdomi 429s the first playback
	// attempt otherwise, which the frontend reports as "blocked"). Stored
	// bytes are always RAW upstream content; the rewrite runs per request so
	// proxyBase, the al strip and the rn nonce are never shared across hits.
	// Reached only after the SSRF/allowlist gates above, so the cache can
	// never answer a request the proxy would not have fetched itself.
	if cachedBody, ok := streaming.VODCacheGet(decodedURL); ok {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			w.Header().Set("Content-Length", strconv.Itoa(len(cachedBody)))
			w.WriteHeader(http.StatusOK)
			return
		}
		h.serveRewrittenPlaylist(w, r, cachedBody, decodedURL, headersJSON, al, http.StatusOK)
		return
	}

	// AnimeX CDN proxy: decode the /uwu/ token to extract the Referer header
	// that the CDN requires. The token format is base64url(xor(url\0referer\0ua, key)).
	if strings.Contains(decodedURL, "cdnx.aniwatchtv.site/uwu/") {
		_, ref, ua := streaming.DecodeAnimeXProxyURL(decodedURL)
		if ref != "" {
			req.Header.Set("Referer", ref)
		}
		if ua != "" && req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", ua)
		}
		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
		}
	}

	// FlixCloud's m3u8 JWT is bound to the IP that hit the decrypt endpoint
	// (the `client_ip` claim), and the CDN validates the actual TCP source IP
	// of every request. It does not honor X-Forwarded-For, X-Real-IP, or
	// CF-Connecting-IP for this check. The previous code sent the local proxy
	// peer (often 127.0.0.1) in those headers, which could not fix an IP
	// mismatch and could make the request misleading to diagnose.
	if host := strings.ToLower(parsed.Hostname()); host == "fetch8.flixcloud.cc" || strings.HasSuffix(host, ".flixcloud.cc") {
		if clientIP, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			h.log.Debug().
				Str("proxy_url", decodedURL).
				Str("r_remote_addr", r.RemoteAddr).
				Str("jwt_client_ip_guess", clientIP).
				Msg("flixcloud proxy: IP-bound JWT; verify Go egress IP matches the client_ip claim")
		}
	}

	// Forward the client's Range header for media/segment requests so the
	// video element can seek through the proxy. Without it every seek turns
	// into a fresh full-body download and playback restarts from the start.
	// Playlists and keys are excluded — their paths need full bodies (HLS
	// rewrite / key cache), and neither hls.js nor native HLS ranges them.
	if rng := r.Header.Get("Range"); rng != "" {
		p := strings.ToLower(parsed.Path)
		if !strings.HasSuffix(p, ".m3u8") && !strings.HasSuffix(p, ".m3u") && !strings.HasSuffix(p, ".key") {
			req.Header.Set("Range", rng)
		}
	}

	// Cache key responses — hls.js makes one request per unique KEY URI
	// (we append &sn=N to each per-segment KEY tag). The backend strips sn
	// from the upstream URL, so every request fetches the same CDN key URL.
	cacheKeyURL := decodedURL
	if idx := strings.IndexByte(decodedURL, '?'); idx >= 0 {
		baseQ, _ := url.ParseQuery(decodedURL[idx+1:])
		for _, skip := range []string{"sn", "iv", "t"} {
			baseQ.Del(skip)
		}
		if clean := baseQ.Encode(); clean != "" {
			cacheKeyURL = decodedURL[:idx+1] + clean
		} else {
			cacheKeyURL = decodedURL[:idx]
		}
	}
	isKey := strings.HasSuffix(strings.ToLower(parsed.Path), ".key")

	if isKey {
		if cached, ok := h.keyCache.Load(cacheKeyURL); ok {
			if entry, ok := cached.(keyCacheEntry); ok && time.Since(entry.fetchedAt) < 5*time.Minute {
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("Cache-Control", "public, max-age=300")
				w.WriteHeader(http.StatusOK)
				w.Write(entry.data)
				return
			}
			h.keyCache.Delete(cacheKeyURL)
		}
	}

	// Relay CDNs rate-limit bursts with 429 (krussdomi answers "slow down"
	// mid-first-playback). One quiet retry after a short pause turns that
	// blip into a success instead of a failed playback start; anything
	// still limited after the retry fails fast through the rejection below.
	pathLower := strings.ToLower(parsed.Path)
	isPlaylistPath := strings.HasSuffix(pathLower, ".m3u8") || strings.HasSuffix(pathLower, ".m3u")
	resp, err := h.doRequest(req, parsed.Scheme == "https")
	if err == nil && resp.StatusCode == http.StatusTooManyRequests &&
		r.Method == http.MethodGet && isPlaylistPath {
		resp.Body.Close()
		select {
		case <-r.Context().Done():
			resp, err = nil, r.Context().Err()
		case <-time.After(600 * time.Millisecond):
			resp, err = h.doRequest(req.Clone(r.Context()), parsed.Scheme == "https")
		}
	}
	if err != nil {
		errStr := err.Error()
		h.log.Warn().Err(err).Str("proxy_url", decodedURL).Msg("proxy upstream connection failed")
		if strings.Contains(errStr, "connection refused") ||
			strings.Contains(errStr, "no route to host") ||
			strings.Contains(errStr, "i/o timeout") {
			h.respondError(w, http.StatusBadGateway, "CDN_BLOCKED: upstream refused connection - CDN blocks datacenter IPs")
			return
		}
		h.respondError(w, http.StatusBadGateway, "upstream request failed")
		return
	}
	defer resp.Body.Close()

	// ponytail: debug — log upstream status for proxy issues
	h.log.Debug().Str("proxy_url", decodedURL).Int("upstream_status", resp.StatusCode).Msg("proxy upstream response")

	// The media proxy is not a navigation relay. A 3xx response can direct a
	// native embedded session to an unreviewed interstitial or popup landing
	// page; refuse it instead of passing its Location header to the client.
	if proxyRedirectBlocked(resp.StatusCode) {
		h.log.Warn().Str("proxy_url", decodedURL).Int("upstream_status", resp.StatusCode).Msg("proxy upstream redirect blocked")
		h.respondError(w, http.StatusBadGateway, "upstream media redirect blocked")
		return
	}

	if resp.StatusCode == 403 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == http.StatusTooManyRequests {
		errStr := fmt.Sprintf("upstream returned %d", resp.StatusCode)
		h.log.Warn().Str("proxy_url", decodedURL).Int("upstream_status", resp.StatusCode).Msg("proxy upstream rejected")
		if resp.StatusCode == http.StatusTooManyRequests {
			// Never forward a 429 body: it is rate-limit error text, and the
			// playlist rewrite would mangle it into a fake URL that hls.js
			// fails on with a confusing parse error instead of a clean retry.
			h.respondError(w, http.StatusBadGateway, "CDN_BLOCKED: upstream rate limited (HTTP 429)")
			return
		}
		if resp.StatusCode == 502 || resp.StatusCode == 403 {
			h.respondError(w, http.StatusBadGateway, "CDN_BLOCKED: upstream rejected (HTTP "+strconv.Itoa(resp.StatusCode)+")")
			return
		}
		h.respondError(w, http.StatusBadGateway, errStr)
		return
	}

	// Learning happens where a trusted playlist names its media hosts
	// (rewriteHLSPlaylist), not here. Recording the host at this point could
	// never learn anything: this code is only reachable once the allowlist
	// gate above has already passed.

	if isKey && resp.StatusCode == http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(body))
		h.keyCache.Store(cacheKeyURL, keyCacheEntry{data: body, fetchedAt: time.Now()})

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Cache-Control", "public, max-age=300")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
		return
	}

	// Check if this is an HLS playlist — use parsed path, not raw URL (query
	// params break HasSuffix). pathLower is computed once before the dial.
	contentType := resp.Header.Get("Content-Type")
	isHLS := strings.Contains(contentType, "mpegurl") || strings.Contains(contentType, "m3u8") ||
		strings.HasSuffix(pathLower, ".m3u8") || strings.HasSuffix(pathLower, ".m3u")

	if isHLS {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			h.respondError(w, http.StatusBadGateway, "failed to read HLS playlist")
			return
		}
		if resp.StatusCode != http.StatusOK {
			// A non-200 body is upstream error text, never a playlist —
			// running it through the rewrite would mangle it into a fake
			// URL and hls.js would die on a confusing parse error. Answer
			// with a clean upstream-rejection error instead.
			h.log.Warn().Str("proxy_url", decodedURL).Int("upstream_status", resp.StatusCode).Msg("playlist upstream rejected")
			h.respondError(w, http.StatusBadGateway, "upstream returned "+strconv.Itoa(resp.StatusCode))
			return
		}
		// Keep the RAW pre-rewrite body for the short VOD window: the next
		// request for this URL (any lang, any proxyBase) rewrites it fresh.
		streaming.VODCacheSet(decodedURL, body)
		h.serveRewrittenPlaylist(w, r, body, decodedURL, headersJSON, al, resp.StatusCode)
		return
	}
	// Stream non-HLS content directly through the proxy.
	// Force correct Content-Type for TS segments — CDN lies with "image/jpeg"
	// to bypass Cloudflare media blocking.
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	if strings.HasSuffix(pathLower, ".jpg") || strings.HasSuffix(pathLower, ".ts") {
		if !strings.HasSuffix(pathLower, ".m3u8") && !strings.HasSuffix(pathLower, ".key") {
			ct = "video/mp2t"
		}
	}
	if strings.HasSuffix(pathLower, ".key") {
		ct = "application/octet-stream"
	}
	// Subtitle tracks served as downloadable blobs (moe /captions answers
	// application/octet-stream): players need text/vtt to parse cues and
	// otherwise download the file. Override only generic binary types,
	// never a specific upstream type — working .vtt subtitles pass through
	// untouched.
	if isSubtitleURL(pathLower) && (ct == "" || ct == "application/octet-stream" || ct == "application/binary") {
		ct = "text/vtt; charset=utf-8"
	}

	// Partial-content responses (206) from a forwarded Range: pass through
	// Content-Range and advertise Accept-Ranges so the video element knows it
	// can seek. Content-Length is echoed only for 206 to keep the response
	// non-chunked; for full-body (200) streaming we omit it so Go uses
	// chunked transfer encoding — this prevents ERR_CONTENT_LENGTH_MISMATCH
	// when the upstream CDN drops the connection or returns fewer bytes than
	// its Content-Length promised.
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		w.Header().Set("Content-Range", cr)
	}
	if ar := resp.Header.Get("Accept-Ranges"); ar != "" {
		w.Header().Set("Accept-Ranges", ar)
	} else if resp.StatusCode == http.StatusPartialContent {
		w.Header().Set("Accept-Ranges", "bytes")
	}
	if resp.StatusCode == http.StatusPartialContent {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			w.Header().Set("Content-Length", cl)
		}
	}

	// Media segments are small, but a slow client reading a large one can
	// still outrun the server-wide 60s WriteTimeout; lift it for this
	// response — the request context cancels on client disconnect anyway.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(resp.StatusCode)
	if n, err := io.Copy(w, resp.Body); err != nil {
		// The status line is already sent; the client will see a truncated
		// chunked response. Log it so upstream drops are distinguishable
		// from nginx/app-level truncation.
		h.log.Warn().Err(err).Int64("bytes_copied", n).Str("proxy_url", decodedURL).Msg("proxy stream copy aborted")
	}
}

// serveRewrittenPlaylist answers a playlist request from raw upstream bytes:
// the rewrite (child-URI proxying, the al audio strip, the rn nonce) runs on
// every request so nothing request-specific is ever shared between clients —
// both the live-fetch path and the VOD cache path answer through here.
func (h *Handlers) serveRewrittenPlaylist(w http.ResponseWriter, r *http.Request, raw []byte, decodedURL, headersJSON, al string, status int) {
	// Behind a reverse proxy, r.Host may be the loopback bind address;
	// emitting that address makes a remote browser request 127.0.0.1 on its
	// own machine.
	proxyBase := requestPublicBaseURL(r)
	rewritten := h.rewriteHLSPlaylist(string(raw), decodedURL, headersJSON, proxyBase, al)
	if len(rewritten) < 1500 {
		h.log.Debug().Str("playlist_body", rewritten).Str("proxy_url", decodedURL).Msg("rewritten HLS playlist")
	} else {
		h.log.Debug().Str("playlist_preview", rewritten[:1500]).Str("proxy_url", decodedURL).Msg("rewritten HLS playlist (truncated)")
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.WriteHeader(status)
	w.Write([]byte(rewritten))
}

// Download proxies a direct video download URL through the backend so the
// client never needs to handle CDN headers or CORS restrictions. Unlike the
// media Proxy endpoint, this handler streams the full file and sets
// Content-Disposition so the client can save it locally.
func (h *Handlers) Download(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, private")

	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		h.respondError(w, http.StatusBadRequest, "url parameter required")
		return
	}

	decodedURL, err := url.QueryUnescape(targetURL)
	if err != nil {
		decodedURL = targetURL
	}

	parsed, err := url.Parse(decodedURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		h.respondError(w, http.StatusBadRequest, "invalid URL scheme")
		return
	}
	if port := parsed.Port(); port != "" && blockedProxyPorts[port] {
		h.respondError(w, http.StatusForbidden, "download target port not allowed")
		return
	}
	if host := strings.ToLower(parsed.Hostname()); isNuisanceProxyHost(host) || validateProxyTarget(host) != nil {
		h.respondError(w, http.StatusForbidden, "download target not allowed")
		return
	} else if !isAllowedProxyHost(host) {
		// Same boundary as the media proxy: without the CDN allowlist this
		// endpoint would be a general public relay.
		h.log.Warn().Str("download_host", host).Msg("download host not on CDN allowlist")
		h.respondError(w, http.StatusForbidden, "download target not allowed")
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), "GET", decodedURL, nil)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid URL")
		return
	}

	headersJSON := r.URL.Query().Get("headers")
	applyProxyQueryHeaders(req, headersJSON)

	resp, err := h.downloadClient.Do(req)
	if err != nil {
		h.log.Warn().Err(err).Str("download_url", decodedURL).Msg("download proxy failed")
		h.respondError(w, http.StatusBadGateway, "download source unreachable")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		h.respondError(w, resp.StatusCode, fmt.Sprintf("download source returned %d", resp.StatusCode))
		return
	}

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "video/mp4"
	}

	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		w.Header().Set("Content-Range", cr)
	}
	// Full-file downloads outrun the server-wide 60s WriteTimeout; lift the
	// write deadline for this response (the request context still bounds it).
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Disposition", "attachment")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func requestPublicBaseURL(r *http.Request) string {
	scheme := "https"
	if r.TLS == nil && !strings.EqualFold(strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]), "https") {
		scheme = "http"
	}
	host := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Host"), ",")[0])
	if host == "" {
		host = r.Host
	}
	return scheme + "://" + host
}

func proxyRedirectBlocked(status int) bool {
	return status >= http.StatusMultipleChoices && status < http.StatusBadRequest
}

// stripProxyNonce removes the cache-busting "rn" query parameter from a
// proxied URL. The playlist rewrite adds it so every playback session uses
// fresh edge-cache keys; it must never reach the upstream CDN.
func stripProxyNonce(u string) string {
	idx := strings.IndexByte(u, '?')
	if idx < 0 {
		return u
	}
	q, err := url.ParseQuery(u[idx+1:])
	if err != nil {
		return u
	}
	q.Del("rn")
	if enc := q.Encode(); enc != "" {
		return u[:idx+1] + enc
	}
	return u[:idx]
}

func (h *Handlers) doRequest(req *http.Request, https bool) (*http.Response, error) {
	// The uTLS fingerprinting chain (h2 → h1 → native Go TLS) exists to get
	// past CDN bot detection. Every transport dials through baseDialer, whose
	// ssrfGuardControl re-validates the resolved IP on each connection, so
	// redirects to private targets are rejected at dial time and no client
	// follows redirects at all (noRedirects).
	if !https {
		return h.proxyHTTPClient.Do(req)
	}

	resp, err := h.proxyH2Client.Do(req)
	if err == nil {
		return resp, nil
	}
	resp, err = h.proxyH1Client.Do(req)
	if err == nil {
		return resp, nil
	}
	// ponytail: standard Go TLS fallback — utls Chrome fingerprint triggers
	// bot detection on some CDNs (nekostream, watching.onl). Native Go TLS works fine.
	return h.proxyGoTLSClient.Do(req)
}

// applyProxyQueryHeaders applies the ?headers= JSON to a proxy upstream
// request exactly as the media proxy does: filter dangerous headers, add
// the referer/UA defaults for known CDN classes, and disable compression
// (the uTLS transports don't auto-decompress; gzipped upstream bytes would
// corrupt AES-128 keys and segment data).
func applyProxyQueryHeaders(req *http.Request, headersJSON string) {
	// Disable compression — the uTLS/http2 transport doesn't auto-decompress
	// like a default http.Transport does. Upstream gzipped bytes would arrive
	// garbled, corrupting AES-128 keys and segment data.
	req.Header.Set("Accept-Encoding", "identity")

	if headersJSON != "" {
		var headers map[string]string
		if json.Unmarshal([]byte(headersJSON), &headers) == nil {
			for k, v := range headers {
				// ponytail: strip dangerous headers to prevent injection
				lower := strings.ToLower(k)
				if lower == "host" || lower == "transfer-encoding" || lower == "connection" ||
					lower == "proxy-connection" || lower == "upgrade" || lower == "te" ||
					strings.HasPrefix(lower, "x-") {
					continue
				}
				req.Header.Set(k, v)
			}
		}
	}

	// Set referer for CDNs that require it — client headers take priority where provided
	if req.Header.Get("Referer") == "" {
		u := strings.ToLower(req.URL.String())
		if strings.Contains(u, "uwucdn") || strings.Contains(u, "owocdn") || strings.Contains(u, "185.237.106.79") {
			req.Header.Set("Referer", "https://kwik.cx/")
			req.Header.Set("Origin", "https://kwik.cx")
		} else if strings.Contains(u, "flixcloud") {
			req.Header.Set("Referer", "https://flixcloud.cc/")
		} else if strings.Contains(u, "ninstream") {
			req.Header.Set("Referer", "https://ninstream.com")
		}
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	}
}

func (h *Handlers) rewriteHLSPlaylist(content, baseURL, headersJSON, proxyBase, al string) string {
	// Strip the wrong track from dual-audio masters BEFORE any URI rewriting:
	// dropped lines never get proxied, and the raw master keeps both
	// renditions available for the other language's request (see
	// stripAudioRenditions in proxy_audio.go).
	content = stripAudioRenditions(content, al)
	lines := strings.Split(content, "\n")
	baseParts := strings.Split(baseURL, "/")
	var basePrefix string
	if len(baseParts) < 4 {
		basePrefix = baseURL
	} else {
		basePrefix = strings.Join(baseParts[:len(baseParts)-1], "/")
	}

	// Only a playlist we fetched from an allowed host may vouch for the hosts
	// it references. Proxy() gates on isAllowedProxyHost before fetching, so
	// in production this holds by construction; re-checking keeps the trust
	// chain explicit and correct if this is ever called from elsewhere.
	vouching := false
	if pb, err := url.Parse(baseURL); err == nil {
		vouching = isAllowedProxyHost(strings.ToLower(pb.Hostname()))
	}
	learnPlaylistTarget := func(rawURL string) {
		if !vouching {
			return
		}
		u, err := url.Parse(rawURL)
		if err != nil {
			return
		}
		// A trusted playlist may name any media host — including extension-less
		// segment URLs (observed: p1.ipstatp.com/obj/ad-site-i18n/...). The
		// dialer SSRF guard plus static allowlist gate remain the boundary;
		// the media-path filter was dropping real segment hosts.
		LearnHostFromPlaylist(u.Hostname())
	}

	headersParam := ""
	if headersJSON != "" {
		headersParam = "&headers=" + url.QueryEscape(headersJSON)
	}
	// Per-rewrite nonce: every playlist fetch generates fresh edge-cache keys
	// for the URLs it names, so a stale cached variant (created before the
	// proxy always emitted CORS headers) can never be served to a browser.
	// The Proxy handler strips "rn" before dialing upstream.
	rnParam := fmt.Sprintf("&rn=%d", time.Now().UnixNano())
	// Propagate the audio language on every rewritten child URI so nested
	// playlist fetches keep the dual-audio strip context.
	alParam := ""
	if al != "" {
		alParam = "&al=" + al
	}

	isEncrypted := false
	encKeyURI := ""
	segmentNumRe := regexp.MustCompile(`segment-(\d+)-`)

	// alreadyProxied reports whether a URI line is itself one of our proxy
	// URLs. This happens when an upstream edge serves a stale cached copy of
	// a previously rewritten playlist. Re-wrapping would double-encode the
	// target and 403 at the proxy gate, so such URIs are passed through
	// untouched — they already carry their own headers param.
	alreadyProxied := func(u string) bool {
		return strings.Contains(u, "/api/v1/proxy?")
	}
	// megaSegmentHost matches the TikTok-CDN family megaplay hosts its video
	// segments on. Those CDNs refuse datacenter IPs (the proxy dial gets
	// rejected with CDN_BLOCKED) and prefix every response with a 252-byte
	// canary the client must strip — so they only work fetched directly from
	// the viewer's browser, exactly like megaplay's own player. Such URIs are
	// left absolute and unproxied.
	megaSegmentHost := func(u string) bool {
		if p, err := url.Parse(u); err == nil {
			h := strings.ToLower(p.Hostname())
			return strings.HasSuffix(h, "tiktokcdn.com") || strings.HasSuffix(h, "ipstatp.com") ||
				strings.HasSuffix(h, "ibyteimg.com") || h == "yoot.akirax.buzz"
		}
		return false
	}

	for i, line := range lines {
		original := strings.TrimSpace(line)

		// Handle any HLS tag with URI="..." attribute (#EXT-X-KEY, #EXT-X-MAP, #EXT-X-MEDIA, etc.)
		if strings.HasPrefix(original, "#") && strings.Contains(original, "URI=") {
			re := regexp.MustCompile(`URI="([^"]+)"`)
			rewritten := re.ReplaceAllStringFunc(original, func(match string) string {
				parts := strings.SplitN(match, "=", 2)
				if len(parts) != 2 {
					return match
				}
				uri := strings.Trim(parts[1], "\"")
				absoluteURL := resolveURL(uri, basePrefix)
				if alreadyProxied(absoluteURL) {
					return match
				}
				learnPlaylistTarget(absoluteURL)
				if !megaSegmentHost(absoluteURL) && (needsProxyRewrite(absoluteURL) || headersJSON != "") {
					proxied := fmt.Sprintf("%s/api/v1/proxy?url=%s%s%s%s", proxyBase, url.QueryEscape(absoluteURL), headersParam, alParam, rnParam)
					return fmt.Sprintf("URI=\"%s\"", proxied)
				}
				return fmt.Sprintf("URI=\"%s\"", absoluteURL)
			})
			if strings.Contains(original, "METHOD=AES-128") {
				isEncrypted = true
				if match := regexp.MustCompile(`URI="([^"]+)"`).FindStringSubmatch(rewritten); len(match) >= 2 {
					encKeyURI = match[1]
				}
			}
			lines[i] = rewritten
			continue
		}

		// #EXTINF and other non-URI HLS tags — left unchanged
		if strings.HasPrefix(original, "#") {
			continue
		}

		// Skip empty lines
		if original == "" {
			continue
		}

		// Resolve relative URLs for segment lines
		var absoluteURL string
		if strings.HasPrefix(original, "http") {
			absoluteURL = original
		} else if strings.HasPrefix(original, "//") {
			absoluteURL = "https:" + original
		} else {
			absoluteURL = basePrefix + "/" + original
		}
		if alreadyProxied(absoluteURL) {
			lines[i] = original
			continue
		}
		learnPlaylistTarget(absoluteURL)

		// If encrypted, insert per-segment KEY tag with file-number-based IV
		// ponytail: CDN uses file number as IV (not MEDIA-SEQUENCE), and the
		// playlist skips segments 2-3, so hls.js default MEDIA-SEQ IV is wrong.
		if isEncrypted && encKeyURI != "" {
			if matches := segmentNumRe.FindStringSubmatch(original); len(matches) >= 2 {
				num, _ := strconv.ParseInt(matches[1], 10, 64)
				iv := fmt.Sprintf("0x%032x", num)
				// hls.js indexes LevelKey by URI — same URI skips IV update.
				// Append segment number to force a separate LevelKey per segment.
				keyTag := fmt.Sprintf(`#EXT-X-KEY:METHOD=AES-128,URI="%s&sn=%d",IV=%s`, encKeyURI, num, iv)
				if megaSegmentHost(absoluteURL) {
					lines[i] = keyTag + "\n" + absoluteURL
				} else if headersJSON != "" || needsProxyRewrite(absoluteURL) {
					lines[i] = keyTag + "\n" + fmt.Sprintf("%s/api/v1/proxy?url=%s%s%s%s", proxyBase, url.QueryEscape(absoluteURL), headersParam, alParam, rnParam)
				} else {
					lines[i] = keyTag + "\n" + absoluteURL
				}
				continue
			}
		}

		if megaSegmentHost(absoluteURL) {
			lines[i] = absoluteURL
		} else if headersJSON != "" || needsProxyRewrite(absoluteURL) {
			lines[i] = fmt.Sprintf("%s/api/v1/proxy?url=%s%s%s%s", proxyBase, url.QueryEscape(absoluteURL), headersParam, alParam, rnParam)
		} else {
			lines[i] = absoluteURL
		}
	}

	result := strings.Join(lines, "\n")

	// VOD playlists without #EXT-X-ENDLIST cause hls.js to loop re-fetching.
	// Append it if missing — the CDN already returned the full segment list.
	if strings.Contains(result, "#EXT-X-PLAYLIST-TYPE:VOD") && !strings.Contains(result, "#EXT-X-ENDLIST") {
		result += "\n#EXT-X-ENDLIST"
	}

	return result
}

func needsProxyRewrite(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return !isAllowedProxyHost(parsed.Hostname()) || hasKnownRewriteKeys(rawURL)
}

// hasKnownRewriteKeys returns true for CDN hostnames that must be proxied even
// if they're on the allowlist — these are hosts where the player's direct
// request would be rejected at the CDN without custom headers or transport
// (e.g. uTLS-required hosts).
func hasKnownRewriteKeys(rawURL string) bool {
	lower := strings.ToLower(rawURL)
	return strings.Contains(lower, "flixcloud") ||
		strings.Contains(lower, "ninstream") ||
		strings.Contains(lower, "uwucdn") ||
		strings.Contains(lower, "owocdn") ||
		strings.Contains(lower, "kotocdn") ||
		strings.Contains(lower, "vivibebe") ||
		strings.Contains(lower, "wixmp") ||
		strings.Contains(lower, "anidb") ||
		strings.Contains(lower, "animegg") ||
		strings.Contains(lower, "nekostream") ||
		strings.Contains(lower, "watching.onl") ||
		strings.Contains(lower, "krussdomi") ||
		strings.Contains(lower, "mewstream") ||
		strings.Contains(lower, "megaplay") ||
		strings.Contains(lower, "fast4speed") ||
		strings.Contains(lower, "ans-bio-video") ||
		strings.Contains(lower, "185.237.106.79")
}

func resolveURL(uri, basePrefix string) string {
	if strings.HasPrefix(uri, "http") {
		return uri
	}
	if strings.HasPrefix(uri, "//") {
		return "https:" + uri
	}
	return basePrefix + "/" + uri
}
