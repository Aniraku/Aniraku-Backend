package streaming

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// browserUA is the default probe User-Agent. It MUST be a Windows Chrome
// UA: the MegaPlay CDN edges (fetch.nexabloom.top et al) serve 403 Cloudflare
// block pages to the X11/Linux UA while serving the identical request with a
// Windows UA (isolated live: same URL, same client, same second — X11 403,
// Windows 200). Every provider probe defaults to this.
const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

// iosSafariUA is the fallback probe/playback User-Agent for CDNs that
// gate by UA class instead of IP. Measured live on Sora's segment layer
// (bl1.advancedairesearchlab.xyz + bl1.habibikun.xyz + ...): every
// desktop UA (Chrome/Edge/Firefox/Safari), Android UA, curl, python and
// "no UA" get Cloudflare 403 — while iPhone Safari gets 200 with real
// MPEG-TS bytes (disguised as .jpg). Try this whenever a probe 403s:
// it costs one request and unlocks sources that are otherwise dropped.
const iosSafariUA = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1"

// Shared segment-depth honesty probe (lenient verdict).
//
// A reachable manifest whose segments are egress-blocked only produces a
// spinning player (observed: quavex.top and bl1.*.xyz return Cloudflare 403
// on every header combination while their playlists serve fine). The
// verdict is deliberately lenient — definitive blocks only (unreachable,
// non-2xx, HTML error pages) — so ambiguous payloads (image cloaks some
// CDNs serve probes while playing video fine) keep a server listed instead
// of regressing a working stream.
func probeSegmentsLenient(ctx context.Context, client *http.Client, masterURL, referer, ua string) bool {
	mhead, ok := fetchURLCapped(ctx, client, masterURL, referer, ua, 65536, 6*time.Second)
	if !ok || !strings.Contains(string(mhead), "#EXTM3U") {
		return false
	}
	// Warm the playback cache: the browser will fetch this exact master
	// seconds later through the media proxy (see vod_cache.go).
	VODCacheSet(masterURL, mhead)
	media := firstPlaylistURL(string(mhead), masterURL)
	if media == "" {
		return false
	}
	mbody, ok := fetchURLCapped(ctx, client, media, referer, ua, 262144, 6*time.Second)
	if !ok || !strings.Contains(string(mbody), "#EXTM3U") {
		return false
	}
	VODCacheSet(media, mbody)
	seg := firstPlaylistURL(string(mbody), media)
	if seg == "" {
		return false
	}
	shead, ok := fetchURLCapped(ctx, client, seg, referer, ua, 8192, 8*time.Second)
	if !ok {
		return false
	}
	return !strings.Contains(strings.ToLower(string(shead)), "<html")
}

// probePlaylistsLenient verifies a resolved master at playlist depth only:
// master -> first media playlist must both serve #EXTM3U. Segment bytes are
// deliberately not fetched — when every hop is served by a relay's own
// servers (Supaplay's hls-proxy fetches upstream server-side, never through
// this egress), a segment fetch proves nothing the media playlist did not,
// while costing 0.3-1.8s on the fan-out's pacing collector. Verdict stays
// lenient: definitive blocks only (unreachable, non-2xx, non-#EXTM3U).
func probePlaylistsLenient(ctx context.Context, client *http.Client, masterURL, referer, ua string) bool {
	mhead, ok := fetchURLCapped(ctx, client, masterURL, referer, ua, 65536, 6*time.Second)
	if !ok || !strings.Contains(string(mhead), "#EXTM3U") {
		return false
	}
	// Warm the playback cache: the browser will fetch this exact master
	// seconds later through the media proxy (see vod_cache.go).
	VODCacheSet(masterURL, mhead)
	media := firstPlaylistURL(string(mhead), masterURL)
	if media == "" {
		return false
	}
	mbody, ok := fetchURLCapped(ctx, client, media, referer, ua, 262144, 6*time.Second)
	if !ok || !strings.Contains(string(mbody), "#EXTM3U") {
		return false
	}
	VODCacheSet(media, mbody)
	return true
}

// probeSegmentsStrict verifies a resolved master playlist is actually
// playable from this egress, end to end: master -> first media playlist ->
// first segment must all serve media bytes judged by magic (TS sync,
// fmp4, nested playlist). HTML block pages and decoy payloads fail. Used
// where edge alternatives exist (MegaPlay ?s= variants): a stricter
// verdict is safe because another variant can still win.
func probeSegmentsStrict(ctx context.Context, client *http.Client, fileURL, origin, ua string) bool {
	master, ok := fetchURLCapped(ctx, client, fileURL, origin, ua, 64*1024, 6*time.Second)
	if !ok || !strings.Contains(string(master), "#EXTM3U") {
		return false
	}
	// Warm the playback cache: the browser will fetch this exact master
	// seconds later through the media proxy (see vod_cache.go).
	VODCacheSet(fileURL, master)
	media := firstPlaylistURL(string(master), fileURL)
	if media == "" {
		return false
	}
	medBody, ok := fetchURLCapped(ctx, client, media, origin, ua, 256*1024, 6*time.Second)
	if !ok || !strings.Contains(string(medBody), "#EXTM3U") {
		return false
	}
	VODCacheSet(media, medBody)
	seg := firstPlaylistURL(string(medBody), media)
	if seg == "" {
		return false
	}
	head, ok := fetchURLCapped(ctx, client, seg, origin, ua, 32768, 8*time.Second)
	if !ok {
		return false
	}
	return segmentBytesPlayable(head)
}

// normalizeOriginReferer ensures an origin-style referer carries the
// trailing slash real browsers send ("https://host/"). MegaPlay's CDN
// edge (fetch.nexabloom.top) 403-Cloudfront-blocks "https://megaplay.buzz"
// (no slash) while serving the identical request with
// "https://megaplay.buzz/" — measured same client, same second: 403 vs
// 200. Sending the slashless form made every segment probe read a block
// page and misreport "blocked from this egress". Referers with an actual
// path are returned untouched.
func normalizeOriginReferer(referer string) string {
	u, err := url.Parse(referer)
	if err != nil || u.Host == "" {
		return referer
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String()
}

// fetchURLCapped GETs url with referer/UA, bounding both time and bytes
// (full GET capped, never Range — identical request shape to players).
func fetchURLCapped(ctx context.Context, client *http.Client, rawURL, referer, ua string, limit int64, timeout time.Duration) ([]byte, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, false
	}
	if ua == "" {
		ua = browserUA
	}
	req.Header.Set("User-Agent", ua)
	if referer != "" {
		req.Header.Set("Referer", normalizeOriginReferer(referer))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, false
	}
	return body, true
}
