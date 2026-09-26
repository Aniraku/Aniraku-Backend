package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// ninServerName is the display slot for the Supaplay source.
const ninServerName = "NiN"

// NiNProvider resolves Supaplay (supaplay.fun) — one of the embed providers
// anistream.one injects into its watch page — to direct HLS sources.
//
// Supaplay's stream API is plaintext JSON end to end (no obfuscation on any
// hop; the "encrypted-looking" tokens elsewhere in this space are the animex
// XOR codec, which supaplay does not use). Both available paths return the
// same episode payload and are RACED for lowest latency:
//
//	GET {base}/api/z-6/{anilistId}/{ep}/{sub|dub}            (one call)
//	GET {base}/api/episode-by-anilist/{id}/{ep}/{lang} -> embed id
//	GET {base}/api/episode-cache/{embedId}/{lang}             (two calls)
//
// Both shapes return one episode payload: `m3u8` (Supaplay's hls-proxy
// relay), `rawM3u8` (direct CDN edge), subtitle tracks and intro/outro skip
// chapters. The raw edges (cdn.imgnex.top, cdn.mewstream.buzz, ...) serve
// Cloudflare 403 to this egress while the hls-proxy relay streams the very
// same files with no auth at all — so the proxy URL is the shipped source
// (Verification "proxy"). The underlying files are the MegaPlay catalog
// Anikoto already serves: NiN is the redundancy path that keeps working
// when the direct edges block us.
//
// Subtitles are NOT shipped from Supaplay: every track source is dead from
// every path we have (rawFile hosts are TLS-blocked from this egress,
// Supaplay's own subtitle-proxy 404s, and the frontend's proxy hits the same
// walls). The fan-out borrows verified sibling tracks for the same episode
// instead — Niko, then Mochi, then Zoko (mergeNiNSubtitles in manager.go).
//
// Hentai titles are never queried (gated at the fan-out and in the explicit
// provider switch): Supaplay's API answers 502 for them.
type NiNProvider struct {
	client   *http.Client
	log      zerolog.Logger
	supaBase string
	// learnHost vouches verified hosts for the media-proxy CDN allowlist
	// (same wiring as the other providers: subtitles ship raw and the
	// frontend's proxied() helper sends them back through /api/v1/proxy).
	learnHost func(host string)
}

func NewNiNProvider(log zerolog.Logger) *NiNProvider {
	base := strings.TrimRight(os.Getenv("ANIRAKU_SUPAPLAY_BASE"), "/")
	if base == "" {
		base = "https://supaplay.fun"
	}
	return &NiNProvider{
		client: &http.Client{
			Timeout:   45 * time.Second,
			Transport: netguard.NewTransport(),
		},
		log:      log,
		supaBase: base,
	}
}

func (p *NiNProvider) Name() string { return "nin" }

// SetHostLearner registers the verified-host callback.
func (p *NiNProvider) SetHostLearner(fn func(host string)) {
	p.learnHost = fn
}

// learnURLHost vouches a resolved URL's host for the media-proxy allowlist.
func (p *NiNProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *NiNProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("nin search not implemented")
}

func (p *NiNProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("nin episode listing not implemented")
}

// ninSkip is an intro/outro skip chapter from the episode payload.
type ninSkip struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

// ninSubtitle is one caption track. rawFile is the direct CDN file the
// frontend's own proxy fetches; file is Supaplay's subtitle-proxy copy
// (currently 404s upstream — kept only as a last-resort fallback).
type ninSubtitle struct {
	File    string `json:"file"`
	RawFile string `json:"rawFile"`
	Label   string `json:"label"`
	Kind    string `json:"kind"`
}

// ninEpisodeData is the shared episode payload: nested one level under
// data.data for z-6, flat under data for episode-cache.
type ninEpisodeData struct {
	EpisodeID int           `json:"episodeId"`
	Type      string        `json:"type"`
	M3U8      string        `json:"m3u8"`
	RawM3U8   string        `json:"rawM3u8"`
	Subtitles []ninSubtitle `json:"subtitles"`
	Intro     *ninSkip      `json:"intro"`
	Outro     *ninSkip      `json:"outro"`
}

// ninZ6Response is the doubly-nested z-6 envelope.
type ninZ6Response struct {
	Success bool `json:"success"`
	Data    struct {
		Success bool           `json:"success"`
		Data    ninEpisodeData `json:"data"`
	} `json:"data"`
}

// ninResolverResponse is the episode-by-anilist answer (embed id lookup).
type ninResolverResponse struct {
	Success        bool   `json:"success"`
	EpisodeEmbedID string `json:"episode_embed_id"`
}

// ninCacheResponse is the episode-cache answer (flat payload).
type ninCacheResponse struct {
	Success bool           `json:"success"`
	Data    ninEpisodeData `json:"data"`
}

// FindEpisodeSource resolves the Supaplay HLS stream for an episode by
// RACING the one-call z-6 endpoint against the two-step resolver chain and
// taking the first payload with a usable m3u8. Measured latencies swing per
// run (z-6 2.5-5.2s, chain ~1.3-1.8s), so racing is what actually sets the
// collector's floor — and with the server-list snapshot gone this collector
// runs on every request. Returns nil (no error) when the episode simply
// does not exist on either path.
func (p *NiNProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}
	data, err := p.fetchEpisodeAnyPath(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, err
	}
	if data == nil || strings.TrimSpace(data.M3U8) == "" {
		return nil, nil
	}
	return p.buildSourceResult(ctx, data), nil
}

// fetchEpisodeAnyPath races z-6 against episode-by-anilist -> episode-cache.
// The loser is cancelled with the race context; the buffered channel means
// the losing goroutine never blocks on send. If neither path carries a
// payload, the first transport/HTTP error is surfaced (hentai and missing
// episodes 502 on both paths) so the collector logs it instead of silently
// claiming "no episode".
func (p *NiNProvider) fetchEpisodeAnyPath(ctx context.Context, anilistID string, episode int, lang string) (*ninEpisodeData, error) {
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type ninFetch struct {
		data *ninEpisodeData
		err  error
	}
	ch := make(chan ninFetch, 2)
	go func() {
		d, err := p.fetchZ6(raceCtx, anilistID, episode, lang)
		ch <- ninFetch{d, err}
	}()
	go func() {
		d, err := p.fetchEpisodeCache(raceCtx, anilistID, episode, lang)
		ch <- ninFetch{d, err}
	}()

	var firstErr error
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		if r.data != nil && strings.TrimSpace(r.data.M3U8) != "" {
			return r.data, nil
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, nil
}

// buildSourceResult probes the hls-proxy chain (browser UA first, iOS Safari
// fallback) and assembles the SourceResult. A relay the probe cannot reach
// would only produce player errors through the media proxy, so it is dropped.
func (p *NiNProvider) buildSourceResult(ctx context.Context, data *ninEpisodeData) *SourceResult {
	master := strings.TrimSpace(data.M3U8)
	if master == "" {
		return nil
	}
	referer := p.supaBase + "/"
	// Playlist-depth probe (master -> media), browser UA first, iOS Safari
	// fallback: the segment hop through this relay is server-side work that
	// never touches this egress, so it adds latency without adding verdict.
	if !probePlaylistsLenient(ctx, p.client, master, referer, browserUA) &&
		!probePlaylistsLenient(ctx, p.client, master, referer, iosSafariUA) {
		p.log.Info().Str("master", master).Msg("nin: supaplay hls-proxy blocked from this egress, dropping source")
		return nil
	}
	p.learnURLHost(master)

	// Subtitles are NOT taken from Supaplay: rawFile hosts are TLS-blocked
	// from this egress, Supaplay's own subtitle-proxy 404s, and the
	// frontend's proxy hits the same wall — the tracks simply never play.
	// The fan-out borrows verified sibling tracks instead (mergeNiNSubtitles:
	// Niko -> Mochi -> Zoko), so a standalone result carries none.

	return &SourceResult{
		Sources: []core.Source{{
			URL:          master,
			Type:         "hls",
			Quality:      "auto",
			Verification: "proxy",
		}},
		Headers:     map[string]string{"Referer": referer},
		ServerNames: []string{ninServerName},
		Intro:       ninSkipTimestamp(data.Intro),
		Outro:       ninSkipTimestamp(data.Outro),
	}
}

// ninSkipTimestamp converts a payload chapter to a SkipTimestamp, keeping
// only well-formed ranges (end must follow start).
func ninSkipTimestamp(s *ninSkip) *core.SkipTimestamp {
	if s == nil || s.End <= s.Start {
		return nil
	}
	return &core.SkipTimestamp{Start: s.Start, End: s.End}
}

// fetchZ6 is the one-call path: GET /api/z-6/{id}/{ep}/{lang}.
func (p *NiNProvider) fetchZ6(ctx context.Context, anilistID string, episode int, lang string) (*ninEpisodeData, error) {
	rawURL := fmt.Sprintf("%s/api/z-6/%s/%d/%s", p.supaBase, anilistID, episode, lang)
	body, err := p.getJSON(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	var outer ninZ6Response
	if err := json.Unmarshal(body, &outer); err != nil {
		return nil, fmt.Errorf("nin z-6 json: %w", err)
	}
	if !outer.Success || !outer.Data.Success {
		return nil, nil
	}
	data := outer.Data.Data
	return &data, nil
}

// fetchEpisodeCache is the two-step fallback: resolve the episode embed id,
// then read the same payload from episode-cache.
func (p *NiNProvider) fetchEpisodeCache(ctx context.Context, anilistID string, episode int, lang string) (*ninEpisodeData, error) {
	resolveURL := fmt.Sprintf("%s/api/episode-by-anilist/%s/%d/%s", p.supaBase, anilistID, episode, lang)
	body, err := p.getJSON(ctx, resolveURL)
	if err != nil {
		return nil, err
	}
	var res ninResolverResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("nin resolver json: %w", err)
	}
	if !res.Success || res.EpisodeEmbedID == "" {
		return nil, nil
	}

	cacheURL := fmt.Sprintf("%s/api/episode-cache/%s/%s", p.supaBase, res.EpisodeEmbedID, lang)
	body, err = p.getJSON(ctx, cacheURL)
	if err != nil {
		return nil, err
	}
	var cache ninCacheResponse
	if err := json.Unmarshal(body, &cache); err != nil {
		return nil, fmt.Errorf("nin cache json: %w", err)
	}
	if !cache.Success {
		return nil, nil
	}
	return &cache.Data, nil
}

// getJSON GETs a Supaplay API endpoint with browser headers and returns the
// body on 200 only. The API is authless; the referer/UA mirror what the
// site's player sends so any future edge check passes silently.
func (p *NiNProvider) getJSON(ctx context.Context, rawURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", p.supaBase+"/")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("nin GET %s: %w", rawURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nin GET %s: HTTP %d", rawURL, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("nin GET %s: %w", rawURL, err)
	}
	return body, nil
}
