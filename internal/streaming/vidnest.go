package streaming

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// VidNestProvider resolves direct HLS streams through VidNest's internal
// API (new.vidnest.fun/hianime/anime/{anilistId}/{ep}/{sub|dub}/hd-2).
// Verified live 2026-10-02 from the production egress: the API serves 200
// with plain HTTP (Referer only, no cookies/auth) and returns MegaPlay
// HLS masters with English subtitle tracks and intro/outro chapters.
//
// The /anime/ and /animepahe/ watch pages call the SAME backend API
// (verified byte-identical), so both page families are one provider with
// one server — duplicating the identical URL as two servers would be a
// dishonest listing.
//
// RESPONSE SHAPE: {"data": "<custom-base64>", "encrypted": true}. The
// "encryption" is base64 with a shuffled alphabet, decoded client-side by
// the watch page (custom decoder in its JS chunk). Decoded JSON:
// {"sources":[{"file": <hls master>, "type":"hls"}],
//
//	"tracks":[{"file": <vtt>, "label":"English", ...}],
//	"intro":{"start":31,"end":111}, "outro":{...}, "status":200}.
//
// FRESHNESS (operator): no resolve cache — every lookup re-runs the full
// chain, so listed URLs are always newly minted, never stale tokens.
//
// EGRESS (observed 2026-10-02): new.vidnest.fun serves Cloudflare 403 to
// datacenter egress (works from residential) while the megap.* file hosts
// serve fine. A 403 therefore means "blocked network", not "no episode" —
// it resolves to a silent skip so a blocked listing never errors playback,
// and the provider lights up on its own if the block lifts.
const (
	vidnestAPIBase  = "https://new.vidnest.fun"
	vidnestPageBase = "https://vidnest.fun"
	vidnestReferer  = "https://megaplay.buzz/"

	// vidnestAlphabet is the shuffled base64 alphabet the watch page
	// decodes API payloads with (call-site constant in its JS chunk).
	// If VidNest redeploys with a new alphabet, decodes fail loudly and
	// the collector skips — watch the logs on any full outage.
	vidnestAlphabet = "RB0fpH8ZEyVLkv7c2i6MAJ5u3IKFDxlS1NTsnGaqmXYdUrtzjwObCgQP94hoeW+/="
)

const vidnestServerName = "Nest"

type VidNestProvider struct {
	log       zerolog.Logger
	client    *http.Client
	api       string
	relay     *RelayClient
	learnHost func(host string)
}

func NewVidNestProvider(log zerolog.Logger, api string) *VidNestProvider {
	if strings.TrimSpace(api) == "" {
		api = vidnestAPIBase
	}
	return &VidNestProvider{
		log:    log,
		client: &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		api:    strings.TrimRight(api, "/"),
		relay:  NewRelayClient(),
	}
}

func (p *VidNestProvider) Name() string { return "vidnest" }

func (p *VidNestProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *VidNestProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *VidNestProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("vidnest search not implemented")
}

func (p *VidNestProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("vidnest episode listing not implemented")
}

// vidnestDecode reverses the watch page's custom-base64 payload codec:
// translate the shuffled alphabet back to standard, then base64-decode.
func vidnestDecode(data string) ([]byte, error) {
	const std = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
	if len(vidnestAlphabet) != len(std) || len(data) == 0 {
		return nil, fmt.Errorf("vidnest: bad payload")
	}
	rev := make(map[rune]rune, len(vidnestAlphabet))
	for i, r := range vidnestAlphabet {
		if i < len(std) {
			rev[r] = rune(std[i])
		} else {
			rev[r] = r // padding passes through
		}
	}
	mapped := strings.Map(func(r rune) rune {
		if s, ok := rev[r]; ok {
			return s
		}
		return -1
	}, data)
	raw, err := base64.StdEncoding.DecodeString(mapped)
	if err != nil {
		return nil, fmt.Errorf("vidnest: payload base64: %w", err)
	}
	return raw, nil
}

type vidnestAPIEnvelope struct {
	Data      string `json:"data"`
	Encrypted bool   `json:"encrypted"`
}

type vidnestPayload struct {
	Error   string `json:"error"`
	Status  int    `json:"status"`
	Success bool   `json:"success"`
	Sources []struct {
		File string `json:"file"`
		Type string `json:"type"`
	} `json:"sources"`
	Tracks []struct {
		File   string `json:"file"`
		Label  string `json:"label"`
		Lang   string `json:"lang"`
		Kind   string `json:"kind"`
		Format string `json:"format"`
	} `json:"tracks"`
	Intro *core.SkipTimestamp `json:"intro"`
	Outro *core.SkipTimestamp `json:"outro"`
}

func (p *VidNestProvider) fetchAPI(ctx context.Context, id, episode int, lang, referer string) (*vidnestPayload, error) {
	// Relay first when configured: the direct API 403s datacenter egress.
	if p.relay != nil {
		body, err := p.relay.VidNestFetch(ctx, id, episode, lang)
		if err != nil {
			return nil, err
		}
		return decodeVidNestEnvelope(body)
	}
	rawURL := fmt.Sprintf("%s/hianime/anime/%d/%d/%s/hd-2", p.api, id, episode, lang)
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		// Per-attempt cap far below the client timeout: a tarpitted
		// block page must never eat the fan-out budget.
		actx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if attempt > 1 {
			select {
			case <-ctx.Done():
				cancel()
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
		req, err := http.NewRequestWithContext(actx, http.MethodGet, rawURL, nil)
		if err != nil {
			cancel()
			return nil, err
		}
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Referer", referer)
		resp, err := p.client.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		status := resp.StatusCode
		resp.Body.Close()
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if status == http.StatusTooManyRequests || status >= 500 {
			lastErr = fmt.Errorf("vidnest: GET %s -> HTTP %d", rawURL, status)
			continue
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("vidnest: GET %s -> HTTP %d", rawURL, status)
		}
		var env vidnestAPIEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			return nil, fmt.Errorf("vidnest: envelope json: %w", err)
		}
		return decodeVidNestEnvelopeBody(env)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("vidnest: request failed")
	}
	return nil, lastErr
}

// decodeVidNestEnvelope parses + decodes one API envelope body (shared by
// the direct and relayed fetch paths).
func decodeVidNestEnvelope(body []byte) (*vidnestPayload, error) {
	var env vidnestAPIEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("vidnest: envelope json: %w", err)
	}
	return decodeVidNestEnvelopeBody(env)
}

func decodeVidNestEnvelopeBody(env vidnestAPIEnvelope) (*vidnestPayload, error) {
	raw, err := vidnestDecode(env.Data)
	if err != nil {
		return nil, err
	}
	var payload vidnestPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("vidnest: payload json: %w", err)
	}
	return &payload, nil
}

// FindEpisodeSource resolves one episode for exactly the requested lang.
// AniList-keyed direct API: no search, no episode listing — the API answers
// per lang, so a missing dub is simply absent (never cross-fallback).
// No resolve cache by operator rule: every call mints fresh URLs.
func (p *VidNestProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("vidnest: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	referer := fmt.Sprintf("%s/anime/%d/%d/%s", vidnestPageBase, id, episode, langKey)
	payload, err := p.fetchAPI(ctx, id, episode, langKey, referer)
	if err != nil {
		if isUpstreamGated(err) {
			return nil, nil
		}
		return nil, err
	}
	if !payload.Success || payload.Status != 200 {
		return nil, nil
	}
	master := ""
	for _, s := range payload.Sources {
		if strings.TrimSpace(s.File) == "" {
			continue
		}
		if strings.EqualFold(s.Type, "hls") || strings.Contains(strings.ToLower(s.File), ".m3u8") {
			master = strings.TrimSpace(s.File)
			break
		}
		if master == "" {
			master = strings.TrimSpace(s.File)
		}
	}
	if master == "" {
		return nil, nil
	}
	// Segment-depth honesty: master -> media -> segment bytes must serve,
	// or the server would only spin at playback.
	if !probeSegmentsLenient(ctx, p.client, master, vidnestReferer, browserUA) {
		p.log.Info().Str("anilistId", anilistID).Int("episode", episode).
			Msg("vidnest: master blocked from this egress, dropping source")
		return nil, nil
	}
	p.learnURLHost(master)

	var subs []core.Subtitle
	for _, t := range payload.Tracks {
		if strings.TrimSpace(t.File) == "" {
			continue
		}
		p.learnURLHost(t.File)
		code := strings.TrimSpace(t.Lang)
		if code == "" {
			code = mapSubtitleLang(t.Label)
		}
		label := strings.TrimSpace(t.Label)
		if label == "" {
			label = code
		}
		subs = append(subs, core.Subtitle{URL: t.File, Lang: code, Label: label})
	}

	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", langKey).Int("subs", len(subs)).Msg("vidnest resolved")
	return &SourceResult{
		Sources: []core.Source{{
			URL:          master,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		}},
		Headers:     map[string]string{"Referer": vidnestReferer},
		ServerNames: []string{vidnestServerName},
		Intro:       payload.Intro,
		Outro:       payload.Outro,
	}, nil
}
