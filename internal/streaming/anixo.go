package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// Anixo (anixo.online front, anixo.buzz player edge) resolves the site's own
// first-party HLS relay. Verified end-to-end from VPS egress 2026-10-18:
//
//	GET /embed/ani/<anilistId>/<episode>/1?parentHost=anixo.online
//	    -> the player page (their SDK's authorized-domain claim; the bare
//	       request 403s as "Unauthorized Embed Domain", the parentHost
//	       query is exactly what anixo.online's own SDK sends)
//	    -> inline jwplayer config naming streamUrl = anixo.buzz/api/stream/
//	       m3u8?t=<opaque token> plus a subtitles[] track array (per entry:
//	       url, label, default, kind — tokens are base64URL)
//	GET /api/stream/m3u8?t=... -> master: one 1440x1080 variant whose child
//	    URLs are absolute anixo.buzz/api/stream/m3u8?t=... (self-hosted edge)
//	inner playlist -> absolute /api/stream/... segments, plain 200s
//
// Playback needs NO cookies and NO Referer (measured with neither), so
// Sources work through the media proxy with plain Referer/UA headers.
// The embed is AniList-keyed: no search, no episode list, no mapping step —
// the anilist ID IS the path key.
//
// Dub: the platform's own availability flag (hasDub on their episode-index
// space) was false for every show probed (One Piece 21 and JJK 11061 alike)
// and the audio delivery for a flagged dub was never observable — so this
// provider is listed sub-only for now. A dub enablement must first be
// verified live end-to-end; claiming a dub whose audio is unverified is
// exactly the megavid language-swap class of bug.
const (
	anixoEmbedBase  = "https://anixo.buzz"
	anixoParentHost = "anixo.online"
	anixoReferer    = "https://anixo.online/"
)

// anixoServerName: the platform lists a single slot, "Server 1 (Sora Edge)";
// server aliases can't carry "sora" (animex already owns it) so the display
// name is its own.
const anixoServerName = "Kohaku"

var (
	// {"success":true,"server":"Server 1 (Sora Edge)","serverId":1,
	//  "streamUrl":"https://anixo.buzz/api/stream/m3u8?t=<token>","..."}
	anixoStreamURLRe = regexp.MustCompile(`"streamUrl"\s*:\s*"([^"]+)"`)
	// Track tokens are base64URL (alphabet includes '-' and '_' — observed
	// live: "...EVoUFks-Y2loaDtQ...bT8-UlBXUFdSV19dX1k..."). Keeping the raw
	// single-URL match as the fallback for older page shapes; the primary
	// path parses the subtitles[] array (anixoSubsArrRe).
	anixoVTTURLRe  = regexp.MustCompile(`"(https://anixo\.buzz/api/stream/vtt\?t=[A-Za-z0-9+/=_-]{16,})"`)
	anixoSubsArrRe = regexp.MustCompile(`"subtitles"\s*:\s*(\[[^\]]*\])`)
)

// anixoSubtitleTrack mirrors one entry of the player page's subtitles[]
// array: {"url":"https://anixo.buzz/api/stream/vtt?t=<base64url token>",
// "label":"English","default":true,"kind":"captions"}. Live pages ship the
// full multi-language list (English/German/Spanish/French/Italian/
// Portuguese observed on 185874 ep9); the old single-URLregex path matched
// at most one and its base64-only alphabet could never terminate on
// base64URL tokens, silently yielding zero subtitles for those episodes.
type anixoSubtitleTrack struct {
	URL   string `json:"url"`
	Label string `json:"label"`
	Kind  string `json:"kind"`
}

// parseAnixoSubtitles extracts the player page's subtitle tracks. Primary
// shape: the subtitles[] JSON array (bound by regex, parsed as JSON — track
// URLs carry only base64URL characters, so no unescaped quotes inside).
// Fallback for older page shapes: a bare vtt URL match (now base64URL-aware).
func parseAnixoSubtitles(html string) []anixoSubtitleTrack {
	if m := anixoSubsArrRe.FindStringSubmatch(html); len(m) > 1 {
		var tracks []anixoSubtitleTrack
		if err := json.Unmarshal([]byte(m[1]), &tracks); err == nil && len(tracks) > 0 {
			return tracks
		}
	}
	if m := anixoVTTURLRe.FindStringSubmatch(html); len(m) > 1 {
		return []anixoSubtitleTrack{{URL: m[1], Label: "English"}}
	}
	return nil
}

type AnixoProvider struct {
	log    zerolog.Logger
	client *http.Client
}

func NewAnixoProvider(log zerolog.Logger) *AnixoProvider {
	return &AnixoProvider{
		log:    log,
		client: &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
	}
}

func (p *AnixoProvider) Name() string { return "anixo" }

// Search: AniList-keyed provider; catalog search is not part of playback.
func (p *AnixoProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, nil
}

// FindEpisodes: episode lists come from AniList upstream, not this platform.
func (p *AnixoProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, nil
}

func (p *AnixoProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	if strings.EqualFold(lang, "dub") {
		// Sub-only platform (see header comment): a logical miss, without
		// any breaker implications — the same shape as tensho's "no dub".
		p.log.Info().Str("anilistId", anilistID).Int("episode", episode).
			Msg("anixo: dub not listed for this platform")
		return nil, nil
	}

	embedURL := fmt.Sprintf("%s/embed/ani/%s/%d/1?parentHost=%s",
		anixoEmbedBase, anilistID, episode, anixoParentHost)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, embedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Referer", anixoReferer)
	req.Header.Set("User-Agent", browserUA)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anixo: embed returned status %d", resp.StatusCode)
	}
	page, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil, err
	}
	html := string(page)

	streamURL := ""
	if m := anixoStreamURLRe.FindStringSubmatch(html); len(m) > 1 {
		streamURL = strings.TrimSpace(m[1])
	}
	if streamURL == "" {
		// The 403 body (domain-gate JSON) or an upstream variant change —
		// both surface as a plain resolve failure for this round.
		return nil, fmt.Errorf("anixo: no streamUrl in player page")
	}

	// Subtitle tracks, when the platform ships them for the episode. The
	// "default":true entry is English; hint=en covers any label the lang
	// table does not normalize.
	var subs []core.Subtitle
	for _, t := range parseAnixoSubtitles(html) {
		if strings.TrimSpace(t.URL) == "" {
			continue
		}
		subs = append(subs, buildSubtitle(t.URL, "", t.Label, "en"))
	}

	// Segment-depth honesty, playback shape: no Referer was needed for the
	// actual segment fetch (verified), but the media proxy replays the
	// headers below anyway, so the probe matches the proxied shape.
	if !probeSegmentsLenient(ctx, p.client, streamURL, anixoEmbedBase+"/", browserUA) {
		p.log.Info().Str("anilistId", anilistID).Int("episode", episode).
			Msg("anixo: stream blocked from this egress, dropping source")
		return nil, nil
	}

	p.log.Info().Str("anilistId", anilistID).Int("episode", episode).
		Msg("anixo: resolved")
	return &SourceResult{
		Sources: []core.Source{{
			URL:          streamURL,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		}},
		Headers: map[string]string{
			"Referer":    anixoEmbedBase + "/",
			"User-Agent": browserUA,
		},
		ServerName:  anixoServerName,
		ServerNames: []string{anixoServerName},
	}, nil
}
