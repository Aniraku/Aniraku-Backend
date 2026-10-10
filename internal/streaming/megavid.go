package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
)

// MegaVidProvider resolves direct HLS streams through megavid.buzz
// ({mal|ani}/{id}/{ep}/{lang}/source). Verified live 2026-10-03 from the
// production egress: plain HTTP, AniList- and MAL-keyed, no egress block.
//
// The API answers JSON:
// {"status":"ok","source":<cdnx animex-codec URL>,"provider":<mirror id>,
//
//	"providers":[...],"tracks":[...],"chapters":[{Intro|Outro,start,end}],
//	"type":"hls"}.
//
// Sources decode with the AnimeX XOR codec to direct MegaPlay files — the
// SAME catalog Anikoto serves (identical file paths). The decoded file URL
// ships as-is (its embedded referer rides along); per-provider re-wraps
// are deliberately skipped — the segment probe arbitrates playability.
//
// LANGUAGE RULE (operator): megavid sometimes serves the wrong audio for
// the requested lang (dub file on a sub request and vice versa — proven
// live: an unreleased "dub" carried jpn segments). Sources are verified
// in three layers (Manager.verifyMegaVidLang) and a proven mismatch is
// dropped:
//  1. m3u audio declarations: #EXT-X-MEDIA TYPE=AUDIO LANGUAGE tags name
//     the actual tracks; a master declaring audio but not the requested
//     lang is a proven swap. Masters without declarations (muxed audio,
//     the common MegaPlay shape) are unknown at this layer.
//  2. segment audio descriptors: the first segment's TS PMT names the
//     carried audio (ground truth); a mismatch drops, unreadable
//     segments fall through.
//  3. file identity: the decoded file path is compared against Anikoto's
//     same-episode files (same MegaPlay catalog, identical paths); a file
//     proving to be the other lang's encode is dropped — but only when a
//     same-lang reference exists, so the comparison can never hide Vidy
//     on shaky grounds.
//
// Anything unverifiable lists as-is — never drop blind.
//
// FRESHNESS (operator): no resolve cache — /vid/ gateway URLs are
// session tokens of unknown (short) lifetime, so every lookup re-runs
// the API call. Single cheap GET per resolve.
//
// SERVER NAMES (operator): single fixed cute name — Vidy. Never raw
// mirror ids (they collide with animex display names).
const (
	megavidDefaultBase = "https://megavid.buzz"
	megavidReferer     = "https://megavid.buzz/"
	megavidServerName  = "Vidy"
)

type MegaVidProvider struct {
	log       zerolog.Logger
	client    *http.Client
	api       string
	learnHost func(host string)
}

func NewMegaVidProvider(log zerolog.Logger, api string) *MegaVidProvider {
	if strings.TrimSpace(api) == "" {
		api = megavidDefaultBase
	}
	return &MegaVidProvider{
		log:    log,
		client: &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		api:    strings.TrimRight(api, "/"),
	}
}

func (p *MegaVidProvider) Name() string { return "megavid" }

func (p *MegaVidProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *MegaVidProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *MegaVidProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("megavid search not implemented")
}

func (p *MegaVidProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("megavid episode listing not implemented")
}

type megavidSourceJSON struct {
	Status   string `json:"status"`
	Source   string `json:"source"`
	Provider string `json:"provider"`
	Tracks   []struct {
		File  string `json:"file"`
		Label string `json:"label"`
		Kind  string `json:"kind"`
	} `json:"tracks"`
	Chapters []struct {
		Title string  `json:"title"`
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	} `json:"chapters"`
	Type string `json:"type"`
}

func (p *MegaVidProvider) fetchSource(ctx context.Context, key, id string, episode int, lang string) (*megavidSourceJSON, error) {
	rawURL := fmt.Sprintf("%s/%s/%s/%d/%s/source", p.api, key, url.PathEscape(id), episode, lang)
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		actx, cancel := context.WithTimeout(ctx, 12*time.Second)
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
		req.Header.Set("Referer", megavidReferer)
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
			lastErr = fmt.Errorf("megavid: GET %s -> HTTP %d", rawURL, status)
			continue
		}
		if status == http.StatusNotFound {
			// This key carries no source (not an error): silent skip so
			// the other key can still resolve.
			return nil, nil
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("megavid: GET %s -> HTTP %d", rawURL, status)
		}
		var out megavidSourceJSON
		if err := json.Unmarshal(body, &out); err != nil {
			return nil, fmt.Errorf("megavid: json %s: %w", rawURL, err)
		}
		if !strings.EqualFold(out.Status, "ok") || strings.TrimSpace(out.Source) == "" {
			return nil, nil
		}
		return &out, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("megavid: request failed")
	}
	return nil, lastErr
}

// FindEpisodeSource resolves one episode for the requested lang: AniList
// key first, MAL key fallback (same fallthrough shape as zoko — one Vidy
// server either way). Fresh API call per resolve (operator rule): /vid/
// gateway URLs are session tokens of unknown lifetime. Language
// correctness is NOT decided here; the manager verifies in two layers
// (verifyMegaVidLang).
func (p *MegaVidProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("megavid: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	sr, err := p.resolveKey(ctx, "ani", anilistID, episode, langKey)
	if err != nil {
		return nil, err
	}
	if sr == nil || len(sr.Sources) == 0 {
		malID := tmdb.FetchMalID(ctx, p.client, id)
		if malID <= 0 {
			malID = fetchAniListMALID(ctx, p.client, id)
		}
		if malID > 0 {
			malStr := strconv.Itoa(malID)
			// MAL numbering can follow a different season cut than
			// AniList — a fallback resolve is correct only when the
			// mapping is 1:1, so it always logs loudly.
			p.log.Info().Str("anilistId", anilistID).Int("malId", malID).
				Int("episode", episode).Msg("megavid: AniList key missed, trying MAL key")
			sr, err = p.resolveKey(ctx, "mal", malStr, episode, langKey)
			if err != nil {
				return nil, err
			}
		}
		if sr == nil || len(sr.Sources) == 0 {
			return nil, nil
		}
	}
	return sr, nil
}

func (p *MegaVidProvider) resolveKey(ctx context.Context, key, id string, episode int, lang string) (*SourceResult, error) {
	js, err := p.fetchSource(ctx, key, id, episode, lang)
	if err != nil {
		return nil, err
	}
	if js == nil {
		return nil, nil
	}
	raw, referer, _ := DecodeAnimeXProxyURL(strings.TrimSpace(js.Source))
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	// Referer is per family: the /vid/ gateway only serves its own
	// referer (anything else 403s); codec-decoded files carry their
	// embedded referer (megaplay.buzz fallback).
	referer = megavidRefererFor(raw, referer)
	// The decoded URL is already the final direct file (verified live
	// with its embedded referer) — never re-wrap it through the animex
	// proxy codec: that only adds a third-party hop, and the segment
	// probe below is the arbiter of playability either way.
	file := raw
	typ := "hls"
	if strings.EqualFold(js.Type, "mp4") || hasMP4Suffix(file) {
		typ = "mp4"
	}
	if typ == "hls" {
		if !probeSegmentsLenient(ctx, p.client, file, referer, browserUA) {
			p.log.Info().Str("key", key+":"+id).Int("episode", episode).
				Msg("megavid: master blocked from this egress, dropping source")
			return nil, nil
		}
	} else if !probeMediaFileLenient(ctx, p.client, file, referer, browserUA) {
		return nil, nil
	}
	p.learnURLHost(file)

	var subs []core.Subtitle
	for _, t := range js.Tracks {
		if strings.TrimSpace(t.File) == "" {
			continue
		}
		p.learnURLHost(t.File)
		subs = append(subs, buildSubtitle(t.File, "", t.Label, ""))
	}
	var intro, outro *core.SkipTimestamp
	for _, c := range js.Chapters {
		if c.End <= c.Start {
			continue
		}
		ts := &core.SkipTimestamp{Start: c.Start, End: c.End}
		switch strings.ToLower(strings.TrimSpace(c.Title)) {
		case "intro":
			intro = ts
		case "outro":
			outro = ts
		}
	}

	p.log.Info().Str("key", key+":"+id).Int("episode", episode).
		Str("lang", lang).Str("mirror", js.Provider).Msg("megavid resolved")
	return &SourceResult{
		Sources: []core.Source{{
			URL:          file,
			Type:         typ,
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		}},
		Headers:     map[string]string{"Referer": referer},
		ServerNames: []string{megavidServerName},
		Intro:       intro,
		Outro:       outro,
	}, nil
}

// megavidRefererFor picks the playback/probe referer for a resolved file:
// embedded codec referers win, the /vid/ gateway needs its own origin,
// anything else falls back to megaplay.buzz.
func megavidRefererFor(raw, embedded string) string {
	if strings.TrimSpace(embedded) != "" {
		return embedded
	}
	if u, err := url.Parse(strings.TrimSpace(raw)); err == nil {
		if strings.Contains(strings.ToLower(u.Host), "megavid.buzz") {
			return megavidReferer
		}
	}
	return "https://megaplay.buzz/"
}

// hasMP4Suffix reports a .mp4 file suffix, query-tolerant.
func hasMP4Suffix(rawURL string) bool {
	lower := strings.ToLower(rawURL)
	if i := strings.IndexAny(lower, "?#"); i >= 0 {
		lower = lower[:i]
	}
	return strings.HasSuffix(lower, ".mp4")
}

// m3uAudioLangRe matches audio rendition declarations in master
// playlists: #EXT-X-MEDIA:TYPE=AUDIO,...,LANGUAGE="en",...
var m3uAudioLangRe = regexp.MustCompile(`(?i)#EXT-X-MEDIA:[^\n\r]*TYPE=AUDIO[^\n\r]*LANGUAGE="([^"]+)"`)

// normalizeAudioLang maps a playlist language tag to a two-letter code:
// "en"/"eng"/"English" -> en, "ja"/"jpn"/"Japanese" -> ja, region
// qualified ("es-419", "zh-Hans") folds to its base. Empty when unknown.
func normalizeAudioLang(tag string) string {
	s := strings.ToLower(strings.TrimSpace(tag))
	if i := strings.IndexAny(s, "-_"); i >= 0 {
		s = s[:i]
	}
	if len(s) == 2 {
		ok := true
		for _, r := range s {
			if r < 'a' || r > 'z' {
				ok = false
			}
		}
		if ok {
			return s
		}
	}
	switch s {
	case "eng", "english":
		return "en"
	case "jpn", "japanese":
		return "ja"
	case "spa", "spanish", "espanol", "español":
		return "es"
	case "fra", "fre", "french", "francais", "français":
		return "fr"
	case "deu", "ger", "german":
		return "de"
	case "por", "portuguese", "portugues", "português":
		return "pt"
	case "ara", "arabic":
		return "ar"
	case "hin", "hindi":
		return "hi"
	case "kor", "korean":
		return "ko"
	case "rus", "russian":
		return "ru"
	case "ita", "italian":
		return "it"
	case "tha", "thai":
		return "th"
	case "vie", "vietnamese":
		return "vi"
	case "ind", "indonesian":
		return "id"
	case "may", "malay", "melayu":
		return "ms"
	case "zho", "chi", "chinese":
		return "zh"
	case "tur", "turkish":
		return "tr"
	case "pol", "polish":
		return "pl"
	case "ukr", "ukrainian":
		return "uk"
	case "nld", "dutch", "nederlands":
		return "nl"
	case "swe", "swedish":
		return "sv"
	case "dan", "danish":
		return "da"
	case "fin", "finnish":
		return "fi"
	case "nor", "norwegian", "nob", "nno":
		return "no"
	case "ell", "greek":
		return "el"
	case "heb", "hebrew":
		return "he"
	case "ces", "czech":
		return "cs"
	case "hun", "hungarian":
		return "hu"
	case "ron", "romanian":
		return "ro"
	}
	return ""
}

// m3uAudioLangs lists the normalized audio languages a master playlist
// declares across its AUDIO renditions. Empty when the master declares
// none (muxed single audio — unknowable at playlist level).
func m3uAudioLangs(body []byte) map[string]bool {
	out := map[string]bool{}
	for _, m := range m3uAudioLangRe.FindAllSubmatch(body, -1) {
		if len(m) == 2 {
			if code := normalizeAudioLang(string(m[1])); code != "" {
				out[code] = true
			}
		}
	}
	return out
}

// wantAudioLang maps our request lang to the playlist audio code:
// sub carries Japanese audio, dub carries English.
func wantAudioLang(lang string) string {
	if strings.EqualFold(lang, "dub") {
		return "en"
	}
	return "ja"
}

// megavidVerdict classifies one resolved file key against the trusted
// same-lang and other-lang reference sets: confirmed proves the requested
// audio, swapped proves the opposite audio, neither means unknown (the
// caller keeps unverifiable titles rather than dropping blind).
func megavidVerdict(key string, same, other map[string]bool) (confirmed, swapped bool) {
	if same[key] {
		return true, false
	}
	if len(other) > 0 && other[key] {
		return false, true
	}
	return false, false
}

// megavidFileKey normalizes a resolved file URL to its content identity
// for cross-provider comparison: tokens and nonces rotate per resolve, so
// only scheme+host+path identify the encode.
func megavidFileKey(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" {
		return strings.TrimSpace(rawURL)
	}
	u.RawQuery, u.Fragment = "", ""
	u.Host = strings.ToLower(u.Host)
	return u.String()
}
