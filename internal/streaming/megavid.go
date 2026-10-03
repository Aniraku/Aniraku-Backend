package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
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
// the requested lang (dub file on a sub request and vice versa). The file
// identity is therefore VERIFIED against Anikoto's same-episode files
// (same catalog, same content hashes) at the fan-out and explicit layers
// (Manager.verifyMegaVidLang): a proven swap is dropped, an unverifiable
// title (no Anikoto reference, or identical files both langs) lists
// as-is. The provider itself never guesses — it ships what upstream
// returned, probe-verified.
//
// SERVER NAMES (operator): single fixed cute name — Vidy. Never raw
// mirror ids (they collide with animex display names).
const (
	megavidDefaultBase = "https://megavid.buzz"
	megavidReferer     = "https://megavid.buzz/"
	megavidServerName  = "Vidy"

	megavidResolveTTL        = 5 * time.Minute
	maxMegavidResolveEntries = 500
)

type MegaVidProvider struct {
	log       zerolog.Logger
	client    *http.Client
	api       string
	learnHost func(host string)

	mu       sync.Mutex
	resolved map[megavidResolveKey]*megavidResolvedEntry
}

type megavidResolveKey struct {
	key     string // "ani" or "mal"
	id      string
	episode int
	lang    string
}

type megavidResolvedEntry struct {
	result  *SourceResult
	fetched time.Time
}

func NewMegaVidProvider(log zerolog.Logger, api string) *MegaVidProvider {
	if strings.TrimSpace(api) == "" {
		api = megavidDefaultBase
	}
	return &MegaVidProvider{
		log:      log,
		client:   &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		api:      strings.TrimRight(api, "/"),
		resolved: make(map[megavidResolveKey]*megavidResolvedEntry),
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

func (p *MegaVidProvider) loadResolved(key megavidResolveKey) *SourceResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[key]
	if !ok || time.Since(e.fetched) > megavidResolveTTL {
		return nil
	}
	return cloneSourceResult(e.result)
}

func (p *MegaVidProvider) storeResolved(key megavidResolveKey, sr *SourceResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, e := range p.resolved {
		if time.Since(e.fetched) > megavidResolveTTL {
			delete(p.resolved, k)
		}
	}
	if len(p.resolved) >= maxMegavidResolveEntries {
		for k := range p.resolved {
			delete(p.resolved, k)
			break
		}
	}
	p.resolved[key] = &megavidResolvedEntry{result: cloneSourceResult(sr), fetched: time.Now()}
}

// FindEpisodeSource resolves one episode for the requested lang: AniList
// key first, MAL key fallback (same fallthrough shape as zoko — one Vidy
// server either way). Language correctness is NOT decided here; the
// manager verifies file identity against Anikoto (verifyMegaVidLang).
func (p *MegaVidProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("megavid: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	if got := p.loadResolved(megavidResolveKey{key: "ani", id: anilistID, episode: episode, lang: langKey}); got != nil {
		return got, nil
	}
	sr, err := p.resolveKey(ctx, "ani", anilistID, episode, langKey)
	if err != nil {
		return nil, err
	}
	usedKey := "ani"
	if sr == nil || len(sr.Sources) == 0 {
		malID := tmdb.FetchMalID(ctx, p.client, id)
		if malID <= 0 {
			malID = fetchAniListMALID(ctx, p.client, id)
		}
		if malID > 0 {
			malStr := strconv.Itoa(malID)
			if got := p.loadResolved(megavidResolveKey{key: "mal", id: malStr, episode: episode, lang: langKey}); got != nil {
				return got, nil
			}
			sr, err = p.resolveKey(ctx, "mal", malStr, episode, langKey)
			if err != nil {
				return nil, err
			}
			usedKey = "mal"
			if sr != nil && len(sr.Sources) > 0 {
				p.storeResolved(megavidResolveKey{key: "mal", id: malStr, episode: episode, lang: langKey}, sr)
				return sr, nil
			}
		}
		return nil, nil
	}
	p.storeResolved(megavidResolveKey{key: usedKey, id: anilistID, episode: episode, lang: langKey}, sr)
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
	if referer == "" {
		referer = "https://megaplay.buzz/"
	}
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
		label := strings.TrimSpace(t.Label)
		code := mapSubtitleLang(label)
		if label == "" {
			label = code
		}
		subs = append(subs, core.Subtitle{URL: t.File, Lang: code, Label: label})
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

// hasMP4Suffix reports a .mp4 file suffix, query-tolerant.
func hasMP4Suffix(rawURL string) bool {
	lower := strings.ToLower(rawURL)
	if i := strings.IndexAny(lower, "?#"); i >= 0 {
		lower = lower[:i]
	}
	return strings.HasSuffix(lower, ".mp4")
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
