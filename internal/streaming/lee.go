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
)

// LeeProvider resolves direct HLS streams through ani.pm (AniList-keyed
// series API + playback bootstrap + settlar session + embed session).
// Verified live 2026-10-02 from the production egress (which serves every
// hop; some residential egresses are challenged on settlar hosts instead).
//
// CHAIN (observed from the watch page traffic):
//  1. GET /api/anime/ani/{anilistId}?phase=core&routes=e4
//     -> settlarId + episode list (per-episode sub/dub flags).
//  2. GET /api/anime/playback-bootstrap/settlar/{id}?ep={n}&lang={l}&backup=1
//     -> availability{sub,dub} + settlarSelection token.
//  3. GET /api/anime/settlar/session?selection=...&provider=anipm&ep={n}
//     &channel={lang}&telemetry=0 -> signed embedUrl (~10h).
//  4. GET embed.settlar.io/api/embed/session?t={token}
//     -> media m3u8 URL (kind hls) + subtitle tracks.
//
// SOURCE RULES (operator):
//   - Strict per-lang from the episode flags AND the bootstrap
//     availability: a dub-only title (Astro Boy 1963) lists no sub server
//     even though the API answers with an effectiveLanguage fallback.
//   - Fresh chain per resolve (no episode cache): media object tokens die
//     fast (a 1h-old token already challenges) while the series mapping is
//     cached 24h.
//   - Skip chapters via /api/anime/skip are best-effort (often {}).
const (
	leeDefaultBase      = "https://ani.pm"
	leeEmbedBase        = "https://embed.settlar.io"
	leeReferer          = "https://ani.pm/"
	leeServerName       = "Lee"
	leeSeriesTTL        = 24 * time.Hour
	maxLeeSeriesEntries = 500
)

type LeeProvider struct {
	log       zerolog.Logger
	client    *http.Client
	base      string
	embedBase string
	learnHost func(host string)

	mu     sync.Mutex
	series map[int]*leeSeriesEntry
}

type leeSeriesEntry struct {
	settlarID int
	malID     int
	episodes  []leeEpisode
	fetched   time.Time
}

type leeEpisode struct {
	number       int
	sourceNumber string
	hasSub       bool
	hasDub       bool
}

func NewLeeProvider(log zerolog.Logger, base, embedBase string) *LeeProvider {
	if strings.TrimSpace(base) == "" {
		base = leeDefaultBase
	}
	if strings.TrimSpace(embedBase) == "" {
		embedBase = leeEmbedBase
	}
	return &LeeProvider{
		log:       log,
		client:    &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		base:      strings.TrimRight(base, "/"),
		embedBase: strings.TrimRight(embedBase, "/"),
		series:    make(map[int]*leeSeriesEntry),
	}
}

func (p *LeeProvider) Name() string { return "lee" }

func (p *LeeProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *LeeProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *LeeProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("lee search not implemented")
}

func (p *LeeProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("lee episode listing not implemented")
}

func (p *LeeProvider) getJSON(ctx context.Context, rawURL, referer string, out any) error {
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		actx, cancel := context.WithTimeout(ctx, 12*time.Second)
		if attempt > 1 {
			select {
			case <-ctx.Done():
				cancel()
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
		req, err := http.NewRequestWithContext(actx, http.MethodGet, rawURL, nil)
		if err != nil {
			cancel()
			return err
		}
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Accept", "application/json")
		if referer != "" {
			req.Header.Set("Referer", referer)
		}
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
			lastErr = fmt.Errorf("lee: GET %s -> HTTP %d", rawURL, status)
			continue
		}
		if status != http.StatusOK {
			return fmt.Errorf("lee: GET %s -> HTTP %d", rawURL, status)
		}
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("lee: json %s: %w", rawURL, err)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("lee: request failed")
	}
	return lastErr
}

type leeSeriesJSON struct {
	SettlarID int `json:"settlarId"`
	MalID     int `json:"malId"`
	Episodes  []struct {
		Number       int  `json:"number"`
		SourceNumber any  `json:"sourceNumber"`
		HasSub       bool `json:"sub"`
		HasDub       bool `json:"dub"`
	} `json:"episodes"`
}

func leeSourceNumber(v any, fallback int) string {
	switch n := v.(type) {
	case string:
		if strings.TrimSpace(n) != "" {
			return strings.TrimSpace(n)
		}
	case float64:
		return strconv.Itoa(int(n))
	case int:
		return strconv.Itoa(n)
	}
	return strconv.Itoa(fallback)
}

func (p *LeeProvider) resolveSeries(ctx context.Context, id int) (*leeSeriesEntry, error) {
	p.mu.Lock()
	if e, ok := p.series[id]; ok && time.Since(e.fetched) < leeSeriesTTL {
		p.mu.Unlock()
		return e, nil
	}
	p.mu.Unlock()

	var sj leeSeriesJSON
	if err := p.getJSON(ctx, fmt.Sprintf("%s/api/anime/ani/%d?phase=core&routes=e4", p.base, id), p.base+"/", &sj); err != nil {
		return nil, err
	}
	if sj.SettlarID <= 0 || len(sj.Episodes) == 0 {
		return nil, fmt.Errorf("lee: no series mapping for anilist %d", id)
	}
	entry := &leeSeriesEntry{settlarID: sj.SettlarID, malID: sj.MalID, fetched: time.Now()}
	for _, e := range sj.Episodes {
		if e.Number < 1 {
			continue
		}
		entry.episodes = append(entry.episodes, leeEpisode{
			number:       e.Number,
			sourceNumber: leeSourceNumber(e.SourceNumber, e.Number),
			hasSub:       e.HasSub,
			hasDub:       e.HasDub,
		})
	}
	if len(entry.episodes) == 0 {
		return nil, fmt.Errorf("lee: no episodes for anilist %d", id)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, e := range p.series {
		if time.Since(e.fetched) > leeSeriesTTL {
			delete(p.series, k)
		}
	}
	if len(p.series) >= maxLeeSeriesEntries {
		for k := range p.series {
			delete(p.series, k)
			break
		}
	}
	p.series[id] = entry
	return entry, nil
}

type leeBootstrap struct {
	Availability struct {
		Sub bool `json:"sub"`
		Dub bool `json:"dub"`
	} `json:"availability"`
	Selection string `json:"settlarSelection"`
}

type leeSession struct {
	EmbedURL string `json:"embedUrl"`
}

type leeEmbedSession struct {
	Source    string `json:"source"`
	Kind      string `json:"kind"`
	AudioLang string `json:"audioLang"`
	Subtitles []struct {
		URL     string `json:"url"`
		File    string `json:"file"`
		Label   string `json:"label"`
		Name    string `json:"name"`
		Lang    string `json:"lang"`
		SrcLang string `json:"srclang"`
	} `json:"subtitles"`
}

// FindEpisodeSource resolves one episode for exactly the requested lang.
// Fresh token chain per call (operator rule): media object tokens die
// fast, so nothing below the series mapping is cached.
func (p *LeeProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("lee: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	series, err := p.resolveSeries(ctx, id)
	if err != nil {
		return nil, err
	}
	var ep *leeEpisode
	for i := range series.episodes {
		if series.episodes[i].number == episode {
			ep = &series.episodes[i]
			break
		}
	}
	if ep == nil {
		return nil, fmt.Errorf("lee: episode %d not listed", episode)
	}
	if langKey == "sub" && !ep.hasSub || langKey == "dub" && !ep.hasDub {
		return nil, nil
	}
	referer := fmt.Sprintf("%s/anime/", p.base)
	var boot leeBootstrap
	bootURL := fmt.Sprintf("%s/api/anime/playback-bootstrap/settlar/%d?ep=%s&lang=%s&backup=1",
		p.base, series.settlarID, url.QueryEscape(ep.sourceNumber), langKey)
	if err := p.getJSON(ctx, bootURL, referer, &boot); err != nil {
		if isUpstreamGated(err) {
			return nil, nil
		}
		return nil, err
	}
	if langKey == "sub" && !boot.Availability.Sub || langKey == "dub" && !boot.Availability.Dub {
		// Never fall back to the API's effectiveLanguage: a dub-only
		// title must not surface a dub server in a sub listing.
		return nil, nil
	}
	if strings.TrimSpace(boot.Selection) == "" {
		return nil, fmt.Errorf("lee: no playback selection for episode %d", episode)
	}
	var sess leeSession
	sessURL := fmt.Sprintf("%s/api/anime/settlar/session?selection=%s&provider=anipm&ep=%d&channel=%s&telemetry=0",
		p.base, url.QueryEscape(boot.Selection), episode, langKey)
	if err := p.getJSON(ctx, sessURL, referer, &sess); err != nil {
		if isUpstreamGated(err) {
			return nil, nil
		}
		return nil, err
	}
	token := ""
	if i := strings.Index(sess.EmbedURL, "?t="); i >= 0 {
		token = sess.EmbedURL[i+3:]
	}
	if token == "" {
		return nil, fmt.Errorf("lee: no embed token for episode %d", episode)
	}
	var es leeEmbedSession
	if err := p.getJSON(ctx, p.embedBase+"/api/embed/session?t="+url.QueryEscape(token), referer, &es); err != nil {
		if isUpstreamGated(err) {
			return nil, nil
		}
		return nil, err
	}
	master := strings.TrimSpace(es.Source)
	if master == "" {
		return nil, nil
	}
	typ := "hls"
	if strings.EqualFold(es.Kind, "mp4") || regexpMatchMP4(master) {
		typ = "mp4"
	}
	if typ == "hls" {
		if !probeSegmentsLenient(ctx, p.client, master, referer, browserUA) {
			p.log.Info().Str("anilistId", anilistID).Int("episode", episode).
				Msg("lee: master blocked from this egress, dropping source")
			return nil, nil
		}
	} else if !probeMediaFileLenient(ctx, p.client, master, referer, browserUA) {
		return nil, nil
	}
	p.learnURLHost(master)

	var subs []core.Subtitle
	for _, s := range es.Subtitles {
		u := strings.TrimSpace(s.URL)
		if u == "" {
			u = strings.TrimSpace(s.File)
		}
		if u == "" {
			continue
		}
		p.learnURLHost(u)
		label := strings.TrimSpace(s.Label)
		if label == "" {
			label = strings.TrimSpace(s.Name)
		}
		code := strings.TrimSpace(s.Lang)
		if code == "" {
			code = strings.TrimSpace(s.SrcLang)
		}
		if code == "" {
			code = mapSubtitleLang(label)
		}
		if label == "" {
			label = code
		}
		subs = append(subs, core.Subtitle{URL: u, Lang: code, Label: label})
	}

	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", langKey).Int("subs", len(subs)).Msg("lee resolved")
	intro, outro := p.fetchSkip(ctx, series.malID, id, episode, langKey)
	sr := &SourceResult{
		Sources: []core.Source{{
			URL:          master,
			Type:         typ,
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		}},
		Headers:     map[string]string{"Referer": referer},
		ServerNames: []string{leeServerName},
		Intro:       intro,
		Outro:       outro,
	}
	return sr, nil
}

// regexpMatchMP4 reports a .mp4 file suffix (query-tolerant) without
// compiling a regex per call.
func regexpMatchMP4(rawURL string) bool {
	lower := strings.ToLower(rawURL)
	if i := strings.IndexAny(lower, "?#"); i >= 0 {
		lower = lower[:i]
	}
	return strings.HasSuffix(lower, ".mp4")
}

// fetchSkip reads intro/outro chapters best-effort; empty on any failure
// (skip data must never fail playback).
func (p *LeeProvider) fetchSkip(ctx context.Context, malID, anilistID, episode int, lang string) (intro, outro *core.SkipTimestamp) {
	if malID <= 0 {
		return nil, nil
	}
	var out struct {
		Intro *core.SkipTimestamp `json:"intro"`
		Outro *core.SkipTimestamp `json:"outro"`
	}
	u := fmt.Sprintf("%s/api/anime/skip?malId=%d&ep=%d&anilistId=%d&lang=%s",
		p.base, malID, episode, anilistID, url.QueryEscape(lang))
	if err := p.getJSON(ctx, u, p.base+"/", &out); err != nil {
		return nil, nil
	}
	if out.Intro != nil && out.Intro.End <= out.Intro.Start {
		out.Intro = nil
	}
	if out.Outro != nil && out.Outro.End <= out.Outro.Start {
		out.Outro = nil
	}
	return out.Intro, out.Outro
}
