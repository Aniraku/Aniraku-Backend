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
)

// AniPMProvider resolves direct HLS streams through ani.pm (AniList-keyed
// series API + playback bootstrap + settlar session + embed session).
// Verified live from the production VPS egress; the client pins tcp4
// because the VPS IPv6 egress is Cloudflare-blocked (error 1106).
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
//     availability: a dub-only title lists no sub server even though the
//     API answers with an effectiveLanguage fallback.
//   - Fresh chain per resolve (no episode cache): media object tokens die
//     fast (a 1h-old token already challenges) while the series mapping is
//     cached 24h.
//   - Skip chapters via /api/anime/skip are best-effort (often {}).
const (
	anipmDefaultBase = "https://ani.pm"
	anipmEmbedBase   = "https://embed.settlar.io"
	anipmReferer     = "https://ani.pm/"
	// Server names (sweets theme): Warabi = the soft-sub settlar embed
	// (warabimochi), Daifuku = the backup embed (a filled mochi), Nama =
	// the beta embed (raw dough — beta server, for episodes ani.pm flags
	// beta/betaSub/betaDub).
	anipmServerName = "Warabi"
	anipmBackupName = "Daifuku"
	anipmBetaName   = "Nama"
	// Chrome 131: embed.settlar.io's Cloudflare 403s the older shared
	// browserUA (Chrome 124); a current UA passes.
	anipmUserAgent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	anipmSeriesTTL  = 24 * time.Hour
	maxAniPMEntries = 500
)

type AniPMProvider struct {
	log        zerolog.Logger
	client     *http.Client
	base       string
	embedBase  string
	anilistURL string
	learnHost  func(host string)

	mu     sync.Mutex
	series map[int]*anipmSeriesEntry
}

type anipmSeriesEntry struct {
	bootSource string // playback-bootstrap path segment: the series' source
	bootID     int    // bootstrap series id (settlar id, else the series id)
	settlarID  int
	malID      int
	anilistID  string
	current    bool
	episodes   []anipmEpisode
	fetched    time.Time
}

type anipmEpisode struct {
	number       int
	sourceNumber string
	hasSub       bool
	hasDub       bool
	beta         bool // episode lives only on the beta embed
	betaSub      bool // the sub track comes from the beta embed
	betaDub      bool // the dub track comes from the beta embed
}

// anipmTransport was replaced by newChromeTLSClient (2026-11-04): ani.pm's
// Cloudflare began rejecting the stock Go TLS fingerprint with error 1106
// on every request; the Chrome-fingerprinted uTLS handshake answers 200.

func NewAniPMProvider(log zerolog.Logger, base, embedBase string) *AniPMProvider {
	if strings.TrimSpace(base) == "" {
		base = anipmDefaultBase
	}
	if strings.TrimSpace(embedBase) == "" {
		embedBase = anipmEmbedBase
	}
	return &AniPMProvider{
		log:        log,
		client:     newChromeTLSClient(45 * time.Second),
		base:       strings.TrimRight(base, "/"),
		embedBase:  strings.TrimRight(embedBase, "/"),
		anilistURL: core.AnilistGraphQLEndpoint(),
		series:     make(map[int]*anipmSeriesEntry),
	}
}

func (p *AniPMProvider) Name() string { return "anipm" }

func (p *AniPMProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *AniPMProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *AniPMProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("anipm search not implemented")
}

func (p *AniPMProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("anipm episode listing not implemented")
}

func (p *AniPMProvider) getJSON(ctx context.Context, rawURL, referer string, out any) error {
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
		req.Header.Set("User-Agent", anipmUserAgent)
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
			lastErr = fmt.Errorf("anipm: GET %s -> HTTP %d", rawURL, status)
			continue
		}
		if status != http.StatusOK {
			return fmt.Errorf("anipm: GET %s -> HTTP %d", rawURL, status)
		}
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("anipm: json %s: %w", rawURL, err)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("anipm: request failed")
	}
	return lastErr
}

type anipmSeriesJSON struct {
	ID        int    `json:"id"`
	Source    string `json:"source"`
	SettlarID int    `json:"settlarId"`
	MalID     int    `json:"malId"`
	Episodes  []struct {
		Number       int  `json:"number"`
		SourceNumber any  `json:"sourceNumber"`
		HasSub       bool `json:"sub"`
		HasDub       bool `json:"dub"`
		Beta         bool `json:"beta"`
		BetaSub      bool `json:"betaSub"`
		BetaDub      bool `json:"betaDub"`
	} `json:"episodes"`
}

func anipmSourceNumber(v any, fallback int) string {
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

func (p *AniPMProvider) resolveSeries(ctx context.Context, id int) (*anipmSeriesEntry, error) {
	p.mu.Lock()
	if e, ok := p.series[id]; ok && time.Since(e.fetched) < anipmSeriesTTL {
		p.mu.Unlock()
		return e, nil
	}
	p.mu.Unlock()

	var sj anipmSeriesJSON
	if err := p.getJSON(ctx, fmt.Sprintf("%s/api/anime/ani/%d?phase=core&routes=e4", p.base, id), p.base+"/", &sj); err != nil {
		p.log.Debug().Err(err).Int("anilistId", id).Msg("anipm: legacy route unavailable, trying current route")
	} else if (sj.SettlarID > 0 || sj.ID > 0) && len(sj.Episodes) > 0 {
		// Route-based shows carry no settlar id: the bootstrap rides the
		// series' own source segment + id instead (2026-11-05, HxH 11061).
		entry := &anipmSeriesEntry{bootSource: sj.Source, bootID: sj.SettlarID, settlarID: sj.SettlarID, malID: sj.MalID, anilistID: strconv.Itoa(id), fetched: time.Now()}
		if entry.bootSource == "" {
			entry.bootSource = "settlar"
		}
		if entry.bootID <= 0 {
			entry.bootID = sj.ID
		}
		for _, e := range sj.Episodes {
			if e.Number < 1 {
				continue
			}
			entry.episodes = append(entry.episodes, anipmEpisode{
				number:       e.Number,
				sourceNumber: anipmSourceNumber(e.SourceNumber, e.Number),
				hasSub:       e.HasSub,
				hasDub:       e.HasDub,
				beta:         e.Beta,
				betaSub:      e.BetaSub,
				betaDub:      e.BetaDub,
			})
		}
		if len(entry.episodes) > 0 {
			p.storeSeries(id, entry)
			return entry, nil
		}
	}
	// Legacy route miss/failure: fall back to the current-route path
	// (AniList title search → series by routeId), which costs extra hops.
	if entry, err := p.resolveCurrentSeries(ctx, id); err == nil && entry != nil {
		p.storeSeries(id, entry)
		return entry, nil
	}
	return nil, fmt.Errorf("anipm: no series mapping for anilist %d", id)
}

func (p *AniPMProvider) storeSeries(id int, entry *anipmSeriesEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, e := range p.series {
		if time.Since(e.fetched) > anipmSeriesTTL {
			delete(p.series, k)
		}
	}
	if len(p.series) >= maxAniPMEntries {
		for k := range p.series {
			delete(p.series, k)
			break
		}
	}
	p.series[id] = entry
}

type anipmCurrentSearchItem struct {
	ID        int    `json:"id"`
	RouteID   string `json:"routeId"`
	Title     string `json:"title"`
	AniListID string `json:"anilistId"`
}

type anipmCurrentSeries struct {
	ID        int    `json:"id"`
	RouteID   string `json:"routeId"`
	AniListID string `json:"anilistId"`
	Source    string `json:"source"`
	MalID     int    `json:"malId"`
	Episodes  []struct {
		Number       int  `json:"number"`
		SourceNumber any  `json:"sourceNumber"`
		HasSub       bool `json:"sub"`
		HasDub       bool `json:"dub"`
		Beta         bool `json:"beta"`
		BetaSub      bool `json:"betaSub"`
		BetaDub      bool `json:"betaDub"`
	} `json:"episodes"`
}

func (p *AniPMProvider) resolveCurrentSeries(ctx context.Context, id int) (*anipmSeriesEntry, error) {
	titles, err := fetchAnilistTitles(ctx, p.client, p.anilistURL, strconv.Itoa(id))
	if err != nil {
		return nil, err
	}
	var hits []anipmCurrentSearchItem
	for _, title := range titles {
		u := p.base + "/api/anime/search?q=" + url.QueryEscape(title)
		if err := p.getJSON(ctx, u, p.base+"/", &hits); err == nil && len(hits) > 0 {
			break
		}
	}
	if len(hits) == 0 {
		return nil, fmt.Errorf("anipm: current search returned no results")
	}
	var match anipmCurrentSearchItem
	for _, item := range hits {
		if item.AniListID == strconv.Itoa(id) {
			match = item
			break
		}
		if match.RouteID == "" && normTitleKey(item.Title) == normTitleKey(titles[0]) {
			match = item
		}
	}
	if match.RouteID == "" {
		return nil, fmt.Errorf("anipm: current search has no matching route")
	}
	var series anipmCurrentSeries
	if err := p.getJSON(ctx, p.base+"/api/anime/series/"+url.PathEscape(match.RouteID)+"?routes=e4", p.base+"/", &series); err != nil {
		return nil, err
	}
	if series.ID <= 0 {
		series.ID = match.ID
	}
	bootSource := series.Source
	if bootSource == "" {
		bootSource = "settlar"
	}
	bootID := series.ID // current-route series id (anilist-keyed shows: == anilist id)
	entry := &anipmSeriesEntry{
		bootSource: bootSource, bootID: bootID, settlarID: series.ID, malID: series.MalID, anilistID: strconv.Itoa(id), current: true,
		fetched: time.Now(),
	}
	for _, episode := range series.Episodes {
		if episode.Number < 1 {
			continue
		}
		entry.episodes = append(entry.episodes, anipmEpisode{
			number: episode.Number, sourceNumber: anipmSourceNumber(episode.SourceNumber, episode.Number),
			hasSub: episode.HasSub, hasDub: episode.HasDub,
			beta: episode.Beta, betaSub: episode.BetaSub, betaDub: episode.BetaDub,
		})
	}
	if len(entry.episodes) == 0 {
		return nil, fmt.Errorf("anipm: current series has no episodes")
	}
	return entry, nil
}

type anipmBootstrap struct {
	Availability struct {
		Sub bool `json:"sub"`
		Dub bool `json:"dub"`
	} `json:"availability"`
	Selection string `json:"settlarSelection"`
	// BackupEmbed is the site's second server (the site UI fetches the same
	// bootstrap with backup=1 and renders core + backupEmbed as separate
	// tabs). Shape for the available case is lenient-parsed: it carries
	// either its own settlar selection (same session chain as core) or a
	// direct source/embed URL.
	BackupEmbed anipmBackupEmbed `json:"backupEmbed"`
	// BetaEmbed appears on the lean (core=0) bootstrap for episodes the
	// site flags beta/betaSub/betaDub — the beta server's own offer.
	BetaEmbed anipmBackupEmbed `json:"betaEmbed"`
}

// anipmBackupEmbed is one backup server offer from the bootstrap.
type anipmBackupEmbed struct {
	Available bool   `json:"available"`
	Source    string `json:"source"`
	Kind      string `json:"kind"`
	Selection string `json:"selection"`
	EmbedURL  string `json:"embedUrl"`
}

type anipmSession struct {
	EmbedURL string `json:"embedUrl"`
}

type anipmEmbedSession struct {
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
// Fresh token chain per call: media object tokens die
// fast, so nothing below the series mapping is cached.
func (p *AniPMProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("anipm: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	series, err := p.resolveSeries(ctx, id)
	if err != nil {
		return nil, err
	}
	var ep *anipmEpisode
	for i := range series.episodes {
		if series.episodes[i].number == episode {
			ep = &series.episodes[i]
			break
		}
	}
	if ep == nil {
		return nil, fmt.Errorf("anipm: episode %d not listed", episode)
	}
	betaServes := ep.beta || (langKey == "sub" && ep.betaSub) || (langKey == "dub" && ep.betaDub)
	coreHasLang := (langKey == "sub" && ep.hasSub) || (langKey == "dub" && ep.hasDub)
	if !coreHasLang && !betaServes {
		p.log.Debug().Str("anilistId", anilistID).Int("episode", episode).Str("lang", langKey).Msg("anipm: episode flag missing lang")
		return nil, nil
	}
	// Beta-flagged-for-this-lang episodes skip the core bootstrap: the site
	// nulls their core version (episode.betaSub/betaDub/beta) and serves
	// them through the lean (core=0) bootstrap + beta embed, named Nama.
	useBeta := betaServes && !coreHasLang
	referer := fmt.Sprintf("%s/anime/", p.base)
	var boot anipmBootstrap
	bootExtra := "&backup=1"
	if useBeta {
		bootExtra = "&core=0"
	}
	bootURL := fmt.Sprintf("%s/api/anime/playback-bootstrap/%s/%d?ep=%s&lang=%s%s",
		p.base, series.bootSource, series.bootID, url.QueryEscape(ep.sourceNumber), langKey, bootExtra)
	if err := p.getJSON(ctx, bootURL, referer, &boot); err != nil {
		if isUpstreamGated(err) {
			p.log.Debug().Err(err).Str("anilistId", anilistID).Int("episode", episode).Msg("anipm: bootstrap gated")
			return nil, nil
		}
		return nil, err
	}
	if langKey == "sub" && !boot.Availability.Sub || langKey == "dub" && !boot.Availability.Dub {
		// Never fall back to the API's effectiveLanguage: a dub-only
		// title must not surface a dub server in a sub listing.
		p.log.Debug().Str("anilistId", anilistID).Int("episode", episode).Str("lang", langKey).Msg("anipm: bootstrap availability lacks lang")
		return nil, nil
	}
	if strings.TrimSpace(boot.Selection) == "" {
		return nil, fmt.Errorf("anipm: no playback selection for episode %d", episode)
	}
	var sess anipmSession
	sessURL := fmt.Sprintf("%s/api/anime/settlar/session?selection=%s&provider=anipm&ep=%d&channel=%s&telemetry=0",
		p.base, url.QueryEscape(boot.Selection), episode, langKey)
	if err := p.getJSON(ctx, sessURL, referer, &sess); err != nil {
		if isUpstreamGated(err) {
			p.log.Debug().Err(err).Str("anilistId", anilistID).Int("episode", episode).Msg("anipm: session gated")
			return nil, nil
		}
		return nil, err
	}
	token := ""
	if i := strings.Index(sess.EmbedURL, "?t="); i >= 0 {
		token = sess.EmbedURL[i+3:]
	}
	if token == "" {
		return nil, fmt.Errorf("anipm: no embed token for episode %d", episode)
	}
	var es anipmEmbedSession
	if err := p.getJSON(ctx, p.embedBase+"/api/embed/session?t="+url.QueryEscape(token), referer, &es); err != nil {
		if isUpstreamGated(err) {
			p.log.Debug().Err(err).Str("anilistId", anilistID).Int("episode", episode).Msg("anipm: embed gated")
			return nil, nil
		}
		return nil, err
	}
	master := strings.TrimSpace(es.Source)
	if master == "" {
		p.log.Debug().Str("anilistId", anilistID).Int("episode", episode).Str("lang", langKey).Msg("anipm: embed session returned empty source")
		return nil, nil
	}
	typ := "hls"
	if strings.EqualFold(es.Kind, "mp4") || regexpMatchMP4(master) {
		typ = "mp4"
	}
	resolved, ok := p.resolveFinalSource(ctx, master, referer, typ)
	if !ok {
		p.log.Info().Str("anilistId", anilistID).Int("episode", episode).
			Msg("anipm: source redirect or fetch failed, dropping source")
		return nil, nil
	}
	master = resolved
	if typ == "hls" {
		// Playlist-depth probe only: segments ride the same host/gating as
		// the playlists (and reach players through our tcp4 proxy anyway),
		// while the extra segment fetch just slows every /servers call.
		if !probePlaylistsLenient(ctx, p.client, master, referer, anipmUserAgent) {
			p.log.Info().Str("anilistId", anilistID).Int("episode", episode).
				Msg("anipm: master blocked from this egress, dropping source")
			return nil, nil
		}
	} else if !probeMediaFileLenient(ctx, p.client, master, referer, anipmUserAgent) {
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
		explicit := strings.TrimSpace(s.Lang)
		if explicit == "" {
			explicit = strings.TrimSpace(s.SrcLang)
		}
		subs = append(subs, buildSubtitle(u, explicit, label, ""))
	}

	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", langKey).Int("subs", len(subs)).Msg("anipm resolved")
	intro, outro := p.fetchSkip(ctx, series.malID, id, episode, langKey)
	sources := []core.Source{{
		URL:          master,
		Type:         typ,
		Quality:      "auto",
		Subtitles:    subs,
		Verification: "proxy",
	}}
	serverName := anipmServerName
	if useBeta {
		serverName = anipmBetaName
	}
	names := []string{serverName}

	// Daifuku (the site's backup server): resolve the backup embed through
	// the same session chain when the bootstrap offers one. Best-effort —
	// most episodes have no backup, and a failed backup never costs the
	// core Mochi source.
	if backup, ok := p.resolveBackupSource(ctx, boot.BackupEmbed, referer); ok {
		sources = append(sources, backup)
		names = append(names, anipmBackupName)
	}

	sr := &SourceResult{
		Sources: sources,
		// User-Agent must ride along: the media proxy defaults to a stale
		// Chrome/124 that Cloudflare 403s on settlar hosts.
		Headers:     map[string]string{"Referer": referer, "User-Agent": anipmUserAgent},
		ServerNames: names,
		ServerName:  serverName,
		Intro:       intro,
		Outro:       outro,
	}
	return sr, nil
}

// resolveBackupSource turns the bootstrap's backupEmbed into a second
// playable source (the site's Daifuku tab). Lenient by design: returns
// ok=false for the common unavailable case.
func (p *AniPMProvider) resolveBackupSource(ctx context.Context, backup anipmBackupEmbed, referer string) (core.Source, bool) {
	if !backup.Available {
		return core.Source{}, false
	}
	master := strings.TrimSpace(backup.Source)
	if master == "" && backup.Selection != "" {
		// Same settlar session chain as the core embed, just with the
		// backup's own selection token.
		var sess anipmSession
		sessURL := p.base + "/api/anime/settlar/session?selection=" + url.QueryEscape(backup.Selection) + "&provider=anipm&telemetry=0"
		if err := p.getJSON(ctx, sessURL, referer, &sess); err != nil {
			return core.Source{}, false
		}
		token := ""
		if i := strings.Index(sess.EmbedURL, "?t="); i >= 0 {
			token = sess.EmbedURL[i+3:]
		}
		if token == "" {
			return core.Source{}, false
		}
		var es anipmEmbedSession
		if err := p.getJSON(ctx, p.embedBase+"/api/embed/session?t="+url.QueryEscape(token), referer, &es); err != nil {
			return core.Source{}, false
		}
		master = strings.TrimSpace(es.Source)
	}
	if master == "" && strings.TrimSpace(backup.EmbedURL) != "" {
		master = strings.TrimSpace(backup.EmbedURL)
	}
	if master == "" {
		return core.Source{}, false
	}
	typ := "hls"
	if strings.EqualFold(backup.Kind, "mp4") || regexpMatchMP4(master) {
		typ = "mp4"
	}
	resolved, ok := p.resolveFinalSource(ctx, master, referer, typ)
	if !ok {
		return core.Source{}, false
	}
	if typ == "hls" && !probePlaylistsLenient(ctx, p.client, resolved, referer, anipmUserAgent) {
		return core.Source{}, false
	}
	p.learnURLHost(resolved)
	return core.Source{
		URL:          resolved,
		Type:         typ,
		Quality:      "auto",
		Verification: "proxy",
	}, true
}

// resolveFinalSource follows the source URL once before it reaches the media
// proxy. The proxy intentionally rejects redirects, so publishing a valid
// player URL that only redirects at playback creates a server that appears in
// /servers but cannot start. The returned URL is the final request URL.
func (p *AniPMProvider) resolveFinalSource(ctx context.Context, rawURL, referer, typ string) (string, bool) {
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", false
	}
	req.Header.Set("User-Agent", anipmUserAgent)
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return "", false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, vodCacheMaxBody+1))
	if err != nil || len(body) == 0 || len(body) > vodCacheMaxBody {
		return "", false
	}
	if typ == "hls" && !strings.Contains(string(body), "#EXTM3U") {
		return "", false
	}
	if typ == "mp4" && !segmentBytesPlayable(body) {
		return "", false
	}
	finalURL := rawURL
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}
	if typ == "hls" {
		VODCacheSet(finalURL, body)
	}
	return finalURL, true
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
func (p *AniPMProvider) fetchSkip(ctx context.Context, malID, anilistID, episode int, lang string) (intro, outro *core.SkipTimestamp) {
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
