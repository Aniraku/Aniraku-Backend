package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
)

const (
	ogflixBase = "https://ogflix.tr"
	// ogflixPlayerUA is the UA used for all OGFLix API calls.
	ogflixPlayerUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
	// ogflixXHRHeader is the value of X-Requested-With the OGFLix embedded
	// player (api.anizen.tr/player/...) sends to its /resolve endpoint. The
	// endpoint 403s ("unsupported_browser") without it.
	ogflixXHRHeader = "ZenPlayer"
)

// ogflixServers maps the two MegaPlay embed variants to display names:
// the base embed becomes Hoshi, the ?s=tcdn variant becomes Yume.
var ogflixServers = [2]string{"Hoshi", "Yume"}

// ogflixStreamResp is the /api/stream envelope: the selected server's
// streaming link plus the full server list, each carrying an encrypted
// player URL (api.anizen.tr/player/<CryptoJS blob>).
type ogflixStreamResp struct {
	Success bool `json:"success"`
	Results struct {
		StreamingLink struct {
			Server string `json:"server"`
			Type   string `json:"type"`
			Link   struct {
				File string `json:"file"`
				Type string `json:"type"`
			} `json:"link"`
		} `json:"streamingLink"`
		Servers []struct {
			ServerName string `json:"serverName"`
			Type       string `json:"type"`
			Embed      string `json:"embed"`
		} `json:"servers"`
	} `json:"results"`
}

// ogflixResolveResp is the player /resolve payload: real (unencrypted) embed
// URLs for every server behind the player.
type ogflixResolveResp struct {
	URL     string `json:"url"`
	Index   int    `json:"index"`
	Servers []struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"servers"`
}

// ogflixSearchResp is the /api/search envelope.
type ogflixSearchResp struct {
	Success bool `json:"success"`
	Results struct {
		Data []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
			JName string `json:"jname"`
		} `json:"data"`
	} `json:"results"`
}

// ogflixInfoResp is the /api/info envelope. anilistId/malId are the exact-ID
// verification hook: when a search slug does not map 1:1 to our AniList ID,
// the info payload proves (or refutes) the match.
type ogflixInfoResp struct {
	Success bool `json:"success"`
	Results struct {
		Data struct {
			ID        string `json:"id"`
			AnilistID *int   `json:"anilistId"`
			MalID     *int   `json:"malId"`
			Episodes  struct {
				TotalEpisodes int `json:"totalEpisodes"`
				Episodes      []struct {
					EpisodeNo int    `json:"episode_no"`
					Title     string `json:"title"`
					Filler    bool   `json:"filler"`
					HasSub    bool   `json:"hasSub"`
					HasDub    bool   `json:"hasDub"`
				} `json:"episodes"`
			} `json:"episodes"`
		} `json:"data"`
	} `json:"results"`
}

// OGFLixProvider resolves streams from OGFLix (Zen API on ogflix.tr, player
// on api.anizen.tr). Chain: AniList ID -> title search + exact anilistId/malId
// verification -> /api/stream -> player /resolve (X-Requested-With: ZenPlayer)
// -> MegaPlay embeds -> shared MegaPlay decrypt chain -> verified m3u8. The
// decrypted sources are referer-gated, so they ship with Verification
// "proxy" and play through the Aniraku media proxy.
type OGFLixProvider struct {
	client    *http.Client
	log       zerolog.Logger
	learnHost func(host string)

	slugMu    sync.RWMutex
	slugCache map[string]string // anilistID -> ogflix slug (verified only)
}

func (p *OGFLixProvider) SetHostLearner(fn func(host string)) {
	p.learnHost = fn
}

func (p *OGFLixProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func NewOGFlixProvider(log zerolog.Logger) *OGFLixProvider {
	jar, _ := cookiejar.New(nil)
	return &OGFLixProvider{
		client:    &http.Client{Timeout: 15 * time.Second, Jar: jar, Transport: netguard.NewTransport()},
		log:       log,
		slugCache: map[string]string{},
	}
}

func (p *OGFLixProvider) Name() string { return "ogflix" }

func (p *OGFLixProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("ogflix search not implemented")
}

// FindEpisodes lists episode numbers for an OGFLix slug via /api/info.
func (p *OGFLixProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	info, err := p.fetchInfo(ctx, providerID)
	if err != nil {
		return nil, err
	}
	eps := info.Results.Data.Episodes.Episodes
	out := make([]Episode, 0, len(eps))
	for _, e := range eps {
		title := e.Title
		out = append(out, Episode{Number: e.EpisodeNo, Title: title, Filler: e.Filler})
	}
	return out, nil
}

// resolveShow maps an AniList ID to an OGFLix slug. Same mechanism as the
// Anikoto resolver for shows whose slug/ID do not match ours: search every
// AniList title keyword, score all candidates with the Anivexa additive
// scorer, then verify the top candidates by exact anilistId/malId match in
// /api/info. If no candidate is ID-verifiable, the top-scored one is used
// (Anivexa parity: no threshold).
func (p *OGFLixProvider) resolveShow(ctx context.Context, anilistID string) (string, error) {
	p.slugMu.RLock()
	slug := p.slugCache[anilistID]
	p.slugMu.RUnlock()
	if slug != "" {
		return slug, nil
	}

	meta, err := fetchAniListMetaFor(ctx, p.client, anilistID)
	if err != nil {
		return "", fmt.Errorf("ogflix: anilist meta failed: %w", err)
	}

	// MAL ID for exact verification (some OGFLix entries have anilistId:null
	// but always carry malId). Fetched in parallel with the search fan-out —
	// it is only needed at verification time and costs 1-2s otherwise.
	var malID int
	var malWG sync.WaitGroup
	if id, convErr := strconv.Atoi(anilistID); convErr == nil && id > 0 {
		malWG.Add(1)
		go func() {
			defer malWG.Done()
			malID = tmdb.FetchMalID(ctx, p.client, id)
		}()
	}

	// Fan out searches over every title keyword.
	type result struct {
		cands []titleCand
	}
	results := make([]result, len(meta.keywords()))
	var wg sync.WaitGroup
	for i, q := range meta.keywords() {
		wg.Add(1)
		go func(i int, q string) {
			defer wg.Done()
			results[i].cands = p.searchCandidates(ctx, q)
		}(i, q)
	}
	wg.Wait()
	malWG.Wait()
	seen := map[string]bool{}
	candBySlug := map[string]titleCand{}
	var order []string
	for _, r := range results {
		for _, c := range r.cands {
			if !seen[c.slug] {
				seen[c.slug] = true
				order = append(order, c.slug)
				candBySlug[c.slug] = c
			}
		}
	}
	scored := make([]titleCand, 0, len(order))
	for _, slug := range order {
		c := candBySlug[slug]
		c.score = scoreShowCandidate(c, meta)
		scored = append(scored, c)
	}
	if len(scored) == 0 {
		return "", fmt.Errorf("ogflix: no search results for anilistId=%s", anilistID)
	}
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].score > scored[j].score })

	// Verify top candidates concurrently: /api/info must expose our exact
	// anilistId or malId. First verified hit wins and is cached.
	n := len(scored)
	if n > 4 {
		n = 4
	}
	type verification struct {
		slug   string
		verified bool
	}
	verifications := make([]verification, n)
	var vwg sync.WaitGroup
	for i := 0; i < n; i++ {
		vwg.Add(1)
		go func(i int) {
			defer vwg.Done()
			ok := p.verifySlug(ctx, scored[i].slug, anilistID, malID)
			verifications[i] = verification{slug: scored[i].slug, verified: ok}
		}(i)
	}
	vwg.Wait()
	for _, v := range verifications {
		if v.verified {
			p.log.Info().Str("anilistId", anilistID).Str("slug", v.slug).Msg("ogflix: resolved via exact ID match")
			p.cacheSlug(anilistID, v.slug)
			return v.slug, nil
		}
	}

	// No exact ID proof: fall back to the top-scored candidate (same
	// no-threshold behavior as the Anikoto resolver). Not cached, so a
	// better verification can still win later.
	p.log.Info().Str("anilistId", anilistID).Str("slug", scored[0].slug).Float64("score", scored[0].score).Msg("ogflix: no ID match, using top-scored candidate")
	return scored[0].slug, nil
}

func (p *OGFLixProvider) cacheSlug(anilistID, slug string) {
	p.slugMu.Lock()
	p.slugCache[anilistID] = slug
	p.slugMu.Unlock()
}

// searchCandidates searches /api/search and returns scored-ready candidates.
func (p *OGFLixProvider) searchCandidates(ctx context.Context, q string) []titleCand {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/search?keyword=%s&page=1", ogflixBase, url.QueryEscape(q)), nil)
	if err != nil {
		return nil
	}
	p.setAPIHeaders(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil
	}
	var data ogflixSearchResp
	if err := json.Unmarshal(body, &data); err != nil {
		return nil
	}
	var out []titleCand
	for _, item := range data.Results.Data {
		if item.ID == "" {
			continue
		}
		out = append(out, titleCand{slug: item.ID, name: strings.TrimSpace(item.Title), jp: strings.TrimSpace(item.JName)})
	}
	return out
}

// verifySlug checks /api/info for an exact anilistId or malId match.
func (p *OGFLixProvider) verifySlug(ctx context.Context, slug, anilistID string, malID int) bool {
	info, err := p.fetchInfo(ctx, slug)
	if err != nil {
		return false
	}
	data := info.Results.Data
	if data.AnilistID != nil && strconv.Itoa(*data.AnilistID) == anilistID {
		return true
	}
	return malID > 0 && data.MalID != nil && *data.MalID == malID
}

// fetchInfo fetches /api/info for a slug.
func (p *OGFLixProvider) fetchInfo(ctx context.Context, slug string) (*ogflixInfoResp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/api/info?id=%s", ogflixBase, url.QueryEscape(slug)), nil)
	if err != nil {
		return nil, err
	}
	p.setAPIHeaders(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil, err
	}
	var info ogflixInfoResp
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	if !info.Success || info.Results.Data.ID == "" {
		return nil, fmt.Errorf("ogflix: info returned no data for %s", slug)
	}
	return &info, nil
}

// FindEpisodeSource resolves OGFLix streams for an episode:
// /api/stream -> player /resolve (ZenPlayer header) -> MegaPlay decrypt.
func (p *OGFLixProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}

	slug, err := p.resolveShow(ctx, anilistID)
	if err != nil {
		return nil, err
	}

	playerURLs, err := p.fetchStreamPlayerURLs(ctx, slug, episode, lang)
	if err != nil {
		return nil, err
	}

	// One player resolve returns the full server list for the episode.
	embedURLs, err := p.resolvePlayerServers(ctx, playerURLs)
	if err != nil {
		return nil, err
	}

	var sources []core.Source
	var variants []string
	var intro, outro *core.SkipTimestamp
	referer := ""
	seenEmbed := map[string]bool{}
	seenFile := map[string]bool{}

	for _, embed := range embedURLs {
		if seenEmbed[embed] {
			continue
		}
		seenEmbed[embed] = true
		// Only MegaPlay embeds carry the data-id + getSources chain we can
		// decrypt in-process. VidTube/VidWish have their own player stacks
		// (and VidWish is routinely dead) — skip them without a network call
		// instead of burning a 15s timeout on each.
		if !strings.Contains(embed, "megaplay.buzz") {
			p.log.Debug().Str("embed", embed).Msg("ogflix: non-megaplay embed, skipping decrypt")
			continue
		}
		// Resolve across every MegaPlay CDN edge until one decrypts AND
		// probes clean — the edge mapping rotates per request, so a single
		// attempt can land on the datacenter-blocked edge by luck.
		file, tracks, inTs, outTs, origin, pOK, rErr := resolveMegaPlayPlayable(ctx, p.client, embed, func(f, o string) bool {
			return probeManifestHLS(ctx, p.client, f, o)
		})
		if rErr != nil || file == "" || !pOK {
			// No playable edge from this egress: the media proxy would 403
			// too, so the server is dropped instead of surfacing a 502.
			p.log.Info().Err(rErr).Str("embed", embed).Msg("ogflix: no playable CDN edge, dropping server")
			continue
		}
		// base and ?s=tcdn often decrypt to the SAME file; keep one.
		if seenFile[file] {
			continue
		}
		seenFile[file] = true
		p.learnURLHost(file)
		var subs []core.Subtitle
		for _, t := range tracks {
			if strings.TrimSpace(t.URL) == "" {
				continue
			}
			p.learnURLHost(t.URL)
			subs = append(subs, core.Subtitle{
				URL:   t.URL,
				Lang:  mapSubtitleLang(t.Label),
				Label: t.Label,
			})
		}
		if referer == "" {
			referer = strings.TrimSuffix(origin, "/") + "/"
		}
		if intro == nil && inTs != nil && inTs.End > inTs.Start {
			intro = &core.SkipTimestamp{Start: inTs.Start, End: inTs.End}
		}
		if outro == nil && outTs != nil && outTs.End > outTs.Start {
			outro = &core.SkipTimestamp{Start: outTs.Start, End: outTs.End}
		}
		sources = append(sources, core.Source{
			URL:          file,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		})
		variants = append(variants, embedVariant(embed))
	}

	sources = dedupeSourcesByURL(sources, variants)
	if len(sources) == 0 {
		return nil, nil
	}
	if referer == "" {
		referer = "https://megaplay.buzz/"
	}
	return &SourceResult{
		Sources: sources,
		Headers: map[string]string{"Referer": referer},
		Intro:   intro,
		Outro:   outro,
	}, nil
}

// fetchStreamPlayerURLs calls /api/stream for the episode and returns the
// encrypted player URLs (one per server).
func (p *OGFLixProvider) fetchStreamPlayerURLs(ctx context.Context, slug string, episode int, lang string) ([]string, error) {
	// Docs format: id = "slug?ep=N". The inner ? and = are query-escaped.
	streamID := fmt.Sprintf("%s?ep=%d", slug, episode)
	apiURL := fmt.Sprintf("%s/api/stream?id=%s&type=%s", ogflixBase, url.QueryEscape(streamID), url.QueryEscape(lang))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	p.setAPIHeaders(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ogflix: stream request failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil, err
	}
	var data ogflixStreamResp
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("ogflix: stream decode failed: %w", err)
	}
	if !data.Success {
		return nil, fmt.Errorf("ogflix: stream returned failure for %s ep %d", slug, episode)
	}

	var out []string
	add := func(u string) {
		u = strings.TrimSpace(u)
		if u == "" || !strings.HasPrefix(u, "http") {
			return
		}
		for _, existing := range out {
			if existing == u {
				return
			}
		}
		out = append(out, u)
	}
	add(data.Results.StreamingLink.Link.File)
	for _, s := range data.Results.Servers {
		add(s.Embed)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("ogflix: episode %d has no servers on %s (%s)", episode, slug, lang)
	}
	return out, nil
}

// resolvePlayerServers calls the player's /resolve endpoint (which decrypts
// the server blobs server-side) and returns the real embed URLs. Every
// player URL resolves to the same server list, so the first success wins.
func (p *OGFLixProvider) resolvePlayerServers(ctx context.Context, playerURLs []string) ([]string, error) {
	var lastErr error
	for _, player := range playerURLs {
		resolveURL := strings.TrimSuffix(player, "/") + "/resolve"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, resolveURL, nil)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", ogflixPlayerUA)
		req.Header.Set("Accept", "application/json, text/plain, */*")
		req.Header.Set("Referer", player)
		req.Header.Set("Cache-Control", "no-store")
		req.Header.Set("X-Requested-With", ogflixXHRHeader)
		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, rErr := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
		resp.Body.Close()
		if rErr != nil || resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("ogflix: player resolve returned HTTP %d", resp.StatusCode)
			continue
		}
		var data ogflixResolveResp
		if err := json.Unmarshal(body, &data); err != nil {
			lastErr = err
			continue
		}
		var out []string
		if data.URL != "" {
			out = append(out, data.URL)
		}
		for _, s := range data.Servers {
			if s.URL != "" {
				out = append(out, s.URL)
			}
		}
		if len(out) == 0 {
			lastErr = fmt.Errorf("ogflix: player resolve returned no servers")
			continue
		}
		return out, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("ogflix: player resolve failed: %w", lastErr)
	}
	return nil, fmt.Errorf("ogflix: player resolve failed")
}

// setAPIHeaders sets the standard headers for OGFLix API requests.
func (p *OGFLixProvider) setAPIHeaders(req *http.Request) {
	req.Header.Set("User-Agent", ogflixPlayerUA)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Referer", ogflixBase+"/")
}
