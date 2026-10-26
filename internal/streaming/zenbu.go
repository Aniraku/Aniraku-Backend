package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// Zenbu resolves zen-bu.cc through its own stream index. The site is an
// AniList-keyed player: /anime/<anilistId>/watch/<episode> is a client-side
// shell, and every playable file comes from the index service the player
// calls. Verified end-to-end from the VPS egress:
//
//	GET /episodes/<anilistId>/<episode>[?provider=<id>]
//	    -> {"success":true,"data":[{"providerId","resolution","format",
//	       "url","audio","subtitles","absoluteExpiryMs"}]}
//	url -> HLS master on a Cloudflare Worker edge (edge.*.workers.dev);
//	       child playlists are absolute, segments answer plain 200s
//
// The default call returns the primary pool; other pools are selected with
// the provider query. The index carries no token, no session cookie, and no
// Referer gate (measured with the origin headers the player sends, and with
// none at all). The player's own provider ids are internal labels, so each
// index pool maps to a fixed set of display servers; every published URL is
// probed at segment depth, and mirror URLs without a name slot are dropped
// rather than collapsed into a duplicate server.
//
// Language comes from the index's own audio field, never inferred: the dub
// lane carries English dialogue with English caption tracks, and a title
// without a dub entry lists no dub servers rather than borrowing the sub
// master. absoluteExpiryMs is deliberately ignored: the index reports
// timestamps in the past for URLs that still serve every hop, so it cannot
// gate playback.
const (
	zenbuIndexDefault = "https://megadown.zen-bu.cc"
	zenbuSiteDefault  = "https://zen-bu.cc"

	zenbuPrimaryProvider = "shorekeeper"
	zenbuMirrorProvider  = "encore"
	zenbuSubProxyDefault = "roccia"
	zenbuSubAltProvider  = "carlotta"

	zenbuMaxBody = 1 << 20
)

// zenbuPool is one index provider and the display names its playable URLs
// occupy, in sorted-host order. Pools without a dub lane simply return no
// dub candidates.
type zenbuPool struct {
	provider string
	names    []string
}

var zenbuPools = []zenbuPool{
	{provider: zenbuPrimaryProvider, names: []string{"Yuzu", "Ume"}},
	{provider: zenbuMirrorProvider, names: []string{"Ringo", "Budo", "Kaki", "Kuri"}},
	{provider: zenbuSubProxyDefault, names: []string{"Biwa", "Ichijiku"}},
	{provider: zenbuSubAltProvider, names: []string{"Melon", "Suika"}},
}

// zenbuServerNames is the flat name order used as the positional fallback
// when a result carries fewer names than sources.
var zenbuServerNames = func() []string {
	var out []string
	for _, pool := range zenbuPools {
		out = append(out, pool.names...)
	}
	return out
}()

type zenbuSubtitle struct {
	URL   string `json:"url"`
	Label string `json:"label"`
	Kind  string `json:"kind"`
}

type zenbuSource struct {
	ProviderID string          `json:"providerId"`
	Resolution string          `json:"resolution"`
	Format     string          `json:"format"`
	URL        string          `json:"url"`
	Audio      string          `json:"audio"`
	Subtitles  []zenbuSubtitle `json:"subtitles"`
}

type zenbuResponse struct {
	Success bool          `json:"success"`
	Data    []zenbuSource `json:"data"`
	Error   string        `json:"error"`
}

type ZenbuProvider struct {
	log       zerolog.Logger
	client    *http.Client
	indexBase string
	site      string
	learnHost func(string)
}

func NewZenbuProvider(log zerolog.Logger, indexBase, site string) *ZenbuProvider {
	if strings.TrimSpace(indexBase) == "" {
		indexBase = zenbuIndexDefault
	}
	if strings.TrimSpace(site) == "" {
		site = zenbuSiteDefault
	}
	return &ZenbuProvider{
		log:       log,
		client:    &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		indexBase: strings.TrimRight(indexBase, "/"),
		site:      strings.TrimRight(site, "/"),
	}
}

func (p *ZenbuProvider) Name() string { return "zenbu" }

// Search: AniList-keyed provider; catalog search is not part of playback.
func (p *ZenbuProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, nil
}

// FindEpisodes: episode lists come from AniList upstream, not this index.
func (p *ZenbuProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, nil
}

func (p *ZenbuProvider) SetHostLearner(fn func(string)) { p.learnHost = fn }

func (p *ZenbuProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Hostname() == "" {
		return
	}
	p.learnHost(u.Hostname())
}

// FindEpisodeSource resolves every pool for one episode, in parallel, and
// keeps only the URLs that survived a segment-depth probe. A pool the index
// does not carry for this language is simply absent; transport failures only
// surface when every pool failed.
func (p *ZenbuProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	lang = strings.ToLower(strings.TrimSpace(lang))
	if lang != "dub" {
		lang = "sub"
	}

	type slot struct {
		name   string
		source zenbuSource
	}
	picked := make([][]zenbuSource, len(zenbuPools))
	fetchErrs := make([]error, len(zenbuPools))
	var wg sync.WaitGroup
	for i, pool := range zenbuPools {
		wg.Add(1)
		go func(i int, pool zenbuPool) {
			defer wg.Done()
			candidates, err := p.fetchEpisode(ctx, anilistID, episode, pool.provider)
			if err != nil {
				fetchErrs[i] = err
				p.log.Warn().Err(err).Str("anilistId", anilistID).Int("episode", episode).
					Str("lang", lang).Str("indexProvider", pool.provider).
					Msg("zenbu: index provider failed")
				return
			}
			picked[i] = pickZenbuSources(candidates, lang, len(pool.names))
		}(i, pool)
	}
	wg.Wait()

	var slots []slot
	var firstErr error
	for i, pool := range zenbuPools {
		if fetchErrs[i] != nil && firstErr == nil {
			firstErr = fetchErrs[i]
		}
		for j, source := range picked[i] {
			slots = append(slots, slot{name: pool.names[j], source: source})
		}
	}

	if len(slots) == 0 {
		if firstErr != nil && countErrors(fetchErrs) == len(zenbuPools) {
			return nil, firstErr
		}
		return nil, nil
	}

	out := &SourceResult{
		Headers: map[string]string{
			"Referer":    p.site + "/",
			"User-Agent": browserUA,
		},
	}

	kept := make([]bool, len(slots))
	type probeJob struct {
		index int
		url   string
	}
	jobs := make(chan probeJob)
	var probeWG sync.WaitGroup
	for i := 0; i < 6; i++ {
		probeWG.Add(1)
		go func() {
			defer probeWG.Done()
			for job := range jobs {
				kept[job.index] = probeSegmentsLenient(ctx, p.client, job.url, p.site+"/", browserUA)
			}
		}()
	}
	go func() {
		for i := range slots {
			jobs <- probeJob{index: i, url: slots[i].source.URL}
		}
		close(jobs)
	}()
	probeWG.Wait()

	for i, slot := range slots {
		if !kept[i] {
			p.log.Info().Str("anilistId", anilistID).Int("episode", episode).
				Str("lang", lang).Str("server", slot.name).
				Msg("zenbu: stream blocked from this egress, dropping slot")
			continue
		}
		source := core.Source{
			URL:          slot.source.URL,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    zenbuSubtitles(slot.source),
			Verification: "proxy",
		}
		p.learnURLHost(source.URL)
		for _, subtitle := range source.Subtitles {
			p.learnURLHost(subtitle.URL)
		}
		out.Sources = append(out.Sources, source)
		out.ServerNames = append(out.ServerNames, slot.name)
	}
	if len(out.Sources) == 0 {
		return nil, nil
	}
	return out, nil
}

func countErrors(errs []error) int {
	n := 0
	for _, err := range errs {
		if err != nil {
			n++
		}
	}
	return n
}

func zenbuSubtitles(source zenbuSource) []core.Subtitle {
	var subs []core.Subtitle
	for _, track := range source.Subtitles {
		if strings.TrimSpace(track.URL) == "" || strings.TrimSpace(track.Label) == "" {
			// Sprite previews carry no label; only real caption tracks
			// belong in the subtitle list.
			continue
		}
		subs = append(subs, buildSubtitle(track.URL, "", track.Label, "en"))
	}
	return subs
}

func (p *ZenbuProvider) fetchEpisode(ctx context.Context, anilistID string, episode int, providerID string) ([]zenbuSource, error) {
	endpoint := fmt.Sprintf("%s/episodes/%s/%d",
		p.indexBase, url.PathEscape(strings.TrimSpace(anilistID)), episode)
	if providerID != "" {
		endpoint += "?provider=" + url.QueryEscape(providerID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Origin", p.site)
	req.Header.Set("Referer", p.site+"/")
	req.Header.Set("User-Agent", browserUA)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("zenbu: index HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, zenbuMaxBody))
	if err != nil {
		return nil, err
	}
	var parsed zenbuResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("zenbu: index decode: %w", err)
	}
	if !parsed.Success {
		if parsed.Error != "" && parsed.Error != "no_sources" {
			return nil, fmt.Errorf("zenbu: index error %s", parsed.Error)
		}
		return nil, nil
	}
	return parsed.Data, nil
}

// pickZenbuSources enforces the language strictly, dedupes mirror URLs, and
// returns at most limit entries ordered by host. Host order keeps a display
// name attached to the same CDN across episodes even though the index rotates
// tokens.
func pickZenbuSources(sources []zenbuSource, lang string, limit int) []zenbuSource {
	if limit <= 0 {
		return nil
	}
	var picked []zenbuSource
	seen := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if !strings.EqualFold(strings.TrimSpace(source.Audio), lang) {
			continue
		}
		if !zenbuPlayableURL(source.URL) {
			continue
		}
		if _, ok := seen[source.URL]; ok {
			continue
		}
		seen[source.URL] = struct{}{}
		picked = append(picked, source)
	}
	sort.SliceStable(picked, func(i, j int) bool {
		hi, hj := zenbuHost(picked[i].URL), zenbuHost(picked[j].URL)
		if hi != hj {
			return hi < hj
		}
		return picked[i].URL < picked[j].URL
	})
	if len(picked) > limit {
		picked = picked[:limit]
	}
	return picked
}

func zenbuPlayableURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	return (u.Scheme == "https" || u.Scheme == "http") && u.Host != ""
}

func zenbuHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Host
}
