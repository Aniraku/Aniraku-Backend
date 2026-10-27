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
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

const (
	novuscloudSiteDefault = "https://novuscloud.buzz"
	novuscloudAPIPath     = "/stream/getSources"
	novuscloudEmbedPath   = "/embed/%s/ani/%d/%d/%s?k=1"

	novuscloudSlugTTL           = 24 * time.Hour
	novuscloudResolveTTL        = 30 * time.Minute
	maxNovuscloudSlugEntries    = 500
	maxNovuscloudResolveEntries = 500
)

// novuscloudMirror is one Cloudflare mirror and its cute server name.
type novuscloudMirror struct {
	mirror string
	server string
}

var novuscloudMirrors = []novuscloudMirror{
	{mirror: "hd-1", server: "Nova"},
	{mirror: "hd-2", server: "Nebula"},
	{mirror: "hd-3", server: "Stella"},
	{mirror: "hd-4", server: "Lyra"},
}

// novuscloudServerNames maps mirror IDs to cute space-themed server names.
var novuscloudServerNames = map[string]string{
	"hd-1": "Nova",
	"hd-2": "Nebula",
	"hd-3": "Stella",
	"hd-4": "Lyra",
}

type novuscloudSource struct {
	File    string `json:"file"`
	Type    string `json:"type"`
	Label   string `json:"label"`
	Kind    string `json:"kind"`
	Default bool   `json:"default"`
}

type novuscloudGetSourcesResponse struct {
	Sources []novuscloudSource `json:"sources"`
	Tracks  []novuscloudSource `json:"tracks"`
}

type novuscloudSlugEntry struct {
	showID  string
	title   string
	fetched time.Time
}

type novuscloudResolveKey struct {
	showID  string
	episode int
	mirror  string
	lang    string
}

type novuscloudResolvedEntry struct {
	result  *SourceResult
	fetched time.Time
}

type NovuscloudProvider struct {
	log       zerolog.Logger
	client    *http.Client
	site      string
	mirror    string // hd-1, hd-2, hd-3, hd-4
	server    string // Nova, Nebula, Stella, Lyra
	learnHost func(string)

	mu       sync.Mutex
	resolved map[novuscloudResolveKey]*novuscloudResolvedEntry
}

func NewNovuscloudProvider(log zerolog.Logger, site, mirror, server string) *NovuscloudProvider {
	if strings.TrimSpace(site) == "" {
		site = novuscloudSiteDefault
	}
	site = strings.TrimRight(site, "/")
	if strings.TrimSpace(mirror) == "" {
		mirror = "hd-1"
	}
	if strings.TrimSpace(server) == "" {
		server = novuscloudServerNames[mirror]
		if server == "" {
			server = "Nova"
		}
	}
	return &NovuscloudProvider{
		log:      log,
		client:   &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		site:     site,
		mirror:   mirror,
		server:   server,
		resolved: make(map[novuscloudResolveKey]*novuscloudResolvedEntry),
	}
}

func (p *NovuscloudProvider) Name() string { return "novuscloud" }

func (p *NovuscloudProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *NovuscloudProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *NovuscloudProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, nil
}

func (p *NovuscloudProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, nil
}

// FindEpisodeSource resolves all mirrors for one episode, in parallel,
// keeping only the URLs that survive a segment-depth probe. Each mirror's
// sources are published under that mirror's own server name.
func (p *NovuscloudProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("novuscloud: bad anilist id %q", anilistID)
	}
	lang = strings.ToLower(strings.TrimSpace(lang))
	if lang != "dub" {
		lang = "sub"
	}

	// Per-mirror timeout so a slow or blocked mirror doesn't drag down the
	// whole fan-out. 8s is enough for embed + API + probe.
	mirrorCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	sr, err := p.resolveMirror(mirrorCtx, id, episode, lang)
	if err != nil {
		return nil, err
	}
	if sr == nil || len(sr.Sources) == 0 {
		return nil, nil
	}
	return sr, nil
}

// resolveMirror fetches the embed page, extracts the getSources token, calls
// the API, and probes every returned source URL.
func (p *NovuscloudProvider) resolveMirror(ctx context.Context, id int, episode int, lang string) (*SourceResult, error) {
	key := novuscloudResolveKey{showID: strconv.Itoa(id), episode: episode, mirror: p.mirror, lang: lang}
	if cached := p.loadResolved(key); cached != nil {
		return cached, nil
	}

	// Step 1: fetch the embed page to extract the getSources token.
	embedURL := fmt.Sprintf("%s/embed/%s/ani/%d/%d/%s?k=1", p.site, p.mirror, id, episode, lang)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, embedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", p.site+"/")
	req.Header.Set("Origin", p.site)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("novuscloud: embed HTTP %d (%s)", resp.StatusCode, p.server)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	html := string(body)

	// sourcesUrl = '/stream/getSources?t=<token>'
	re := regexp.MustCompile(`sourcesUrl\s*=\s*'/stream/getSources\?t=([^']+)'`)
	m := re.FindStringSubmatch(html)
	if len(m) < 2 {
		return nil, fmt.Errorf("novuscloud: no sourcesUrl token in embed page (%s)", p.server)
	}
	token := m[1]

	// Step 2: call the getSources API.
	getSourcesURL := p.site + novuscloudAPIPath + "?t=" + url.QueryEscape(token)
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, getSourcesURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", embedURL)
	req.Header.Set("Origin", p.site)
	req.Header.Set("Accept", "application/json")
	resp, err = p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("novuscloud: getSources HTTP %d (%s)", resp.StatusCode, p.server)
	}
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var parsed novuscloudGetSourcesResponse
	if err := json.Unmarshal(rb, &parsed); err != nil {
		return nil, fmt.Errorf("novuscloud: getSources decode: %w", err)
	}

	sr := &SourceResult{}
	for _, src := range parsed.Sources {
		u := strings.TrimSpace(src.File)
		if u == "" {
			continue
		}
		// Resolve relative URLs against the site.
		if !strings.HasPrefix(u, "http") {
			u = p.site + u
		}
		lu := strings.ToLower(u)
		var typ string
		switch {
		case strings.Contains(lu, ".m3u8"):
			typ = "hls"
		case strings.Contains(lu, ".mp4"):
			typ = "mp4"
		default:
			switch strings.ToLower(strings.TrimSpace(src.Type)) {
			case "hls":
				typ = "hls"
			case "mp4":
				typ = "mp4"
			default:
				continue
			}
		}
		// Probe before listing.
		var ok bool
		if typ == "hls" {
			ok = probeSegmentsLenient(ctx, p.client, u, p.site+"/", browserUA)
		} else {
			ok = probeMediaFileLenient(ctx, p.client, u, p.site+"/", browserUA)
		}
		if !ok {
			p.log.Info().Str("url", u).Str("mirror", p.server).Msg("novuscloud: source blocked from this egress, dropping")
			continue
		}
		p.learnURLHost(u)
		sr.Sources = append(sr.Sources, core.Source{
			URL:          u,
			Type:         typ,
			Quality:      "auto",
			Verification: "proxy",
		})
		sr.ServerNames = append(sr.ServerNames, p.server)
	}

	// Collect subtitle tracks from the API response. Caption tracks (kind
	// "captions") with a URL are attached to every video source from this
	// mirror — subtitles belong to the episode, not to a specific CDN URL.
	var subs []core.Subtitle
	for _, tr := range parsed.Tracks {
		if strings.TrimSpace(tr.Kind) != "captions" || strings.TrimSpace(tr.File) == "" {
			continue
		}
		subURL := strings.TrimSpace(tr.File)
		if !strings.HasPrefix(subURL, "http") {
			subURL = p.site + subURL
		}
		// Probe the subtitle track (VTT) before attaching. VTT files are
		// plain text, not binary media, so they need their own probe.
		if !probeSubtitleLenient(ctx, p.client, subURL, p.site+"/", browserUA) {
			p.log.Info().Str("url", subURL).Str("mirror", p.server).Msg("novuscloud: subtitle blocked from this egress, dropping")
			continue
		}
		p.learnURLHost(subURL)
		label := strings.TrimSpace(tr.Label)
		if label == "" {
			label = strings.TrimSpace(tr.Kind)
		}
		subs = append(subs, buildSubtitle(subURL, "", label, "en"))
	}

	if len(sr.Sources) == 0 {
		return nil, nil
	}
	// Attach subtitles to every source from this mirror.
	for i := range sr.Sources {
		sr.Sources[i].Subtitles = subs
	}
	p.storeResolved(key, sr)
	return sr, nil
}

// cache helpers
func (p *NovuscloudProvider) loadResolved(key novuscloudResolveKey) *SourceResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[key]
	if !ok || time.Since(e.fetched) >= novuscloudResolveTTL {
		if ok {
			delete(p.resolved, key)
		}
		return nil
	}
	return cloneSourceResult(e.result)
}

func (p *NovuscloudProvider) storeResolved(key novuscloudResolveKey, sr *SourceResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.resolved {
		if time.Since(e.fetched) > novuscloudResolveTTL {
			delete(p.resolved, k)
		}
	}
	if len(p.resolved) >= maxNovuscloudResolveEntries {
		var oldest novuscloudResolveKey
		var oldestAt time.Time
		first := true
		for k, e := range p.resolved {
			if first || e.fetched.Before(oldestAt) {
				oldest, oldestAt, first = k, e.fetched, false
			}
		}
		delete(p.resolved, oldest)
	}
	p.resolved[key] = &novuscloudResolvedEntry{result: cloneSourceResult(sr), fetched: now}
}
