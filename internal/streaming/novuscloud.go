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

// novuscloudMirror is one Cloudflare mirror (hd-1, hd-2) and its server name.
type novuscloudMirror struct {
	mirror string
	server string
}

var novuscloudMirrors = []novuscloudMirror{
	{mirror: "hd-1", server: "Novus-hd1"},
	{mirror: "hd-2", server: "Novus-hd2"},
}

// novuscloudServerNames maps mirror IDs to cute server names.
var novuscloudServerNames = map[string]string{
	"hd-1": "Novus-hd1",
	"hd-2": "Novus-hd2",
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
	learnHost func(string)

	mu       sync.Mutex
	slugs    map[string]*novuscloudSlugEntry
	resolved map[novuscloudResolveKey]*novuscloudResolvedEntry
}

func NewNovuscloudProvider(log zerolog.Logger, site string) *NovuscloudProvider {
	if strings.TrimSpace(site) == "" {
		site = novuscloudSiteDefault
	}
	site = strings.TrimRight(site, "/")
	return &NovuscloudProvider{
		log:      log,
		client:   &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		site:     site,
		slugs:    make(map[string]*novuscloudSlugEntry),
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

	var out *SourceResult
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, m := range novuscloudMirrors {
		wg.Add(1)
		go func(mirror novuscloudMirror) {
			defer wg.Done()
			sr, err := p.resolveMirror(ctx, id, episode, lang, mirror)
			if err != nil || sr == nil {
				return
			}
			mu.Lock()
			if out == nil {
				out = &SourceResult{
					Headers: map[string]string{
						"Referer":    p.site + "/",
						"User-Agent": browserUA,
					},
				}
			}
			out.Sources = append(out.Sources, sr.Sources...)
			out.ServerNames = append(out.ServerNames, sr.ServerNames...)
			mu.Unlock()
		}(m)
	}
	wg.Wait()

	if out == nil || len(out.Sources) == 0 {
		return nil, nil
	}
	return out, nil
}

// resolveMirror fetches the embed page, extracts the getSources token, calls
// the API, and probes every returned source URL.
func (p *NovuscloudProvider) resolveMirror(ctx context.Context, id int, episode int, lang string, mirror novuscloudMirror) (*SourceResult, error) {
	key := novuscloudResolveKey{showID: strconv.Itoa(id), episode: episode, mirror: mirror.mirror, lang: lang}
	if cached := p.loadResolved(key); cached != nil {
		return cached, nil
	}

	// Step 1: fetch the embed page to extract the getSources token.
	embedURL := fmt.Sprintf("%s/embed/hd-%s/ani/%d/%d/%s?k=1", p.site, mirror.mirror, id, episode, lang)
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
		return nil, fmt.Errorf("novuscloud: embed HTTP %d (%s)", resp.StatusCode, mirror.server)
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
		return nil, fmt.Errorf("novuscloud: no sourcesUrl token in embed page (%s)", mirror.server)
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
		return nil, fmt.Errorf("novuscloud: getSources HTTP %d (%s)", resp.StatusCode, mirror.server)
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
			p.log.Info().Str("url", u).Str("mirror", mirror.server).Msg("novuscloud: source blocked from this egress, dropping")
			continue
		}
		p.learnURLHost(u)
		sr.Sources = append(sr.Sources, core.Source{
			URL:          u,
			Type:         typ,
			Quality:      "auto",
			Verification: "proxy",
		})
		sr.ServerNames = append(sr.ServerNames, mirror.server)
	}

	if len(sr.Sources) == 0 {
		return nil, nil
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
