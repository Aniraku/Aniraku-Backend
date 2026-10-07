package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// Tensho resolves zangetsu.cc (the Zangetsu hianime-family catalog),
// verified end-to-end from VPS egress 2026-10-07. The site guards its
// episode endpoints with a session-bound pair: the watch page mints both a
// PHPSESSID cookie and a window.AJAX_TOKEN, and /ajax/* answers 403 unless
// the same session cookie + token arrive together (measured: token without
// cookie = 403, pair = 200). Every resolve fetches a fresh watch page and
// calls the ajax endpoints immediately after with both halves:
//
//	/search?keyword=<title>                      -> poster anchors + titles
//	/watch/<slug>-<id>?ep=1                      -> Set-Cookie PHPSESSID + AJAX_TOKEN
//	/ajax/episodes?animeId=<id>                  -> episode list (cookie + token)
//	/ajax/server?episodeId=<id>&sub=&dub=        -> server slots (cookie + token)
//
// The ajax responses carry no playable media: server slots map to flixera.co
// and cdn.4animo.xyz embed pages (the site's own loadPlayer switch), so
// Tensho returns embed-type sources exactly like FlixCloud — no probe, no
// m3u8 extraction. Both embed hosts answer 200 without a Referer (measured).

const (
	tenshoDefaultBase = "https://zangetsu.cc"
	tenshoServerName  = "Tensho"
	tenshoAnilistURL  = "https://graphql.aniraku.tech"
	tenshoShowTTL     = 10 * time.Minute
	// The page token is short-lived (403 after minutes): keep the cached
	// window below that and refresh once on 403 anyway.
	tenshoEpisodesTTL = 3 * time.Minute
	tenshoFlixera     = "https://flixera.co"
	tenshoCDN         = "https://cdn.4animo.xyz"
)

// tenshoServerNames labels Zangetsu's server slots positionally (s-1
// flixera, s-2 hd-1, s-3 hd-2) — extras past the third collide on the last
// name inside appendNamedServers, so FindEpisodeSource caps at three.
var tenshoServerNames = [...]string{"Tsuki", "Kaze", "Hoshi"}

const (
	tenshoFlixeraQuery = "?autoplay=0&skipintro=0&skipoutro=0"
	tenshoAnimoQuery   = "?k=1&autoPlay=0&skipIntro=0&skipOutro=0"
)

var (
	// Poster anchors: <a class="film-poster-ahref" href="/naruto-shippuden-1493"\n title="Naruto Shippuden">
	tenshoSearchRe = regexp.MustCompile(`(?s)<a[^>]*class=["']film-poster-ahref["'][^>]*href=["']/([^"'/]+)["'][^>]*title=["']([^"']+)["']`)
	// <script>window.AJAX_TOKEN = "254640a56e0bdfad8233fa93a051de1a";</script>
	tenshoTokenRe = regexp.MustCompile(`window\.AJAX_TOKEN\s*=\s*["']([A-Za-z0-9]+)["']`)
)

type tenshoShowEntry struct {
	path    string // watch path: "naruto-shippuden-1493"
	id      string // numeric id from the path suffix: "1493"
	expires time.Time
}

type tenshoEpisode struct {
	ID     string  `json:"id"`
	Number float64 `json:"number"`
	Sub    bool    `json:"sub"`
	Dub    bool    `json:"dub"`
	ANI    string  `json:"ani"`
	MAL    string  `json:"mal"`
	Filler bool    `json:"filler"`
}

type tenshoEpisodesEntry struct {
	list []tenshoEpisode
	// sess is the watch-page pair the ajax endpoints require — token
	// without the session cookie is rejected with 403.
	sess    tenshoSession
	expires time.Time
}

// tenshoSession is one watch-page fetch's credentials: AJAX token, the
// page URL (ajax Referer) and the PHPSESSID cookie from Set-Cookie.
type tenshoSession struct {
	token   string
	referer string
	cookie  string
}

type tenshoServerSlot struct {
	ServerID   string `json:"serverId"`
	ServerName string `json:"serverName"`
	Index      int    `json:"index"`
}

type tenshoServerResp struct {
	Sub []tenshoServerSlot `json:"sub"`
	Dub []tenshoServerSlot `json:"dub"`
}

type TenshoProvider struct {
	log        zerolog.Logger
	client     *http.Client
	base       string
	anilistURL string
	learnHost  func(string)

	mu       sync.Mutex
	shows    map[string]*tenshoShowEntry     // anilistID -> watch path + id
	episodes map[string]*tenshoEpisodesEntry // show path -> episode list + token
}

func NewTenshoProvider(log zerolog.Logger, base, anilistURL string) *TenshoProvider {
	if strings.TrimSpace(base) == "" {
		base = tenshoDefaultBase
	}
	if strings.TrimSpace(anilistURL) == "" {
		anilistURL = tenshoAnilistURL
	}
	return &TenshoProvider{
		log:        log,
		client:     &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		base:       strings.TrimRight(base, "/"),
		anilistURL: anilistURL,
		shows:      make(map[string]*tenshoShowEntry),
		episodes:   make(map[string]*tenshoEpisodesEntry),
	}
}

func (p *TenshoProvider) Name() string { return "tensho" }

func (p *TenshoProvider) SetHostLearner(fn func(string)) { p.learnHost = fn }

func (p *TenshoProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return
	}
	p.learnHost(u.Hostname())
}

// get fetches a page (browser headers) and returns its Set-Cookie pairs —
// the watch page's PHPSESSID is required by the ajax endpoints. status
// 403 is returned, not an error, so the caller can refresh and retry.
func (p *TenshoProvider) get(ctx context.Context, rawURL, referer string) (int, string, []*http.Cookie, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, "", nil, err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return resp.StatusCode, "", nil, err
	}
	return resp.StatusCode, string(body), resp.Cookies(), nil
}

// ajaxGet calls one /ajax endpoint with the session pair the page JS
// sends: X-Page-Token + the PHPSESSID cookie from the same watch fetch
// (token without cookie = 403 — measured).
func (p *TenshoProvider) ajaxGet(ctx context.Context, rawURL string, sess tenshoSession) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("X-Page-Token", sess.token)
	req.Header.Set("Referer", sess.referer)
	if sess.cookie != "" {
		req.Header.Set("Cookie", sess.cookie)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

// joinCookies flattens Set-Cookie pairs into one Cookie request header.
func joinCookies(cookies []*http.Cookie) string {
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		if c.Name != "" {
			parts = append(parts, c.Name+"="+c.Value)
		}
	}
	return strings.Join(parts, "; ")
}

// Search lists zangetsu shows by keyword. SearchResult.ID is the watch
// path (slug-id) — what FindEpisodes and the watch URL consume.
func (p *TenshoProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	rawURL := p.base + "/search?keyword=" + url.QueryEscape(title)
	status, body, _, err := p.get(ctx, rawURL, p.base+"/")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("tensho: search HTTP %d", status)
	}
	var out []SearchResult
	seen := make(map[string]bool)
	for _, m := range tenshoSearchRe.FindAllStringSubmatch(body, -1) {
		path, name := m[1], strings.TrimSpace(m[2])
		if path == "" || name == "" || seen[path] {
			continue
		}
		seen[path] = true
		out = append(out, SearchResult{ID: path, Title: name})
	}
	return out, nil
}

// splitShowPath splits "naruto-shippuden-1493" into the watch path and its
// numeric id ("1493").
func splitShowPath(path string) (string, string, bool) {
	path = strings.Trim(path, "/")
	if i := strings.LastIndex(path, "-"); i > 0 && i < len(path)-1 {
		id := path[i+1:]
		if n := len(id); n > 0 && n <= 12 {
			allDigits := true
			for _, c := range id {
				if c < '0' || c > '9' {
					allDigits = false
					break
				}
			}
			if allDigits {
				return path, id, true
			}
		}
	}
	return path, "", false
}

// FindEpisodes lists one show's episodes. providerID is the watch path
// returned by Search.
func (p *TenshoProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	path, id, ok := splitShowPath(providerID)
	if !ok {
		return nil, fmt.Errorf("tensho: provider id %q must be a watch path like \"naruto-shippuden-1493\"", providerID)
	}
	entry, err := p.episodeList(ctx, &tenshoShowEntry{path: path, id: id}, false)
	if err != nil {
		return nil, err
	}
	list := append([]tenshoEpisode(nil), entry.list...)
	sort.Slice(list, func(i, j int) bool { return list[i].Number < list[j].Number })
	out := make([]Episode, 0, len(list))
	for _, e := range list {
		out = append(out, Episode{
			Number: int(e.Number),
			Title:  fmt.Sprintf("Episode %g", e.Number),
			Filler: e.Filler,
		})
	}
	return out, nil
}

// fetchSession loads a watch page and returns its fresh credentials:
// AJAX token, the page URL (ajax Referer) and the PHPSESSID cookie.
func (p *TenshoProvider) fetchSession(ctx context.Context, show *tenshoShowEntry) (tenshoSession, error) {
	watchURL := p.base + "/watch/" + show.path + "?ep=1"
	status, body, cookies, err := p.get(ctx, watchURL, p.base+"/")
	if err != nil {
		return tenshoSession{}, err
	}
	if status == http.StatusForbidden {
		return tenshoSession{}, fmt.Errorf("tensho: watch page HTTP 403")
	}
	if status != http.StatusOK {
		return tenshoSession{}, fmt.Errorf("tensho: watch page HTTP %d", status)
	}
	m := tenshoTokenRe.FindStringSubmatch(body)
	if m == nil {
		return tenshoSession{}, fmt.Errorf("tensho: no AJAX token on watch page %s", show.path)
	}
	return tenshoSession{token: m[1], referer: watchURL, cookie: joinCookies(cookies)}, nil
}

// episodeList returns the cached episode list + session, or fetches a
// fresh watch page and calls /ajax/episodes immediately (stale sessions
// 403).
func (p *TenshoProvider) episodeList(ctx context.Context, show *tenshoShowEntry, force bool) (*tenshoEpisodesEntry, error) {
	if !force {
		p.mu.Lock()
		if e, ok := p.episodes[show.path]; ok && time.Now().Before(e.expires) {
			p.mu.Unlock()
			return e, nil
		}
		p.mu.Unlock()
	}

	sess, err := p.fetchSession(ctx, show)
	if err != nil {
		return nil, err
	}
	status, body, err := p.ajaxGet(ctx, p.base+"/ajax/episodes?animeId="+show.id, sess)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("tensho: episodes ajax HTTP %d", status)
	}
	var resp struct {
		Episodes []tenshoEpisode `json:"episodes"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, fmt.Errorf("tensho: episodes decode: %w", err)
	}
	if len(resp.Episodes) == 0 {
		return nil, fmt.Errorf("tensho: no episodes for show %s", show.path)
	}

	entry := &tenshoEpisodesEntry{
		list:    resp.Episodes,
		sess:    sess,
		expires: time.Now().Add(tenshoEpisodesTTL),
	}
	p.mu.Lock()
	p.episodes[show.path] = entry
	p.mu.Unlock()

	p.log.Info().Str("showId", show.id).Int("episodes", len(resp.Episodes)).Msg("tensho: episode list loaded")
	return entry, nil
}

// serverSlots calls /ajax/server with the cached session; on 403 (stale
// session) it refreshes the watch page once and retries.
func (p *TenshoProvider) serverSlots(ctx context.Context, show *tenshoShowEntry, ep tenshoEpisode) (sub, dub []tenshoServerSlot, err error) {
	entry, err := p.episodeList(ctx, show, false)
	if err != nil {
		return nil, nil, err
	}
	for attempt := 0; attempt < 2; attempt++ {
		rawURL := fmt.Sprintf("%s/ajax/server?episodeId=%s&sub=%t&dub=%t",
			p.base, url.QueryEscape(ep.ID), ep.Sub, ep.Dub)
		status, body, err := p.ajaxGet(ctx, rawURL, entry.sess)
		if err != nil {
			return nil, nil, err
		}
		if status == http.StatusForbidden && attempt == 0 {
			// Stale session: refresh the watch page + episode list, retry once.
			if refreshed, rerr := p.episodeList(ctx, show, true); rerr == nil {
				entry = refreshed
				continue
			}
		}
		if status != http.StatusOK {
			return nil, nil, fmt.Errorf("tensho: server ajax HTTP %d", status)
		}
		var resp tenshoServerResp
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			return nil, nil, fmt.Errorf("tensho: server decode: %w", err)
		}
		return resp.Sub, resp.Dub, nil
	}
	return nil, nil, fmt.Errorf("tensho: server ajax rejected token twice")
}

// tenshoEmbedURL mirrors the site's loadPlayer switch: s-1 (and any
// unknown slot) is the flixera player keyed by the episode's ani/mal ids,
// s-2 is the 4animo hd-1 player keyed by episode id, s-3 is the 4animo
// hd-2 player keyed by ani/mal.
func tenshoEmbedURL(slot string, ep tenshoEpisode, lang string) string {
	switch slot {
	case "s-2":
		return tenshoCDN + "/embed/hd-1/" + ep.ID + "/" + lang + tenshoAnimoQuery
	case "s-3":
		switch {
		case ep.ANI != "":
			return tenshoCDN + "/embed/hd-2/ani/" + ep.ANI + "/" + lang + tenshoAnimoQuery
		case ep.MAL != "":
			return tenshoCDN + "/embed/hd-2/mal/" + ep.MAL + "/" + lang + tenshoAnimoQuery
		default:
			return tenshoCDN + "/embed/hd-2/" + ep.ID + "/" + lang + tenshoAnimoQuery
		}
	default: // s-1 and anything new the site adds
		switch {
		case ep.ANI != "":
			return tenshoFlixera + "/embed/ani/" + ep.ANI + "/" + lang + tenshoFlixeraQuery
		case ep.MAL != "":
			return tenshoFlixera + "/embed/mal/" + ep.MAL + "/" + lang + tenshoFlixeraQuery
		default:
			return tenshoFlixera + "/embed/ani/" + ep.ID + "/" + lang + tenshoFlixeraQuery
		}
	}
}

// FindEpisodeSource resolves one episode to its embed lineup for lang.
// Returns nil (no error) when the show, episode, language or server list
// is absent — the fan-out treats that as "this provider has nothing".
func (p *TenshoProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}

	show, err := p.findShow(ctx, providerID)
	if err != nil {
		return nil, err
	}
	entry, err := p.episodeList(ctx, show, false)
	if err != nil {
		return nil, err
	}

	var ep *tenshoEpisode
	for i := range entry.list {
		if entry.list[i].Number == float64(episode) {
			ep = &entry.list[i]
			break
		}
	}
	if ep == nil {
		p.log.Info().Str("showId", show.id).Int("episode", episode).
			Msg("tensho: episode not in list")
		return nil, nil
	}
	if langKey == "sub" && !ep.Sub {
		p.log.Info().Str("showId", show.id).Int("episode", episode).Msg("tensho: no sub for episode")
		return nil, nil
	}
	if langKey == "dub" && !ep.Dub {
		p.log.Info().Str("showId", show.id).Int("episode", episode).Msg("tensho: no dub for episode")
		return nil, nil
	}

	sub, dub, err := p.serverSlots(ctx, show, *ep)
	if err != nil {
		return nil, err
	}
	slots := sub
	if langKey == "dub" {
		slots = dub
	}
	if len(slots) == 0 {
		p.log.Info().Str("showId", show.id).Int("episode", episode).Str("lang", langKey).
			Msg("tensho: no server slots for lang")
		return nil, nil
	}
	if len(slots) > len(tenshoServerNames) {
		slots = slots[:len(tenshoServerNames)]
	}

	sources := make([]core.Source, 0, len(slots))
	names := make([]string, 0, len(slots))
	for _, slot := range slots {
		u := tenshoEmbedURL(slot.ServerName, *ep, langKey)
		p.learnURLHost(u)
		sources = append(sources, core.Source{
			URL:          u,
			Type:         "embed",
			Quality:      "auto",
			Verification: "embed",
		})
		names = append(names, tenshoServerNames[len(names)])
	}

	p.log.Info().Str("showId", show.id).Int("episode", episode).Str("lang", langKey).
		Int("embeds", len(sources)).Msg("tensho: resolved")
	return &SourceResult{
		Sources:     sources,
		Headers:     map[string]string{},
		ServerName:  names[0],
		ServerNames: names,
	}, nil
}

// findShow maps an AniList id to a zangetsu watch path via AniList titles +
// /search (cached; negative results are not).
func (p *TenshoProvider) findShow(ctx context.Context, anilistID string) (*tenshoShowEntry, error) {
	p.mu.Lock()
	if e, ok := p.shows[anilistID]; ok && time.Now().Before(e.expires) {
		p.mu.Unlock()
		return e, nil
	}
	p.mu.Unlock()

	titles, err := fetchAnilistTitles(ctx, p.client, p.anilistURL, anilistID)
	if err != nil {
		return nil, fmt.Errorf("tensho: %w", err)
	}
	if len(titles) == 0 {
		return nil, fmt.Errorf("tensho: anilist %s has no titles", anilistID)
	}

	queries := make([]string, 0, 2)
	for _, t := range titles {
		if len([]rune(t)) < 3 {
			continue
		}
		dup := false
		for _, q := range queries {
			if strings.EqualFold(q, t) {
				dup = true
				break
			}
		}
		if !dup {
			queries = append(queries, t)
		}
		if len(queries) == 2 {
			break
		}
	}

	var lastErr error
	for _, q := range queries {
		results, err := p.Search(ctx, q)
		if err != nil {
			lastErr = err
			continue
		}
		best, ok := pickProviderTitle(results, titles)
		if !ok {
			continue
		}
		path, id, splitOK := splitShowPath(best.ID)
		if !splitOK {
			lastErr = fmt.Errorf("tensho: bad show path %q", best.ID)
			continue
		}
		entry := &tenshoShowEntry{path: path, id: id, expires: time.Now().Add(tenshoShowTTL)}
		p.mu.Lock()
		p.shows[anilistID] = entry
		p.mu.Unlock()
		p.log.Info().Str("anilistId", anilistID).Str("path", path).Str("matched", best.Title).
			Msg("tensho: show resolved")
		return entry, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("tensho: no show match for anilist %s", anilistID)
}
