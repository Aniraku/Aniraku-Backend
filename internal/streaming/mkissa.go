package streaming

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

const (
	mkissaBuildID = "180"
	mkissaLane    = "k7"

	mkissaSlugTTL           = 24 * time.Hour
	mkissaResolveTTL        = 30 * time.Minute
	maxMkissaSlugEntries    = 500
	maxMkissaResolveEntries = 500

	mkissaEpisodeQuery     = `query($showId:String!,$translationType:VaildTranslationTypeEnumType!,$episodeString:String!){episode(showId:$showId,translationType:$translationType,episodeString:$episodeString){episodeString sourceUrls}}`
	mkissaEpisodeQueryHash = "670bbf38d0868f446e2346c1e956ca2c40c416e733ca248fd54e04f1c8b99145"

	mkissaAPIHostDefault = "https://api.mkissa.net"
	mkissaReferer        = "https://mkissa.to/"
	mkissaOrigin         = "https://mkissa.to"
)

// mkissaAPIGap serializes secured API calls to avoid burning the per-egress
// throttle bucket. Var so tests can disable it.
var mkissaAPIGap = 4 * time.Second

var mkissaMaskParts = []string{
	"AFEIqKvfcWk=",
	"SPqwGgteqB4=",
	"s/WpUzSxn8I=",
	"R7bCnc9cO5A=",
}

// mkissaServerNames maps source kinds to stable cute server names.
var mkissaServerNames = map[string]string{
	"default": "Chuu",
	"uv-mp4":  "Mua",
	"mp4":     "Kissy",
	"ss-hls":  "Smooch",
	"sl-mp4":  "Peck",
	"ok":      "Xoxo",
	"uni":     "Umi",
}

type mkissaSlugEntry struct {
	showID  string
	title   string
	fetched time.Time
}

type mkissaResolveKey struct {
	showID  string
	episode int
	lang    string
}

type mkissaResolvedEntry struct {
	result  *SourceResult
	fetched time.Time
}

type mkissaEdge struct {
	ID       string           `json:"_id"`
	Name     string           `json:"name"`
	English  string           `json:"englishName"`
	Native   string           `json:"nativeName"`
	Episodes any              `json:"availableEpisodes"`
	Detail   map[string][]any `json:"availableEpisodesDetail"`
	AniList  any              `json:"aniListId"`
}

type mkissaSource struct {
	Name         string  `json:"name"`
	SourceName   string  `json:"sourceName"`
	URL          string  `json:"url"`
	SourceURL    string  `json:"sourceUrl"`
	ExtractedURL string  `json:"extractedUrl"`
	Type         string  `json:"type"`
	Priority     float64 `json:"priority"`
}

func (s mkissaSource) kind() string {
	if strings.TrimSpace(s.Name) != "" {
		return s.Name
	}
	return s.SourceName
}

func (s mkissaSource) directURL() string {
	if strings.TrimSpace(s.ExtractedURL) != "" {
		return s.ExtractedURL
	}
	if strings.TrimSpace(s.URL) != "" {
		return s.URL
	}
	return s.SourceURL
}

type mkissaBootstrap struct {
	Epoch int64  `json:"epoch"`
	PartB string `json:"partB"`
	K     string `json:"k"`
}

type mkissaDecrypted struct {
	Episode struct {
		EpisodeString string         `json:"episodeString"`
		SourceUrls    []mkissaSource `json:"sourceUrls"`
	} `json:"episode"`
	SourceUrls []mkissaSource `json:"sourceUrls"`
}

func (d mkissaDecrypted) sources() []mkissaSource {
	if len(d.Episode.SourceUrls) > 0 {
		return d.Episode.SourceUrls
	}
	return d.SourceUrls
}

type MkissaProvider struct {
	log        zerolog.Logger
	client     *http.Client
	apiHost    string
	apiBase    string
	anilistURL string
	learnHost  func(string)

	mu       sync.Mutex
	slugs    map[string]*mkissaSlugEntry
	resolved map[mkissaResolveKey]*mkissaResolvedEntry

	apiMu       sync.Mutex
	lastAPICall time.Time
	apiGap      time.Duration

	laneMu       sync.Mutex
	laneCache    *mkissaLaneCache
	laneCacheTTL time.Duration

	throttleMu          sync.Mutex
	consecutiveThrottle int
	blockedUntil        time.Time
}

type mkissaLaneCache struct {
	boot    mkissaBootstrap
	laneKey []byte
	fetched time.Time
}

func NewMkissaProvider(log zerolog.Logger) *MkissaProvider {
	host := strings.TrimSpace(os.Getenv("MKISSA_API"))
	if host == "" {
		host = mkissaAPIHostDefault
	}
	host = strings.TrimRight(host, "/")
	p := &MkissaProvider{
		log:          log,
		client:       &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		apiHost:      host,
		apiBase:      host + "/api",
		anilistURL:   core.AnilistGraphQLEndpoint(),
		slugs:        make(map[string]*mkissaSlugEntry),
		resolved:     make(map[mkissaResolveKey]*mkissaResolvedEntry),
		apiGap:       mkissaAPIGap,
		laneCacheTTL: 5 * time.Minute,
	}
	return p
}

func (p *MkissaProvider) Name() string { return "mkissa" }

func (p *MkissaProvider) SetHostLearner(fn func(string)) { p.learnHost = fn }

func (p *MkissaProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *MkissaProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	edges, err := p.searchShows(ctx, title, "sub")
	if err != nil {
		return nil, err
	}
	var out []SearchResult
	for _, e := range edges {
		t := e.English
		if t == "" {
			t = e.Name
		}
		out = append(out, SearchResult{ID: e.ID, Title: t})
	}
	return out, nil
}

func (p *MkissaProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	edges, err := p.searchShows(ctx, providerID, "sub")
	if err != nil {
		return nil, err
	}
	seen := map[int]bool{}
	var out []Episode
	for _, e := range edges {
		for _, s := range episodeStrings(e, "sub") {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil || f != float64(int(f)) {
				continue
			}
			if !seen[int(f)] {
				seen[int(f)] = true
				out = append(out, Episode{Number: int(f)})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out, nil
}

func (p *MkissaProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (sr *SourceResult, err error) {
	defer func() {
		if err != nil {
			if mkissaIsThrottleErr(err) {
				p.recordThrottle(false)
			}
		} else if sr != nil && len(sr.Sources) > 0 {
			p.recordThrottle(true)
		}
	}()
	if p.isThrottled() {
		return nil, fmt.Errorf("mkissa: throttled")
	}
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("mkissa: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	showID, showTitle, err := p.resolveShow(ctx, id, langKey)
	if err != nil {
		return nil, err
	}
	if cached := p.loadResolved(showID, episode, langKey); cached != nil {
		return cached, nil
	}
	epStr, err := p.matchEpisode(ctx, showID, showTitle, langKey, episode)
	if err != nil {
		return nil, err
	}
	boot, laneKey, err := p.getLane(ctx)
	if err != nil {
		return nil, err
	}
	ts := time.Now().UnixMilli() / 300000 * 300000
	aaReq, err := mkissaAaReq(laneKey, boot.Epoch, mkissaBuildID, mkissaEpisodeQueryHash, ts)
	if err != nil {
		return nil, err
	}
	vars := map[string]any{
		"showId":          showID,
		"translationType": langKey,
		"episodeString":   epStr,
	}
	body := map[string]any{
		"variables": vars,
		"extensions": map[string]any{
			"persistedQuery": map[string]any{"version": 1, "sha256Hash": mkissaEpisodeQueryHash},
			"k":              mkissaLane,
			"aaReq":          aaReq,
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBase, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Referer", mkissaReferer)
	req.Header.Set("Origin", mkissaOrigin)
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("x-build-id", mkissaBuildID)
	p.acquireAPI(ctx)
	resp, err := p.client.Do(req)
	p.releaseAPI()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mkissa: episode POST -> HTTP %d", resp.StatusCode)
	}
	var parsed struct {
		Data struct {
			Tobeparsed string `json:"tobeparsed"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rb, &parsed); err != nil {
		return nil, fmt.Errorf("mkissa: episode decode: %w", err)
	}
	if len(parsed.Errors) > 0 {
		msgs := make([]string, 0, len(parsed.Errors))
		for _, e := range parsed.Errors {
			msgs = append(msgs, e.Message)
		}
		return nil, fmt.Errorf("mkissa: %s", strings.Join(msgs, " | "))
	}
	if parsed.Data.Tobeparsed == "" {
		return nil, fmt.Errorf("mkissa: missing tobeparsed")
	}
	pt, err := mkissaDecryptTobeparsed(laneKey, parsed.Data.Tobeparsed)
	if err != nil {
		return nil, fmt.Errorf("mkissa: decrypt: %w", err)
	}
	var dec mkissaDecrypted
	if err := json.Unmarshal(pt, &dec); err != nil {
		return nil, fmt.Errorf("mkissa: decrypted decode: %w", err)
	}
	sr = &SourceResult{
		Headers: map[string]string{
			"Referer":    mkissaReferer,
			"User-Agent": browserUA,
		},
	}

	type mkissaCandidate struct {
		name string
		url  string
		typ  string
	}
	var cands []mkissaCandidate
	for _, s := range dec.sources() {
		kind := strings.ToLower(strings.TrimSpace(s.kind()))
		name, ok := mkissaServerNames[kind]
		if !ok {
			p.log.Debug().Str("kind", s.kind()).Msg("mkissa: unnamed source kind skipped")
			continue
		}
		u := strings.TrimSpace(s.directURL())
		if u == "" {
			u = strings.TrimSpace(p.extractMkissaSource(ctx, s))
		}
		if u == "" {
			continue // pure embed / unextractable
		}
		lu := strings.ToLower(u)
		var typ string
		switch {
		case strings.Contains(lu, ".m3u8"):
			typ = "hls"
		case strings.Contains(lu, ".mp4"):
			typ = "mp4"
		default:
			continue // not a direct stream URL
		}
		cands = append(cands, mkissaCandidate{name: name, url: u, typ: typ})
	}

	kept := make([]bool, len(cands))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, c := range cands {
		wg.Add(1)
		go func(i int, c mkissaCandidate) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if c.typ == "hls" {
				kept[i] = probeSegmentsLenient(ctx, p.client, c.url, mkissaReferer, browserUA)
			} else {
				kept[i] = probeMediaFileLenient(ctx, p.client, c.url, mkissaReferer, browserUA)
			}
		}(i, c)
	}
	wg.Wait()

	for i, c := range cands {
		if !kept[i] {
			p.log.Info().Str("url", c.url).Msg("mkissa: source blocked from this egress, dropping")
			continue
		}
		p.learnURLHost(c.url)
		sr.Sources = append(sr.Sources, core.Source{
			URL:          c.url,
			Type:         c.typ,
			Quality:      "auto",
			Verification: "proxy",
		})
		sr.ServerNames = append(sr.ServerNames, c.name)
	}
	if len(sr.Sources) == 0 {
		return nil, nil
	}
	p.storeResolved(showID, episode, langKey, sr)
	return sr, nil
}

func (p *MkissaProvider) getLane(ctx context.Context) (mkissaBootstrap, []byte, error) {
	p.laneMu.Lock()
	if p.laneCache != nil && time.Since(p.laneCache.fetched) < p.laneCacheTTL {
		cache := p.laneCache
		p.laneMu.Unlock()
		return cache.boot, append([]byte(nil), cache.laneKey...), nil
	}
	p.laneMu.Unlock()

	p.acquireAPI(ctx)
	boot, err := p.bootstrap(ctx)
	p.releaseAPI()
	if err != nil {
		return mkissaBootstrap{}, nil, err
	}
	partB, err := base64.StdEncoding.DecodeString(boot.PartB)
	if err != nil {
		return mkissaBootstrap{}, nil, fmt.Errorf("mkissa: partB decode: %w", err)
	}
	if len(partB) < 32 {
		return mkissaBootstrap{}, nil, fmt.Errorf("mkissa: partB too short")
	}
	mask := mkissaDeriveMask(mkissaBuildID, mkissaMaskParts)
	laneKey := make([]byte, 32)
	for i := 0; i < 32; i++ {
		laneKey[i] = partB[i] ^ mask[i]
	}
	if boot.Epoch == 0 {
		boot.Epoch = mkissaEpoch(time.Now())
	}
	p.laneMu.Lock()
	p.laneCache = &mkissaLaneCache{boot: boot, laneKey: append([]byte(nil), laneKey...), fetched: time.Now()}
	p.laneMu.Unlock()
	return boot, laneKey, nil
}

func (p *MkissaProvider) bootstrap(ctx context.Context) (mkissaBootstrap, error) {
	mask := mkissaDeriveMask(mkissaBuildID, mkissaMaskParts)
	now := time.Now()
	ms := now.UnixMilli()
	base := ms / 604800000
	epochs := []int64{base}
	if base > 0 && ms-base*604800000 < 86400000 {
		epochs = []int64{base - 1, base}
	}
	// dedupe
	seen := map[int64]bool{}
	var uniq []int64
	for _, e := range epochs {
		if seen[e] {
			continue
		}
		seen[e] = true
		uniq = append(uniq, e)
	}
	var lastErr error
	for _, ep := range uniq {
		token := mkissaBootToken(mask, mkissaBuildID, ep)
		reqURL := fmt.Sprintf("%s/client-crypto/v1/bootstrap?buildId=%s&k=%s", p.apiHost, mkissaBuildID, mkissaLane)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("x-build-id", mkissaBuildID)
		req.Header.Set("x-aa-boot", token)
		req.Header.Set("Referer", mkissaReferer)
		req.Header.Set("Origin", mkissaOrigin)
		req.Header.Set("User-Agent", browserUA)
		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("mkissa: bootstrap HTTP %d", resp.StatusCode)
			continue
		}
		var boot mkissaBootstrap
		if err := json.Unmarshal(rb, &boot); err != nil {
			lastErr = fmt.Errorf("mkissa: bootstrap decode: %w", err)
			continue
		}
		if boot.PartB == "" {
			lastErr = fmt.Errorf("mkissa: bootstrap missing partB")
			continue
		}
		if boot.Epoch == 0 {
			boot.Epoch = ep
		}
		return boot, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("mkissa: bootstrap failed")
	}
	return mkissaBootstrap{}, lastErr
}

// resolveShow maps an AniList ID to a mkissa showId (24h cache).
func (p *MkissaProvider) resolveShow(ctx context.Context, id int, langKey string) (string, string, error) {
	if showID, title, ok := p.loadSlug(id, langKey); ok {
		return showID, title, nil
	}
	titles, err := p.anilistTitles(ctx, id)
	if err != nil {
		return "", "", err
	}
	want := strconv.Itoa(id)
	var best *mkissaEdge
	bestScore := -1.0
	exact := false
	seen := make(map[string]bool, len(titles))
	for _, t := range titles {
		key := strings.ToLower(strings.TrimSpace(t))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		edges, err := p.searchShows(ctx, t, langKey)
		if err != nil || len(edges) == 0 {
			continue
		}
		for i := range edges {
			if mkissaAniListID(edges[i].AniList) == want {
				e := edges[i]
				best, bestScore, exact = &e, 1000, true
				break
			}
		}
		if exact {
			break
		}
		var local *mkissaEdge
		localScore := -1.0
		for i := range edges {
			if edgeID := mkissaAniListID(edges[i].AniList); edgeID != "" && edgeID != want {
				continue
			}
			s := mkissaScoreHit(edges[i], titles)
			if s < mkissaFuzzyMinScore {
				continue
			}
			if s > localScore {
				e := edges[i]
				local, localScore = &e, s
			}
		}
		if local != nil && localScore > bestScore {
			best, bestScore = local, localScore
		}
	}
	if best == nil {
		return "", "", fmt.Errorf("mkissa: no show match for anilist %d", id)
	}
	title := best.English
	if title == "" {
		title = best.Name
	}
	if !exact {
		p.log.Warn().Int("anilistId", id).Str("lang", langKey).
			Str("showId", best.ID).Str("showTitle", title).
			Float64("score", bestScore).Msg("mkissa: matched by title score only")
	}
	p.storeSlug(id, langKey, best.ID, title)
	return best.ID, title, nil
}

func (p *MkissaProvider) matchEpisode(ctx context.Context, showID, showTitle, langKey string, episode int) (string, error) {
	edges, err := p.searchShows(ctx, showTitle, langKey)
	if err != nil {
		return "", err
	}
	for i := range edges {
		if edges[i].ID != showID {
			continue
		}
		for _, s := range episodeStrings(edges[i], langKey) {
			if f, err := strconv.ParseFloat(s, 64); err == nil && f == float64(episode) {
				return s, nil
			}
		}
		return "", fmt.Errorf("mkissa: episode %d not listed (%s)", episode, langKey)
	}
	return "", fmt.Errorf("mkissa: show %s not found", showID)
}

func (p *MkissaProvider) searchShows(ctx context.Context, query, mode string) ([]mkissaEdge, error) {
	var out struct {
		Data struct {
			Shows struct {
				Edges []mkissaEdge `json:"edges"`
			} `json:"shows"`
		} `json:"data"`
	}
	err := p.doJSON(ctx, map[string]any{
		"query": mkissaSearchQuery,
		"variables": map[string]any{
			"search":          map[string]any{"allowAdult": false, "allowUnknown": false, "query": query},
			"limit":           40,
			"page":            1,
			"translationType": mode,
			"countryOrigin":   "ALL",
		},
	}, &out)
	if err != nil {
		return nil, err
	}
	return out.Data.Shows.Edges, nil
}

const mkissaSearchQuery = `query($search:SearchInput $limit:Int $page:Int $translationType:VaildTranslationTypeEnumType $countryOrigin:VaildCountryOriginEnumType){shows(search:$search limit:$limit page:$page translationType:$translationType countryOrigin:$countryOrigin){edges{_id name englishName nativeName slugTime availableEpisodes availableEpisodesDetail aniListId __typename}}}`

func (p *MkissaProvider) doJSON(ctx context.Context, payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBase, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Referer", mkissaReferer)
	req.Header.Set("Origin", mkissaOrigin)
	req.Header.Set("User-Agent", browserUA)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mkissa: search POST -> HTTP %d", resp.StatusCode)
	}
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(rb, out)
}

func (p *MkissaProvider) anilistTitles(ctx context.Context, id int) ([]string, error) {
	gql := `query($id:Int){Media(id:$id,type:ANIME){title{romaji english}}}`
	var out struct {
		Data struct {
			Media struct {
				Title struct {
					Romaji  string `json:"romaji"`
					English string `json:"english"`
				} `json:"title"`
			} `json:"Media"`
		} `json:"data"`
	}
	body, _ := json.Marshal(map[string]any{"query": gql, "variables": map[string]any{"id": id}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.anilistURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", browserUA)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mkissa: anilist titles -> HTTP %d", resp.StatusCode)
	}
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, err
	}
	var titles []string
	if t := strings.TrimSpace(out.Data.Media.Title.English); t != "" {
		titles = append(titles, t)
	}
	if t := strings.TrimSpace(out.Data.Media.Title.Romaji); t != "" {
		titles = append(titles, t)
	}
	if len(titles) == 0 {
		return nil, fmt.Errorf("mkissa: anilist %d has no titles", id)
	}
	return titles, nil
}

func mkissaAniListID(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return strconv.Itoa(int(t))
	case json.Number:
		return strings.TrimSpace(t.String())
	}
	return ""
}

func episodeStrings(e mkissaEdge, langKey string) []string {
	raw := e.Detail[langKey]
	if len(raw) == 0 && langKey == "dub" {
		return nil
	}
	var out []string
	for _, v := range raw {
		if s, ok := mkissaEpString(v); ok {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		fi, _ := strconv.ParseFloat(out[i], 64)
		fj, _ := strconv.ParseFloat(out[j], 64)
		return fi < fj
	})
	return out
}

func mkissaEpString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		if strings.TrimSpace(t) == "" {
			return "", false
		}
		return strings.TrimSpace(t), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	case json.Number:
		return strings.TrimSpace(t.String()), true
	}
	return "", false
}

func mkissaScoreHit(e mkissaEdge, titles []string) float64 {
	text := strings.ToLower(strings.Join([]string{e.Name, e.English, e.Native}, " "))
	var score float64
	for _, t := range titles {
		if t == "" {
			continue
		}
		tl := strings.ToLower(t)
		switch {
		case tl == strings.ToLower(e.English) || tl == strings.ToLower(e.Name):
			score = math.Max(score, 100)
		case strings.Contains(text, tl):
			score = math.Max(score, 80)
		default:
			tt := map[string]bool{}
			for _, w := range strings.FieldsFunc(tl, func(r rune) bool {
				return !(r == '_' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z'))
			}) {
				tt[w] = true
			}
			ct := map[string]bool{}
			for _, w := range strings.FieldsFunc(text, func(r rune) bool {
				return !(r == '_' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z'))
			}) {
				ct[w] = true
			}
			if len(tt) > 0 && len(ct) > 0 {
				var overlap float64
				for w := range tt {
					if ct[w] {
						overlap++
					}
				}
				score = math.Max(score, overlap/float64(len(tt))*60)
			}
		}
	}
	for _, eps := range e.Detail {
		if len(eps) > 0 {
			score += 5
			break
		}
	}
	return score
}

const mkissaFuzzyMinScore = 80

// cache helpers
func (p *MkissaProvider) loadSlug(id int, langKey string) (string, string, bool) {
	key := strconv.Itoa(id) + "/" + langKey
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.slugs[key]
	if !ok || time.Since(e.fetched) >= mkissaSlugTTL {
		if ok {
			delete(p.slugs, key)
		}
		return "", "", false
	}
	return e.showID, e.title, true
}

func (p *MkissaProvider) storeSlug(id int, langKey, showID, title string) {
	key := strconv.Itoa(id) + "/" + langKey
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.slugs {
		if time.Since(e.fetched) > mkissaSlugTTL {
			delete(p.slugs, k)
		}
	}
	if len(p.slugs) >= maxMkissaSlugEntries {
		for k := range p.slugs {
			delete(p.slugs, k)
			break
		}
	}
	p.slugs[key] = &mkissaSlugEntry{showID: showID, title: title, fetched: now}
}

func (p *MkissaProvider) loadResolved(showID string, episode int, langKey string) *SourceResult {
	key := mkissaResolveKey{showID: showID, episode: episode, lang: langKey}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[key]
	if !ok || time.Since(e.fetched) >= mkissaResolveTTL {
		if ok {
			delete(p.resolved, key)
		}
		return nil
	}
	return cloneSourceResult(e.result)
}

func (p *MkissaProvider) storeResolved(showID string, episode int, langKey string, sr *SourceResult) {
	key := mkissaResolveKey{showID: showID, episode: episode, lang: langKey}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.resolved {
		if time.Since(e.fetched) > mkissaResolveTTL {
			delete(p.resolved, k)
		}
	}
	if len(p.resolved) >= maxMkissaResolveEntries {
		var oldest mkissaResolveKey
		var oldestAt time.Time
		first := true
		for k, e := range p.resolved {
			if first || e.fetched.Before(oldestAt) {
				oldest, oldestAt, first = k, e.fetched, false
			}
		}
		delete(p.resolved, oldest)
	}
	p.resolved[key] = &mkissaResolvedEntry{result: cloneSourceResult(sr), fetched: now}
}

// crypto helpers
func mkissaDeriveSalt(buildID string) []byte {
	out := make([]byte, 32)
	for i := 0; i < 32; i++ {
		c := byte(0)
		if len(buildID) > 0 {
			c = buildID[i%len(buildID)]
		}
		out[i] = c ^ byte((i*12+227)&255)
	}
	return out
}

func mkissaDeriveMask(buildID string, parts []string) []byte {
	salt := mkissaDeriveSalt(buildID)
	out := make([]byte, 32)
	for i := 0; i < len(parts); i++ {
		part, err := base64.StdEncoding.DecodeString(parts[i])
		if err != nil {
			continue
		}
		offset := i * 8
		for j := 0; j < 8; j++ {
			if offset+j >= len(out) {
				continue
			}
			pv := byte(0)
			if j < len(part) {
				pv = part[j]
			}
			sv := byte(0)
			if offset+j < len(salt) {
				sv = salt[offset+j]
			}
			out[offset+j] = pv ^ sv ^ byte((j*32+i*217)&255)
		}
	}
	return out
}

func mkissaEpoch(now time.Time) int64 {
	ms := now.UnixMilli()
	epoch := ms / 604800000
	if epoch > 0 && ms-epoch*604800000 < 86400000 {
		epoch--
	}
	return epoch
}

func mkissaBootToken(mask []byte, buildID string, epoch int64) string {
	bootKey := mkissaHMACSHA256(mask, []byte("NmmeXF:"+buildID))
	msg := mkissaLane + "+" + buildID + "+" + "mkissa.to" + "+" + "mkissa" + "+" + strconv.FormatInt(epoch, 10)
	return hex.EncodeToString(mkissaHMACSHA256(bootKey, []byte(msg)))
}

func mkissaHMACSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

// sha256Sum returns the hex SHA-256 of s, used so the persisted-query hash
// always matches the query string even if the query text is edited.
func sha256Sum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func mkissaAaReq(laneKey []byte, epoch int64, buildID, qh string, ts int64) (string, error) {
	payload := map[string]any{
		"v":       1,
		"ts":      ts,
		"epoch":   epoch,
		"buildId": buildID,
		"qh":      qh,
		"k":       mkissaLane,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	ivInput := fmt.Sprintf("%d:%s:%s:%d:%s", epoch, buildID, qh, ts, mkissaLane)
	sum := sha256.Sum256([]byte(ivInput))
	iv := sum[:12]
	block, err := aes.NewCipher(laneKey)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	ct := gcm.Seal(nil, iv, b, nil)
	out := make([]byte, 0, 1+12+len(ct))
	out = append(out, 1)
	out = append(out, iv...)
	out = append(out, ct...)
	return base64.StdEncoding.EncodeToString(out), nil
}

func mkissaDecryptTobeparsed(laneKey []byte, b64 string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	if len(raw) < 1+12+16 {
		return nil, fmt.Errorf("mkissa: tobeparsed too short")
	}
	iv := raw[1:13]
	ct := raw[13:]
	block, err := aes.NewCipher(laneKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return gcm.Open(nil, iv, ct, nil)
}

var mkissaHexTable = map[string]string{
	"79": "A", "7a": "B", "7b": "C", "7c": "D", "7d": "E", "7e": "F", "7f": "G",
	"70": "H", "71": "I", "72": "J", "73": "K", "74": "L", "75": "M", "76": "N", "77": "O",
	"68": "P", "69": "Q", "6a": "R", "6b": "S", "6c": "T", "6d": "U", "6e": "V", "6f": "W",
	"60": "X", "61": "Y", "62": "Z", "59": "a", "5a": "b", "5b": "c", "5c": "d", "5d": "e",
	"5e": "f", "5f": "g", "50": "h", "51": "i", "52": "j", "53": "k", "54": "l", "55": "m",
	"56": "n", "57": "o", "48": "p", "49": "q", "4a": "r", "4b": "s", "4c": "t", "4d": "u",
	"4e": "v", "4f": "w", "40": "x", "41": "y", "42": "z", "08": "0", "09": "1", "0a": "2",
	"0b": "3", "0c": "4", "0d": "5", "0e": "6", "0f": "7", "00": "8", "01": "9", "15": "-",
	"16": ".", "67": "_", "46": "~", "02": ":", "17": "/", "07": "?", "1b": "#", "63": "[",
	"65": "]", "78": "@", "19": "!", "1c": "$", "1e": "&", "10": "(", "11": ")", "12": "*",
	"13": "+", "14": ",", "03": ";", "05": "=", "1d": "%",
}

func mkissaDecodeHex(s string) string {
	out := ""
	for i := 0; i+1 < len(s); i += 2 {
		pair := strings.ToLower(s[i : i+2])
		if v, ok := mkissaHexTable[pair]; ok {
			out += v
		} else {
			out += pair
		}
	}
	return out
}

func (p *MkissaProvider) extractMkissaSource(ctx context.Context, s mkissaSource) string {
	urlRaw := strings.TrimSpace(s.SourceURL)
	if urlRaw == "" {
		urlRaw = strings.TrimSpace(s.URL)
	}
	if urlRaw == "" {
		return ""
	}
	if strings.HasPrefix(urlRaw, "--") {
		urlRaw = mkissaDecodeHex(urlRaw[2:])
	}
	if urlRaw != "" && strings.HasPrefix(urlRaw, "/apivtwo/clock") {
		urlRaw = "https://allanime.day" + strings.Replace(urlRaw, "/clock", "/clock.json", 1)
	}
	if strings.HasPrefix(urlRaw, "https://allanime.day") && strings.Contains(urlRaw, "clock") {
		if !strings.Contains(urlRaw, "clock.json") {
			urlRaw = strings.Replace(urlRaw, "/clock", "/clock.json", 1)
		}
		// ensure query separator
		if !strings.Contains(urlRaw, "?") && strings.Contains(urlRaw, "clock.json") {
			// nothing
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlRaw, nil)
		if err == nil {
			req.Header.Set("User-Agent", browserUA)
			req.Header.Set("Referer", "https://allanime.day/player.html")
			req.Header.Set("Accept", "*/*")
			resp, err := p.client.Do(req)
			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
					var data struct {
						Links []struct {
							HLS  *bool  `json:"hls"`
							Link string `json:"link"`
						} `json:"links"`
					}
					if json.Unmarshal(b, &data) == nil {
						for _, l := range data.Links {
							if l.Link != "" && l.HLS != nil && *l.HLS {
								return l.Link
							}
						}
						for _, l := range data.Links {
							if l.Link != "" {
								return l.Link
							}
						}
					}
				}
			}
		}
		return ""
	}
	host := ""
	if u, err := url.Parse(urlRaw); err == nil {
		host = strings.ToLower(u.Hostname())
	}
	switch {
	case s.Type == "player" && urlRaw != "":
		return urlRaw
	case strings.Contains(host, "mp4upload.com"):
		m := regexp.MustCompile(`embed-([a-zA-Z0-9]+)\.html`).FindStringSubmatch(urlRaw)
		if len(m) < 2 {
			return ""
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.mp4upload.com/embed-"+m[1]+".html", nil)
		if err != nil {
			return ""
		}
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Referer", "https://mp4upload.com/")
		resp, err := p.client.Do(req)
		if err != nil || resp == nil {
			return ""
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return ""
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		h := string(b)
		re := regexp.MustCompile(`player\.src\s*\(\s*\{[^}]*\bsrc\s*:\s*"([^"]+)"`)
		if m2 := re.FindStringSubmatch(h); len(m2) >= 2 {
			return strings.ReplaceAll(m2[1], "\\", "")
		}
		re2 := regexp.MustCompile(`"file"\s*:\s*"(https?:[^"]+\.mp4[^"]*)"`)
		if m2 := re2.FindStringSubmatch(h); len(m2) >= 2 {
			return strings.ReplaceAll(m2[1], "\\", "")
		}
		re3 := regexp.MustCompile(`\bsrc\s*:\s*"(https?:[^"]+\.mp4[^"]*)"`)
		if m2 := re3.FindStringSubmatch(h); len(m2) >= 2 {
			return strings.ReplaceAll(m2[1], "\\", "")
		}
	case strings.Contains(host, "ok.ru"):
		m := regexp.MustCompile(`/videoembed/(\d+)`).FindStringSubmatch(urlRaw)
		if len(m) < 2 {
			m = regexp.MustCompile(`/(\d+)(?:[/?#]|$)`).FindStringSubmatch(urlRaw)
		}
		if len(m) < 2 {
			return ""
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://ok.ru/videoembed/"+m[1], nil)
		if err != nil {
			return ""
		}
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Referer", "https://ok.ru/")
		resp, err := p.client.Do(req)
		if err != nil || resp == nil {
			return ""
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return ""
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		h := string(b)
		re := regexp.MustCompile(`ondemandHls(?:\\&quot;|&quot;)\s*:\s*(?:\\&quot;|&quot;)(https?:\/\/.*?)(?:\\&quot;|&quot;)`)
		if m2 := re.FindStringSubmatch(h); len(m2) >= 2 {
			u := m2[1]
			u = strings.ReplaceAll(u, "\\u0026", "&")
			u = strings.ReplaceAll(u, "&amp;", "&")
			u = strings.ReplaceAll(u, "\\/", "/")
			return u
		}
		re2 := regexp.MustCompile(`ondemandHls["']\s*:\s*["'](https?:\/\/.*?)["']`)
		if m2 := re2.FindStringSubmatch(h); len(m2) >= 2 {
			return m2[1]
		}
	case strings.Contains(host, "streamlare.com"):
		m := regexp.MustCompile(`/e/([a-zA-Z0-9]+)`).FindStringSubmatch(urlRaw)
		if len(m) < 2 {
			return ""
		}
		body := strings.NewReader(`{"id":"` + m[1] + `"}`)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://streamlare.com/api/video/stream/get", body)
		if err != nil {
			return ""
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Referer", "https://streamlare.com/")
		req.Header.Set("Origin", "https://streamlare.com")
		req.Header.Set("Accept", "application/json, */*")
		resp, err := p.client.Do(req)
		if err != nil || resp == nil {
			return ""
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return ""
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var data struct {
			Data struct {
				File string `json:"file"`
			} `json:"data"`
		}
		if json.Unmarshal(b, &data) == nil && data.Data.File != "" {
			return data.Data.File
		}
	}
	return ""
}

// acquireAPI serializes secured calls and enforces the inter-call gap.
func (p *MkissaProvider) acquireAPI(ctx context.Context) {
	p.apiMu.Lock()
	if !p.lastAPICall.IsZero() {
		wait := p.apiGap - time.Since(p.lastAPICall)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
			timer.Stop()
		}
	}
}

func (p *MkissaProvider) releaseAPI() {
	p.lastAPICall = time.Now()
	p.apiMu.Unlock()
}

func (p *MkissaProvider) isThrottled() bool {
	p.throttleMu.Lock()
	defer p.throttleMu.Unlock()
	return time.Now().Before(p.blockedUntil)
}

func (p *MkissaProvider) recordThrottle(success bool) {
	p.throttleMu.Lock()
	defer p.throttleMu.Unlock()
	if success {
		p.consecutiveThrottle = 0
		p.blockedUntil = time.Time{}
		return
	}
	p.consecutiveThrottle++
	if p.consecutiveThrottle >= 2 {
		p.blockedUntil = time.Now().Add(10 * time.Minute)
	}
}

func mkissaIsThrottleErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "need_captcha") ||
		strings.Contains(msg, "try again") ||
		strings.Contains(msg, "http 429") ||
		strings.Contains(msg, "throttl") ||
		strings.Contains(msg, "timeout")
}
