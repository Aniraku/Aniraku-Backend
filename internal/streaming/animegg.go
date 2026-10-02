package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
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

// AnimeGGProvider resolves direct mp4 streams through animegg.org
// (AniList-keyed search + series match + watch-page mirrors + embed
// videoSources). Verified live 2026-10-02 from the production egress:
// search / series / watch / embed all serve 200 with plain HTTP, and the
// /play/ mp4 URLs 302 to vidcache.net file bytes.
//
// PIPELINE (ported from the reference scraper):
//
//	AniList ID -> GraphQL titles -> /search/?q=... (+ compact alnum token)
//	-> dice-title scoring -> /series/{slug} episode lists (sub/dub badges)
//	-> best-series select (episode coverage vs expected, prequel offset)
//	-> /{epSlug} watch page (data-toggle=tab mirrors)
//	-> /embed/{id} (var videoSources) -> direct mp4 360p-1080p.
//
// The /play/ mp4 URLs 302 to vidcache.net file hosts, and the media proxy
// never follows redirects by design (it refuses 3xx rather than relaying
// clients to unreviewed targets). The provider therefore resolves every
// mp4 to its FINAL redirect target before shipping it — the server list
// only ever carries the playable file URL, never the /play/ hop.
//
// SOURCE RULES (operator):
//   - One source per mirror tab, always that tab's HIGHEST quality mp4.
//     The frontend starts playback on the highest rendition, so shipping
//     lower mp4s would only add dead menu entries that restart playback.
//   - Dub is listed only when its best mp4 is >= 720p. AnimeGG dubs are
//     frequently 360p-only while sub goes to 1080p; a low-quality dub must
//     not surface as a source.
//   - Embed fallbacks are never shipped (flixcloud owns embeds).
//
// SERVER NAMES (operator): positional cute names — Sunny (1st tab), Yolky
// (2nd), Eggy (3rd+). Never raw mirror labels.
const (
	animeggDefaultBase = "https://www.animegg.org"
	animeggReferer     = "https://www.animegg.org/"

	animeggShowTTL    = 24 * time.Hour
	animeggResolveTTL = 10 * time.Minute
	maxAnimeGGEntries = 500
)

var animeggServerNames = []string{"Sunny", "Yolky", "Eggy"}

func animeggServerName(i int) string {
	if i >= 0 && i < len(animeggServerNames) {
		return animeggServerNames[i]
	}
	return fmt.Sprintf("Egg-%d", i+1)
}

type AnimeGGProvider struct {
	log        zerolog.Logger
	client     *http.Client
	base       string
	anilistURL string
	learnHost  func(host string)

	mu       sync.Mutex
	shows    map[int]*animeggShowEntry
	resolved map[animeggResolveKey]*animeggResolvedEntry
}

type animeggShowEntry struct {
	slug    string
	title   string
	fetched time.Time
}

type animeggResolveKey struct {
	anilistID int
	episode   int
	lang      string
}

type animeggResolvedEntry struct {
	result  *SourceResult
	fetched time.Time
}

func NewAnimeGGProvider(log zerolog.Logger, base, anilistURL string) *AnimeGGProvider {
	if strings.TrimSpace(base) == "" {
		base = animeggDefaultBase
	}
	if strings.TrimSpace(anilistURL) == "" {
		anilistURL = "https://graphql.aniraku.tech"
	}
	return &AnimeGGProvider{
		log:        log,
		client:     &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		base:       strings.TrimRight(base, "/"),
		anilistURL: anilistURL,
		shows:      make(map[int]*animeggShowEntry),
		resolved:   make(map[animeggResolveKey]*animeggResolvedEntry),
	}
}

func (p *AnimeGGProvider) Name() string { return "animegg" }

func (p *AnimeGGProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *AnimeGGProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *AnimeGGProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("animegg search not implemented")
}

func (p *AnimeGGProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("animegg episode listing not implemented")
}

// ------------------------- HTTP -------------------------

func (p *AnimeGGProvider) getText(ctx context.Context, rawURL, referer string, limit int64) (string, error) {
	if referer == "" {
		referer = p.base + "/"
	}
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
		req.Header.Set("Referer", referer)
		resp, err := p.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
		status := resp.StatusCode
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if status == http.StatusTooManyRequests || status >= 500 {
			lastErr = fmt.Errorf("animegg: GET %s -> HTTP %d", rawURL, status)
			continue
		}
		if status != http.StatusOK {
			return "", fmt.Errorf("animegg: GET %s -> HTTP %d", rawURL, status)
		}
		return string(body), nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("animegg: GET %s failed", rawURL)
	}
	return "", lastErr
}

func (p *AnimeGGProvider) postAnilist(ctx context.Context, payload any, out any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.anilistURL, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", browserUA)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("animegg: anilist HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

// ------------------------- text helpers -------------------------

var animeggTagRe = regexp.MustCompile(`<[^>]*>`)

func animeggStrip(s string) string {
	s = animeggTagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	return strings.Join(strings.Fields(s), " ")
}

func animeggAttr(tag, name string) string {
	lower := strings.ToLower(tag)
	key := strings.ToLower(name) + `="`
	if i := strings.Index(lower, key); i >= 0 {
		rest := tag[i+len(key):]
		if j := strings.Index(rest, `"`); j >= 0 {
			return html.UnescapeString(rest[:j])
		}
	}
	key = strings.ToLower(name) + `='`
	if i := strings.Index(lower, key); i >= 0 {
		rest := tag[i+len(key):]
		if j := strings.Index(rest, `'`); j >= 0 {
			return html.UnescapeString(rest[:j])
		}
	}
	return ""
}

func animeggNorm(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func animeggDice(a, b string) float64 {
	na, nb := animeggNorm(a), animeggNorm(b)
	if na == nb {
		return 1.0
	}
	if len(na) < 2 || len(nb) < 2 {
		return 0.0
	}
	grams := map[string]int{}
	for i := 0; i+1 < len(na); i++ {
		grams[na[i:i+2]]++
	}
	hits := 0
	for i := 0; i+1 < len(nb); i++ {
		if grams[nb[i:i+2]] > 0 {
			hits++
			grams[nb[i:i+2]]--
		}
	}
	return (2 * float64(hits)) / float64(len(na)+len(nb)-2)
}

var (
	animeggNumRe   = regexp.MustCompile(`\d+`)
	animeggMovieRe = regexp.MustCompile(`(?i)\b(movie|film|the movie)\b`)
	animeggSlugMv  = regexp.MustCompile(`movie|film`)
)

func animeggTitleScore(query, candidate, slug string) float64 {
	slugged := strings.ReplaceAll(slug, "-", " ")
	base := animeggDice(query, candidate)
	if d := animeggDice(query, slugged); d > base {
		base = d
	}
	qn, sn := "", ""
	if m := animeggNumRe.FindAllString(animeggNorm(query), -1); len(m) > 0 {
		qn = m[0]
	}
	if m := animeggNumRe.FindAllString(slug, -1); len(m) > 0 {
		sn = m[0]
	}
	if qn != "" && sn != "" && qn != sn {
		return base * 0.65
	}
	if qn != "" && sn == "" {
		return base * 0.65
	}
	if qn == "" && sn != "" {
		if n, err := strconv.Atoi(sn); err == nil && n > 1 && n < 1900 {
			base *= 1 - 0.06*float64(n-1)
		}
	}
	isMovieQuery := animeggMovieRe.MatchString(query)
	isMovieMatch := animeggMovieRe.MatchString(candidate) || animeggSlugMv.MatchString(slug)
	if isMovieQuery && !isMovieMatch {
		return base * 0.4
	}
	if ql, sl := len(animeggNorm(query)), len(animeggNorm(slugged)); sl > ql*16/10+4 {
		return base * 0.8
	}
	return base
}

// animeggQualityRank maps a source label to a comparable height:
// "1080p" -> 1080. Unknown labels rank 0 (dub gate hides them).
func animeggQualityRank(label string) int {
	s := strings.ToLower(strings.TrimSpace(label))
	if m := regexp.MustCompile(`(\d{3,4})\s*p`).FindStringSubmatch(s); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil {
			return n
		}
	}
	switch s {
	case "hd", "hq":
		return 720
	case "sd":
		return 480
	}
	return 0
}

// ------------------------- AniList -------------------------

type animeggRelation struct {
	Edges []struct {
		RelationType string `json:"relationType"`
		Node         struct {
			ID        int             `json:"id"`
			Type      string          `json:"type"`
			Episodes  int             `json:"episodes"`
			Relations animeggRelation `json:"relations"`
		} `json:"node"`
	} `json:"edges"`
}

type animeggMedia struct {
	ID    int `json:"id"`
	Title struct {
		English string `json:"english"`
		Romaji  string `json:"romaji"`
		Native  string `json:"native"`
	} `json:"title"`
	Synonyms   []string `json:"synonyms"`
	Status     string   `json:"status"`
	Format     string   `json:"format"`
	Episodes   *int     `json:"episodes"`
	SeasonYear int      `json:"seasonYear"`
	StartDate  struct {
		Year int `json:"year"`
	} `json:"startDate"`
	Relations animeggRelation `json:"relations"`
}

func (p *AnimeGGProvider) anilistMedia(ctx context.Context, id int) (*animeggMedia, error) {
	q := `query($id:Int){Media(id:$id,type:ANIME){id title{english romaji native} synonyms status format episodes seasonYear startDate{year} relations{edges{relationType(version:2) node{id type episodes relations{edges{relationType(version:2) node{id type episodes relations{edges{relationType(version:2) node{id type episodes}}}}}}}}}}}`
	var out struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
		Data struct {
			Media *animeggMedia `json:"Media"`
		} `json:"data"`
	}
	if err := p.postAnilist(ctx, map[string]any{"query": q, "variables": map[string]any{"id": id}}, &out); err != nil {
		return nil, err
	}
	if len(out.Errors) > 0 {
		return nil, fmt.Errorf("animegg: anilist: %s", out.Errors[0].Message)
	}
	if out.Data.Media == nil {
		return nil, fmt.Errorf("animegg: no anilist data for %d", id)
	}
	return out.Data.Media, nil
}

func animeggTitles(m *animeggMedia) []string {
	var out []string
	for _, t := range append([]string{m.Title.English, m.Title.Romaji, m.Title.Native}, m.Synonyms...) {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func animeggExpected(m *animeggMedia) int {
	if m.Episodes != nil && *m.Episodes > 0 {
		return *m.Episodes
	}
	return 0
}

func animeggPrequelOffset(rel animeggRelation, depth int) int {
	if depth > 5 {
		return 0
	}
	for _, e := range rel.Edges {
		if e.RelationType == "PREQUEL" && e.Node.Type == "ANIME" && e.Node.Episodes >= 5 {
			return e.Node.Episodes + animeggPrequelOffset(e.Node.Relations, depth+1)
		}
	}
	return 0
}

// ------------------------- search / match -------------------------

type animeggCandidate struct {
	slug  string
	title string
	score float64
}

var animeggAnchorRe = regexp.MustCompile(`(?is)<a\b([^>]*)>(.*?)</a>`)
var animeggMseRe = regexp.MustCompile(`\bmse\b`)

func animeggParseSearch(htmlBody string) []animeggCandidate {
	var out []animeggCandidate
	for _, m := range animeggAnchorRe.FindAllStringSubmatch(htmlBody, 200) {
		attrs, inner := m[1], m[2]
		if !animeggMseRe.MatchString(animeggAttr("<a "+attrs+">", "class")) {
			continue
		}
		href := animeggAttr("<a "+attrs+">", "href")
		if !strings.HasPrefix(href, "/series/") {
			continue
		}
		slug := strings.SplitN(strings.TrimPrefix(href, "/series/"), "/", 2)[0]
		slug = strings.SplitN(slug, "?", 2)[0]
		if slug == "" {
			continue
		}
		text := slug
		if sm := regexp.MustCompile(`(?is)<strong[^>]*>(.*?)</strong>`).FindStringSubmatch(inner); sm != nil {
			if t := animeggStrip(sm[1]); t != "" {
				text = t
			}
		} else if t := animeggStrip(inner); t != "" {
			text = strings.ReplaceAll(t, "-", " ")
		}
		out = append(out, animeggCandidate{slug: slug, title: strings.ReplaceAll(text, "-", " ")})
	}
	return out
}

func (p *AnimeGGProvider) searchOne(ctx context.Context, query string) []animeggCandidate {
	body, err := p.getText(ctx, p.base+"/search/?q="+url.QueryEscape(query), p.base+"/", 1<<20)
	if err != nil {
		return nil
	}
	return animeggParseSearch(body)
}

// searchAnimeGG mirrors search_fn: plain query plus a compact alnum token
// of the first word ("Re:Zero" -> "ReZero") to surface season variants.
func (p *AnimeGGProvider) searchAnimeGG(ctx context.Context, query string) []animeggCandidate {
	r1 := p.searchOne(ctx, query)
	words := strings.Fields(query)
	if len(words) > 0 {
		var b strings.Builder
		for _, r := range words[0] {
			if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
				b.WriteRune(r)
			}
		}
		if tok := b.String(); len(tok) >= 4 && !strings.EqualFold(tok, query) {
			seen := map[string]bool{}
			for _, r := range r1 {
				seen[r.slug] = true
			}
			for _, r := range p.searchOne(ctx, tok) {
				if !seen[r.slug] {
					r1 = append(r1, r)
				}
			}
		}
	}
	return r1
}

var (
	animeggSeasonRe = regexp.MustCompile(`(?i)\bseason\s*\d+\b`)
	animeggPartRe   = regexp.MustCompile(`(?i)\bpart\s*\d+\b`)
	animeggOrdRe    = regexp.MustCompile(`(?i)\b\d+(?:rd|th|st|nd)\b`)
	animeggSpaceRe  = regexp.MustCompile(`\s+`)
)

func animeggBuildQueries(title string) []string {
	set := map[string]bool{title: true}
	words := strings.Fields(title)
	if len(words) > 4 {
		set[strings.Join(words[:4], " ")] = true
	}
	if len(words) > 3 {
		set[strings.Join(words[:3], " ")] = true
	}
	stripped := animeggSeasonRe.ReplaceAllString(title, "")
	stripped = animeggPartRe.ReplaceAllString(stripped, "")
	stripped = animeggOrdRe.ReplaceAllString(stripped, "")
	stripped = strings.TrimSpace(animeggSpaceRe.ReplaceAllString(stripped, " "))
	if stripped != "" && stripped != title {
		set[stripped] = true
	}
	var out []string
	for q := range set {
		if len(q) >= 3 {
			out = append(out, q)
		}
	}
	return out
}

func (p *AnimeGGProvider) findTopSlugs(ctx context.Context, titles []string) []animeggCandidate {
	var queries []string
	seenQ := map[string]bool{}
	limit := titles
	if len(limit) > 4 {
		limit = limit[:4]
	}
	for _, t := range limit {
		for _, q := range animeggBuildQueries(t) {
			if !seenQ[q] {
				seenQ[q] = true
				queries = append(queries, q)
			}
		}
	}
	type res struct {
		cands []animeggCandidate
	}
	ch := make(chan res, len(queries))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 6)
	for _, q := range queries {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(query string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			ch <- res{p.searchAnimeGG(ctx, query)}
		}(q)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	merged := map[string]string{}
	for r := range ch {
		for _, c := range r.cands {
			if _, ok := merged[c.slug]; !ok {
				merged[c.slug] = c.title
			}
		}
	}
	head := titles
	if len(head) > 2 {
		head = head[:2]
	}
	var scored []animeggCandidate
	for slug, text := range merged {
		best := 0.0
		for _, t := range head {
			if s := animeggTitleScore(t, text, slug); s > best {
				best = s
			}
		}
		if best >= 0.5 {
			scored = append(scored, animeggCandidate{slug: slug, title: text, score: best})
		}
	}
	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	if len(scored) > 6 {
		scored = scored[:6]
	}
	return scored
}

type animeggEpisode struct {
	number int
	title  string
	epSlug string
	hasSub bool
	hasDub bool
}

var (
	animeggLiRe      = regexp.MustCompile(`(?is)<li\b[^>]*>(.*?)</li>`)
	animeggPopLinkRe = regexp.MustCompile(`(?i)<a\b[^>]*class=["'][^"']*anm_det_pop[^"']*["'][^>]*>`)
	animeggStrongRe  = regexp.MustCompile(`(?is)<strong[^>]*>(.*?)</strong>`)
	animeggRangeRe   = regexp.MustCompile(`(\d+)-(\d+)\s*$`)
	animeggNumEndRe  = regexp.MustCompile(`(\d+)\s*$`)
	animeggAnititle  = regexp.MustCompile(`(?is)<i\b[^>]*class=["'][^"']*anititle[^"']*["'][^>]*>(.*?)</i>`)
)

func animeggParseSeries(htmlBody string) []animeggEpisode {
	var out []animeggEpisode
	for _, m := range animeggLiRe.FindAllStringSubmatch(htmlBody, 2000) {
		block := m[1]
		if !strings.Contains(block, "anm_det_pop") {
			continue
		}
		link := animeggPopLinkRe.FindString(block)
		if link == "" {
			continue
		}
		href := strings.SplitN(animeggAttr(link, "href"), "#", 2)[0]
		href = strings.TrimLeft(href, "/")
		if href == "" {
			continue
		}
		strong := ""
		if sm := animeggStrongRe.FindStringSubmatch(block); sm != nil {
			strong = animeggStrip(sm[1])
		}
		var num int
		if rm := animeggRangeRe.FindStringSubmatch(strong); rm != nil {
			num, _ = strconv.Atoi(rm[1])
		} else if nm := animeggNumEndRe.FindStringSubmatch(strong); nm != nil {
			num, _ = strconv.Atoi(nm[1])
		} else {
			continue
		}
		if num <= 0 {
			continue
		}
		title := strong
		if im := animeggAnititle.FindStringSubmatch(block); im != nil {
			if t := animeggStrip(im[1]); t != "" {
				title = t
			}
		}
		out = append(out, animeggEpisode{
			number: num,
			title:  title,
			epSlug: href,
			hasSub: strings.Contains(block, "btn-subbed"),
			hasDub: strings.Contains(block, "btn-dubbed"),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].number < out[j].number })
	seen := map[int]bool{}
	dedup := out[:0]
	for _, e := range out {
		if !seen[e.number] {
			seen[e.number] = true
			dedup = append(dedup, e)
		}
	}
	return dedup
}

func (p *AnimeGGProvider) scrapeSeries(ctx context.Context, slug string) []animeggEpisode {
	body, err := p.getText(ctx, p.base+"/series/"+slug, p.base+"/", 1<<20)
	if err != nil {
		return nil
	}
	return animeggParseSeries(body)
}

type animeggSeries struct {
	slug     string
	title    string
	score    float64
	mode     string
	offset   int
	episodes []animeggEpisode
}

func animeggSelectSeries(cands []animeggCandidate, fetch func(string) []animeggEpisode, expected int, status string, offset int, isMovie bool) *animeggSeries {
	minScore := 0.65
	if isMovie {
		minScore = 0.9
	}
	type scored struct {
		cand animeggCandidate
		mode string
		sc   float64
		eps  []animeggEpisode
	}
	results := make(chan scored, len(cands))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for _, c := range cands {
		wg.Add(1)
		go func(cand animeggCandidate) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			default:
			}
			eps := fetch(cand.slug)
			if len(eps) == 0 {
				return
			}
			localHits := len(eps)
			if expected > 0 {
				localHits = 0
				for _, e := range eps {
					if e.number >= 1 && e.number <= expected {
						localHits++
					}
				}
			}
			offsetHits := 0
			if expected > 0 && offset > 0 {
				for _, e := range eps {
					if e.number > offset && e.number <= offset+expected {
						offsetHits++
					}
				}
			}
			mode := "local"
			hits := localHits
			if offsetHits > localHits {
				mode, hits = "offset", offsetHits
			}
			countScore := 1.0
			if expected >= 6 {
				needed := expected - 3
				if needed < 1 {
					needed = 1
				}
				if status == "FINISHED" {
					needed = (expected*9 + 9) / 10 // ceil(expected*0.9)
					if needed < 1 {
						needed = 1
					}
				}
				if hits >= needed {
					countScore = 1.0
				} else if needed > 0 {
					countScore = float64(hits) / float64(needed)
				}
			}
			if sc := cand.score*0.7 + countScore*0.3; sc >= minScore {
				results <- scored{cand, mode, sc, eps}
			}
		}(c)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	var best *scored
	for r := range results {
		rr := r
		if best == nil || rr.sc > best.sc {
			best = &rr
		}
	}
	if best == nil {
		return nil
	}
	return &animeggSeries{slug: best.cand.slug, title: best.cand.title, score: best.sc, mode: best.mode, offset: offset, episodes: best.eps}
}

// ------------------------- watch -------------------------

type animeggTab struct {
	embedID string
	mirror  string
	audio   string
}

var animeggTabRe = regexp.MustCompile(`(?i)<a\b[^>]*data-toggle=["']tab["'][^>]*>`)

func animeggParseTabs(htmlBody, audio string) []animeggTab {
	var out []animeggTab
	for _, tag := range animeggTabRe.FindAllString(htmlBody, 40) {
		id := animeggAttr(tag, "data-id")
		if id == "" {
			continue
		}
		mirror := animeggAttr(tag, "data-mirror")
		if mirror == "" {
			mirror = "AnimeGG"
		}
		version := animeggAttr(tag, "data-version")
		if version == "" {
			version = "subbed"
		}
		normalized := "sub"
		if strings.HasPrefix(version, "dub") {
			normalized = "dub"
		}
		if audio != "all" && audio != normalized {
			continue
		}
		out = append(out, animeggTab{embedID: id, mirror: mirror, audio: normalized})
	}
	return out
}

type animeggStream struct {
	url     string
	quality string
}

var (
	animeggSourcesRe = regexp.MustCompile(`(?s)var\s+videoSources\s*=\s*(\[.*?\]);`)
	animeggBlockRe   = regexp.MustCompile(`\{[^{}]*\}`)
	animeggFileDQRe  = regexp.MustCompile(`file:\s*"([^"]*)"`)
	animeggFileSQRe  = regexp.MustCompile(`file:\s*'([^']*)'`)
	animeggLabelDQRe = regexp.MustCompile(`label:\s*"([^"]*)"`)
	animeggLabelSQRe = regexp.MustCompile(`label:\s*'([^']*)'`)
)

func animeggParseVideoSources(embedHTML string) []animeggStream {
	m := animeggSourcesRe.FindStringSubmatch(embedHTML)
	if m == nil {
		return nil
	}
	var out []animeggStream
	for _, block := range animeggBlockRe.FindAllString(m[1], 40) {
		file := ""
		if fm := animeggFileDQRe.FindStringSubmatch(block); fm != nil {
			file = fm[1]
		} else if fm := animeggFileSQRe.FindStringSubmatch(block); fm != nil {
			file = fm[1]
		}
		if file == "" {
			continue
		}
		label := "unknown"
		if lm := animeggLabelDQRe.FindStringSubmatch(block); lm != nil {
			label = lm[1]
		} else if lm := animeggLabelSQRe.FindStringSubmatch(block); lm != nil {
			label = lm[1]
		}
		if !strings.HasPrefix(file, "http") && !strings.HasPrefix(file, "/") {
			continue
		}
		out = append(out, animeggStream{url: file, quality: label})
	}
	return out
}

func (p *AnimeGGProvider) absURL(file string) string {
	if strings.HasPrefix(file, "http") {
		return file
	}
	if strings.HasPrefix(file, "/") {
		return p.base + file
	}
	return ""
}

// resolveMP4Final follows the /play/ redirect chain to the final file URL
// (vidcache.net) and verifies it serves media bytes. The media proxy
// refuses 3xx by design, so shipping the unredirected /play/ URL would only
// produce "upstream media redirect blocked" at playback — the probe must
// both verify AND resolve. Returns the final URL.
func (p *AnimeGGProvider) resolveMP4Final(ctx context.Context, playURL string) (string, bool) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, playURL, nil)
	if err != nil {
		return "", false
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Referer", animeggReferer)
	resp, err := p.client.Do(req)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return "", false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32768))
	if err != nil || !segmentBytesPlayable(body) {
		return "", false
	}
	final := ""
	if resp.Request != nil && resp.Request.URL != nil {
		final = resp.Request.URL.String()
	}
	if final == "" {
		final = playURL
	}
	return final, true
}

// ------------------------- resolve -------------------------

func (p *AnimeGGProvider) loadResolved(key animeggResolveKey) *SourceResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[key]
	if !ok || time.Since(e.fetched) > animeggResolveTTL {
		return nil
	}
	return cloneSourceResult(e.result)
}

func (p *AnimeGGProvider) storeResolved(key animeggResolveKey, sr *SourceResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, e := range p.resolved {
		if time.Since(e.fetched) > animeggResolveTTL {
			delete(p.resolved, k)
		}
	}
	if len(p.resolved) >= maxAnimeGGEntries {
		for k := range p.resolved {
			delete(p.resolved, k)
			break
		}
	}
	p.resolved[key] = &animeggResolvedEntry{result: cloneSourceResult(sr), fetched: time.Now()}
}

func (p *AnimeGGProvider) loadShow(id int) (string, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.shows[id]; ok && time.Since(e.fetched) < animeggShowTTL {
		return e.slug, e.title, true
	}
	return "", "", false
}

func (p *AnimeGGProvider) storeShow(id int, slug, title string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, e := range p.shows {
		if time.Since(e.fetched) > animeggShowTTL {
			delete(p.shows, k)
		}
	}
	if len(p.shows) >= maxAnimeGGEntries {
		for k := range p.shows {
			delete(p.shows, k)
			break
		}
	}
	p.shows[id] = &animeggShowEntry{slug: slug, title: title, fetched: time.Now()}
}

// FindEpisodeSource resolves one episode for exactly the requested lang.
// Strict per-lang like kaa: sub reads sub badges/tabs, dub reads dub — no
// cross-lang fallback. Dub below 720p best quality is hidden entirely.
func (p *AnimeGGProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("animegg: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	key := animeggResolveKey{anilistID: id, episode: episode, lang: langKey}
	if got := p.loadResolved(key); got != nil {
		return got, nil
	}
	sr, err := p.resolveEpisode(ctx, id, episode, langKey)
	if err != nil {
		return nil, err
	}
	if sr == nil || len(sr.Sources) == 0 {
		return nil, nil
	}
	p.storeResolved(key, sr)
	return sr, nil
}

func (p *AnimeGGProvider) resolveEpisode(ctx context.Context, id, episode int, lang string) (*SourceResult, error) {
	media, err := p.anilistMedia(ctx, id)
	if err != nil {
		return nil, err
	}
	titles := animeggTitles(media)
	if len(titles) == 0 {
		return nil, fmt.Errorf("animegg: anilist %d has no titles", id)
	}
	expected := animeggExpected(media)
	offset := animeggPrequelOffset(media.Relations, 0)
	isMovie := strings.EqualFold(strings.TrimSpace(media.Format), "MOVIE") || expected == 1

	var series *animeggSeries
	if slug, title, ok := p.loadShow(id); ok {
		eps := p.scrapeSeries(ctx, slug)
		mode := "local"
		if expected > 0 && offset > 0 {
			local, shifted := 0, 0
			for _, e := range eps {
				if e.number >= 1 && e.number <= expected {
					local++
				}
				if e.number > offset && e.number <= offset+expected {
					shifted++
				}
			}
			if shifted > local {
				mode = "offset"
			}
		}
		series = &animeggSeries{slug: slug, title: title, mode: mode, offset: offset, episodes: eps}
	} else {
		cands := p.findTopSlugs(ctx, titles)
		if len(cands) == 0 {
			return nil, fmt.Errorf("animegg: no show match for anilist %d", id)
		}
		series = animeggSelectSeries(cands, func(slug string) []animeggEpisode {
			return p.scrapeSeries(ctx, slug)
		}, expected, media.Status, offset, isMovie)
		if series == nil {
			return nil, fmt.Errorf("animegg: no show match for anilist %d", id)
		}
		p.storeShow(id, series.slug, series.title)
	}
	providerEp := episode
	if series.mode == "offset" {
		providerEp = episode + series.offset
	}
	var ep *animeggEpisode
	for i := range series.episodes {
		if series.episodes[i].number == providerEp {
			ep = &series.episodes[i]
			break
		}
	}
	if ep == nil {
		return nil, fmt.Errorf("animegg: episode %d not listed", providerEp)
	}
	if lang == "sub" && !ep.hasSub || lang == "dub" && !ep.hasDub {
		return nil, nil
	}
	watchHTML, err := p.getText(ctx, p.base+"/"+strings.TrimLeft(ep.epSlug, "/"), p.base+"/", 1<<20)
	if err != nil {
		return nil, err
	}
	tabs := animeggParseTabs(watchHTML, lang)
	if len(tabs) == 0 {
		return nil, nil
	}
	type tabHit struct {
		tab  animeggTab
		best animeggStream
	}
	var hits []tabHit
	for _, tab := range tabs {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		embedHTML, err := p.getText(ctx, p.base+"/embed/"+tab.embedID, p.base+"/", 1<<20)
		if err != nil {
			continue
		}
		var cands []animeggStream
		for _, s := range animeggParseVideoSources(embedHTML) {
			abs := p.absURL(s.url)
			if abs == "" {
				continue
			}
			cands = append(cands, animeggStream{url: abs, quality: s.quality})
		}
		// Highest quality first; a deleted top file falls through to the
		// next live one instead of killing the whole mirror.
		sort.Slice(cands, func(i, j int) bool {
			return animeggQualityRank(cands[i].quality) > animeggQualityRank(cands[j].quality)
		})
		placed := false
		for _, c := range cands {
			final, ok := p.resolveMP4Final(ctx, c.url)
			if !ok {
				continue
			}
			hits = append(hits, tabHit{tab: tab, best: animeggStream{url: final, quality: c.quality}})
			placed = true
			break
		}
		if !placed {
			p.log.Info().Str("anilistId", strconv.Itoa(id)).Int("episode", episode).
				Msg("animegg: mp4 probe failed, trying next mirror")
		}
	}
	if len(hits) == 0 {
		return nil, fmt.Errorf("animegg: no playable mirror for episode %d", providerEp)
	}
	if lang == "dub" {
		bestRank := 0
		for _, h := range hits {
			if r := animeggQualityRank(h.best.quality); r > bestRank {
				bestRank = r
			}
		}
		if bestRank < 720 {
			p.log.Info().Str("anilistId", strconv.Itoa(id)).Int("episode", episode).
				Int("bestDub", bestRank).Msg("animegg: dub below 720p, hiding dub")
			return nil, nil
		}
	}
	headers := map[string]string{"Referer": animeggReferer}
	sr := &SourceResult{Headers: headers}
	for i, h := range hits {
		p.learnURLHost(h.best.url)
		quality := strings.TrimSpace(h.best.quality)
		if quality == "" {
			quality = "auto"
		}
		sr.Sources = append(sr.Sources, core.Source{
			URL:          h.best.url,
			Type:         "mp4",
			Quality:      quality,
			Verification: "proxy",
		})
		sr.ServerNames = append(sr.ServerNames, animeggServerName(i))
	}
	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", lang).Int("mirrors", len(hits)).Msg("animegg resolved")
	if len(sr.ServerNames) > 0 {
		sr.ServerName = sr.ServerNames[0]
	}
	return sr, nil
}
