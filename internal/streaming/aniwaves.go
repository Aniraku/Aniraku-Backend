package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math"
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

// AniWavesProvider resolves direct HLS/mp4 streams through aniwaves.ru
// (filter search + watch detail + ajax episode/server/source chain +
// echovideo extractors). Verified live 2026-10-02 from the production
// egress: every hop serves 200 with plain HTTP.
//
// PIPELINE (ported from the reference scraper):
//
//	AniList ID -> GraphQL titles -> /filter?keyword=... search cards
//	-> dice-title + type/year/coverage validation (0.82 confidence)
//	-> /ajax/episode/list/{siteId} (data-sub/data-dub badges)
//	-> /ajax/server/list (sub/dub server groups, data-link-id)
//	-> /ajax/sources?id= (embed url per server)
//	-> echovideo extractors: embed-0/embed-1 -> HLS master (?t.m3u8),
//	   embed-20 -> mp4 per quality (savedly).
//
// SOURCE RULES (operator):
//   - HLS masters FIRST: one multi-rendition master per server
//     (Vidplay 360-1080, MyCloud 360/720/1080) lets the player's hls.js
//     levels switch quality without restarting playback. The master URL
//     ships with Quality "auto"; the menu lists real levels.
//   - Savedly mp4s ship only as that mirror's single HIGHEST file
//     (resilience spare, same rule as animegg).
//   - Embed-only servers (no direct file) are never shipped — flixcloud
//     owns embeds.
//   - No dub gate: dub HLS masters carry full renditions, so every dub
//     the listing carries is listed (strict per-lang, no cross fallback).
//
// SERVER NAMES (operator): cute per-upstream names — Nami (Vidplay),
// Coral (MyCloud), Pearl (DatSaV/savedly); unknown mirrors fall back to
// Wavy/Bubbles/Shelly positionally. Never raw upstream labels.
const (
	aniwavesDefaultBase = "https://aniwaves.ru"
	// All direct files served today hang off play.echovideo.ru embeds, so
	// one result-level Referer covers every shipped source (the media proxy
	// forwards it to the master, media and segment hops alike).
	aniwavesPlaybackReferer = "https://play.echovideo.ru/"

	aniwavesShowTTL    = 24 * time.Hour
	aniwavesResolveTTL = 10 * time.Minute
	maxAniWavesEntries = 500
)

func aniwavesCuteName(upstream string, index int) string {
	switch strings.ToLower(strings.TrimSpace(upstream)) {
	case "vidplay":
		return "Nami"
	case "mycloud":
		return "Coral"
	case "datsav", "savedly":
		return "Pearl"
	}
	fallback := []string{"Wavy", "Bubbles", "Shelly"}
	if index >= 0 && index < len(fallback) {
		return fallback[index]
	}
	return fmt.Sprintf("Wavy-%d", index+1)
}

type AniWavesProvider struct {
	log        zerolog.Logger
	client     *http.Client
	base       string
	anilistURL string
	learnHost  func(host string)

	mu       sync.Mutex
	shows    map[int]*aniwavesShowEntry
	resolved map[aniwavesResolveKey]*aniwavesResolvedEntry
}

type aniwavesShowEntry struct {
	slug    string
	siteID  int
	title   string
	fetched time.Time
}

type aniwavesResolveKey struct {
	anilistID int
	episode   int
	lang      string
}

type aniwavesResolvedEntry struct {
	result  *SourceResult
	fetched time.Time
}

func NewAniWavesProvider(log zerolog.Logger, base, anilistURL string) *AniWavesProvider {
	if strings.TrimSpace(base) == "" {
		base = aniwavesDefaultBase
	}
	if strings.TrimSpace(anilistURL) == "" {
		anilistURL = "https://graphql.aniraku.tech"
	}
	return &AniWavesProvider{
		log:        log,
		client:     &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		base:       strings.TrimRight(base, "/"),
		anilistURL: anilistURL,
		shows:      make(map[int]*aniwavesShowEntry),
		resolved:   make(map[aniwavesResolveKey]*aniwavesResolvedEntry),
	}
}

func (p *AniWavesProvider) Name() string { return "aniwaves" }

func (p *AniWavesProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *AniWavesProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *AniWavesProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("aniwaves search not implemented")
}

func (p *AniWavesProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("aniwaves episode listing not implemented")
}

// ------------------------- HTTP -------------------------

func (p *AniWavesProvider) getText(ctx context.Context, rawURL string, headers map[string]string, limit int64) (string, error) {
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
		for k, v := range headers {
			req.Header.Set(k, v)
		}
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
			lastErr = fmt.Errorf("aniwaves: GET %s -> HTTP %d", rawURL, status)
			continue
		}
		if status != http.StatusOK {
			return "", fmt.Errorf("aniwaves: GET %s -> HTTP %d", rawURL, status)
		}
		return string(body), nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("aniwaves: GET %s failed", rawURL)
	}
	return "", lastErr
}

type aniwavesAjax struct {
	Status  int             `json:"status"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

func (p *AniWavesProvider) getAjax(ctx context.Context, path, referer string) (json.RawMessage, error) {
	body, err := p.getText(ctx, p.base+path, map[string]string{
		"Accept":           "application/json, text/javascript, */*; q=0.01",
		"X-Requested-With": "XMLHttpRequest",
		"Referer":          referer,
	}, 1<<20)
	if err != nil {
		return nil, err
	}
	var env aniwavesAjax
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return nil, fmt.Errorf("aniwaves: ajax %s invalid JSON", path)
	}
	if env.Status != 200 {
		msg := env.Message
		if msg == "" {
			msg = "request failed"
		}
		return nil, fmt.Errorf("aniwaves: ajax %s: %s", path, msg)
	}
	return env.Result, nil
}

func (p *AniWavesProvider) postAnilist(ctx context.Context, payload any, out any) error {
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
		return fmt.Errorf("aniwaves: anilist HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

// ------------------------- text helpers -------------------------

var aniwavesTagRe = regexp.MustCompile(`<[^>]*>`)

func aniwavesStrip(s string) string {
	s = aniwavesTagRe.ReplaceAllString(s, " ")
	s = html.UnescapeString(s)
	return strings.Join(strings.Fields(s), " ")
}

func aniwavesAttr(tag, name string) string {
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

func aniwavesNorm(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func aniwavesDice(a, b string) float64 {
	na, nb := aniwavesNorm(a), aniwavesNorm(b)
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

func aniwavesFormatName(v string) string {
	name := strings.ToUpper(strings.TrimSpace(v))
	switch {
	case strings.Contains(name, "SPECIAL"):
		return "special"
	case name == "TV" || name == "TV_SHORT":
		return "tv"
	case name == "MOVIE":
		return "movie"
	case name == "OVA":
		return "ova"
	case name == "ONA":
		return "ona"
	}
	return ""
}

// ------------------------- AniList -------------------------

type aniwavesMedia struct {
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
}

func (p *AniWavesProvider) anilistMedia(ctx context.Context, id int) (*aniwavesMedia, error) {
	q := `query($id:Int){Media(id:$id,type:ANIME){id title{english romaji native} synonyms status format episodes seasonYear startDate{year}}}`
	var out struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
		Data struct {
			Media *aniwavesMedia `json:"Media"`
		} `json:"data"`
	}
	if err := p.postAnilist(ctx, map[string]any{"query": q, "variables": map[string]any{"id": id}}, &out); err != nil {
		return nil, err
	}
	if len(out.Errors) > 0 {
		return nil, fmt.Errorf("aniwaves: anilist: %s", out.Errors[0].Message)
	}
	if out.Data.Media == nil {
		return nil, fmt.Errorf("aniwaves: no anilist data for %d", id)
	}
	return out.Data.Media, nil
}

func aniwavesTitles(m *aniwavesMedia) []string {
	var out []string
	for _, t := range append([]string{m.Title.English, m.Title.Romaji, m.Title.Native}, m.Synonyms...) {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func aniwavesExpected(m *aniwavesMedia) int {
	if m.Episodes != nil && *m.Episodes > 0 {
		return *m.Episodes
	}
	return 0
}

// ------------------------- search / match -------------------------

var (
	aniwavesFinalRe   = regexp.MustCompile(`(?i)(?:the\s+)?final\s+chapters?\b`)
	aniwavesFinal2Re  = regexp.MustCompile(`(?i)final\s+(?:arc|edition)\b`)
	aniwavesKanketsu  = regexp.MustCompile(`(?i)(?:kanketsu|kouhen|zenpen)\s*(?:hen)?\b`)
	aniwavesMovieRe   = regexp.MustCompile(`(?i)(?:the\s+)?movie\b`)
	aniwavesSeasonRe  = regexp.MustCompile(`(?i)(?:season|part|cour|chapter)\s*(?:\d+|one|two|three|four|final)?\b`)
	aniwavesFinSpRe   = regexp.MustCompile(`(?i)(?:final|special)\s*(?:\d+|one|two|three|four)?\b`)
	aniwavesNonWordRe = regexp.MustCompile(`[^\w]+`)
	aniwavesSpaceRe   = regexp.MustCompile(`\s+`)
)

func aniwavesSearchQueries(titles []string) []string {
	var queries []string
	seen := map[string]bool{}
	add := func(q string) {
		if q != "" && !seen[q] {
			seen[q] = true
			queries = append(queries, q)
		}
	}
	limit := titles
	if len(limit) > 8 {
		limit = limit[:8]
	}
	for _, raw := range limit {
		title := strings.TrimSpace(aniwavesSpaceRe.ReplaceAllString(strings.TrimSpace(raw), " "))
		if title == "" {
			continue
		}
		add(title)
		plain := strings.TrimSpace(aniwavesSpaceRe.ReplaceAllString(aniwavesNonWordRe.ReplaceAllString(title, " "), " "))
		if len(plain) >= 3 {
			add(plain)
		}
		words := strings.Fields(plain)
		if len(words) > 4 {
			add(strings.Join(words[:4], " "))
		}
		if len(words) > 6 {
			add(strings.Join(words[:6], " "))
		}
		family := plain
		for _, re := range []*regexp.Regexp{aniwavesFinalRe, aniwavesFinal2Re, aniwavesKanketsu, aniwavesMovieRe, aniwavesSeasonRe, aniwavesFinSpRe} {
			family = re.ReplaceAllString(family, " ")
		}
		family = strings.TrimSpace(aniwavesSpaceRe.ReplaceAllString(family, " "))
		if len(family) >= 3 {
			add(family)
		}
	}
	var out []string
	for _, q := range queries {
		if len(q) >= 3 {
			out = append(out, q)
		}
		if len(out) >= 18 {
			break
		}
	}
	return out
}

type aniwavesCandidate struct {
	slug     string
	siteID   int
	title    string
	japanese string
}

var (
	aniwavesAnchorRe = regexp.MustCompile(`(?is)<a\b([^>]*)>(.*?)</a>`)
	aniwavesSlugRe   = regexp.MustCompile(`(?i)^/watch/([a-z0-9-]+)$`)
	aniwavesSlugIDRe = regexp.MustCompile(`-(\d+)$`)
)

func aniwavesParseCards(htmlBody string) []aniwavesCandidate {
	seen := map[string]bool{}
	var out []aniwavesCandidate
	for _, m := range aniwavesAnchorRe.FindAllStringSubmatch(htmlBody, 500) {
		attrs, inner := m[1], m[2]
		class := strings.ToLower(aniwavesAttr("<a "+attrs+">", "class"))
		if !strings.Contains(class, "name") || !strings.Contains(class, "d-title") {
			continue
		}
		href := aniwavesAttr("<a "+attrs+">", "href")
		sm := aniwavesSlugRe.FindStringSubmatch(href)
		if sm == nil || seen[sm[1]] {
			continue
		}
		idm := aniwavesSlugIDRe.FindStringSubmatch(sm[1])
		if idm == nil {
			continue
		}
		siteID, err := strconv.Atoi(idm[1])
		if err != nil {
			continue
		}
		title := aniwavesStrip(inner)
		if title == "" {
			continue
		}
		seen[sm[1]] = true
		out = append(out, aniwavesCandidate{
			slug:     sm[1],
			siteID:   siteID,
			title:    title,
			japanese: aniwavesAttr("<a "+attrs+">", "data-jp"),
		})
	}
	return out
}

func (p *AniWavesProvider) searchOne(ctx context.Context, query string) []aniwavesCandidate {
	body, err := p.getText(ctx, p.base+"/filter?keyword="+url.QueryEscape(query), map[string]string{
		"Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Referer": p.base + "/",
	}, 1<<20)
	if err != nil {
		return nil
	}
	return aniwavesParseCards(body)
}

type aniwavesDetail struct {
	aniwavesCandidate
	typ      string
	year     int
	avail    int
	total    int
	resolved string
}

var aniwavesYearRe = regexp.MustCompile(`\d{4}`)

func aniwavesDetailField(htmlBody, label string) string {
	re := regexp.MustCompile(`(?is)<div>\s*` + regexp.QuoteMeta(label) + `:\s*<span[^>]*>(.*?)</span>`)
	if m := re.FindStringSubmatch(htmlBody); m != nil {
		return aniwavesStrip(m[1])
	}
	return ""
}

func aniwavesParseCounts(v string) (avail, total int) {
	var nums []int
	for _, m := range regexp.MustCompile(`\d+`).FindAllString(v, 3) {
		if n, err := strconv.Atoi(m); err == nil {
			nums = append(nums, n)
		}
	}
	if len(nums) == 0 {
		return 0, 0
	}
	if len(nums) == 1 {
		return nums[0], nums[0]
	}
	return nums[0], nums[1]
}

func (p *AniWavesProvider) fetchDetail(ctx context.Context, c aniwavesCandidate) *aniwavesDetail {
	body, err := p.getText(ctx, p.base+"/watch/"+c.slug, map[string]string{
		"Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Referer": p.base + "/",
	}, 1<<20)
	if err != nil {
		return nil
	}
	d := &aniwavesDetail{aniwavesCandidate: c}
	d.typ = aniwavesFormatName(aniwavesDetailField(body, "Type"))
	aired := aniwavesDetailField(body, "Date aired")
	if aired == "" {
		aired = aniwavesDetailField(body, "Premiered")
	}
	if m := aniwavesYearRe.FindString(aired); m != "" {
		d.year, _ = strconv.Atoi(m)
	}
	d.avail, d.total = aniwavesParseCounts(aniwavesDetailField(body, "Episodes"))
	title := c.title
	if m := regexp.MustCompile(`(?is)<h1[^>]*>(.*?)</h1>`).FindStringSubmatch(body); m != nil {
		if t := aniwavesStrip(m[1]); t != "" {
			title = t
		}
	}
	d.resolved = title
	return d
}

func aniwavesTitleScore(titles []string, c aniwavesCandidate) float64 {
	vals := []string{c.title, c.japanese, strings.ReplaceAll(c.slug, "-", " ")}
	best := 0.0
	for _, t := range titles {
		for _, v := range vals {
			if v == "" {
				continue
			}
			if s := aniwavesDice(t, v); s > best {
				best = s
			}
		}
	}
	return best
}

func aniwavesCoverage(d *aniwavesDetail, expected int, status string) float64 {
	if expected < 1 {
		return 0.5
	}
	if d.avail < 1 {
		return 0.0
	}
	if expected < 6 {
		return 1.0
	}
	needed := expected - 3
	if needed < 1 {
		needed = 1
	}
	if status == "FINISHED" {
		needed = int(math.Ceil(float64(expected) * 0.8))
	}
	if needed < 1 {
		needed = 1
	}
	if got := float64(d.avail) / float64(needed); got < 1.0 {
		return got
	}
	return 1.0
}

func aniwavesValidate(d *aniwavesDetail, m *aniwavesMedia, titles []string, expected int) (float64, bool) {
	tscore := aniwavesTitleScore(titles, d.aniwavesCandidate)
	if tscore < 0.68 {
		return 0, false
	}
	expType := aniwavesFormatName(m.Format)
	expYear := m.StartDate.Year
	if expYear == 0 {
		expYear = m.SeasonYear
	}
	if expType != "" && d.typ != "" && expType != d.typ {
		return 0, false
	}
	if expYear != 0 && d.year != 0 && expYear != d.year {
		return 0, false
	}
	cov := aniwavesCoverage(d, expected, m.Status)
	if expected >= 6 && cov < 0.8 {
		return 0, false
	}
	typeBonus, yearBonus := 0.07, 0.04
	if expType != "" && d.typ == expType {
		typeBonus = 0.14
	}
	if expYear != 0 && d.year == expYear {
		yearBonus = 0.10
	}
	return tscore*0.72 + typeBonus + yearBonus + cov*0.04, true
}

type aniwavesShow struct {
	slug   string
	siteID int
	title  string
}

func (p *AniWavesProvider) resolveShow(ctx context.Context, m *aniwavesMedia, id int) (*aniwavesShow, error) {
	titles := aniwavesTitles(m)
	expected := aniwavesExpected(m)
	discovered := map[string]aniwavesCandidate{}
	var dmu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 6)
	for _, q := range aniwavesSearchQueries(titles) {
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
			cands := p.searchOne(ctx, query)
			if len(cands) == 0 {
				return
			}
			dmu.Lock()
			for _, c := range cands {
				if _, ok := discovered[c.slug]; !ok {
					discovered[c.slug] = c
				}
			}
			dmu.Unlock()
		}(q)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	type short struct {
		c aniwavesCandidate
		s float64
	}
	var shortlist []short
	for _, c := range discovered {
		if s := aniwavesTitleScore(titles, c); s >= 0.5 {
			shortlist = append(shortlist, short{c, s})
		}
	}
	sort.Slice(shortlist, func(i, j int) bool { return shortlist[i].s > shortlist[j].s })
	if len(shortlist) > 12 {
		shortlist = shortlist[:12]
	}
	type valid struct {
		d *aniwavesDetail
		s float64
	}
	ch := make(chan valid, len(shortlist))
	var dWg sync.WaitGroup
	dSem := make(chan struct{}, 4)
	for _, s := range shortlist {
		if ctx.Err() != nil {
			break
		}
		dWg.Add(1)
		go func(c aniwavesCandidate) {
			defer dWg.Done()
			select {
			case dSem <- struct{}{}:
				defer func() { <-dSem }()
			case <-ctx.Done():
				return
			}
			d := p.fetchDetail(ctx, c)
			if d == nil {
				return
			}
			if s, ok := aniwavesValidate(d, m, titles, expected); ok {
				ch <- valid{d, s}
			}
		}(s.c)
	}
	go func() {
		dWg.Wait()
		close(ch)
	}()
	var validated []valid
	for v := range ch {
		validated = append(validated, v)
	}
	sort.Slice(validated, func(i, j int) bool { return validated[i].s > validated[j].s })
	if len(validated) == 0 {
		return nil, fmt.Errorf("aniwaves: no show match for anilist %d", id)
	}
	best := validated[0]
	runner := -1.0
	if len(validated) > 1 {
		runner = validated[1].s
	}
	if best.s < 0.82 || (runner >= 0 && best.s-runner < 0.08) {
		return nil, fmt.Errorf("aniwaves: no show match for anilist %d", id)
	}
	title := best.d.resolved
	if title == "" {
		title = best.d.title
	}
	return &aniwavesShow{slug: best.d.slug, siteID: best.d.siteID, title: title}, nil
}

// ------------------------- episodes -------------------------

type aniwavesEpisode struct {
	number    int
	sourceNum string
	title     string
	hasSub    bool
	hasDub    bool
}

func aniwavesParseEpisodes(htmlBody string) []aniwavesEpisode {
	var out []aniwavesEpisode
	seen := map[int]bool{}
	for _, m := range aniwavesAnchorRe.FindAllStringSubmatch(htmlBody, 5000) {
		attrs, inner := m[1], m[2]
		num, err := strconv.Atoi(aniwavesAttr("<a "+attrs+">", "data-num"))
		if err != nil || num < 1 || seen[num] {
			continue
		}
		if aniwavesAttr("<a "+attrs+">", "data-ids") == "" {
			continue
		}
		seen[num] = true
		src := aniwavesAttr("<a "+attrs+">", "data-slug")
		if src == "" {
			src = strconv.Itoa(num)
		}
		title := regexp.MustCompile(`^\d+\s*`).ReplaceAllString(aniwavesStrip(inner), "")
		if title == "" {
			title = fmt.Sprintf("Episode %d", num)
		}
		out = append(out, aniwavesEpisode{
			number:    num,
			sourceNum: src,
			title:     title,
			hasSub:    aniwavesAttr("<a "+attrs+">", "data-sub") == "1",
			hasDub:    aniwavesAttr("<a "+attrs+">", "data-dub") == "1",
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].number < out[j].number })
	return out
}

func (p *AniWavesProvider) fetchEpisodes(ctx context.Context, show *aniwavesShow) ([]aniwavesEpisode, error) {
	raw, err := p.getAjax(ctx,
		fmt.Sprintf("/ajax/episode/list/%d?vrf=", show.siteID),
		p.base+"/watch/"+show.slug)
	if err != nil {
		return nil, err
	}
	var htmlBody string
	if err := json.Unmarshal(raw, &htmlBody); err != nil {
		return nil, fmt.Errorf("aniwaves: episode list invalid JSON")
	}
	eps := aniwavesParseEpisodes(htmlBody)
	if len(eps) == 0 {
		return nil, fmt.Errorf("aniwaves: no episodes for %s", show.slug)
	}
	return eps, nil
}

var aniwavesOrdinalRe = regexp.MustCompile(`(?i)\b(?:part|special|chapter)\s*(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\b`)

func aniwavesOrdinal(v string) int {
	m := aniwavesOrdinalRe.FindStringSubmatch(v)
	if m == nil {
		return 0
	}
	words := map[string]int{"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}
	w := strings.ToLower(m[1])
	if n, err := strconv.Atoi(w); err == nil {
		return n
	}
	return words[w]
}

// alignEpisodes renumbers split-cours listings (source eps > expected) to
// the requested part, same rule as the reference scraper.
func aniwavesAlign(episodes []aniwavesEpisode, titles []string, expected int) []aniwavesEpisode {
	if expected <= 0 || len(episodes) <= expected {
		return episodes
	}
	target := 0
	for _, t := range titles {
		if o := aniwavesOrdinal(t); o > target {
			target = o
		}
	}
	if target < 2 {
		return episodes
	}
	start := -1
	for i, ep := range episodes {
		if aniwavesOrdinal(ep.title) == target {
			start = i
			break
		}
	}
	if start < 0 || len(episodes)-start < expected {
		return episodes
	}
	renamed := make([]aniwavesEpisode, 0, expected)
	for i, ep := range episodes[start : start+expected] {
		ep.number = i + 1
		renamed = append(renamed, ep)
	}
	return renamed
}

// ------------------------- servers / sources -------------------------

type aniwavesServer struct {
	audio  string
	linkID string
	name   string
}

var (
	aniwavesDivRe = regexp.MustCompile(`(?i)<div\b[^>]*>`)
	aniwavesLiRe  = regexp.MustCompile(`(?is)<li\b([^>]*)>(.*?)</li>`)
)

func aniwavesParseServerGroups(htmlBody string) []aniwavesServer {
	type marker struct {
		index int
		audio string
	}
	var markers []marker
	for _, idx := range aniwavesDivRe.FindAllStringIndex(htmlBody, -1) {
		tag := htmlBody[idx[0]:idx[1]]
		t := aniwavesAttr(tag, "data-type")
		if t == "sub" || t == "dub" {
			markers = append(markers, marker{idx[0], t})
		}
	}
	var out []aniwavesServer
	for i, cur := range markers {
		end := len(htmlBody)
		if i+1 < len(markers) {
			end = markers[i+1].index
		}
		for _, m := range aniwavesLiRe.FindAllStringSubmatch(htmlBody[cur.index:end], 100) {
			attrs, inner := m[1], m[2]
			linkID := aniwavesAttr("<li "+attrs+">", "data-link-id")
			if linkID == "" {
				continue
			}
			name := aniwavesStrip(inner)
			if name == "" {
				name = "AniWaves"
			}
			out = append(out, aniwavesServer{audio: cur.audio, linkID: linkID, name: name})
		}
	}
	return out
}

func (p *AniWavesProvider) fetchServers(ctx context.Context, show *aniwavesShow, ep aniwavesEpisode) ([]aniwavesServer, error) {
	raw, err := p.getAjax(ctx,
		"/ajax/server/list?servers="+url.QueryEscape(strconv.Itoa(show.siteID))+"&eps="+url.QueryEscape(ep.sourceNum),
		p.base+"/watch/"+show.slug+"/ep-"+ep.sourceNum)
	if err != nil {
		return nil, err
	}
	var htmlBody string
	if err := json.Unmarshal(raw, &htmlBody); err != nil {
		return nil, fmt.Errorf("aniwaves: server list invalid JSON")
	}
	return aniwavesParseServerGroups(htmlBody), nil
}

type aniwavesSourcePayload struct {
	URL      string           `json:"url"`
	SkipData map[string][]int `json:"skip_data"`
}

func (p *AniWavesProvider) fetchSource(ctx context.Context, linkID, referer string) (*aniwavesSourcePayload, error) {
	raw, err := p.getAjax(ctx,
		"/ajax/sources?id="+url.QueryEscape(linkID)+"&asi=0&autoPlay=0", referer)
	if err != nil {
		return nil, err
	}
	var payload aniwavesSourcePayload
	if err := json.Unmarshal(raw, &payload); err != nil || strings.TrimSpace(payload.URL) == "" {
		return nil, fmt.Errorf("aniwaves: source has no embed url")
	}
	return &payload, nil
}

var aniwavesEchoRe = regexp.MustCompile(`(?i)^/(embed-[01]|embed-20)/([^/]+)`)

type aniwavesDirect struct {
	url     string
	typ     string // "hls" or "mp4"
	quality string
}

func (p *AniWavesProvider) extractEchovideo(ctx context.Context, embedURL string) []aniwavesDirect {
	parts, err := url.Parse(embedURL)
	if err != nil {
		return nil
	}
	m := aniwavesEchoRe.FindStringSubmatch(parts.Path)
	if m == nil {
		return nil
	}
	etype := strings.ToLower(m[1])
	endpoint := parts.Scheme + "://" + parts.Host + "/" + m[1] + "/getSources?id=" + url.QueryEscape(m[2])
	body, err := p.getText(ctx, endpoint, map[string]string{
		"User-Agent":       browserUA,
		"Referer":          embedURL,
		"X-Requested-With": "XMLHttpRequest",
	}, 1<<20)
	if err != nil {
		return nil
	}
	var data struct {
		Sources json.RawMessage `json:"sources"`
	}
	if err := json.Unmarshal([]byte(body), &data); err != nil || len(data.Sources) == 0 {
		return nil
	}
	var out []aniwavesDirect
	if etype == "embed-20" {
		// mp4 per quality: {"1080": url|[urls], ...}
		var dict map[string]json.RawMessage
		if err := json.Unmarshal(data.Sources, &dict); err != nil {
			return nil
		}
		for quality, raw := range dict {
			var one string
			var many []string
			if json.Unmarshal(raw, &one) == nil {
				if strings.TrimSpace(one) != "" {
					out = append(out, aniwavesDirect{url: one, typ: "mp4", quality: quality})
				}
				continue
			}
			if json.Unmarshal(raw, &many) == nil {
				for _, u := range many {
					if strings.TrimSpace(u) != "" {
						out = append(out, aniwavesDirect{url: u, typ: "mp4", quality: quality})
					}
				}
			}
		}
		return out
	}
	appendItem := func(item json.RawMessage) {
		var s string
		if json.Unmarshal(item, &s) == nil {
			if strings.TrimSpace(s) != "" {
				out = append(out, aniwavesDirect{url: s, typ: "hls"})
			}
			return
		}
		var obj struct {
			File string `json:"file"`
			URL  string `json:"url"`
		}
		if json.Unmarshal(item, &obj) == nil {
			if u := strings.TrimSpace(obj.File); u != "" {
				out = append(out, aniwavesDirect{url: u, typ: "hls"})
			} else if u := strings.TrimSpace(obj.URL); u != "" {
				out = append(out, aniwavesDirect{url: u, typ: "hls"})
			}
		}
	}
	var list []json.RawMessage
	if json.Unmarshal(data.Sources, &list) == nil {
		for _, item := range list {
			appendItem(item)
		}
		return out
	}
	appendItem(data.Sources)
	return out
}

var (
	// Path-only on purpose (the reference scraper also pins the host, but a
	// non-echovideo /embed-N/ URL simply 404s getSources and skips — same
	// outcome, and path matching keeps the flow testable off-host).
	aniwavesEchoURLRe = regexp.MustCompile(`(?i)/embed-(?:[01]|20)/([^/]+)`)
	aniwavesM3U8Re    = regexp.MustCompile(`(?i)\.m3u8(\?|$)`)
	aniwavesMP4Re     = regexp.MustCompile(`(?i)\.mp4(\?|$)`)
)

// probeAniWavesMaster verifies a master at segment depth: master -> a
// variant media playlist -> first segment bytes must all serve media. A
// master whose playlists serve but whose segments are egress-blocked or
// deleted would otherwise list a server that can only spin at playback
// (observed class: upstream file removals after listing). Variants are
// tried highest-bandwidth first — the player's default path — so a dead
// top rendition falls through to a live one instead of killing the mirror.
func (p *AniWavesProvider) probeAniWavesMaster(ctx context.Context, master, referer string) bool {
	mhead, ok := fetchURLCapped(ctx, p.client, master, referer, browserUA, 65536, 6*time.Second)
	if !ok || !strings.Contains(string(mhead), "#EXTM3U") {
		return false
	}
	VODCacheSet(master, mhead)
	variants := kaaParseVariants(string(mhead), master)
	if len(variants) == 0 {
		// Single media playlist (no STREAM-INF): probe its segments direct.
		seg := firstPlaylistURL(string(mhead), master)
		if seg == "" {
			return false
		}
		shead, ok := fetchURLCapped(ctx, p.client, seg, referer, browserUA, 8192, 8*time.Second)
		return ok && !strings.Contains(strings.ToLower(string(shead)), "<html")
	}
	for _, v := range variants {
		if ctx.Err() != nil {
			return false
		}
		mbody, ok := fetchURLCapped(ctx, p.client, v.url, referer, browserUA, 262144, 6*time.Second)
		if !ok || !strings.Contains(string(mbody), "#EXTM3U") {
			continue
		}
		VODCacheSet(v.url, mbody)
		seg := firstPlaylistURL(string(mbody), v.url)
		if seg == "" {
			continue
		}
		shead, ok := fetchURLCapped(ctx, p.client, seg, referer, browserUA, 8192, 8*time.Second)
		if !ok {
			continue
		}
		if strings.Contains(strings.ToLower(string(shead)), "<html") {
			continue
		}
		return true
	}
	return false
}

func (p *AniWavesProvider) resolveDirect(ctx context.Context, embedURL string) []aniwavesDirect {
	if aniwavesEchoURLRe.MatchString(embedURL) {
		return p.extractEchovideo(ctx, embedURL)
	}
	if aniwavesM3U8Re.MatchString(embedURL) {
		return []aniwavesDirect{{url: embedURL, typ: "hls"}}
	}
	if aniwavesMP4Re.MatchString(embedURL) {
		return []aniwavesDirect{{url: embedURL, typ: "mp4"}}
	}
	return nil
}

// aniwavesSavedlyRank orders savedly mp4 quality keys (numeric "1080"
// first, then HD/HQ/SD) so the mirror ships its single best file.
func aniwavesSavedlyRank(quality string) int {
	s := strings.ToLower(strings.TrimSpace(quality))
	if m := regexp.MustCompile(`(\d{3,4})`).FindStringSubmatch(s); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil {
			return 10000 + n
		}
	}
	switch {
	case strings.Contains(s, "hd") || strings.Contains(s, "hq") || strings.Contains(s, "1080"):
		return 720
	case strings.Contains(s, "720"):
		return 720
	case strings.Contains(s, "480"):
		return 480
	case strings.Contains(s, "sd") || strings.Contains(s, "360"):
		return 360
	}
	return 0
}

func aniwavesSkipRange(v []int) *core.SkipTimestamp {
	if len(v) < 2 || v[1] <= v[0] {
		return nil
	}
	return &core.SkipTimestamp{Start: float64(v[0]), End: float64(v[1])}
}

// ------------------------- resolve -------------------------

func (p *AniWavesProvider) loadResolved(key aniwavesResolveKey) *SourceResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[key]
	if !ok || time.Since(e.fetched) > aniwavesResolveTTL {
		return nil
	}
	return cloneSourceResult(e.result)
}

func (p *AniWavesProvider) storeResolved(key aniwavesResolveKey, sr *SourceResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, e := range p.resolved {
		if time.Since(e.fetched) > aniwavesResolveTTL {
			delete(p.resolved, k)
		}
	}
	if len(p.resolved) >= maxAniWavesEntries {
		for k := range p.resolved {
			delete(p.resolved, k)
			break
		}
	}
	p.resolved[key] = &aniwavesResolvedEntry{result: cloneSourceResult(sr), fetched: time.Now()}
}

func (p *AniWavesProvider) loadShow(id int) (*aniwavesShow, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.shows[id]; ok && time.Since(e.fetched) < aniwavesShowTTL {
		return &aniwavesShow{slug: e.slug, siteID: e.siteID, title: e.title}, true
	}
	return nil, false
}

func (p *AniWavesProvider) storeShow(id int, s *aniwavesShow) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, e := range p.shows {
		if time.Since(e.fetched) > aniwavesShowTTL {
			delete(p.shows, k)
		}
	}
	if len(p.shows) >= maxAniWavesEntries {
		for k := range p.shows {
			delete(p.shows, k)
			break
		}
	}
	p.shows[id] = &aniwavesShowEntry{slug: s.slug, siteID: s.siteID, title: s.title, fetched: time.Now()}
}

// FindEpisodeSource resolves one episode for exactly the requested lang
// (strict per-lang: no dub server when the listing carries no dub).
func (p *AniWavesProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("aniwaves: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	key := aniwavesResolveKey{anilistID: id, episode: episode, lang: langKey}
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

func (p *AniWavesProvider) resolveEpisode(ctx context.Context, id, episode int, lang string) (*SourceResult, error) {
	media, err := p.anilistMedia(ctx, id)
	if err != nil {
		return nil, err
	}
	var show *aniwavesShow
	if s, ok := p.loadShow(id); ok {
		show = s
	} else {
		show, err = p.resolveShow(ctx, media, id)
		if err != nil {
			return nil, err
		}
		p.storeShow(id, show)
	}
	episodes, err := p.fetchEpisodes(ctx, show)
	if err != nil {
		return nil, err
	}
	episodes = aniwavesAlign(episodes, aniwavesTitles(media), aniwavesExpected(media))
	var ep *aniwavesEpisode
	for i := range episodes {
		if episodes[i].number == episode {
			ep = &episodes[i]
			break
		}
	}
	if ep == nil {
		return nil, fmt.Errorf("aniwaves: episode %d not listed", episode)
	}
	if lang == "sub" && !ep.hasSub || lang == "dub" && !ep.hasDub {
		return nil, nil
	}
	servers, err := p.fetchServers(ctx, show, *ep)
	if err != nil {
		return nil, err
	}
	referer := p.base + "/watch/" + show.slug + "/ep-" + ep.sourceNum
	var wanted []aniwavesServer
	for _, s := range servers {
		if s.audio == lang {
			wanted = append(wanted, s)
		}
	}
	if len(wanted) == 0 {
		return nil, nil
	}
	sr := &SourceResult{Headers: map[string]string{"Referer": aniwavesPlaybackReferer}}
	var intro, outro *core.SkipTimestamp
	for i, srv := range wanted {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		payload, err := p.fetchSource(ctx, srv.linkID, referer)
		if err != nil {
			continue
		}
		if intro == nil {
			intro = aniwavesSkipRange(payload.SkipData["intro"])
		}
		if outro == nil {
			outro = aniwavesSkipRange(payload.SkipData["outro"])
		}
		directs := p.resolveDirect(ctx, payload.URL)
		if len(directs) == 0 {
			// Embed-only mirror (BYFMS/DGHG style): never shipped.
			continue
		}
		// HLS-first within a mirror: a multi-rendition master beats any
		// single mp4 file from the same extractor payload; otherwise the
		// highest-ranked mp4 wins.
		best := directs[0]
		for _, d := range directs[1:] {
			if d.typ == "hls" && best.typ != "hls" {
				best = d
			} else if d.typ == best.typ && best.typ == "mp4" &&
				aniwavesSavedlyRank(d.quality) > aniwavesSavedlyRank(best.quality) {
				best = d
			}
		}
		var ok bool
		if best.typ == "hls" {
			ok = p.probeAniWavesMaster(ctx, best.url, aniwavesPlaybackReferer)
		} else {
			ok = probeMediaFileLenient(ctx, p.client, best.url, aniwavesPlaybackReferer, browserUA)
		}
		if !ok {
			p.log.Info().Str("anilistId", strconv.Itoa(id)).Int("episode", episode).
				Str("mirror", srv.name).Msg("aniwaves: probe failed, trying next mirror")
			continue
		}
		p.learnURLHost(best.url)
		quality := "auto"
		if best.typ == "mp4" {
			quality = strings.TrimSpace(best.quality)
			if quality == "" {
				quality = "auto"
			}
		}
		sr.Sources = append(sr.Sources, core.Source{
			URL:          best.url,
			Type:         best.typ,
			Quality:      quality,
			Verification: "proxy",
		})
		sr.ServerNames = append(sr.ServerNames, aniwavesCuteName(srv.name, i))
	}
	if len(sr.Sources) == 0 {
		return nil, fmt.Errorf("aniwaves: no playable mirror for episode %d", episode)
	}
	sr.Intro, sr.Outro = intro, outro
	if len(sr.ServerNames) > 0 {
		sr.ServerName = sr.ServerNames[0]
	}
	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", lang).Int("mirrors", len(sr.Sources)).Msg("aniwaves resolved")
	return sr, nil
}
