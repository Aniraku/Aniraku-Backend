package tmdb

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TMDBAPIBase           = "https://api.themoviedb.org/3"
	TMDBImageBase         = "https://image.tmdb.org/t/p/w780"
	AnibridgeMappingsAPI  = "https://mappings.anibridge.eliasbenb.dev/api/v3/mappings"
	MaxEpisodeNumbers     = 2000
	MappingResponseLimit  = 100
	RequestTimeout        = 30 * time.Second
	MappingTTL            = 24 * time.Hour
	EpisodeTTL            = 5 * time.Minute
)

var (
	responseCache sync.Map // key -> cacheEntry
	inFlight      sync.Map // key -> chan struct{} + result
	cacheMu       sync.Mutex
)

type cacheEntry struct {
	value     any
	expiresAt time.Time
}

// --- Episode metadata cache (per-anime, survives across requests) ---

var episodeCache sync.Map // anilistID -> *episodeCacheEntry

type episodeCacheEntry struct {
	mu        sync.RWMutex
	episodes  map[int]*EpisodeMetadata
	fetchedAt time.Time
}

const episodeCacheTTL = 30 * time.Minute

func GetCachedEpisodes(anilistID int, nums []int) map[int]*EpisodeMetadata {
	val, ok := episodeCache.Load(anilistID)
	if !ok {
		return nil
	}
	entry := val.(*episodeCacheEntry)
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	if time.Since(entry.fetchedAt) > episodeCacheTTL {
		return nil
	}
	// Check if we have data for all requested numbers.
	result := make(map[int]*EpisodeMetadata, len(nums))
	for _, n := range nums {
		if ep, ok := entry.episodes[n]; ok {
			result[n] = ep
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func setCachedEpisodes(anilistID int, data map[int]*EpisodeMetadata) {
	val, ok := episodeCache.Load(anilistID)
	var entry *episodeCacheEntry
	if !ok {
		entry = &episodeCacheEntry{episodes: data, fetchedAt: time.Now()}
		episodeCache.Store(anilistID, entry)
		return
	}
	entry = val.(*episodeCacheEntry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.episodes == nil {
		entry.episodes = data
	} else {
		for k, v := range data {
			entry.episodes[k] = v
		}
	}
	entry.fetchedAt = time.Now()
}

// EnrichInBackground fetches TMDB episode metadata and caches it.
// Called as a fire-and-forget goroutine from the handler.
func EnrichInBackground(anilistID int, nums []int, client *http.Client, token string) {
	if client == nil {
		client = &http.Client{Timeout: RequestTimeout}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	result, err := ResolveEpisodes(ctx, client, token, anilistID, nums)
	if err != nil || result == nil {
		return
	}
	data := make(map[int]*EpisodeMetadata, len(result.Episodes))
	for _, ep := range result.Episodes {
		data[ep.Number] = ep
	}
	setCachedEpisodes(anilistID, data)
}

// CacheEpisodes stores resolved TMDB episode metadata in the persistent cache.
func CacheEpisodes(anilistID int, data map[int]*EpisodeMetadata) {
	setCachedEpisodes(anilistID, data)
}

type ResolverError struct {
	Code       string
	Message    string
	Status     int
	RetryAfter *int
}

func (e *ResolverError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func newResolverError(code, msg string, status int) *ResolverError {
	return &ResolverError{Code: code, Message: msg, Status: status}
}

func positiveInteger(v any) *int {
	switch x := v.(type) {
	case int:
		if x > 0 {
			return &x
		}
	case int64:
		if x > 0 {
			i := int(x)
			return &i
		}
	case float64:
		if x > 0 && x == float64(int(x)) {
			i := int(x)
			return &i
		}
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		if err == nil && n > 0 {
			return &n
		}
	}
	return nil
}

func text(v any) string {
	if s, ok := v.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func cacheKey(prefix string, value any) string {
	b, _ := json.Marshal(value)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%s:%x", prefix, h[:8])
}

func cached(key string, ttl time.Duration, loader func() (any, error)) (any, error) {
	now := time.Now()
	if stored, ok := responseCache.Load(key); ok {
		if e, ok := stored.(cacheEntry); ok && e.expiresAt.After(now) {
			return e.value, nil
		}
		responseCache.Delete(key)
	}
	// in-flight dedup
	ch := make(chan struct{})
	actual, loaded := inFlight.LoadOrStore(key, ch)
	if loaded {
		// wait for other
		if c, ok := actual.(chan struct{}); ok {
			<-c
			if stored, ok := responseCache.Load(key); ok {
				if e, ok := stored.(cacheEntry); ok && e.expiresAt.After(time.Now()) {
					return e.value, nil
				}
			}
			return nil, fmt.Errorf("in-flight failed")
		}
	}
	defer func() {
		inFlight.Delete(key)
		close(ch)
	}()
	val, err := loader()
	if err != nil {
		return nil, err
	}
	responseCache.Store(key, cacheEntry{value: val, expiresAt: time.Now().Add(ttl)})
	return val, nil
}

// --- AniZip episode metadata ---

const AniZipBase = "https://api.ani.zip"

type AniZipEpisode struct {
	Number    int               `json:"number"`
	Title     map[string]string `json:"title"`
	Thumbnail string            `json:"image"`
	Airdate   string            `json:"airdate"`
}

func (a AniZipEpisode) BestTitle() string {
	if t, ok := a.Title["en"]; ok && t != "" {
		return t
	}
	if t, ok := a.Title["x-jat"]; ok && t != "" {
		return t
	}
	if t, ok := a.Title["ja"]; ok && t != "" {
		return t
	}
	// fallback to any first available
	for _, v := range a.Title {
		if v != "" {
			return v
		}
	}
	return ""
}

type AniZipResponse struct {
	Episodes map[string]AniZipEpisode `json:"episodes"`
}

func FetchAniZipEpisodes(ctx context.Context, client *http.Client, anilistID int) (map[string]AniZipEpisode, error) {
	if client == nil {
		client = &http.Client{Timeout: RequestTimeout}
	}
	u := fmt.Sprintf("%s/mappings?anilist_id=%d", AniZipBase, anilistID)
	key := cacheKey("anizip", anilistID)
	val, err := cached(key, EpisodeTTL, func() (any, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("anizip request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("anizip returned %d: %s", resp.StatusCode, string(body))
		}
		var data AniZipResponse
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			return nil, fmt.Errorf("failed to decode anizip: %w", err)
		}
		return data.Episodes, nil
	})
	if err != nil {
		return nil, err
	}
	episodes, _ := val.(map[string]AniZipEpisode)
	return episodes, nil
}

// ResolveEpisodesWithAnizip resolves episode metadata using AniZip + TMDB bidirectional merge.
func ResolveEpisodesWithAnizip(ctx context.Context, client *http.Client, token string, anilistID int, episodeNumbers []int) (*ResolveResult, error) {
	// Fetch TMDB metadata
	tmdbResult, tmdbErr := ResolveEpisodes(ctx, client, token, anilistID, episodeNumbers)
	// Fetch AniZip metadata
	anizipData, anizipErr := FetchAniZipEpisodes(ctx, client, anilistID)

	if tmdbErr != nil && anizipErr != nil {
		return nil, fmt.Errorf("both TMDB and AniZip failed: tmdb=%v anizip=%v", tmdbErr, anizipErr)
	}

	// Build TMDB lookup by episode number
	tmdbByNumber := map[int]*EpisodeMetadata{}
	if tmdbResult != nil {
		for _, ep := range tmdbResult.Episodes {
			tmdbByNumber[ep.Number] = ep
		}
	}

	// Merge using MergeEpisode
	merged := make([]*EpisodeMetadata, 0, len(episodeNumbers))
	for _, num := range episodeNumbers {
		anizipEp, hasAnizip := anizipData[fmt.Sprintf("%d", num)]
		var aniTitle, aniThumb string
		if hasAnizip {
			aniTitle = anizipEp.BestTitle()
			aniThumb = anizipEp.Thumbnail
		}
		tmdbMeta := tmdbByNumber[num]
		finalTitle, finalThumb := MergeEpisode(anilistID, num, aniTitle, aniThumb, tmdbMeta)

		meta := &EpisodeMetadata{
			Number: num,
			Title:  finalTitle,
		}
		if finalThumb != "" {
			meta.Thumbnail = &finalThumb
		}
		// Carry description/airdate from TMDB if available
		if tmdbMeta != nil {
			if tmdbMeta.Description != nil {
				meta.Description = tmdbMeta.Description
			}
			if tmdbMeta.Airdate != nil {
				meta.Airdate = tmdbMeta.Airdate
			}
		} else if hasAnizip && anizipEp.Airdate != "" {
			meta.Airdate = &anizipEp.Airdate
		}
		merged = append(merged, meta)
	}

	source := "anizip+tmdb"
	if tmdbErr != nil {
		source = "anizip"
	} else if anizipErr != nil {
		source = "tmdb"
	}
	return &ResolveResult{
		AnilistID:    anilistID,
		Source:       source,
		CacheSeconds: int(EpisodeTTL.Seconds()),
		Episodes:     merged,
	}, nil
}

// --- Range parsing (port of JS parseRange / mapEpisodeNumber) ---

type rng struct {
	start int
	end   *int // nil = open ended
}

func parseRange(s string) *rng {
	s = strings.TrimSpace(s)
	m := regexp.MustCompile(`^(\d+)(?:-(\d*)?)?$`).FindStringSubmatch(s)
	if m == nil {
		return nil
	}
	start, _ := strconv.Atoi(m[1])
	if start <= 0 {
		return nil
	}
	if m[2] == "" && !strings.Contains(s, "-") {
		// single number without dash? like "5" → 5-5? In JS "1" would be "1" alone? Actually parseRange("1") => start=1 end=nil? But JS expects "1-". For safety handle both.
		// The JS regex: ^(\d+)(?:-(\d*)?)?$ → "1" => start=1 end=nil (since second group undefined but dash not present? Actually group2 undefined, but we treat as nil)
		// But then map logic would treat as open ended, which is correct for "1" ?

		// However for "1" alone we want 1-1? In AniBridge mappings, single mapping like "1": "1" means entry "1" key → parseRange("1") should be 1-1? Let's see JS: text("1").match(/^(\d+)(?:-(\d*)?)?$/) → match[1]="1", match[2]=undefined → period. Then they do end = match[2] === undefined ? null : ... So "1" => end null. Then hasOpenEnded would think open ended. Might be intentional - but we will handle single as start..start if no dash?
		// To avoid break, if original string doesn't contain "-", treat end = start
		if !strings.Contains(s, "-") {
			e := start
			return &rng{start: start, end: &e}
		}
		return &rng{start: start, end: nil}
	}
	if m[2] == "" {
		// "1-" open ended
		return &rng{start: start, end: nil}
	}
	end, _ := strconv.Atoi(m[2])
	if end < start {
		return nil
	}
	return &rng{start: start, end: &end}
}

func mapEpisodeNumber(rangeMap map[string]string, anilistEpisode int) *int {
	if anilistEpisode <= 0 || rangeMap == nil {
		return nil
	}
	for srcRangeVal, targetRangeVal := range rangeMap {
		src := parseRange(srcRangeVal)
		if src == nil || anilistEpisode < src.start || (src.end != nil && anilistEpisode > *src.end) {
			continue
		}
		parts := strings.SplitN(strings.TrimSpace(targetRangeVal), "|", 2)
		if len(parts) == 2 {
			if r, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil && r != 1 {
				return nil
			}
		}
		targetRangesStr := strings.TrimSpace(parts[0])
		offset := anilistEpisode - src.start
		for _, trStr := range strings.Split(targetRangesStr, ",") {
			tr := parseRange(strings.TrimSpace(trStr))
			if tr == nil {
				continue
			}
			var length int
			if tr.end == nil {
				length = 1 << 30 // infinity
			} else {
				length = *tr.end - tr.start + 1
			}
			if offset < length {
				v := tr.start + offset
				return &v
			}
			offset -= length
		}
		return nil
	}
	return nil
}

// --- Mapping extraction ---

type tmdbMapping struct {
	Type         string            `json:"type"` // tv or movie
	ShowID       int               `json:"showId,omitempty"`
	SeasonNumber int               `json:"seasonNumber,omitempty"`
	MovieID      int               `json:"movieId,omitempty"`
	Ranges       map[string]string `json:"ranges"`
	TmdbNumber   *int              `json:"-"`
}

func extractMappingSource(payload map[string]any, anilistID int) (map[string]any, error) {
	data, _ := payload["data"].(map[string]any)
	if data == nil {
		return nil, newResolverError("TMDB_MAPPING_NOT_FOUND", "No verified AniList-to-TMDB mapping exists for this anime.", 404)
	}
	key := fmt.Sprintf("anilist:%d", anilistID)
	src, ok := data[key].(map[string]any)
	if !ok || src == nil {
		return nil, newResolverError("TMDB_MAPPING_NOT_FOUND", "No verified AniList-to-TMDB mapping exists for this anime.", 404)
	}
	return src, nil
}

func extractTmdbShowMappings(payload map[string]any, anilistId int) ([]tmdbMapping, error) {
	src, err := extractMappingSource(payload, anilistId)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`^tmdb_show:(\d+):s(\d+)$`)
	var out []tmdbMapping
	for descriptor, rangesVal := range src {
		m := re.FindStringSubmatch(descriptor)
		if m == nil {
			continue
		}
		rangesMap := toStringMap(rangesVal)
		if rangesMap == nil {
			continue
		}
		showId, _ := strconv.Atoi(m[1])
		seasonNum, _ := strconv.Atoi(m[2])
		out = append(out, tmdbMapping{Type: "tv", ShowID: showId, SeasonNumber: seasonNum, Ranges: rangesMap})
	}
	return out, nil
}

func extractTmdbMovieMappings(payload map[string]any, anilistId int) ([]tmdbMapping, error) {
	src, err := extractMappingSource(payload, anilistId)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`^tmdb_movie:(\d+)$`)
	var out []tmdbMapping
	for descriptor, rangesVal := range src {
		m := re.FindStringSubmatch(descriptor)
		if m == nil {
			continue
		}
		rangesMap := toStringMap(rangesVal)
		if rangesMap == nil {
			continue
		}
		movieId, _ := strconv.Atoi(m[1])
		out = append(out, tmdbMapping{Type: "movie", MovieID: movieId, Ranges: rangesMap})
	}
	return out, nil
}

func toStringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, vv := range m {
		if s, ok := vv.(string); ok {
			out[k] = s
		} else {
			out[k] = fmt.Sprintf("%v", vv)
		}
	}
	return out
}

func selectTmdbMappingForEpisode(mappings []tmdbMapping, anilistEpisode int) *tmdbMapping {
	var candidates []tmdbMapping
	for _, m := range mappings {
		n := mapEpisodeNumber(m.Ranges, anilistEpisode)
		if n != nil {
			c := m
			c.TmdbNumber = n
			candidates = append(candidates, c)
		}
	}
	if len(candidates) == 1 {
		return &candidates[0]
	}
	return nil
}

func hasOpenEndedSourceRange(rangeMap map[string]string, anilistEpisode int) bool {
	if anilistEpisode <= 0 || rangeMap == nil {
		return false
	}
	for srcRangeVal := range rangeMap {
		src := parseRange(srcRangeVal)
		if src != nil && src.end == nil && anilistEpisode >= src.start {
			return true
		}
	}
	return false
}

func continuationSeasonNumbers(show map[string]any, afterSeasonNumber int) []int {
	if afterSeasonNumber <= 0 {
		return nil
	}
	seasons, _ := show["seasons"].([]any)
	set := map[int]bool{}
	for _, s := range seasons {
		m, _ := s.(map[string]any)
		if m == nil {
			continue
		}
		var n int
		switch v := m["season_number"].(type) {
		case float64:
			n = int(v)
		case int:
			n = v
		}
		if n > afterSeasonNumber {
			set[n] = true
		}
	}
	var out []int
	for k := range set {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

func safeStillUrl(path string) string {
	p := strings.TrimSpace(path)
	if matched, _ := regexp.MatchString(`^\/[A-Za-z0-9_-]+\.(?:jpg|jpeg|png|webp)$`, p); !matched {
		return ""
	}
	return TMDBImageBase + p
}

func isPublishedTitle(v string) bool {
	t := strings.TrimSpace(v)
	if t == "" {
		return false
	}
	if matched, _ := regexp.MatchString(`(?i)^episode\s+\d+$`, t); matched {
		return false
	}
	if matched, _ := regexp.MatchString(`(?i)^(?:tba|tbd|untitled|unknown)$`, t); matched {
		return false
	}
	return true
}

// --- HTTP helpers ---

func requestJson(ctx context.Context, client *http.Client, url string, headers map[string]string, unavailableCode, unavailableMsg string) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, newResolverError(unavailableCode, unavailableMsg, 502)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	ctx2, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	req = req.WithContext(ctx2)
	resp, err := client.Do(req)
	if err != nil {
		if ctx2.Err() == context.DeadlineExceeded {
			return nil, newResolverError(unavailableCode, unavailableMsg+" Request timed out.", 502)
		}
		return nil, newResolverError(unavailableCode, unavailableMsg, 502)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var payload map[string]any
	_ = json.Unmarshal(body, &payload)
	if resp.StatusCode == 429 {
		retry := 0
		if v := resp.Header.Get("Retry-After"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				retry = n
			}
		}
		return nil, &ResolverError{Code: "TMDB_RATE_LIMITED", Message: "TMDB is busy. Please try again shortly.", Status: 429, RetryAfter: &retry}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, newResolverError(unavailableCode, unavailableMsg, 502)
	}
	if payload == nil {
		payload = map[string]any{}
		_ = json.Unmarshal(body, &payload)
		if payload == nil {
			// try generic
			var generic map[string]any
			if err := json.Unmarshal(body, &generic); err == nil {
				payload = generic
			}
		}
	}
	// Ensure we return raw JSON map; if API returned array, wrap?
	return payload, nil
}

func mappingRequestUrl(anilistID int) string {
	return fmt.Sprintf("%s?provider=anilist&id=%d&limit=%d", AnibridgeMappingsAPI, anilistID, MappingResponseLimit)
}

func getMapping(ctx context.Context, client *http.Client, anilistID int) (map[string]any, error) {
	key := cacheKey("anibridge-mapping", anilistID)
	val, err := cached(key, MappingTTL, func() (any, error) {
		return requestJson(ctx, client, mappingRequestUrl(anilistID), map[string]string{"Accept": "application/json"}, "MAPPING_UNAVAILABLE", "The verified episode mapping service is unavailable.")
	})
	if err != nil {
		return nil, err
	}
	if m, ok := val.(map[string]any); ok {
		return m, nil
	}
	return nil, fmt.Errorf("invalid mapping cache type")
}

func getTmdbSeason(ctx context.Context, client *http.Client, token string, showID, seasonNumber int) (map[string]any, error) {
	if strings.TrimSpace(token) == "" {
		return nil, newResolverError("TMDB_NOT_CONFIGURED", "TMDB episode metadata is not configured.", 503)
	}
	u := fmt.Sprintf("%s/tv/%d/season/%d?language=en-US", TMDBAPIBase, showID, seasonNumber)
	key := cacheKey("tmdb-season", map[string]int{"showId": showID, "seasonNumber": seasonNumber})
	val, err := cached(key, EpisodeTTL, func() (any, error) {
		return requestJson(ctx, client, u, map[string]string{"Accept": "application/json", "Authorization": "Bearer " + token}, "TMDB_UNAVAILABLE", "TMDB episode metadata is unavailable.")
	})
	if err != nil {
		return nil, err
	}
	return val.(map[string]any), nil
}

func getTmdbShow(ctx context.Context, client *http.Client, token string, showID int) (map[string]any, error) {
	if strings.TrimSpace(token) == "" {
		return nil, newResolverError("TMDB_NOT_CONFIGURED", "TMDB episode metadata is not configured.", 503)
	}
	u := fmt.Sprintf("%s/tv/%d?language=en-US", TMDBAPIBase, showID)
	key := cacheKey("tmdb-show", showID)
	val, err := cached(key, EpisodeTTL, func() (any, error) {
		return requestJson(ctx, client, u, map[string]string{"Accept": "application/json", "Authorization": "Bearer " + token}, "TMDB_UNAVAILABLE", "TMDB episode metadata is unavailable.")
	})
	if err != nil {
		return nil, err
	}
	return val.(map[string]any), nil
}

func getTmdbMovie(ctx context.Context, client *http.Client, token string, movieID int) (map[string]any, error) {
	if strings.TrimSpace(token) == "" {
		return nil, newResolverError("TMDB_NOT_CONFIGURED", "TMDB episode metadata is not configured.", 503)
	}
	u := fmt.Sprintf("%s/movie/%d?language=en-US", TMDBAPIBase, movieID)
	key := cacheKey("tmdb-movie", movieID)
	val, err := cached(key, EpisodeTTL, func() (any, error) {
		return requestJson(ctx, client, u, map[string]string{"Accept": "application/json", "Authorization": "Bearer " + token}, "TMDB_UNAVAILABLE", "TMDB episode metadata is unavailable.")
	})
	if err != nil {
		return nil, err
	}
	return val.(map[string]any), nil
}

type EpisodeMetadata struct {
	Number      int     `json:"number"`
	Title       string  `json:"title"`
	Thumbnail   *string `json:"thumbnail"`
	Description *string `json:"description"`
	Airdate     *string `json:"airdate"`
}

func toTmdbEpisodeMetadata(entry map[string]any, anilistNumber, tmdbNumber int) *EpisodeMetadata {
	var epNum int
	switch v := entry["episode_number"].(type) {
	case float64:
		epNum = int(v)
	case int:
		epNum = v
	default:
		return nil
	}
	if epNum != tmdbNumber {
		return nil
	}
	name, _ := entry["name"].(string)
	if !isPublishedTitle(name) {
		return nil
	}
	thumb := safeStillUrl(text(entry["still_path"]))
	var thumbPtr *string
	if thumb != "" {
		thumbPtr = &thumb
	}
	var descPtr *string
	if d := strings.TrimSpace(text(entry["overview"])); d != "" {
		descPtr = &d
	}
	var airPtr *string
	if a := strings.TrimSpace(text(entry["air_date"])); a != "" {
		airPtr = &a
	}
	return &EpisodeMetadata{
		Number:      anilistNumber,
		Title:       strings.TrimSpace(name),
		Thumbnail:   thumbPtr,
		Description: descPtr,
		Airdate:     airPtr,
	}
}

func toTmdbMovieMetadata(entry map[string]any, anilistNumber, tmdbNumber int) *EpisodeMetadata {
	if tmdbNumber != 1 {
		return nil
	}
	title, _ := entry["title"].(string)
	if !isPublishedTitle(title) {
		return nil
	}
	var thumb string
	if bp, _ := entry["backdrop_path"].(string); strings.TrimSpace(bp) != "" {
		thumb = safeStillUrl(bp)
	}
	if thumb == "" {
		if pp, _ := entry["poster_path"].(string); strings.TrimSpace(pp) != "" {
			thumb = safeStillUrl(pp)
		}
	}
	var thumbPtr *string
	if thumb != "" {
		thumbPtr = &thumb
	}
	var descPtr *string
	if d := strings.TrimSpace(text(entry["overview"])); d != "" {
		descPtr = &d
	}
	var airPtr *string
	if a := strings.TrimSpace(text(entry["release_date"])); a != "" {
		airPtr = &a
	}
	return &EpisodeMetadata{
		Number:      anilistNumber,
		Title:       strings.TrimSpace(title),
		Thumbnail:   thumbPtr,
		Description: descPtr,
		Airdate:     airPtr,
	}
}

type ResolveResult struct {
	AnilistID    int                `json:"anilistId"`
	Source       string             `json:"source"`
	CacheSeconds int                `json:"cacheSeconds"`
	Episodes     []*EpisodeMetadata `json:"episodes"`
	Mapped       []int              `json:"mapped"`
	Missing      []int              `json:"missing"`
	Mapping      map[string]any     `json:"mapping,omitempty"`
}

// ResolveEpisodes is the Go port of api/tmdb-episodes.js:267 resolveEpisodes
// It first tries AniBridge verified mappings, then falls back to Fribb/anime-lists exhaustive mapping for 100% coverage.
func ResolveEpisodes(ctx context.Context, client *http.Client, token string, anilistID int, episodeNumbers []int) (*ResolveResult, error) {
	if client == nil {
		client = &http.Client{Timeout: RequestTimeout}
	}
	// dedup & sort & limit
	uniq := map[int]bool{}
	for _, n := range episodeNumbers {
		if n > 0 {
			uniq[n] = true
		}
	}
	var nums []int
	for k := range uniq {
		nums = append(nums, k)
	}
	sort.Ints(nums)
	if len(nums) == 0 {
		return nil, newResolverError("INVALID_EPISODES", "Provide one or more positive episode numbers.", 400)
	}
	if len(nums) > MaxEpisodeNumbers {
		return nil, newResolverError("TOO_MANY_EPISODES", fmt.Sprintf("Request at most %d episode numbers.", MaxEpisodeNumbers), 400)
	}
	payload, err := getMapping(ctx, client, anilistID)
	var mappings []tmdbMapping
	if err == nil {
		showMappings, _ := extractTmdbShowMappings(payload, anilistID)
		movieMappings, _ := extractTmdbMovieMappings(payload, anilistID)
		mappings = append([]tmdbMapping{}, showMappings...)
		mappings = append(mappings, movieMappings...)
	}
	if len(mappings) == 0 {
		// AniBridge missing or unverified -> try Fribb fallback (exhaustive but less verified)
		if fribb, ferr := getFribbMappings(ctx, client, anilistID); ferr == nil && len(fribb) > 0 {
			mappings = fribb
		} else {
			if err != nil {
				return nil, err
			}
			return nil, newResolverError("TMDB_MEDIA_MAPPING_NOT_FOUND", "No verified TMDB television or movie mapping exists for this anime.", 404)
		}
	}
	requested := map[int]*tmdbMapping{}
	for _, n := range nums {
		requested[n] = selectTmdbMappingForEpisode(mappings, n)
	}
	// active mappings dedup
	activeMap := map[string]tmdbMapping{}
	for _, m := range requested {
		if m == nil {
			continue
		}
		key := ""
		if m.Type == "tv" {
			key = fmt.Sprintf("tv:%d:%d", m.ShowID, m.SeasonNumber)
		} else {
			key = fmt.Sprintf("movie:%d", m.MovieID)
		}
		activeMap[key] = *m
	}
	active := make([]tmdbMapping, 0, len(activeMap))
	for _, v := range activeMap {
		active = append(active, v)
	}
	if len(active) == 0 {
		return &ResolveResult{
			AnilistID:    anilistID,
			Source:       "tmdb",
			CacheSeconds: int(EpisodeTTL.Seconds()),
			Episodes:     []*EpisodeMetadata{},
			Missing:      nums,
		}, nil
	}
	// fetch TMDB metadata
	tmdbMeta := map[string]map[string]any{} // key -> entry
	// also need to handle movie
	for _, mapping := range active {
		if mapping.Type == "movie" {
			movie, err := getTmdbMovie(ctx, client, token, mapping.MovieID)
			if err != nil {
				return nil, err
			}
			tmdbMeta[fmt.Sprintf("movie:%d:1", mapping.MovieID)] = movie
			continue
		}
		season, err := getTmdbSeason(ctx, client, token, mapping.ShowID, mapping.SeasonNumber)
		if err != nil {
			return nil, err
		}
		// verify season_number
		var seasonNum int
		switch v := season["season_number"].(type) {
		case float64:
			seasonNum = int(v)
		case int:
			seasonNum = v
		}
		if seasonNum != mapping.SeasonNumber {
			return nil, newResolverError("TMDB_SEASON_MISMATCH", "TMDB returned an unexpected season for the verified mapping.", 502)
		}
		eps, _ := season["episodes"].([]any)
		for _, e := range eps {
			entry, _ := e.(map[string]any)
			if entry == nil {
				continue
			}
			var epNum int
			switch v := entry["episode_number"].(type) {
			case float64:
				epNum = int(v)
			case int:
				epNum = v
			}
			key := fmt.Sprintf("tv:%d:%d:%d", mapping.ShowID, mapping.SeasonNumber, epNum)
			tmdbMeta[key] = entry
		}
	}
	// continuation groups for open-ended
	type contGroup struct {
		showId           int
		afterSeasonNumber int
		targetNumbers    map[int]bool
	}
	contGroups := map[string]*contGroup{}
	for _, n := range nums {
		m := requested[n]
		if m == nil || m.Type != "tv" || !hasOpenEndedSourceRange(m.Ranges, n) {
			continue
		}
		directKey := fmt.Sprintf("tv:%d:%d:%d", m.ShowID, m.SeasonNumber, *m.TmdbNumber)
		if _, ok := tmdbMeta[directKey]; ok {
			continue
		}
		gk := fmt.Sprintf("%d:%d", m.ShowID, m.SeasonNumber)
		g, ok := contGroups[gk]
		if !ok {
			g = &contGroup{showId: m.ShowID, afterSeasonNumber: m.SeasonNumber, targetNumbers: map[int]bool{}}
			contGroups[gk] = g
		}
		g.targetNumbers[*m.TmdbNumber] = true
	}
	for _, g := range contGroups {
		show, err := getTmdbShow(ctx, client, token, g.showId)
		if err != nil {
			return nil, err
		}
		pending := map[int]bool{}
		for k := range g.targetNumbers {
			pending[k] = true
		}
		for _, seasonNum := range continuationSeasonNumbers(show, g.afterSeasonNumber) {
			season, err := getTmdbSeason(ctx, client, token, g.showId, seasonNum)
			if err != nil {
				continue
			}
			eps, _ := season["episodes"].([]any)
			for _, e := range eps {
				entry, _ := e.(map[string]any)
				if entry == nil {
					continue
				}
				var epNum int
				switch v := entry["episode_number"].(type) {
				case float64:
					epNum = int(v)
				case int:
					epNum = v
				}
				if !pending[epNum] {
					continue
				}
				tmdbMeta[fmt.Sprintf("tv:continuation:%d:%d", g.showId, epNum)] = entry
				delete(pending, epNum)
			}
			if len(pending) == 0 {
				break
			}
		}
	}
	// build episodes
	var episodes []*EpisodeMetadata
	for _, n := range nums {
		m := requested[n]
		if m == nil {
			continue
		}
		var meta *EpisodeMetadata
		if m.Type == "movie" {
			meta = toTmdbMovieMetadata(tmdbMeta[fmt.Sprintf("movie:%d:1", m.MovieID)], n, *m.TmdbNumber)
		} else {
			// try direct then continuation
			entry := tmdbMeta[fmt.Sprintf("tv:%d:%d:%d", m.ShowID, m.SeasonNumber, *m.TmdbNumber)]
			if entry == nil {
				entry = tmdbMeta[fmt.Sprintf("tv:continuation:%d:%d", m.ShowID, *m.TmdbNumber)]
			}
			meta = toTmdbEpisodeMetadata(entry, n, *m.TmdbNumber)
		}
		if meta != nil {
			episodes = append(episodes, meta)
		}
	}
	found := map[int]bool{}
	for _, e := range episodes {
		found[e.Number] = true
	}
	var mapped []int
	var missing []int
	for _, n := range nums {
		if requested[n] != nil {
			mapped = append(mapped, n)
		}
		if !found[n] {
			missing = append(missing, n)
		}
	}
	// Build mapping debug
	var segments []map[string]any
	for _, m := range active {
		if m.Type == "movie" {
			segments = append(segments, map[string]any{"type": "movie", "movieId": m.MovieID})
		} else {
			segments = append(segments, map[string]any{"type": "tv", "showId": m.ShowID, "seasonNumber": m.SeasonNumber})
		}
	}
	return &ResolveResult{
		AnilistID:    anilistID,
		Source:       "tmdb",
		CacheSeconds: int(EpisodeTTL.Seconds()),
		Episodes:     episodes,
		Mapped:       mapped,
		Missing:      missing,
		Mapping:      map[string]any{"provider": "anibridge", "segments": segments},
	}, nil
}

// --- Fribb/anime-lists fallback (exhaustive, less verified than AniBridge) ---

var fribbCache struct {
	sync.RWMutex
	entries map[int]fribbEntry
	fetched time.Time
}

type fribbEntry struct {
	AnilistID    int `json:"anilist_id"`
	Type         string `json:"type"`
	ThemoviedbID struct {
		TV    any `json:"tv"`    // int or null
		Movie []int `json:"movie"`
	} `json:"themoviedb_id"`
	Season struct {
		Tmdb int `json:"tmdb"`
	} `json:"season"`
	EpisodeOffset struct {
		Tmdb int `json:"tmdb"`
	} `json:"episode_offset"`
}

func getFribbMappings(ctx context.Context, client *http.Client, anilistID int) ([]tmdbMapping, error) {
	// Load/cache full list (once per 24h)
	fribbCache.RLock()
	if fribbCache.entries != nil && time.Since(fribbCache.fetched) < 24*time.Hour {
		entries := fribbCache.entries
		fribbCache.RUnlock()
		return fribbToMappings(entries[anilistID]), nil
	}
	fribbCache.RUnlock()

	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	// Use mini version for speed (same content, no whitespace)
	url := "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-mini.json"
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("fribb http %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 20<<20))
	var list []fribbEntry
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	m := make(map[int]fribbEntry, len(list))
	for _, e := range list {
		if e.AnilistID > 0 {
			m[e.AnilistID] = e
		}
	}
	fribbCache.Lock()
	fribbCache.entries = m
	fribbCache.fetched = time.Now()
	fribbCache.Unlock()

	entry, ok := m[anilistID]
	if !ok {
		return nil, fmt.Errorf("fribb: no entry for %d", anilistID)
	}
	return fribbToMappings(entry), nil
}

func fribbToMappings(e fribbEntry) []tmdbMapping {
	var out []tmdbMapping
	// TV
	var tvID int
	switch v := e.ThemoviedbID.TV.(type) {
	case float64:
		tvID = int(v)
	case int:
		tvID = v
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			tvID = n
		}
	}
	if tvID > 0 {
		season := e.Season.Tmdb
		if season == 0 {
			season = 1
		}
		offset := e.EpisodeOffset.Tmdb
		// Build range: anilist 1- => tmdb (offset+1)-
		target := "1-"
		if offset > 0 {
			target = fmt.Sprintf("%d-", offset+1)
		}
		ranges := map[string]string{"1-": target}
		out = append(out, tmdbMapping{Type: "tv", ShowID: tvID, SeasonNumber: season, Ranges: ranges})
	}
	// Movies
	for _, mid := range e.ThemoviedbID.Movie {
		if mid > 0 {
			out = append(out, tmdbMapping{Type: "movie", MovieID: mid, Ranges: map[string]string{"1": "1"}})
		}
	}
	return out
}

// Helper for URL encoding check
func mustParseURL(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}

var _ = mustParseURL
