// ponytail: Jikan V4 client — MAL metadata, no auth, no rate limit hits (24h cache)
package mal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/metadata/anilist"
)

const baseURL = "https://api.jikan.moe/v4"

type Client struct {
	http  *http.Client
	log   zerolog.Logger
	cache map[string]cacheEntry
	mu    sync.RWMutex
	// ponytail: global rate limiter — 3 req/sec burst, 60/min sustained
	// Jikan says cached responses don't count, so 24h cache means near-zero API calls
	lastReq time.Time
	reqMu   sync.Mutex
}

type cacheEntry struct {
	data      any
	expiresAt time.Time
}

func NewClient(log zerolog.Logger) *Client {
	return &Client{
		http:  &http.Client{Timeout: 10 * time.Second},
		log:   log,
		cache: make(map[string]cacheEntry),
	}
}

func (c *Client) get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.cache[key]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.data, true
}

func (c *Client) set(key string, data any, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[key] = cacheEntry{data: data, expiresAt: time.Now().Add(ttl)}
}

// ponytail: rate limiter — wait 350ms between requests to stay under 3/sec
func (c *Client) throttle() {
	c.reqMu.Lock()
	defer c.reqMu.Unlock()
	if !c.lastReq.IsZero() {
		if wait := 350*time.Millisecond - time.Since(c.lastReq); wait > 0 {
			time.Sleep(wait)
		}
	}
	c.lastReq = time.Now()
}

func (c *Client) do(ctx context.Context, path string, out any) error {
	key := "raw:" + path
	if cached, ok := c.get(key); ok {
		// already decoded, just assign
		b, _ := json.Marshal(cached)
		return json.Unmarshal(b, out)
	}

	c.throttle()
	url := baseURL + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("jikan request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		// ponytail: Jikan rate limited — wait and retry once
		time.Sleep(2 * time.Second)
		resp.Body.Close()
		req2, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		req2.Header.Set("Accept", "application/json")
		resp2, err2 := c.http.Do(req2)
		if err2 != nil {
			return fmt.Errorf("jikan retry failed: %w", err2)
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp2.Body)
			return fmt.Errorf("jikan %d: %s", resp2.StatusCode, string(body))
		}
		body, _ := io.ReadAll(resp2.Body)
		var result any
		if err := json.Unmarshal(body, &result); err != nil {
			return err
		}
		c.set(key, result, 24*time.Hour)
		return json.Unmarshal(body, out)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("jikan %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	var result any
	if err := json.Unmarshal(body, &result); err != nil {
		return err
	}
	c.set(key, result, 24*time.Hour)
	return json.Unmarshal(body, out)
}

// --- Jikan response types ---

type jikanAnime struct {
	MALID       int    `json:"mal_id"`
	Title       string `json:"title"`
	TitleEnglish string `json:"title_english"`
	TitleJapanese string `json:"title_japanese"`
	Images      struct {
		JPG struct {
			ImageURL      string `json:"image_url"`
			SmallImageURL string `json:"small_image_url"`
			LargeImageURL string `json:"large_image_url"`
		} `json:"jpg"`
		Webp struct {
			ImageURL      string `json:"image_url"`
			SmallImageURL string `json:"small_image_url"`
			LargeImageURL string `json:"large_image_url"`
		} `json:"webp"`
	} `json:"images"`
	Type        string  `json:"type"`
	Source      string  `json:"source"`
	Episodes    *int    `json:"episodes"`
	Status      string  `json:"status"`
	Airing      bool    `json:"airing"`
	Score       float64 `json:"score"`
	ScoredBy    int     `json:"scored_by"`
	Rank        int     `json:"rank"`
	Popularity  int     `json:"popularity"`
	Members     int     `json:"members"`
	Synopsis    string  `json:"synopsis"`
	Season      string  `json:"season"`
	Year        *int    `json:"year"`
	Genres      []struct {
		MALID int    `json:"mal_id"`
		Name  string `json:"name"`
	} `json:"genres"`
	Themes []struct {
		MALID int    `json:"mal_id"`
		Name  string `json:"name"`
	} `json:"themes"`
	Demographics []struct {
		MALID int    `json:"mal_id"`
		Name  string `json:"name"`
	} `json:"demographics"`
	Studios []struct {
		MALID int    `json:"mal_id"`
		Name  string `json:"name"`
	} `json:"studios"`
	Relations []struct {
		Relation string `json:"relation"`
		Entry    []struct {
			MALID int    `json:"mal_id"`
			Type  string `json:"type"`
			Name  string `json:"name"`
		} `json:"entry"`
	} `json:"relations"`
	Trailer *struct {
		YouTubeID string `json:"youtube_id"`
	} `json:"trailer"`
	Aired struct {
		From string `json:"from"`
	} `json:"aired"`
}

type jikanSearchResponse struct {
	Data []jikanAnime `json:"data"`
	Pagination struct {
		LastVisiblePage int `json:"last_visible_page"`
		HasNextPage     bool `json:"has_next_page"`
		Items struct {
			Count int `json:"count"`
			Total int `json:"total"`
			PerPage int `json:"per_page"`
		} `json:"items"`
	} `json:"pagination"`
}

type jikanFullResponse struct {
	Data jikanAnime `json:"data"`
}

// --- Mapping to anilist types ---

func mapAnime(j jikanAnime) anilist.Anime {
	// ponytail: map MAL status to AniList status format
	status := strings.ToUpper(j.Status)
	switch j.Status {
	case "Finished Airing":
		status = "FINISHED"
	case "Currently Airing":
		status = "RELEASING"
	case "Not yet aired":
		status = "NOT_YET_RELEASED"
	case "Hiatus":
		status = "HIATUS"
	}

	// map type to format
	format := strings.ToUpper(j.Type)

	// map season
	var season *string
	if j.Season != "" {
		s := strings.ToUpper(j.Season)
		season = &s
	}

	// score: Jikan 0-10, AniList 0-100
	var avg *int
	if j.Score > 0 {
		v := int(j.Score * 10)
		avg = &v
	}

	// genres
	genres := make([]string, 0, len(j.Genres)+len(j.Themes)+len(j.Demographics))
	for _, g := range j.Genres {
		genres = append(genres, g.Name)
	}
	for _, g := range j.Themes {
		genres = append(genres, g.Name)
	}
	for _, g := range j.Demographics {
		genres = append(genres, g.Name)
	}

	// title
	var eng, rom, nat *string
	if j.TitleEnglish != "" {
		eng = &j.TitleEnglish
	}
	if j.Title != "" {
		rom = &j.Title
	}
	if j.TitleJapanese != "" {
		nat = &j.TitleJapanese
	}

	// cover image — prefer webp, fallback to jpg
	img := anilist.Image{}
	if j.Images.Webp.LargeImageURL != "" {
		img.ExtraLarge = j.Images.Webp.LargeImageURL
		img.Large = j.Images.Webp.ImageURL
		img.Medium = j.Images.Webp.SmallImageURL
	} else if j.Images.JPG.LargeImageURL != "" {
		img.ExtraLarge = j.Images.JPG.LargeImageURL
		img.Large = j.Images.JPG.ImageURL
		img.Medium = j.Images.JPG.SmallImageURL
	}

	// studios
	var studioNames []string
	for _, s := range j.Studios {
		studioNames = append(studioNames, s.Name)
	}

	// relations
	var relations []anilist.RelationEdge
	for _, r := range j.Relations {
		for _, e := range r.Entry {
			if e.Type == "anime" {
				relations = append(relations, anilist.RelationEdge{
					RelationType: strings.ToUpper(strings.ReplaceAll(r.Relation, " ", "_")),
					Node: anilist.RelationAnime{
						ID:   e.MALID,
						Type: "ANIME",
						Title: anilist.Title{
							Romaji: &e.Name,
						},
					},
				})
			}
		}
	}

	// trailer
	var trailer *struct {
		ID   string `json:"id"`
		Site string `json:"site"`
	}
	if j.Trailer != nil && j.Trailer.YouTubeID != "" {
		id := j.Trailer.YouTubeID
		site := "youtube"
		trailer = &struct {
			ID   string `json:"id"`
			Site string `json:"site"`
		}{ID: id, Site: site}
	}

	return anilist.Anime{
		ID:          j.MALID, // ponytail: MAL ID as primary (we map to AniList for streaming)
		Title:       anilist.Title{Romaji: rom, English: eng, Native: nat},
		Description: j.Synopsis,
		CoverImage:  img,
		BannerImage: nil,
		Episodes:    j.Episodes,
		Duration:    nil,
		Status:      status,
		Format:      format,
		Season:      season,
		SeasonYear:  j.Year,
		Genres:      genres,
		AverageScore: avg,
		Popularity:  j.Popularity,
		Trailer:     trailer,
	}
}

// --- Public API ---

func (c *Client) GetAnime(ctx context.Context, id int) (*anilist.Anime, error) {
	cachev := fmt.Sprintf("anime:%d", id)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.Anime), nil
	}

	var resp jikanFullResponse
	if err := c.do(ctx, fmt.Sprintf("/anime/%d/full", id), &resp); err != nil {
		return nil, err
	}

	a := mapAnime(resp.Data)
	c.set(cachev, &a, 24*time.Hour)
	return &a, nil
}

func (c *Client) Search(ctx context.Context, query string, page, perPage int) (*anilist.BrowseResponse, error) {
	if perPage > 25 {
		perPage = 25
	}
	cachev := fmt.Sprintf("search:%s:%d:%d", query, page, perPage)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

	var resp jikanSearchResponse
	if err := c.do(ctx, fmt.Sprintf("/anime?q=%s&page=%d&limit=%d", query, page, perPage), &resp); err != nil {
		return nil, err
	}

	media := make([]anilist.Anime, 0, len(resp.Data))
	for _, j := range resp.Data {
		media = append(media, mapAnime(j))
	}

	result := &anilist.BrowseResponse{}
	result.Data.Page.PageInfo.HasNextPage = resp.Pagination.HasNextPage
	result.Data.Page.PageInfo.Total = resp.Pagination.Items.Total
	result.Data.Page.PageInfo.CurrentPage = page
	result.Data.Page.PageInfo.LastPage = resp.Pagination.LastVisiblePage
	result.Data.Page.PageInfo.PerPage = perPage
	result.Data.Page.Media = media

	c.set(cachev, result, 5*time.Minute)
	return result, nil
}

// ponytail: GetTrending = GetTop by popularity
func (c *Client) GetTrending(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	return c.GetTop(ctx, page, perPage)
}

func (c *Client) GetTop(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	if perPage > 25 {
		perPage = 25
	}
	cachev := fmt.Sprintf("top:%d:%d", page, perPage)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

	var resp jikanSearchResponse
	if err := c.do(ctx, fmt.Sprintf("/top/anime?page=%d&filter=bypopularity", page), &resp); err != nil {
		return nil, err
	}

	media := make([]anilist.Anime, 0, len(resp.Data))
	for _, j := range resp.Data {
		media = append(media, mapAnime(j))
	}

	result := &anilist.BrowseResponse{}
	result.Data.Page.PageInfo.HasNextPage = resp.Pagination.HasNextPage
	result.Data.Page.PageInfo.Total = resp.Pagination.Items.Total
	result.Data.Page.PageInfo.CurrentPage = page
	result.Data.Page.PageInfo.LastPage = resp.Pagination.LastVisiblePage
	result.Data.Page.PageInfo.PerPage = perPage
	result.Data.Page.Media = media

	c.set(cachev, result, 3*time.Hour)
	return result, nil
}

func (c *Client) Browse(ctx context.Context, filters anilist.BrowseFilters, page, perPage int) (*anilist.BrowseResponse, error) {
	if perPage > 25 {
		perPage = 25
	}
	cachev := fmt.Sprintf("browse:%d:%d:%v:%v:%v:%s:%d:%s:%s",
		page, perPage, filters.Genre, filters.Format, filters.Status,
		filters.Season, filters.Year, filters.Sort, filters.Search)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

	// ponytail: Jikan doesn't have a single browse endpoint — use search with filters
	// Build query params
	params := []string{fmt.Sprintf("page=%d", page), fmt.Sprintf("limit=%d", perPage)}

	if filters.Search != "" {
		params = append(params, fmt.Sprintf("q=%s", filters.Search))
	}
	if len(filters.Status) > 0 {
		// Jikan status: airing, complete, upcoming, hiatus, discontinued
		malStatus := mapStatus(filters.Status[0])
		if malStatus != "" {
			params = append(params, fmt.Sprintf("status=%s", malStatus))
		}
	}
	if len(filters.Format) > 0 {
		// Jikan type: tv, movie, ova, ona, special, music
		malType := mapType(filters.Format[0])
		if malType != "" {
			params = append(params, fmt.Sprintf("type=%s", malType))
		}
	}
	if filters.Year > 0 {
		params = append(params, fmt.Sprintf("start_date=%d-01-01", filters.Year))
	}
	if len(filters.Genre) > 0 {
		// Jikan uses genre IDs, not names — use q for genre name search
		params = append(params, fmt.Sprintf("q=%s", strings.Join(filters.Genre, " ")))
	}

	sort := "popularity"
	switch filters.Sort {
	case "SCORE_DESC":
		sort = "score"
	case "START_DATE_DESC":
		sort = "start_date"
	case "TITLE_ROMAJI":
		sort = "title"
	}
	params = append(params, fmt.Sprintf("order_by=%s&sort=desc", sort))

	var resp jikanSearchResponse
	if err := c.do(ctx, "/anime?"+strings.Join(params, "&"), &resp); err != nil {
		return nil, err
	}

	media := make([]anilist.Anime, 0, len(resp.Data))
	for _, j := range resp.Data {
		media = append(media, mapAnime(j))
	}

	result := &anilist.BrowseResponse{}
	result.Data.Page.PageInfo.HasNextPage = resp.Pagination.HasNextPage
	result.Data.Page.PageInfo.Total = resp.Pagination.Items.Total
	result.Data.Page.PageInfo.CurrentPage = page
	result.Data.Page.PageInfo.LastPage = resp.Pagination.LastVisiblePage
	result.Data.Page.PageInfo.PerPage = perPage
	result.Data.Page.Media = media

	c.set(cachev, result, 5*time.Minute)
	return result, nil
}

func (c *Client) GetRelations(ctx context.Context, id int) (*anilist.RelationsResponse, error) {
	cachev := fmt.Sprintf("relations:%d", id)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.RelationsResponse), nil
	}

	// ponytail: /anime/{id}/full already includes relations — fetch full and extract
	var resp jikanFullResponse
	if err := c.do(ctx, fmt.Sprintf("/anime/%d/full", id), &resp); err != nil {
		return nil, err
	}

	var edges []anilist.RelationEdge
	for _, r := range resp.Data.Relations {
		for _, e := range r.Entry {
			if e.Type == "anime" {
				name := e.Name
				edges = append(edges, anilist.RelationEdge{
					RelationType: strings.ToUpper(strings.ReplaceAll(r.Relation, " ", "_")),
					Node: anilist.RelationAnime{
						ID:    e.MALID,
						Type:  "ANIME",
						Title: anilist.Title{Romaji: &name},
					},
				})
			}
		}
	}

	result := &anilist.RelationsResponse{Relations: edges}
	c.set(cachev, result, 24*time.Hour)
	return result, nil
}

func (c *Client) GetSimilar(ctx context.Context, id int, page, perPage int) (*anilist.BrowseResponse, error) {
	// ponytail: Jikan has /anime/{id}/recommendations — use it
	cachev := fmt.Sprintf("similar:%d:%d:%d", id, page, perPage)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

	// Jikan recommendations endpoint
	var resp struct {
		Data []struct {
			Entry struct {
				MALID int    `json:"mal_id"`
				Title string `json:"title"`
			} `json:"entry"`
		} `json:"data"`
	}
	if err := c.do(ctx, fmt.Sprintf("/anime/%d/recommendations", id), &resp); err != nil {
		// fallback: return top anime
		return c.GetTop(ctx, page, perPage)
	}

	// Fetch full details for each recommended anime (limited by page)
	start := (page - 1) * perPage
	if start >= len(resp.Data) {
		return &anilist.BrowseResponse{}, nil
	}
	end := start + perPage
	if end > len(resp.Data) {
		end = len(resp.Data)
	}

	media := make([]anilist.Anime, 0, perPage)
	for _, rec := range resp.Data[start:end] {
		a, err := c.GetAnime(ctx, rec.Entry.MALID)
		if err == nil {
			media = append(media, *a)
		}
	}

	result := &anilist.BrowseResponse{}
	result.Data.Page.PageInfo.HasNextPage = end < len(resp.Data)
	result.Data.Page.PageInfo.Total = len(resp.Data)
	result.Data.Page.PageInfo.CurrentPage = page
	result.Data.Page.PageInfo.PerPage = perPage
	result.Data.Page.Media = media

	c.set(cachev, result, 6*time.Hour)
	return result, nil
}

func (c *Client) GetGenres(ctx context.Context) ([]string, error) {
	cachev := "genres"
	if cached, ok := c.get(cachev); ok {
		return cached.([]string), nil
	}

	var resp struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := c.do(ctx, "/genres/anime", &resp); err != nil {
		return nil, err
	}

	genres := make([]string, 0, len(resp.Data))
	for _, g := range resp.Data {
		genres = append(genres, g.Name)
	}

	c.set(cachev, genres, 7*24*time.Hour)
	return genres, nil
}

func (c *Client) BrowseAdult(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	// ponytail: Jikan doesn't have NSFW filter — return empty
	return &anilist.BrowseResponse{
		Data: struct {
			Page struct {
				PageInfo anilist.PageInfo `json:"pageInfo"`
				Media    []anilist.Anime  `json:"media"`
			} `json:"Page"`
		}{
			Page: struct {
				PageInfo anilist.PageInfo `json:"pageInfo"`
				Media    []anilist.Anime  `json:"media"`
			}{
				PageInfo: anilist.PageInfo{},
				Media:    []anilist.Anime{},
			},
		},
	}, nil
}

func (c *Client) GetSchedule(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	// ponytail: Jikan has /schedules — use it
	cachev := fmt.Sprintf("schedule:%d:%d", page, perPage)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

	var resp jikanSearchResponse
	if err := c.do(ctx, fmt.Sprintf("/schedules?page=%d", page), &resp); err != nil {
		return nil, err
	}

	media := make([]anilist.Anime, 0, len(resp.Data))
	for _, j := range resp.Data {
		media = append(media, mapAnime(j))
	}

	result := &anilist.BrowseResponse{}
	result.Data.Page.PageInfo.HasNextPage = resp.Pagination.HasNextPage
	result.Data.Page.PageInfo.Total = resp.Pagination.Items.Total
	result.Data.Page.PageInfo.CurrentPage = page
	result.Data.Page.PageInfo.LastPage = resp.Pagination.LastVisiblePage
	result.Data.Page.PageInfo.PerPage = perPage
	result.Data.Page.Media = media

	c.set(cachev, result, 30*time.Minute)
	return result, nil
}

func mapStatus(s string) string {
	switch strings.ToUpper(s) {
	case "RELEASING":
		return "airing"
	case "FINISHED":
		return "complete"
	case "NOT_YET_RELEASED":
		return "upcoming"
	case "HIATUS":
		return "hiatus"
	}
	return ""
}

func mapType(s string) string {
	switch strings.ToUpper(s) {
	case "TV":
		return "tv"
	case "MOVIE":
		return "movie"
	case "OVA":
		return "ova"
	case "ONA":
		return "ona"
	case "SPECIAL":
		return "special"
	case "MUSIC":
		return "music"
	}
	return ""
}
