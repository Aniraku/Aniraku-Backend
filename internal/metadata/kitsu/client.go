// ponytail: REST client — no GraphQL, no rate limiter, no retries
// Kitsu has no hard rate limit for reasonable use. Cache aggressively upstream.
package kitsu

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/metadata/anilist"
)

type Client struct {
	baseURL string
	http    *http.Client
	log     zerolog.Logger

	mu      sync.RWMutex
	idMap   map[int]int  // anilistID -> kitsuID
	cache   map[string]ce
	pendMu  sync.Mutex
	pending map[string]chan struct{}
}

type ce struct {
	data      any
	expiresAt time.Time
}

func NewClient(log zerolog.Logger) *Client {
	return &Client{
		baseURL: "https://kitsu.io/api/edge",
		http:    &http.Client{Timeout: 10 * time.Second},
		log:     log,
		idMap:   map[int]int{},
		cache:   map[string]ce{},
		pending: map[string]chan struct{}{},
	}
}

// --- Kitsu JSON:API response types ---

type kaDoc struct {
	Data     kaRes        `json:"data"`
	Included []kaRes      `json:"included,omitempty"`
	Meta     *kaMeta      `json:"meta,omitempty"`
	Links    *kaLinks     `json:"links,omitempty"`
}

type kaList struct {
	Data     []kaRes      `json:"data"`
	Included []kaRes      `json:"included,omitempty"`
	Meta     *kaMeta      `json:"meta,omitempty"`
	Links    *kaLinks     `json:"links,omitempty"`
}

type kaRes struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Attributes    json.RawMessage `json:"attributes"`
	Relationships json.RawMessage `json:"relationships,omitempty"`
}

type kaMeta struct {
	Count int `json:"count"`
}

type kaLinks struct {
	First string `json:"first,omitempty"`
	Next  string `json:"next,omitempty"`
	Last  string `json:"last,omitempty"`
}

// --- Kitsu anime attributes ---

type kaAnime struct {
	Slug             string         `json:"slug"`
	Synopsis         string         `json:"synopsis"`
	CanonicalTitle   string         `json:"canonicalTitle"`
	Titles           kaTitles       `json:"titles"`
	AverageRating    *string        `json:"averageRating"`
	UserCount        int            `json:"userCount"`
	FavoritesCount   int            `json:"favoritesCount"`
	PopularityRank   int            `json:"popularityRank"`
	RatingRank       int            `json:"ratingRank"`
	Subtype          string         `json:"subtype"`
	Status           string         `json:"status"`
	NSFW             bool           `json:"nsfw"`
	EpisodeCount     *int           `json:"episodeCount"`
	EpisodeLength    *int           `json:"episodeLength"`
	TotalLength      *int           `json:"totalLength"`
	StartDate        *string        `json:"startDate"`
	EndDate          *string        `json:"endDate"`
	AgeRating        string         `json:"ageRating"`
	YoutubeVideoID   string         `json:"youtubeVideoId"`
	PosterImage      kaImage        `json:"posterImage"`
	CoverImage       *kaCover       `json:"coverImage"`
	AbbreviatedTitles []string      `json:"abbreviatedTitles"`
}

type kaTitles struct {
	En   string `json:"en"`
	EnJP string `json:"en_jp"`
	JaJP string `json:"ja_jp"`
	EnUS string `json:"en_us"`
}

type kaImage struct {
	Tiny    string `json:"tiny"`
	Large   string `json:"large"`
	Small   string `json:"small"`
	Medium  string `json:"medium"`
	Original string `json:"original"`
}

type kaCover struct {
	Tiny     string `json:"tiny"`
	Large    string `json:"large"`
	Small    string `json:"small"`
	Original string `json:"original"`
}

// --- Kitsu media-relationship types ---

type kaRelList struct {
	Data     []kaRelEdge `json:"data"`
	Included []kaRes     `json:"included,omitempty"`
}

type kaRelEdge struct {
	ID            string               `json:"id"`
	Type          string               `json:"type"`
	Attributes    kaRelEdgeAttr        `json:"attributes"`
	Relationships kaRelEdgeRel         `json:"relationships"`
}

type kaRelEdgeAttr struct {
	Role string `json:"role"`
}

type kaRelEdgeRel struct {
	Destination struct {
		Data struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		} `json:"data"`
	} `json:"destination"`
}

// --- categories/genres ---

type kaCatList struct {
	Data []kaCat `json:"data"`
}

type kaCat struct {
	ID         string `json:"id"`
	Attributes struct {
		Title string `json:"title"`
	} `json:"attributes"`
}

// --- API ---

func (c *Client) do(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.api+json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("kitsu %s: HTTP %d: %s", path, resp.StatusCode, string(body))
	}
	return json.Unmarshal(body, out)
}

// --- ID mapping ---

func (c *Client) kitsuID(ctx context.Context, anilistID int) (int, error) {
	c.mu.RLock()
	if k, ok := c.idMap[anilistID]; ok {
		c.mu.RUnlock()
		return k, nil
	}
	c.mu.RUnlock()

	var list kaList
	path := fmt.Sprintf("/mappings?filter[externalSite]=anilist/anime&filter[externalId]=%d&include=item", anilistID)
	if err := c.do(ctx, path, &list); err != nil {
		return 0, err
	}
	if len(list.Data) == 0 {
		return 0, fmt.Errorf("no kitsu mapping for anilist ID %d", anilistID)
	}
	if len(list.Included) > 0 {
		kid, _ := strconv.Atoi(list.Included[0].ID)
		if kid > 0 {
			c.mu.Lock()
			c.idMap[anilistID] = kid
			c.mu.Unlock()
			return kid, nil
		}
	}
	return 0, fmt.Errorf("kitsu mapping for %d has no included anime", anilistID)
}

func (c *Client) anilistID(ctx context.Context, kitsuID int) (int, error) {
	// check reverse cache
	c.mu.RLock()
	for aid, kid := range c.idMap {
		if kid == kitsuID {
			c.mu.RUnlock()
			return aid, nil
		}
	}
	c.mu.RUnlock()

	var mList kaList
	if err := c.do(ctx, fmt.Sprintf("/anime/%d/mappings?filter[externalSite]=anilist/anime", kitsuID), &mList); err != nil {
		return 0, err
	}
	for _, m := range mList.Data {
		var attrs struct {
			ExternalSite string `json:"externalSite"`
			ExternalID   string `json:"externalId"`
		}
		json.Unmarshal(m.Attributes, &attrs)
		if attrs.ExternalSite == "anilist/anime" {
			aid, err := strconv.Atoi(attrs.ExternalID)
			if err == nil && aid > 0 {
				c.mu.Lock()
				c.idMap[aid] = kitsuID
				c.mu.Unlock()
				return aid, nil
			}
		}
	}
	return 0, fmt.Errorf("no anilist mapping for kitsu ID %d", kitsuID)
}

// malToAniList maps MAL ID → Kitsu ID → AniList ID
func (c *Client) malToAniList(ctx context.Context, malID int) (int, error) {
	// ponytail: cache MAL→AniList mapping
	c.mu.RLock()
	if aid, ok := c.idMap[malID]; ok {
		c.mu.RUnlock()
		return aid, nil
	}
	c.mu.RUnlock()

	// MAL ID → Kitsu ID
	var mList struct {
		Data []struct {
			ID         string `json:"id"`
			Attributes struct {
				ExternalID string `json:"externalId"`
			} `json:"attributes"`
		} `json:"data"`
	}
	if err := c.do(ctx, fmt.Sprintf("/mappings?filter[externalSite]=mal/anime&filter[externalId]=%d", malID), &mList); err != nil {
		return 0, fmt.Errorf("no kitsu mapping for MAL ID %d", malID)
	}
	if len(mList.Data) == 0 {
		return 0, fmt.Errorf("no kitsu mapping for MAL ID %d", malID)
	}

	kitsuID, _ := strconv.Atoi(mList.Data[0].ID)
	if kitsuID == 0 {
		return 0, fmt.Errorf("invalid kitsu ID for MAL %d", malID)
	}

	// Kitsu ID → AniList ID
	aid, err := c.anilistID(ctx, kitsuID)
	if err != nil {
		return 0, err
	}

	c.mu.Lock()
	c.idMap[malID] = aid
	c.mu.Unlock()
	return aid, nil
}

// --- data mapping ---

func mapAnime(kid string, attrs kaAnime) anilist.Anime {
	var ep *int
	if attrs.EpisodeCount != nil && *attrs.EpisodeCount > 0 {
		ep = attrs.EpisodeCount
	}
	var dur *int
	if attrs.EpisodeLength != nil && *attrs.EpisodeLength > 0 {
		dur = attrs.EpisodeLength
	}

	status := strings.ToUpper(attrs.Status)
	format := strings.ToUpper(attrs.Subtype)

	var avg *int
	if attrs.AverageRating != nil {
		if r, err := strconv.ParseFloat(*attrs.AverageRating, 64); err == nil {
			v := int(r)
			avg = &v
		}
	}

	var season *string
	var seasonYear *int
	if attrs.StartDate != nil && *attrs.StartDate != "" {
		parts := strings.Split(*attrs.StartDate, "-")
		if len(parts) >= 2 {
			month, _ := strconv.Atoi(parts[1])
			switch {
			case month >= 3 && month <= 5:
				s := "SPRING"; season = &s
			case month >= 6 && month <= 8:
				s := "SUMMER"; season = &s
			case month >= 9 && month <= 11:
				s := "FALL"; season = &s
			default:
				s := "WINTER"; season = &s
			}
			if len(parts) >= 1 {
				y, _ := strconv.Atoi(parts[0])
				if y > 0 {
					seasonYear = &y
				}
			}
		}
	}

	var eng *string
	if attrs.Titles.En != "" {
		eng = &attrs.Titles.En
	}
	var rom *string
	if attrs.Titles.EnJP != "" {
		rom = &attrs.Titles.EnJP
	}
	var nat *string
	if attrs.Titles.JaJP != "" {
		nat = &attrs.Titles.JaJP
	}

	var banner *string
	if attrs.CoverImage != nil && attrs.CoverImage.Large != "" {
		banner = &attrs.CoverImage.Large
	}

	var trailer *struct {
		ID   string `json:"id"`
		Site string `json:"site"`
	}
	if attrs.YoutubeVideoID != "" {
		trailer = &struct {
			ID   string `json:"id"`
			Site string `json:"site"`
		}{ID: attrs.YoutubeVideoID, Site: "youtube"}
	}

	// ponytail: fallback chain for poster images — Kitsu can have empty posterImage
	img := anilist.Image{}
	if attrs.PosterImage.Original != "" {
		img.ExtraLarge = attrs.PosterImage.Original
	} else if attrs.PosterImage.Large != "" {
		img.ExtraLarge = attrs.PosterImage.Large
	} else if attrs.PosterImage.Medium != "" {
		img.ExtraLarge = attrs.PosterImage.Medium
	} else if attrs.PosterImage.Small != "" {
		img.ExtraLarge = attrs.PosterImage.Small
	} else if kid != "" {
		// ponytail: construct from Kitsu CDN pattern if all else fails
		img.ExtraLarge = "https://media.kitsu.io/anime/poster_images/" + kid + "/original.jpg"
	}
	if attrs.PosterImage.Large != "" {
		img.Large = attrs.PosterImage.Large
	} else {
		img.Large = img.ExtraLarge
	}
	if attrs.PosterImage.Medium != "" {
		img.Medium = attrs.PosterImage.Medium
	} else {
		img.Medium = img.Large
	}

	return anilist.Anime{
		ID:          0, // filled by caller after resolving AniList ID
		IDMal:       nil,
		IsAdult:     attrs.NSFW,
		Title: anilist.Title{
			Romaji:  rom,
			English: eng,
			Native:  nat,
		},
		Description: attrs.Synopsis,
		CoverImage:  img,
		BannerImage: banner,
		Episodes:    ep,
		Duration:    dur,
		Status:      status,
		Format:      format,
		Season:      season,
		SeasonYear:  seasonYear,
		Trailer:     trailer,
		AverageScore: avg,
		Popularity:  attrs.PopularityRank,
	}
}

// --- public methods (mirror anilist.Client signatures) ---

func (c *Client) GetAnime(ctx context.Context, id int) (*anilist.Anime, error) {
	cachev := fmt.Sprintf("anime:%d", id)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.Anime), nil
	}

	kid, err := c.kitsuID(ctx, id)
	if err != nil {
		return nil, err
	}

	var doc kaDoc
	if err := c.do(ctx, fmt.Sprintf("/anime/%d", kid), &doc); err != nil {
		return nil, err
	}

	var attrs kaAnime
	if err := json.Unmarshal(doc.Data.Attributes, &attrs); err != nil {
		return nil, err
	}

	a := mapAnime(doc.Data.ID, attrs)
	a.ID = id
	c.set(cachev, &a, 24*time.Hour)
	return &a, nil
}

func (c *Client) GetTrending(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	if perPage > 20 {
		perPage = 20
	}
	cachev := fmt.Sprintf("trending:%d:%d", page, perPage)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

	res, err := c.dedup(cachev, func() (any, error) {
		offset := (page - 1) * perPage
		// ponytail: fields[anime] ensures posterImage is always returned
		var list kaList
		if err := c.do(ctx, fmt.Sprintf("/anime?sort=-popularityRank&page[limit]=%d&page[offset]=%d&fields[anime]=posterImage,coverImage,titles,slug,status,subtype,episodeCount,episodeLength,averageRating,popularityRank,startDate,nsfw", perPage, offset), &list); err != nil {
			return nil, err
		}

		media := make([]anilist.Anime, 0, len(list.Data))
		for _, r := range list.Data {
			var attrs kaAnime
			if err := json.Unmarshal(r.Attributes, &attrs); err != nil {
				continue
			}
			a := mapAnime(r.ID, attrs)
			aid, _ := c.anilistID(ctx, mustInt(r.ID))
			a.ID = aid
			media = append(media, a)
		}

		total := 0
		if list.Meta != nil {
			total = list.Meta.Count
		}

		result := &anilist.BrowseResponse{}
		result.Data.Page.PageInfo.HasNextPage = total > offset+perPage
		result.Data.Page.PageInfo.Total = total
		result.Data.Page.PageInfo.CurrentPage = page
		lastPage := (total + perPage - 1) / perPage
		result.Data.Page.PageInfo.LastPage = lastPage
		result.Data.Page.PageInfo.PerPage = perPage
		result.Data.Page.Media = media

		c.set(cachev, result, 5*time.Minute)
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*anilist.BrowseResponse), nil
}

func (c *Client) Browse(ctx context.Context, filters anilist.BrowseFilters, page, perPage int) (*anilist.BrowseResponse, error) {
	if perPage > 20 {
		perPage = 20
	}
	cachev := fmt.Sprintf("browse:%d:%d:%v:%v:%v:%s:%d:%s:%s",
		page, perPage, filters.Genre, filters.Format, filters.Status,
		filters.Season, filters.Year, filters.Sort, filters.Search)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

	res, err := c.dedup(cachev, func() (any, error) {
		params := url.Values{}
		offset := (page - 1) * perPage
		params.Set("page[limit]", strconv.Itoa(perPage))
		params.Set("page[offset]", strconv.Itoa(offset))
		// ponytail: fields[anime] ensures posterImage is always returned
		params.Set("fields[anime]", "posterImage,coverImage,titles,slug,status,subtype,episodeCount,episodeLength,averageRating,popularityRank,startDate,nsfw,genres")

		if len(filters.Genre) > 0 {
			params.Set("filter[categories]", strings.Join(filters.Genre, ","))
		}
		if len(filters.Format) > 0 {
			params.Set("filter[subtype]", strings.ToLower(strings.Join(filters.Format, ",")))
		}
		if len(filters.Status) > 0 {
			params.Set("filter[status]", strings.ToLower(strings.Join(filters.Status, ",")))
		}
		if filters.Season != "" {
			params.Set("filter[season]", strings.ToLower(filters.Season))
		}
		if filters.Year > 0 {
			params.Set("filter[seasonYear]", strconv.Itoa(filters.Year))
		}
		if filters.Search != "" {
			params.Set("filter[text]", filters.Search)
		}

		sort := "-popularityRank"
		switch filters.Sort {
		case "SCORE_DESC":
			sort = "-averageRating"
		case "POPULARITY_DESC":
			sort = "-popularityRank"
		case "START_DATE_DESC":
			sort = "-startDate"
		case "TITLE_ROMAJI":
			sort = "slug"
		}
		params.Set("sort", sort)

		var list kaList
		if err := c.do(ctx, "/anime?"+params.Encode(), &list); err != nil {
			return nil, err
		}

		media := make([]anilist.Anime, 0, len(list.Data))
		for _, r := range list.Data {
			var attrs kaAnime
			if err := json.Unmarshal(r.Attributes, &attrs); err != nil {
				continue
			}
			a := mapAnime(r.ID, attrs)
			aid, _ := c.anilistID(ctx, mustInt(r.ID))
			a.ID = aid
			media = append(media, a)
		}

		total := 0
		if list.Meta != nil {
			total = list.Meta.Count
		}

		result := &anilist.BrowseResponse{}
		result.Data.Page.PageInfo.HasNextPage = total > offset+perPage
		result.Data.Page.PageInfo.Total = total
		result.Data.Page.PageInfo.CurrentPage = page
		lastPage := (total + perPage - 1) / perPage
		result.Data.Page.PageInfo.LastPage = lastPage
		result.Data.Page.PageInfo.PerPage = perPage
		result.Data.Page.Media = media

		c.set(cachev, result, 3*time.Minute)
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*anilist.BrowseResponse), nil
}

func (c *Client) Search(ctx context.Context, query string, page, perPage int) (*anilist.BrowseResponse, error) {
	if perPage > 20 {
		perPage = 20
	}
	cachev := fmt.Sprintf("search:%s:%d:%d", query, page, perPage)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

		res, err := c.dedup(cachev, func() (any, error) {
		offset := (page - 1) * perPage
		var list kaList
		q := url.Values{}
		q.Set("page[limit]", strconv.Itoa(perPage))
		q.Set("page[offset]", strconv.Itoa(offset))
		q.Set("filter[text]", query)
		// ponytail: fields[anime] ensures posterImage is always returned
		q.Set("fields[anime]", "posterImage,coverImage,titles,slug,status,subtype,episodeCount,episodeLength,averageRating,popularityRank,startDate,nsfw")
		if err := c.do(ctx, "/anime?"+q.Encode(), &list); err != nil {
			return nil, err
		}

		media := make([]anilist.Anime, 0, len(list.Data))
		for _, r := range list.Data {
			var attrs kaAnime
			if err := json.Unmarshal(r.Attributes, &attrs); err != nil {
				continue
			}
			a := mapAnime(r.ID, attrs)
			aid, _ := c.anilistID(ctx, mustInt(r.ID))
			a.ID = aid
			media = append(media, a)
		}

		total := 0
		if list.Meta != nil {
			total = list.Meta.Count
		}

		result := &anilist.BrowseResponse{}
		result.Data.Page.PageInfo.HasNextPage = total > offset+perPage
		result.Data.Page.PageInfo.Total = total
		result.Data.Page.PageInfo.CurrentPage = page
		lastPage := (total + perPage - 1) / perPage
		result.Data.Page.PageInfo.LastPage = lastPage
		result.Data.Page.PageInfo.PerPage = perPage
		result.Data.Page.Media = media

		c.set(cachev, result, 30*time.Second)
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	return res.(*anilist.BrowseResponse), nil
}

func (c *Client) GetGenres(ctx context.Context) ([]string, error) {
	cachev := "genres"
	if cached, ok := c.get(cachev); ok {
		return cached.([]string), nil
	}

	var list kaCatList
	if err := c.do(ctx, "/categories?page[limit]=50&sort=title", &list); err != nil {
		return nil, err
	}

	genres := make([]string, 0, len(list.Data))
	for _, cat := range list.Data {
		g := cat.Attributes.Title
		// filter to anime-relevant categories (skip very niche ones)
		if len(g) > 0 && len(g) <= 30 {
			genres = append(genres, g)
		}
	}

	c.set(cachev, genres, 24*time.Hour)
	return genres, nil
}

func (c *Client) GetSimilar(ctx context.Context, id int, page, perPage int) (*anilist.BrowseResponse, error) {
	if perPage > 20 {
		perPage = 20
	}
	cachev := fmt.Sprintf("similar:%d:%d:%d", id, page, perPage)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.BrowseResponse), nil
	}

	// Get genres for this anime from Kitsu
	kid, err := c.kitsuID(ctx, id)
	if err != nil {
		return c.fallbackTrending(ctx, page, perPage), nil
	}

	// Get categories by fetching /anime/{kid}/categories
	var catList kaCatList
	if err := c.do(ctx, fmt.Sprintf("/anime/%d/categories?page[limit]=20", kid), &catList); err != nil {
		return c.fallbackTrending(ctx, page, perPage), nil
	}

	genreNames := make([]string, 0, len(catList.Data))
	for _, cat := range catList.Data {
		genreNames = append(genreNames, cat.Attributes.Title)
	}
	if len(genreNames) == 0 {
		return c.fallbackTrending(ctx, page, perPage), nil
	}

	similar, err := c.Browse(ctx, anilist.BrowseFilters{Genre: genreNames, Sort: "SCORE_DESC"}, 1, perPage)
	if err != nil || len(similar.Data.Page.Media) == 0 {
		return c.fallbackTrending(ctx, page, perPage), nil
	}

	// Filter out the source anime
	filtered := make([]anilist.Anime, 0, len(similar.Data.Page.Media))
	for _, m := range similar.Data.Page.Media {
		if m.ID != id {
			filtered = append(filtered, m)
		}
		if len(filtered) >= perPage {
			break
		}
	}

	result := &anilist.BrowseResponse{}
	if len(filtered) > 0 {
		result.Data.Page.Media = filtered
		result.Data.Page.PageInfo.Total = len(filtered)
	} else {
		return c.fallbackTrending(ctx, page, perPage), nil
	}

	c.set(cachev, result, 6*time.Hour)
	return result, nil
}

func (c *Client) fallbackTrending(ctx context.Context, page, perPage int) *anilist.BrowseResponse {
	trending, err := c.GetTrending(ctx, 1, perPage)
	if err != nil {
		return &anilist.BrowseResponse{}
	}
	return trending
}

// -- ponytail: relations via Kitsu media-relationships ---

func (c *Client) GetRelations(ctx context.Context, id int) (*anilist.RelationsResponse, error) {
	cachev := fmt.Sprintf("relations:%d", id)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.RelationsResponse), nil
	}

	kid, err := c.kitsuID(ctx, id)
	if err != nil {
		return nil, err
	}

	// Fetch media-relationships for this anime, include the destination anime
	var rel struct {
		Data     []kaRelEdge `json:"data"`
		Included []kaRes     `json:"included"`
	}
	if err := c.do(ctx, fmt.Sprintf("/anime/%d/media-relationships?include=destination", kid), &rel); err != nil {
		return nil, err
	}

	// Build a map of included anime by ID
	included := map[string]kaAnime{}
	for _, inc := range rel.Included {
		if inc.Type == "anime" {
			var a kaAnime
			if err := json.Unmarshal(inc.Attributes, &a); err == nil {
				included[inc.ID] = a
			}
		}
	}

	var edges []anilist.RelationEdge
	for _, e := range rel.Data {
		destID := e.Relationships.Destination.Data.ID
		attrs, ok := included[destID]
		if !ok {
			continue
		}

		a := mapAnime(destID, attrs)
		destAID, _ := c.anilistID(ctx, mustInt(destID))

		role := strings.ToUpper(e.Attributes.Role)

		edges = append(edges, anilist.RelationEdge{
			RelationType: role,
			Node: anilist.RelationAnime{
				ID:         destAID,
				Title:      a.Title,
				CoverImage: a.CoverImage,
				Format:     a.Format,
				Episodes:   a.Episodes,
				Status:     a.Status,
				AverageScore: a.AverageScore,
			},
		})
	}

	result := &anilist.RelationsResponse{Relations: edges}
	c.set(cachev, result, 6*time.Hour)
	return result, nil
}

func (c *Client) GetSchedule(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	// ponytail: Kitsu has no aggregated next-airing endpoint;
	return nil, fmt.Errorf("schedule not available via Kitsu")
}

func (c *Client) BrowseAdult(ctx context.Context, page, perPage int) (*anilist.BrowseResponse, error) {
	// ponytail: skip NSFW on Kitsu — no clean adult filter
	result := &anilist.BrowseResponse{}
	result.Data.Page.PageInfo.Total = 0
	result.Data.Page.Media = []anilist.Anime{}
	return result, nil
}

// --- cache helpers ---

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
	c.cache[key] = ce{data: data, expiresAt: time.Now().Add(ttl)}
}

func (c *Client) dedup(key string, fn func() (any, error)) (any, error) {
	c.pendMu.Lock()
	if ch, ok := c.pending[key]; ok {
		c.pendMu.Unlock()
		<-ch
		if cached, ok := c.get(key); ok {
			return cached, nil
		}
	}
	ch := make(chan struct{})
	c.pending[key] = ch
	c.pendMu.Unlock()

	result, err := fn()

	c.pendMu.Lock()
	delete(c.pending, key)
	c.pendMu.Unlock()
	close(ch)
	return result, err
}

func mustInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// GetAnimeByKitsuID fetches anime directly by Kitsu ID (no AniList resolution needed)
func (c *Client) GetAnimeByKitsuID(ctx context.Context, kitsuID int) (*anilist.Anime, error) {
	cachev := fmt.Sprintf("kitsu_anime:%d", kitsuID)
	if cached, ok := c.get(cachev); ok {
		return cached.(*anilist.Anime), nil
	}

	var doc kaDoc
	if err := c.do(ctx, fmt.Sprintf("/anime/%d", kitsuID), &doc); err != nil {
		return nil, err
	}

	var attrs kaAnime
	if err := json.Unmarshal(doc.Data.Attributes, &attrs); err != nil {
		return nil, err
	}

	a := mapAnime(doc.Data.ID, attrs)

	// Try to get AniList ID mapping
	aid, err := c.anilistID(ctx, kitsuID)
	if err == nil {
		a.ID = aid
	}

	c.set(cachev, &a, 24*time.Hour)
	return &a, nil
}

// GetAniListIDFromMAL maps MAL ID to AniList ID (for Miruro streaming)
func (c *Client) GetAniListIDFromMAL(ctx context.Context, malID int) (int, error) {
	return c.malToAniList(ctx, malID)
}

// ResolveToAniListID accepts either MAL ID or AniList ID and returns AniList ID
func (c *Client) ResolveToAniListID(ctx context.Context, id int) (int, error) {
	// Try as AniList ID first (direct mapping to Kitsu)
	if kid, err := c.kitsuID(ctx, id); err == nil && kid > 0 {
		// Verify it maps back to a valid AniList ID
		if aid, err := c.anilistID(ctx, kid); err == nil && aid > 0 {
			return aid, nil
		}
		return id, nil // already AniList ID
	}
	// Fallback: try as MAL ID
	return c.malToAniList(ctx, id)
}
