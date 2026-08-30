package core

import "time"

type Anime struct {
	ID           int      `json:"id"`
	IDMal        *int     `json:"idMal"`
	Title        Title    `json:"title"`
	Description  string   `json:"description"`
	CoverImage   Image    `json:"coverImage"`
	BannerImage  *string  `json:"bannerImage"`
	Episodes     *int     `json:"episodes"`
	Duration     *int     `json:"duration"`
	Status       string   `json:"status"`
	Format       string   `json:"format"`
	Season       *string  `json:"season"`
	SeasonYear   *int     `json:"seasonYear"`
	Genres       []string `json:"genres"`
	Studios      []Studio `json:"studios"`
	Trailer      *Trailer `json:"trailer"`
	AverageScore *int     `json:"averageScore"`
	MeanScore    *int     `json:"meanScore"`
	Popularity   int      `json:"popularity"`
}

type Title struct {
	Romaji        *string `json:"romaji"`
	English       *string `json:"english"`
	Native        *string `json:"native"`
	UserPreferred *string `json:"userPreferred"`
}

type Image struct {
	ExtraLarge string `json:"extraLarge"`
	Large      string `json:"large"`
	Medium     string `json:"medium"`
	Color      string `json:"color"`
}

type Studio struct {
	Name string `json:"name"`
}

type Trailer struct {
	ID   string `json:"id"`
	Site string `json:"site"`
}

type Episode struct {
	Number    int     `json:"number"`
	Title     *string `json:"title"`
	Thumbnail *string `json:"thumbnail"`
	AiredAt   *string `json:"airedAt"`
	Duration  *int    `json:"duration"`
	Filler    bool    `json:"filler"`
	Recap     bool    `json:"recap"`
}

type EpisodeList struct {
	Episodes []Episode `json:"episodes"`
}

type Manga struct {
	ID           int      `json:"id"`
	IDMal        *int     `json:"idMal"`
	Title        Title    `json:"title"`
	Description  string   `json:"description"`
	CoverImage   Image    `json:"coverImage"`
	BannerImage  *string  `json:"bannerImage"`
	Chapters     *int     `json:"chapters"`
	Volumes      *int     `json:"volumes"`
	Status       string   `json:"status"`
	Format       string   `json:"format"`
	Genres       []string `json:"genres"`
	AverageScore *int     `json:"averageScore"`
	MeanScore    *int     `json:"meanScore"`
	Staff        []Staff  `json:"staff"`
}

type Staff struct {
	Name Name `json:"name"`
}

type Name struct {
	Full string `json:"full"`
}

type Chapter struct {
	Number  int     `json:"number"`
	Title   *string `json:"title"`
	AiredAt *string `json:"airedAt"`
}

type ChapterList struct {
	Chapters []Chapter `json:"chapters"`
}

type Page struct {
	URL string `json:"url"`
}

type PageList struct {
	Pages []Page `json:"pages"`
}

type StreamRequest struct {
	AnimeID  int    `json:"animeId"`
	Slug     string `json:"slug,omitempty"` // frontend watch slug, e.g. one-piece-21
	Episode  int    `json:"episode"`
	Provider string `json:"provider"` // "miruro"
	Lang     string `json:"lang"`     // "sub" or "dub"
	Quality  string `json:"quality"`  // "auto", "1080p", "720p", etc.
	Refresh  bool   `json:"refresh"`  // bypass provider cache, try next internal provider
}

type StreamResult struct {
	Sources   []Source          `json:"sources"`
	Headers   map[string]string `json:"headers,omitempty"`
	Qualities []string          `json:"qualities,omitempty"`
	Servers   []Server          `json:"servers,omitempty"` // working Miruro sub-providers
	// Intro/Outro are Miruro-provided skip segments (seconds). The client
	// shows manual "Skip Intro / Skip Credits" buttons — never auto-skips.
	Intro *SkipTimestamp `json:"intro,omitempty"`
	Outro *SkipTimestamp `json:"outro,omitempty"`
}

type SkipTimestamp struct {
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

type Server struct {
	Name     string            `json:"name"`     // e.g. "kiwi", "ally", "bee"
	Provider string            `json:"provider"` // e.g. "miruro"
	Lang     string            `json:"lang"`     // "sub" or "dub"
	Sources  []Source          `json:"sources"`
	Headers  map[string]string `json:"headers,omitempty"`
}

type Source struct {
	URL       string     `json:"url"`
	Type      string     `json:"type"`
	Quality   string     `json:"quality"`
	Subtitles []Subtitle `json:"subtitles"`
	// Verification is a soft verdict ("proxy", "direct", "embed", "dead")
	// from the server-side playback probe. It is an ordering hint only —
	// CDNs serve different clients differently, so it never filters
	// providers. Clients use it to pick the most reliable path first.
	Verification string `json:"verification,omitempty"`
}

type Subtitle struct {
	URL   string `json:"url"`
	Lang  string `json:"lang"`
	Label string `json:"label"`
}

type SearchResult struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	URL      string `json:"url"`
	SubOrDub string `json:"subOrDub,omitempty"`
}

type ProgressRequest struct {
	Episode   int  `json:"episode"`
	Position  int  `json:"positionSec"`
	Completed bool `json:"completed"`
}

type MangaProgressRequest struct {
	Chapter   int  `json:"chapter"`
	Page      int  `json:"page"`
	Completed bool `json:"completed"`
}

type Profile struct {
	ID          string            `json:"id"`
	Username    string            `json:"username"`
	DisplayName string            `json:"displayName"`
	Bio         *string           `json:"bio"`
	AvatarURL   *string           `json:"avatarUrl"`
	BannerURL   *string           `json:"bannerUrl"`
	Location    *string           `json:"location"`
	Socials     map[string]string `json:"socials"`
	CreatedAt   time.Time         `json:"createdAt"`
}

type UserSettings struct {
	UserID string         `json:"userId"`
	Key    string         `json:"key"`
	Value  map[string]any `json:"value"`
}

type Favorite struct {
	UserID    string    `json:"userId"`
	MediaID   int       `json:"mediaId"`
	MediaType string    `json:"mediaType"`
	AddedAt   time.Time `json:"addedAt"`
}
