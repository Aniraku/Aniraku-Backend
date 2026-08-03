package anilist

type Anime struct {
	ID               int      `json:"id"`
	IDMal            *int     `json:"idMal"`
	IsAdult          bool     `json:"isAdult"`
	Title            Title    `json:"title"`
	Description      string   `json:"description"`
	CoverImage       Image    `json:"coverImage"`
	BannerImage      *string  `json:"bannerImage"`
	Episodes         *int     `json:"episodes"`
	Duration         *int     `json:"duration"`
	Status           string   `json:"status"`
	Format           string   `json:"format"`
	Season           *string  `json:"season"`
	SeasonYear       *int     `json:"seasonYear"`
	Genres           []string `json:"genres"`
	Studios          struct {
		Edges []struct {
			Node struct {
				Name string `json:"name"`
			} `json:"node"`
		} `json:"edges"`
	} `json:"studios"`
	Trailer *struct {
		ID   string `json:"id"`
		Site string `json:"site"`
	} `json:"trailer"`
	AverageScore       *int `json:"averageScore"`
	MeanScore          *int `json:"meanScore"`
	Popularity         int  `json:"popularity"`
	NextAiringEpisode  *struct {
		Episode    int  `json:"episode"`
		AiringAt   int  `json:"airingAt"`
		TimeUntilAiring int `json:"timeUntilAiring"`
	} `json:"nextAiringEpisode"`
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
	Color      *string `json:"color"`
}

type PageInfo struct {
	HasNextPage bool `json:"hasNextPage"`
	Total       int  `json:"total"`
	CurrentPage int  `json:"currentPage"`
	LastPage    int  `json:"lastPage"`
	PerPage     int  `json:"perPage"`
}

type BrowseResponse struct {
	Data struct {
		Page struct {
			PageInfo PageInfo `json:"pageInfo"`
			Media    []Anime  `json:"media"`
		} `json:"Page"`
	} `json:"data"`
}

type BrowseFilters struct {
	Genre    []string
	Format   []string
	Status   []string
	Season   string
	Year     int
	Sort     string
	Search   string
}

type RelationAnime struct {
	ID           int    `json:"id"`
	Type         string `json:"type"`
	Title        Title  `json:"title"`
	CoverImage   Image  `json:"coverImage"`
	Format       string `json:"format"`
	Episodes     *int   `json:"episodes"`
	Status       string `json:"status"`
	AverageScore *int   `json:"averageScore"`
}

type RelationEdge struct {
	RelationType string        `json:"relationType"`
	Node         RelationAnime `json:"node"`
}

type RelationsResponse struct {
	Relations []RelationEdge `json:"relations"`
}
