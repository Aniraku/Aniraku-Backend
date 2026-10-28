package v1

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/metadata/anilist"
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
	"github.com/go-chi/chi/v5"
)

// Logo lookups chain several upstreams (AniList gate, AniBridge, Fribb,
// TMDB search, TMDB images), so results are memoized: logos are static
// assets, unlike the ephemeral streaming tokens.
const (
	logoPositiveTTL = 24 * time.Hour
	logoNegativeTTL = 2 * time.Hour
)

type logoCacheEntry struct {
	logo string
	at   time.Time
	miss bool
}

var logoCache sync.Map // anime id -> logoCacheEntry

// GetAnimeLogo serves the title logo (wordmark) for an AniList ID. The ID
// must exist on AniList first; the logo is then mapped to TMDB through the
// same mapping the episode fetcher uses, falling back to a TMDB title
// search for titles no mapping service knows yet (brand-new seasons).
// 404 when the anime is unknown or no logo exists, so the frontend can
// fall back to the title.
func (h *Handlers) GetAnimeLogo(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil || id <= 0 {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}
	if e, ok := logoCache.Load(id); ok {
		entry := e.(logoCacheEntry)
		ttl := logoPositiveTTL
		expiry := entry.logo
		if entry.miss {
			ttl = logoNegativeTTL
			expiry = ""
		}
		if time.Since(entry.at) < ttl {
			if expiry == "" {
				h.respondError(w, http.StatusNotFound, "logo not found")
				return
			}
			h.respondJSON(w, http.StatusOK, map[string]string{"logo": expiry})
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	// The AniList gate and the mapping/logo chain are independent: run
	// them concurrently so the hops overlap instead of adding up.
	type gate struct {
		anime *anilist.Anime
		err   error
	}
	gateCh := make(chan gate, 1)
	go func() {
		anime, err := h.getAnimeFromAniList(ctx, id)
		gateCh <- gate{anime, err}
	}()
	logo := tmdb.FetchTmdbLogo(ctx, h.httpClient, h.cfg.TMDB.ReadAccessToken, id)
	g := <-gateCh
	if g.err != nil || g.anime == nil {
		logoCache.Store(id, logoCacheEntry{at: time.Now(), miss: true})
		h.respondError(w, http.StatusNotFound, "anime not found")
		return
	}
	if logo == "" {
		// No mapping knows this title: fall back to TMDB title search.
		logo = h.logoByTitle(ctx, g.anime)
	}
	if logo == "" {
		logoCache.Store(id, logoCacheEntry{at: time.Now(), miss: true})
		h.respondError(w, http.StatusNotFound, "logo not found")
		return
	}
	logoCache.Store(id, logoCacheEntry{logo: logo, at: time.Now()})
	h.respondJSON(w, http.StatusOK, map[string]string{"logo": logo})
}

// logoByTitle resolves a TMDB logo via the AniList titles when no mapping
// service has the ID yet. English is tried first because TMDB indexes
// English names; MOVIE formats search TMDB movies, everything else TV.
func (h *Handlers) logoByTitle(ctx context.Context, anime *anilist.Anime) string {
	kind := "tv"
	if anime.Format == "MOVIE" {
		kind = "movie"
	}
	seen := make(map[string]bool, 3)
	for _, t := range []*string{anime.Title.English, anime.Title.UserPreferred, anime.Title.Romaji} {
		if t == nil || *t == "" || seen[*t] {
			continue
		}
		seen[*t] = true
		if logo := tmdb.FetchTmdbLogoByTitle(ctx, h.httpClient, h.cfg.TMDB.ReadAccessToken, kind, *t); logo != "" {
			return logo
		}
	}
	return ""
}
