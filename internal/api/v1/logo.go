package v1

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/metadata/anilist"
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
	"github.com/go-chi/chi/v5"
)

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
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	// Existence gate first: AniList is the source of truth for IDs.
	anime, err := h.getAnimeFromAniList(ctx, id)
	if err != nil || anime == nil {
		h.respondError(w, http.StatusNotFound, "anime not found")
		return
	}
	logo := tmdb.FetchTmdbLogo(ctx, h.httpClient, h.cfg.TMDB.ReadAccessToken, id)
	if logo == "" {
		// No mapping knows this title: fall back to TMDB title search.
		logo = h.logoByTitle(ctx, anime)
	}
	if logo == "" {
		h.respondError(w, http.StatusNotFound, "logo not found")
		return
	}
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
