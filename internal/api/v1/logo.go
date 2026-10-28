package v1

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
	"github.com/go-chi/chi/v5"
)

// GetAnimeLogo serves the title logo (wordmark) for an AniList ID, mapped
// to TMDB through the same mapping the episode fetcher uses. 404 when the
// mapping or a logo is missing so the frontend can fall back to the title.
func (h *Handlers) GetAnimeLogo(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil || id <= 0 {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	logo := tmdb.FetchTmdbLogo(ctx, h.httpClient, h.cfg.TMDB.ReadAccessToken, id)
	if logo == "" {
		h.respondError(w, http.StatusNotFound, "logo not found")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"logo": logo})
}
