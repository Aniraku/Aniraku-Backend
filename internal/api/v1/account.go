package v1

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Aniraku/Aniraku-Backend/internal/metadata/anilist"

	"github.com/Aniraku/Aniraku-Backend/internal/auth"
)

func (h *Handlers) SaveAnimeProgress(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	animeID, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime id")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	defer r.Body.Close()
	var input struct {
		Episode   int  `json:"episode"`
		Position  int  `json:"position_sec"`
		Completed bool `json:"completed"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			h.respondError(w, http.StatusBadRequest, "invalid progress payload")
			return
		}
	}

	payload := []map[string]any{{
		"user_id":      userID,
		"anime_id":     animeID,
		"episode":      input.Episode,
		"position_sec": input.Position,
		"completed":    input.Completed,
		"updated_at":   "now()",
	}}
	raw, _ := json.Marshal(payload)

	resp, err := h.supabaseRequest(r.Context(), "POST",
		"/rest/v1/anime_progress?on_conflict=user_id,anime_id",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("anime progress upsert failed")
		h.respondError(w, http.StatusBadGateway, "failed to save progress")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		h.log.Warn().Str("detail", supabaseErrorBody(resp)).Msg("anime progress upsert rejected")
		h.respondError(w, http.StatusBadGateway, "failed to save progress")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]string{"status": "saved"})
}

type watchHistoryEntry struct {
	AnimeID    int     `json:"anime_id"`
	AnimeTitle string  `json:"anime_title"`
	AnimeImage string  `json:"anime_image"`
	Episode    int     `json:"episode_number"`
	Progress   float64 `json:"progress"`
	Duration   float64 `json:"duration"`
	Timestamp  int64   `json:"timestamp"`
}

type continueWatchingItem struct {
	AnimeID   int     `json:"animeId"`
	Title     string  `json:"title"`
	Image     string  `json:"image"`
	Episode   int     `json:"episode"`
	Time      float64 `json:"time"`
	Duration  float64 `json:"duration"`
	Timestamp int64   `json:"timestamp"`
}

func (h *Handlers) GetContinueWatching(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/watch_history?select=anime_id,anime_title,anime_image,episode_number,progress,duration,timestamp&user_id=eq."+encodePath(userID)+"&order=timestamp.desc&limit=30",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("continue-watching fetch failed")
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}

	var history []watchHistoryEntry
	if err := json.NewDecoder(resp.Body).Decode(&history); err != nil {
		h.respondJSON(w, http.StatusOK, []continueWatchingItem{})
		return
	}

	items := make([]continueWatchingItem, 0, len(history))
	for _, entry := range history {
		items = append(items, continueWatchingItem{
			AnimeID:   entry.AnimeID,
			Title:     entry.AnimeTitle,
			Image:     entry.AnimeImage,
			Episode:   entry.Episode,
			Time:      entry.Progress,
			Duration:  entry.Duration,
			Timestamp: entry.Timestamp,
		})
	}

	h.respondJSON(w, http.StatusOK, items)
}

// AdminStats relays the server-gated admin dashboard stats. The route is
// protected by auth.RequireAdmin (server-side is_admin() check against
// Supabase); this handler then calls Supabase's admin_stats() RPC with the
// caller's JWT, which raises unless the caller's role is admin. Belt and
// suspenders: even if middleware regresses, the RPC still enforces it.
func (h *Handlers) AdminStats(w http.ResponseWriter, r *http.Request) {
	resp, err := h.supabaseRequest(r.Context(), "POST", "/rest/v1/rpc/admin_stats", nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("admin_stats rpc failed")
		h.respondError(w, http.StatusInternalServerError, "failed to load admin stats")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusForbidden, "insufficient privileges")
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		h.log.Warn().Err(err).Msg("admin_stats read failed")
		h.respondError(w, http.StatusInternalServerError, "failed to load admin stats")
		return
	}
	h.respondJSON(w, http.StatusOK, json.RawMessage(body))
}

func (h *Handlers) Search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		h.respondError(w, http.StatusBadRequest, "query parameter 'q' is required")
		return
	}

	page := parsePageParam(r, 1)
	perPage := parsePerPageParam(r, 20)

	if strings.EqualFold(q, "uncensored") {
		results, err := h.browseAdult(r.Context(), page, perPage)
		if err != nil {
			h.log.Warn().Err(err).Msg("adult search failed")
			h.respondError(w, http.StatusBadGateway, "search failed")
			return
		}
		for i := range results.Data.Page.Media {
			suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
		}
		h.respondJSON(w, http.StatusOK, map[string]any{
			"query":    q,
			"results":  results.Data.Page.Media,
			"pageInfo": results.Data.Page.PageInfo,
		})
		return
	}

	// If filter params are present, use Browse with the search term for richer suggestions
	genres := r.URL.Query()["genre"]
	formats := r.URL.Query()["format"]
	statuses := r.URL.Query()["status"]
	if len(genres) > 0 || len(formats) > 0 || len(statuses) > 0 {
		filters := anilist.BrowseFilters{
			Genre:  genres,
			Format: formats,
			Status: statuses,
			Search: q,
			Sort:   "SEARCH_MATCH",
		}
		results, err := h.browseAniList(r.Context(), filters, page, perPage)
		if err != nil {
			h.log.Warn().Err(err).Str("query", q).Msg("anilist filtered search failed, falling back to jikan")
			results, err = h.mal.Browse(r.Context(), filters, page, perPage)
			if err == nil {
				// Jikan results are MAL-keyed; rekey onto AniList IDs so the
				// watch flow (stream/episodes) works unchanged.
				if nerr := h.normalizeSearchResults(r.Context(), results.Data.Page.Media); nerr != nil {
					h.log.Warn().Err(nerr).Msg("failed to normalize jikan browse results")
				}
			}
		}
		if err != nil {
			h.log.Warn().Err(err).Str("query", q).Msg("filtered search failed")
			h.respondError(w, http.StatusBadGateway, "search failed")
			return
		}
		for i := range results.Data.Page.Media {
			if results.Data.Page.Media[i].IsAdult {
				suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
			}
		}
		h.respondJSON(w, http.StatusOK, map[string]any{
			"query":    q,
			"results":  results.Data.Page.Media,
			"pageInfo": results.Data.Page.PageInfo,
		})
		return
	}

	results, err := h.mal.Search(r.Context(), q, page, perPage)
	if err != nil {
		h.log.Warn().Err(err).Str("query", q).Msg("jikan search failed, falling back to anilist")
		// fallback: use AniList search via browseAniList with search term
		fallbackFilters := anilist.BrowseFilters{Search: q, Sort: "SEARCH_MATCH"}
		results, err = h.browseAniList(r.Context(), fallbackFilters, page, perPage)
	} else {
		// Jikan results are MAL-keyed; rekey onto AniList IDs so the watch
		// flow (stream/episodes) works unchanged.
		if nerr := h.normalizeSearchResults(r.Context(), results.Data.Page.Media); nerr != nil {
			h.log.Warn().Err(nerr).Msg("failed to normalize jikan search results")
		}
	}
	if err != nil {
		h.log.Warn().Err(err).Str("query", q).Msg("search failed")
		h.respondError(w, http.StatusBadGateway, "search failed")
		return
	}

	for i := range results.Data.Page.Media {
		if results.Data.Page.Media[i].IsAdult {
			suffixTitle(&results.Data.Page.Media[i].Title, "Uncensored")
		}
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"query":    q,
		"results":  results.Data.Page.Media,
		"pageInfo": results.Data.Page.PageInfo,
	})
}

// ImportMAL and ImportAniList are implemented in importexport.go.
// ImportStatus reports on an import/export job; jobs run synchronously
// today, so this is a compatibility stub.
func (h *Handlers) ImportStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	h.respondJSON(w, http.StatusOK, map[string]string{"jobId": jobID, "status": "pending"})
}

func (h *Handlers) GetProfile(w http.ResponseWriter, r *http.Request) {
	username := chi.URLParam(r, "username")
	if username == "" {
		h.respondError(w, http.StatusBadRequest, "username required")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/profiles?select=username,display_name,avatar_url,bio,location,socials,created_at&username=eq."+url.QueryEscape(username)+"&limit=1",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("profile fetch failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch profile")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.respondError(w, http.StatusNotFound, "profile not found")
		return
	}

	var profiles []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&profiles); err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to decode profile")
		return
	}
	if len(profiles) == 0 {
		h.respondError(w, http.StatusNotFound, "profile not found")
		return
	}
	h.respondJSON(w, http.StatusOK, profiles[0])
}

func (h *Handlers) UpdateProfile(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	r.Body.Close()
	var input map[string]any
	if err := json.Unmarshal(body, &input); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid profile payload")
		return
	}
	// Only known profile columns may be written — a caller-supplied map
	// could otherwise carry id, email, role or any other column straight
	// into the PostgREST PATCH.
	allowed := map[string]bool{
		"username":     true,
		"display_name": true,
		"avatar_url":   true,
		"bio":          true,
		"location":     true,
		"socials":      true,
	}
	filtered := make(map[string]any, len(input))
	for key, value := range input {
		if allowed[key] {
			filtered[key] = value
		}
	}
	if len(filtered) == 0 {
		h.respondError(w, http.StatusBadRequest, "no fields to update")
		return
	}
	filtered["updated_at"] = "now()"
	raw, _ := json.Marshal(filtered)

	resp, err := h.supabaseRequest(r.Context(), "PATCH",
		"/rest/v1/profiles?id=eq."+encodePath(userID),
		bytes.NewReader(raw), nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("profile update failed")
		h.respondError(w, http.StatusBadGateway, "failed to update profile")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to update profile")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *Handlers) AddFavorite(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	r.Body.Close()
	var input struct {
		MediaID   int    `json:"mediaId"`
		MediaType string `json:"mediaType"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.MediaID <= 0 {
		h.respondError(w, http.StatusBadRequest, "valid mediaId required")
		return
	}
	if input.MediaType == "" {
		input.MediaType = "anime"
	}
	if input.MediaType != "anime" && input.MediaType != "manga" {
		h.respondError(w, http.StatusBadRequest, "mediaType must be anime or manga")
		return
	}

	raw, _ := json.Marshal([]map[string]any{{
		"user_id":    userID,
		"media_id":   input.MediaID,
		"media_type": input.MediaType,
	}})

	resp, err := h.supabaseRequest(r.Context(), "POST",
		"/rest/v1/favorites?on_conflict=user_id,media_id,media_type",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("favorite add failed")
		h.respondError(w, http.StatusBadGateway, "failed to add favorite")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to add favorite")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "added"})
}

func (h *Handlers) RemoveFavorite(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	mediaID, err := strconv.Atoi(chi.URLParam(r, "mediaId"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid media id")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "DELETE",
		"/rest/v1/favorites?user_id=eq."+encodePath(userID)+"&media_id=eq."+strconv.Itoa(mediaID),
		nil, map[string]string{"Prefer": "return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("favorite remove failed")
		h.respondError(w, http.StatusBadGateway, "failed to remove favorite")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to remove favorite")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func (h *Handlers) ListFavorites(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/favorites?select=media_id,media_type,added_at&user_id=eq."+encodePath(userID)+"&order=added_at.desc&limit=200",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("favorites fetch failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch favorites")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to fetch favorites")
		return
	}

	var favorites []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&favorites); err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to decode favorites")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{"favorites": favorites})
}

func (h *Handlers) ClientLog(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	r.Body.Close()
	var entry struct {
		Level   string `json:"level"`
		Message string `json:"message"`
		Data    any    `json:"data"`
	}
	_ = json.Unmarshal(body, &entry)
	msg := entry.Message
	if msg == "" {
		msg = "(empty client log)"
	}
	h.log.Info().Str("user", userID).Str("level", entry.Level).Any("data", entry.Data).Msg("client log: " + msg)
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "received"})
}

func (h *Handlers) GetSetting(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	key := chi.URLParam(r, "key")
	if key == "" {
		h.respondError(w, http.StatusBadRequest, "key required")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/user_settings?select=value&user_id=eq."+encodePath(userID)+"&key=eq."+url.QueryEscape(key)+"&limit=1",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("setting fetch failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch setting")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.respondJSON(w, http.StatusOK, map[string]string{"key": key, "value": ""})
		return
	}

	var rows []struct {
		Value any `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		h.respondJSON(w, http.StatusOK, map[string]string{"key": key, "value": ""})
		return
	}
	if len(rows) == 0 {
		h.respondJSON(w, http.StatusOK, map[string]string{"key": key, "value": ""})
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{"key": key, "value": rows[0].Value})
}

func (h *Handlers) UpdateSetting(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	key := chi.URLParam(r, "key")
	if key == "" {
		h.respondError(w, http.StatusBadRequest, "key required")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	r.Body.Close()
	var input struct {
		Value any `json:"value"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid setting payload")
		return
	}

	raw, _ := json.Marshal([]map[string]any{{
		"user_id":    userID,
		"key":        key,
		"value":      input.Value,
		"updated_at": "now()",
	}})

	resp, err := h.supabaseRequest(r.Context(), "POST",
		"/rest/v1/user_settings?on_conflict=user_id,key",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("setting upsert failed")
		h.respondError(w, http.StatusBadGateway, "failed to save setting")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to save setting")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

func (h *Handlers) GetNotifications(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/notifications?select=id,type,message,anime_id,read,created_at&user_id=eq."+encodePath(userID)+"&order=created_at.desc&limit=50",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("notifications fetch failed")
		h.respondJSON(w, http.StatusOK, []any{})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondJSON(w, http.StatusOK, []any{})
		return
	}

	var notifications []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&notifications); err != nil {
		h.respondJSON(w, http.StatusOK, []any{})
		return
	}
	h.respondJSON(w, http.StatusOK, notifications)
}

func (h *Handlers) MarkNotificationRead(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	notifID := chi.URLParam(r, "id")
	if notifID == "" {
		h.respondError(w, http.StatusBadRequest, "notification id required")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "PATCH",
		"/rest/v1/notifications?id=eq."+encodePath(notifID)+"&user_id=eq."+encodePath(userID),
		bytes.NewReader([]byte(`{"read":true}`)), nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("notification read failed")
		h.respondError(w, http.StatusBadGateway, "failed to update notification")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to update notification")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "marked"})
}

func (h *Handlers) MarkAllNotificationsRead(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "PATCH",
		"/rest/v1/notifications?user_id=eq."+encodePath(userID)+"&read=eq.false",
		bytes.NewReader([]byte(`{"read":true}`)), nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("notifications read-all failed")
		h.respondError(w, http.StatusBadGateway, "failed to update notifications")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to update notifications")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]string{"status": "all marked"})
}

func (h *Handlers) SaveEpisodeRating(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	animeID, err := strconv.Atoi(chi.URLParam(r, "animeId"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime id")
		return
	}
	episode, err := strconv.Atoi(chi.URLParam(r, "episode"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid episode number")
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	r.Body.Close()
	var input struct {
		Score int `json:"score"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.Score < 1 || input.Score > 10 {
		h.respondError(w, http.StatusBadRequest, "score must be between 1 and 10")
		return
	}

	raw, _ := json.Marshal([]map[string]any{{
		"user_id":        userID,
		"anime_id":       animeID,
		"episode_number": episode,
		"score":          input.Score,
		"created_at":     time.Now().UTC().Format(time.RFC3339),
	}})
	resp, err := h.supabaseRequest(r.Context(), "POST",
		"/rest/v1/episode_ratings?on_conflict=user_id,anime_id,episode_number",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		h.log.Warn().Err(err).Msg("episode rating save failed")
		h.respondError(w, http.StatusBadGateway, "failed to save rating")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to save rating")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{"status": "saved", "anime_id": animeID, "episode": episode, "score": input.Score})
}

func (h *Handlers) GetEpisodeRatings(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	animeID, err := strconv.Atoi(chi.URLParam(r, "animeId"))
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid anime id")
		return
	}

	resp, err := h.supabaseRequest(r.Context(), "GET",
		"/rest/v1/episode_ratings?select=episode_number,score&user_id=eq."+encodePath(userID)+"&anime_id=eq."+strconv.Itoa(animeID)+"&order=episode_number.asc",
		nil, nil)
	if err != nil {
		h.log.Warn().Err(err).Msg("episode ratings fetch failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch ratings")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.log.Warn().Msg(supabaseErrorBody(resp))
		h.respondError(w, http.StatusBadGateway, "failed to fetch ratings")
		return
	}
	var ratings []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&ratings); err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to decode ratings")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{"ratings": ratings})
}
