package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/auth"
)

// Import / Export of library lists between Aniraku favorites and the
// user's connected MAL / AniList accounts. Both directions reuse the
// OAuth tokens stored by the sync feature (Settings → Library Sync):
//
//	import  = pull the provider's list into Aniraku favorites
//	export  = push Aniraku favorites into the provider's library
//
// All endpoints are idempotent (upsert) and capped so a single request
// stays well inside provider rate limits and the platform's timeout.

const (
	importExportCap = 150 // max entries per request
	importBatchSize = 200 // favorites rows per Supabase POST
)

// requireProviderToken returns the user's stored token for a provider,
// refreshing it first if it is near expiry. The refreshed token is
// persisted so callers never operate on stale credentials.
func (h *Handlers) requireProviderToken(ctx context.Context, userID, provider string) (syncProviderToken, error) {
	tokens, err := h.loadSyncTokens(ctx, userID)
	if err != nil {
		return syncProviderToken{}, err
	}
	token, ok := tokens[provider]
	if !ok || token.AccessToken == "" {
		return syncProviderToken{}, fmt.Errorf("%s is not connected — connect it in Settings first", provider)
	}

	// Refresh when the token is within 5 minutes of expiring or already stale.
	if token.ExpiresAt > 0 && time.Now().Unix() > token.ExpiresAt-300 {
		var refreshed syncProviderToken
		switch provider {
		case "mal":
			refreshed, err = h.refreshMALToken(ctx, token)
		case "anilist":
			refreshed, err = h.refreshAniListToken(ctx, token)
		}
		if err == nil {
			token = refreshed
			_ = h.saveSyncToken(ctx, userID, provider, refreshed)
		} else {
			h.log.Warn().Err(err).Str("provider", provider).Msg("import/export: token refresh failed, using stored token")
		}
	}
	return token, nil
}

// importFavoriteDiff inserts only the ids not already in the user's
// bookmarks, returning (newly inserted, already present). Import stays
// idempotent while the UI can show what actually changed.
func (h *Handlers) importFavoriteDiff(ctx context.Context, userID string, ids []int) (int, int, error) {
	if len(ids) == 0 {
		return 0, 0, nil
	}
	existing, err := h.loadUserFavorites(ctx, userID)
	if err != nil {
		return 0, 0, err
	}
	have := make(map[int]bool, len(existing))
	for _, id := range existing {
		have[id] = true
	}
	fresh := make([]int, 0, len(ids))
	already := 0
	for _, id := range ids {
		if have[id] {
			already++
		} else {
			fresh = append(fresh, id)
		}
	}
	meta, err := h.fetchMediaMeta(ctx, fresh)
	if err != nil {
		h.log.Warn().Err(err).Msg("import: media metadata fetch failed, importing with fallback titles")
	}
	inserted, err := h.insertBookmarks(ctx, userID, fresh, meta)
	if err != nil {
		return 0, already, err
	}
	return inserted, already, nil
}

// mediaMeta holds the display fields the UI needs for a bookmarked title.
type mediaMeta struct {
	title string
	image string
}

// fetchMediaMeta resolves AniList IDs to title + cover image in batched
// GraphQL round trips so imported rows render properly in the UI.
func (h *Handlers) fetchMediaMeta(ctx context.Context, anilistIDs []int) (map[int]mediaMeta, error) {
	meta := make(map[int]mediaMeta, len(anilistIDs))
	for start := 0; start < len(anilistIDs); start += 50 {
		end := start + 50
		if end > len(anilistIDs) {
			end = len(anilistIDs)
		}
		query := `query ($ids: [Int]) {
			Page(perPage: 50) {
				media(id_in: $ids, type: ANIME) {
					id
					title { romaji english }
					coverImage { medium }
				}
			}
		}`
		raw, err := h.anilistClient.do(ctx, query, map[string]any{"ids": anilistIDs[start:end]})
		if err != nil {
			return nil, err
		}
		var out struct {
			Data struct {
				Page struct {
					Media []struct {
						ID    int `json:"id"`
						Title struct {
							Romaji  string `json:"romaji"`
							English string `json:"english"`
						} `json:"title"`
						CoverImage struct {
							Medium string `json:"medium"`
						} `json:"coverImage"`
					} `json:"media"`
				} `json:"Page"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		for _, m := range out.Data.Page.Media {
			if m.ID <= 0 {
				continue
			}
			title := m.Title.English
			if title == "" {
				title = m.Title.Romaji
			}
			if title == "" {
				title = fmt.Sprintf("Anime %d", m.ID)
			}
			meta[m.ID] = mediaMeta{title: title, image: m.CoverImage.Medium}
		}
	}
	return meta, nil
}

func (h *Handlers) insertBookmarks(ctx context.Context, userID string, ids []int, meta map[int]mediaMeta) (int, error) {
	inserted := 0
	for start := 0; start < len(ids); start += importBatchSize {
		end := start + importBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		rows := make([]map[string]any, 0, end-start)
		for _, id := range ids[start:end] {
			m := meta[id]
			title := m.title
			if title == "" {
				title = fmt.Sprintf("Anime %d", id)
			}
			rows = append(rows, map[string]any{
				"user_id":  userID,
				"anime_id": id,
				"title":    title,
				"image":    m.image,
				"added_at": time.Now().UnixMilli(),
			})
		}
		raw, _ := json.Marshal(rows)
		resp, err := h.supabaseRequest(ctx, "POST",
			"/rest/v1/bookmarks?on_conflict=user_id,anime_id",
			bytes.NewReader(raw),
			map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
		if err != nil {
			return inserted, err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			return inserted, fmt.Errorf("bookmarks insert returned %s", resp.Status)
		}
		inserted += end - start
	}
	return inserted, nil
}

func (h *Handlers) loadUserFavorites(ctx context.Context, userID string) ([]int, error) {
	resp, err := h.supabaseRequest(ctx, "GET",
		"/rest/v1/bookmarks?select=anime_id&user_id=eq."+encodePath(userID)+"&limit=500",
		nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bookmarks fetch returned %s", resp.Status)
	}
	var rows []struct {
		AnimeID int `json:"anime_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(rows))
	seen := map[int]bool{}
	for _, r := range rows {
		if r.AnimeID > 0 && !seen[r.AnimeID] {
			seen[r.AnimeID] = true
			ids = append(ids, r.AnimeID)
		}
	}
	return ids, nil
}

// ────────────────────────────────────────────────────────────────
// Import: provider list → Aniraku favorites
// ────────────────────────────────────────────────────────────────

// ImportMAL pulls the user's own MyAnimeList anime list (needs the MAL
// account connected in Settings) and adds every title to Aniraku favorites.
func (h *Handlers) ImportMAL(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !h.syncConfigured("mal") {
		h.respondError(w, http.StatusNotImplemented, "MAL sync is not configured on this server")
		return
	}
	token, err := h.requireProviderToken(r.Context(), userID, "mal")
	if err != nil {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}

	// Fetch the connected user's list, paginated by offset.
	malIDs := []int{}
	offset := 0
	for offset < 1000 {
		u := fmt.Sprintf("https://api.myanimelist.net/v2/users/@me/animelist?limit=100&offset=%d&fields=list_status", offset)
		req, err := http.NewRequestWithContext(r.Context(), "GET", u, nil)
		if err != nil {
			break
		}
		req.Header.Set("Authorization", "Bearer "+token.AccessToken)
		resp, err := h.h2Client.Do(req)
		if err != nil {
			h.respondError(w, http.StatusBadGateway, "could not reach MyAnimeList — try again")
			return
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			h.log.Warn().Msg("mal import: list fetch failed " + resp.Status)
			h.respondError(w, http.StatusBadGateway, "MyAnimeList rejected the request — reconnect the account in Settings")
			return
		}
		var page struct {
			Data []struct {
				Node struct {
					ID int `json:"id"`
				} `json:"node"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			h.respondError(w, http.StatusBadGateway, "could not read the MyAnimeList response")
			return
		}
		if len(page.Data) == 0 {
			break
		}
		for _, e := range page.Data {
			if e.Node.ID > 0 {
				malIDs = append(malIDs, e.Node.ID)
			}
		}
		offset += len(page.Data)
	}

	anilistIDs, err := h.resolveMalIDsToAniList(r.Context(), malIDs)
	if err != nil {
		h.log.Warn().Err(err).Msg("mal import: id mapping failed")
		h.respondError(w, http.StatusBadGateway, "could not map your list to Aniraku IDs")
		return
	}
	ids := make([]int, 0, len(malIDs))
	for _, malID := range malIDs {
		if anID, ok := anilistIDs[malID]; ok {
			ids = append(ids, anID)
		}
	}

	inserted, already, err := h.importFavoriteDiff(r.Context(), userID, ids)
	if err != nil {
		h.log.Warn().Err(err).Msg("mal import: favorites insert failed")
		h.respondError(w, http.StatusBadGateway, "could not save your imported list")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"provider": "mal",
		"imported": inserted,
		"already":  already,
		"total":    len(ids),
	})
}

// ImportAniList pulls the user's AniList anime list (needs the AniList
// account connected in Settings) and adds every title to favorites.
func (h *Handlers) ImportAniList(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !h.syncConfigured("anilist") {
		h.respondError(w, http.StatusNotImplemented, "AniList sync is not configured on this server")
		return
	}
	token, err := h.requireProviderToken(r.Context(), userID, "anilist")
	if err != nil {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}

	query := `query ($userId: Int) {
		Viewer { id }
		MediaListCollection(userId: $userId, type: ANIME) {
			lists { entries { mediaId } }
		}
	}`
	raw, err := h.anilistAuthed(r.Context(), token.AccessToken, query, map[string]any{})
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "could not reach AniList — try again")
		return
	}
	var out struct {
		Data struct {
			Viewer struct {
				ID int `json:"id"`
			} `json:"Viewer"`
			MediaListCollection struct {
				Lists []struct {
					Entries []struct {
						MediaID int `json:"mediaId"`
					} `json:"entries"`
				} `json:"lists"`
			} `json:"MediaListCollection"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		h.respondError(w, http.StatusBadGateway, "could not read the AniList response")
		return
	}
	ids := []int{}
	seen := map[int]bool{}
	for _, list := range out.Data.MediaListCollection.Lists {
		for _, e := range list.Entries {
			if e.MediaID > 0 && !seen[e.MediaID] {
				seen[e.MediaID] = true
				ids = append(ids, e.MediaID)
			}
		}
	}
	if len(ids) == 0 && out.Data.Viewer.ID == 0 {
		h.respondError(w, http.StatusUnauthorized, "AniList token is invalid — reconnect the account in Settings")
		return
	}
	inserted, already, err := h.importFavoriteDiff(r.Context(), userID, ids)
	if err != nil {
		h.log.Warn().Err(err).Msg("anilist import: favorites insert failed")
		h.respondError(w, http.StatusBadGateway, "could not save your imported list")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"provider": "anilist",
		"imported": inserted,
		"already":  already,
		"total":    len(ids),
	})
}

// ────────────────────────────────────────────────────────────────
// Export: Aniraku favorites → provider library
// ────────────────────────────────────────────────────────────────

// ExportMAL pushes Aniraku anime favorites into the user's connected
// MyAnimeList library as "completed". Capped at importExportCap entries
// per request to stay inside MAL's 60 req/min limit.
func (h *Handlers) ExportMAL(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !h.syncConfigured("mal") {
		h.respondError(w, http.StatusNotImplemented, "MAL sync is not configured on this server")
		return
	}
	token, err := h.requireProviderToken(r.Context(), userID, "mal")
	if err != nil {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}
	anilistIDs, err := h.loadUserFavorites(r.Context(), userID)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "could not read your favorites")
		return
	}
	favoriteCount := len(anilistIDs)
	if len(anilistIDs) > importExportCap {
		anilistIDs = anilistIDs[:importExportCap]
	}

	// AniList IDs → MAL IDs (batched, single GraphQL round trip each).
	malIDs, err := h.resolveAniListIDsToMAL(r.Context(), anilistIDs)
	if err != nil {
		h.log.Warn().Err(err).Msg("mal export: id mapping failed")
		h.respondError(w, http.StatusBadGateway, "could not map your favorites to MAL IDs")
		return
	}

	// Skip titles already marked completed on MAL — no pointless writes.
	completed, err := h.fetchMALCompletedSet(r.Context(), token.AccessToken)
	if err != nil {
		h.log.Warn().Err(err).Msg("mal export: completed set fetch failed, exporting all")
	}

	exported, skipped, failed := 0, 0, 0
	limited := favoriteCount > importExportCap
	for i, malID := range malIDs {
		if i > 0 && i%3 == 0 {
			select {
			case <-time.After(1100 * time.Millisecond):
			case <-r.Context().Done():
				h.respondError(w, http.StatusGatewayTimeout, "export interrupted")
				return
			}
		}
		if completed[malID] {
			skipped++
			continue
		}
		form := url.Values{}
		form.Set("status", "completed")
		req, err := http.NewRequestWithContext(r.Context(), "PUT",
			fmt.Sprintf("https://api.myanimelist.net/v2/anime/%d/my_list_status", malID),
			strings.NewReader(form.Encode()))
		if err != nil {
			continue
		}
		req.Header.Set("Authorization", "Bearer "+token.AccessToken)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		resp, err := h.h2Client.Do(req)
		if err != nil {
			failed++
			continue
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			exported++
		} else {
			failed++
		}
	}
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"provider": "mal",
		"exported": exported,
		"skipped":  skipped,
		"failed":   failed,
		"total":    len(anilistIDs),
		"limited":  limited,
	})
}

// ExportAniList pushes Aniraku anime favorites into the user's connected
// AniList library as COMPLETED. Capped at importExportCap per request.
func (h *Handlers) ExportAniList(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if !h.syncConfigured("anilist") {
		h.respondError(w, http.StatusNotImplemented, "AniList sync is not configured on this server")
		return
	}
	token, err := h.requireProviderToken(r.Context(), userID, "anilist")
	if err != nil {
		h.respondError(w, http.StatusConflict, err.Error())
		return
	}
	ids, err := h.loadUserFavorites(r.Context(), userID)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "could not read your favorites")
		return
	}
	if len(ids) > importExportCap {
		ids = ids[:importExportCap]
	}

	query := `mutation ($id: Int) {
		SaveMediaListEntry(mediaId: $id, status: COMPLETED) { id }
	}`
	// Skip titles already marked completed on AniList — no pointless writes.
	completed, err := h.fetchAniListCompletedSet(r.Context(), token.AccessToken)
	if err != nil {
		h.log.Warn().Err(err).Msg("anilist export: completed set fetch failed, exporting all")
	}

	exported, skipped, failed := 0, 0, 0
	limited := len(ids) > importExportCap
	for i, id := range ids {
		if i > 0 && i%3 == 0 {
			select {
			case <-time.After(1100 * time.Millisecond):
			case <-r.Context().Done():
				h.respondError(w, http.StatusGatewayTimeout, "export interrupted")
				return
			}
		}
		if completed[id] {
			skipped++
			continue
		}
		raw, err := h.anilistAuthed(r.Context(), token.AccessToken, query, map[string]any{"id": id})
		if err != nil {
			failed++
			continue
		}
		var out struct {
			Errors []struct {
				Message string `json:"message"`
			} `json:"errors"`
		}
		if json.Unmarshal(raw, &out) == nil && len(out.Errors) == 0 {
			exported++
		} else {
			failed++
		}
	}
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"provider": "anilist",
		"exported": exported,
		"skipped":  skipped,
		"failed":   failed,
		"total":    len(ids),
		"limited":  limited,
	})
}

// fetchAniListCompletedSet returns the set of AniList ids the user has
// already marked completed.
func (h *Handlers) fetchAniListCompletedSet(ctx context.Context, accessToken string) (map[int]bool, error) {
	query := `query ($status: MediaListStatus) {
		Viewer {
			mediaListCollection(type: ANIME, status: $status) {
				lists { entries { mediaId } }
			}
		}
	}`
	raw, err := h.anilistAuthed(ctx, accessToken, query, map[string]any{"status": "COMPLETED"})
	if err != nil {
		return nil, err
	}
	var out struct {
		Data struct {
			Viewer struct {
				MediaListCollection struct {
					Lists []struct {
						Entries []struct {
							MediaID int `json:"mediaId"`
						} `json:"entries"`
					} `json:"lists"`
				} `json:"mediaListCollection"`
			} `json:"Viewer"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	completed := map[int]bool{}
	for _, list := range out.Data.Viewer.MediaListCollection.Lists {
		for _, e := range list.Entries {
			if e.MediaID > 0 {
				completed[e.MediaID] = true
			}
		}
	}
	return completed, nil
}

// resolveAniListIDsToMAL maps a batch of AniList IDs to MAL IDs in a single
// GraphQL round trip (media id_in). IDs without a mapping are dropped.
func (h *Handlers) resolveAniListIDsToMAL(ctx context.Context, anilistIDs []int) ([]int, error) {
	mapped := map[int]int{}
	for start := 0; start < len(anilistIDs); start += 50 {
		end := start + 50
		if end > len(anilistIDs) {
			end = len(anilistIDs)
		}
		query := `query ($ids: [Int]) { Page(perPage: 50) { media(id_in: $ids, type: ANIME) { id idMal } } }`
		raw, err := h.anilistClient.do(ctx, query, map[string]any{"ids": anilistIDs[start:end]})
		if err != nil {
			return nil, err
		}
		var out struct {
			Data struct {
				Page struct {
					Media []struct {
						ID    int `json:"id"`
						IDMal int `json:"idMal"`
					} `json:"media"`
				} `json:"Page"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		for _, m := range out.Data.Page.Media {
			if m.ID > 0 && m.IDMal > 0 {
				mapped[m.ID] = m.IDMal
			}
		}
	}
	malIDs := make([]int, 0, len(anilistIDs))
	for _, id := range anilistIDs {
		if malID, ok := mapped[id]; ok {
			malIDs = append(malIDs, malID)
		}
	}
	return malIDs, nil
}

// fetchMALCompletedSet returns the set of MAL anime ids the user has
// already marked completed, paginated by offset (status filter + fields).
func (h *Handlers) fetchMALCompletedSet(ctx context.Context, accessToken string) (map[int]bool, error) {
	completed := map[int]bool{}
	offset := 0
	for offset < 1000 {
		u := fmt.Sprintf("https://api.myanimelist.net/v2/users/@me/animelist?limit=100&offset=%d&status=completed&fields=list_status", offset)
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			break
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)
		resp, err := h.h2Client.Do(req)
		if err != nil {
			return completed, err
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return completed, fmt.Errorf("mal list fetch returned %s", resp.Status)
		}
		var page struct {
			Data []struct {
				Node struct {
					ID int `json:"id"`
				} `json:"node"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return completed, err
		}
		if len(page.Data) == 0 {
			break
		}
		for _, e := range page.Data {
			if e.Node.ID > 0 {
				completed[e.Node.ID] = true
			}
		}
		offset += len(page.Data)
	}
	return completed, nil
}

// anilistAuthed POSTs a GraphQL request to AniList with a user token.
func (h *Handlers) anilistAuthed(ctx context.Context, accessToken, query string, variables map[string]any) ([]byte, error) {
	payload, _ := json.Marshal(map[string]any{"query": query, "variables": variables})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://graphql.anilist.co", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anilist returned %d", resp.StatusCode)
	}
	return raw, nil
}
