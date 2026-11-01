package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// notifyEpisode queues a background announcement for an episode that just
// resolved to a playable source. Nothing waits on it: GetServers and Stream
// must stay as fast as the source lookup itself.
func (h *Handlers) notifyEpisode(animeID, episode int) {
	if animeID <= 0 || episode <= 0 || h.cfg.Supabase.URL == "" || h.cfg.Supabase.ServiceKey == "" {
		return
	}
	key := strconv.Itoa(animeID) + ":" + strconv.Itoa(episode)
	if _, loaded := h.notifiedEpisodes.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := h.sendEpisodeNotifications(ctx, animeID, episode); err != nil {
			// Allow a later resolve to retry; the recipient pre-check keeps
			// retries idempotent.
			h.notifiedEpisodes.Delete(key)
			h.log.Debug().Err(err).Int("animeId", animeID).Int("episode", episode).Msg("episode notification skipped")
		}
	}()
}

func (h *Handlers) sendEpisodeNotifications(ctx context.Context, animeID, episode int) error {
	media, err := h.anilistAiringMedia(ctx, animeID)
	if err != nil {
		return err
	}
	next := 0
	if media.NextAiringEpisode != nil {
		next = media.NextAiringEpisode.Episode
	}
	if !episodeIsFresh(media.Status, media.Episodes, next, episode) {
		return nil
	}

	userIDs, bookmarkedTitle, err := h.bookmarkUsers(ctx, animeID)
	if err != nil || len(userIDs) == 0 {
		return err
	}
	title := bookmarkedTitle
	if title == "" {
		for _, candidate := range []string{media.Title.English, media.Title.Romaji, media.Title.UserPreferred} {
			if strings.TrimSpace(candidate) != "" {
				title = strings.TrimSpace(candidate)
				break
			}
		}
	}
	if title == "" {
		title = fmt.Sprintf("Anime %d", animeID)
	}

	// Must match the web client's wording exactly so both writers dedupe
	// against each other.
	message := fmt.Sprintf("Episode %d of %s is now available", episode, title)
	notified, err := h.notifiedUsers(ctx, animeID, message)
	if err != nil {
		return err
	}

	rows := make([]map[string]any, 0, len(userIDs))
	for _, userID := range userIDs {
		if notified[userID] {
			continue
		}
		rows = append(rows, map[string]any{
			"user_id":  userID,
			"type":     "new_episode",
			"message":  message,
			"anime_id": animeID,
		})
	}
	if len(rows) == 0 {
		return nil
	}

	const batch = 500
	for start := 0; start < len(rows); start += batch {
		end := start + batch
		if end > len(rows) {
			end = len(rows)
		}
		raw, _ := json.Marshal(rows[start:end])
		resp, err := h.supabaseServiceRequest(ctx, "POST", "/rest/v1/notifications",
			bytes.NewReader(raw), map[string]string{"Prefer": "return=minimal"})
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("%s", supabaseErrorBody(resp))
			resp.Body.Close()
			return err
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
	}
	return nil
}

type airingMedia struct {
	Status            string `json:"status"`
	Episodes          int    `json:"episodes"`
	NextAiringEpisode *struct {
		Episode int `json:"episode"`
	} `json:"nextAiringEpisode"`
	Title struct {
		English       string `json:"english"`
		Romaji        string `json:"romaji"`
		UserPreferred string `json:"userPreferred"`
	} `json:"title"`
}

func (h *Handlers) anilistAiringMedia(ctx context.Context, animeID int) (airingMedia, error) {
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { status episodes nextAiringEpisode { episode } title { english romaji userPreferred } } }`
	raw, err := h.anilistClient.do(ctx, query, map[string]any{"id": animeID})
	if err != nil {
		return airingMedia{}, err
	}
	var payload struct {
		Data struct {
			Media airingMedia `json:"Media"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return airingMedia{}, err
	}
	return payload.Data.Media, nil
}

// episodeIsFresh reports whether an episode is recent enough to announce.
// Only airing shows produce new episodes: the latest aired one is
// nextAiringEpisode-1, and nextAiringEpisode itself lets an early source for
// the upcoming episode through without announcing older backlog.
func episodeIsFresh(status string, episodes, nextAiringEpisode, episode int) bool {
	if status != "RELEASING" || episode <= 0 {
		return false
	}
	latest := episodes
	if nextAiringEpisode > 0 {
		latest = nextAiringEpisode - 1
	}
	return episode == latest || (nextAiringEpisode > 0 && episode == nextAiringEpisode)
}

// bookmarkUsers pages through every user who bookmarked the anime. The first
// bookmark title wins because bookmarks store the title the user actually
// sees, matching the web client's message.
func (h *Handlers) bookmarkUsers(ctx context.Context, animeID int) ([]string, string, error) {
	const pageSize = 1000
	var userIDs []string
	title := ""
	for offset := 0; offset < 100*pageSize; offset += pageSize {
		path := fmt.Sprintf("/rest/v1/bookmarks?select=user_id,title&anime_id=eq.%d&order=user_id&limit=%d&offset=%d",
			animeID, pageSize, offset)
		resp, err := h.supabaseServiceRequest(ctx, "GET", path, nil, nil)
		if err != nil {
			return nil, "", err
		}
		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("%s", supabaseErrorBody(resp))
			resp.Body.Close()
			return nil, "", err
		}
		var rows []struct {
			UserID string `json:"user_id"`
			Title  string `json:"title"`
		}
		err = json.NewDecoder(resp.Body).Decode(&rows)
		resp.Body.Close()
		if err != nil {
			return nil, "", err
		}
		for _, row := range rows {
			if row.UserID != "" {
				userIDs = append(userIDs, row.UserID)
			}
			if title == "" && strings.TrimSpace(row.Title) != "" {
				title = strings.TrimSpace(row.Title)
			}
		}
		if len(rows) < pageSize {
			break
		}
	}
	return userIDs, title, nil
}

// notifiedUsers returns the set of users who already have this exact
// notification, so a retry or the web client's writer never doubles it.
func (h *Handlers) notifiedUsers(ctx context.Context, animeID int, message string) (map[string]bool, error) {
	const pageSize = 1000
	filter := strings.ReplaceAll(url.QueryEscape(message), "+", "%20")
	notified := make(map[string]bool)
	for offset := 0; offset < 100*pageSize; offset += pageSize {
		path := fmt.Sprintf("/rest/v1/notifications?select=user_id&type=eq.new_episode&anime_id=eq.%d&message=eq.%s&order=user_id&limit=%d&offset=%d",
			animeID, filter, pageSize, offset)
		resp, err := h.supabaseServiceRequest(ctx, "GET", path, nil, nil)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("%s", supabaseErrorBody(resp))
			resp.Body.Close()
			return nil, err
		}
		var rows []struct {
			UserID string `json:"user_id"`
		}
		err = json.NewDecoder(resp.Body).Decode(&rows)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			notified[row.UserID] = true
		}
		if len(rows) < pageSize {
			break
		}
	}
	return notified, nil
}

func (h *Handlers) supabaseServiceRequest(ctx context.Context, method, path string, body io.Reader, headers map[string]string) (*http.Response, error) {
	key := h.cfg.Supabase.ServiceKey
	req, err := http.NewRequestWithContext(ctx, method, h.cfg.Supabase.URL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("apikey", key)
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return h.httpClient.Do(req)
}

// hasSources reports whether a server list contains anything playable.
func hasSources(servers []core.Server) bool {
	for _, server := range servers {
		for _, source := range server.Sources {
			if strings.TrimSpace(source.URL) != "" {
				return true
			}
		}
	}
	return false
}
