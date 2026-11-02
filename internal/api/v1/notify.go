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
	"sync"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// Notification pipeline (2026-11 redesign).
//
// Two triggers feed the same writer:
//  1. On-demand: a user resolves sources for an episode (GetServers/Stream)
//     and notifyEpisode announces it to every bookmarked user.
//  2. Scheduled: an airing book tracks when each bookmarked RELEASING show's
//     next episode airs; ~75 minutes after airtime (subs take time to land)
//     the notifier asks MEGAPLAY ONLY whether a source exists — one provider,
//     one upstream call — and retries every 5 minutes for up to 3 hours.
//     Found => announce. Not found by then => stop; the on-demand trigger
//     remains the backstop for late sources.
//
// The old design polled the first 500 bookmarked shows every 5 minutes and
// gated everything on a single third-party site (reanime.to), so most
// episodes were simply never announced. The airing book costs one AniList
// request per bookmarked show per refresh cycle (paced well under AniList's
// rate limit) and one Megaplay call per airing event.

const (
	// notifierFirstCheckDelay is how long after airtime the first Megaplay
	// source call happens. Subs typically land 30-120 minutes after airing;
	// 75 minutes catches the majority on the first attempt.
	notifierFirstCheckDelay = 75 * time.Minute
	// notifierRetryDelay is the cadence of follow-up source calls while a
	// freshly aired episode has no Megaplay source yet.
	notifierRetryDelay = 5 * time.Minute
	// notifierGiveUpAfter bounds the retry window; past it the episode is
	// marked dead and only the on-demand play trigger can announce it.
	notifierGiveUpAfter = 3 * time.Hour
	// scheduleRefreshInterval paces AniList airing lookups: one bookmarked
	// show per tick keeps a 10k-show book cycling in ~3.3 hours, far under
	// AniList's rate limit, and airtimes rarely change faster.
	scheduleRefreshInterval = 1200 * time.Millisecond
	// scheduleStaleAfter is how old a book entry's airing data may get
	// before the refresher re-queries AniList for it.
	scheduleStaleAfter = 30 * time.Minute
	// notifierSentRetention bounds the in-memory dedupe: sent keys older
	// than this are pruned (the database notifiedUsers check is the
	// authoritative dedupe; the map only prevents repeated upstream work).
	notifierSentRetention = 24 * time.Hour
)

// airingSlot tracks one upcoming-or-freshly-aired episode of one bookmarked
// show. Only RELEASING shows with a known nextAiringEpisode get a slot; the
// on-demand trigger covers everything else.
type airingSlot struct {
	episode     int
	nextAir     time.Time
	nextAttempt time.Time // earliest next Megaplay source call
	attempts    int
	notified    bool
	dead        bool
}

// notifierState is the scheduler's bounded memory: the airing book, the
// per-show refresh stamps, the bookmark set it must match, and the sent-key
// dedupe (animeID:episode -> time).
type notifierState struct {
	mu        sync.Mutex
	slots     map[int]*airingSlot
	refreshed map[int]time.Time
	seen      map[int]struct{}
	sent      map[string]time.Time
	cursor    []int // bookmark IDs awaiting their schedule refresh
}

func (h *Handlers) notifier() *notifierState {
	h.notifyOnce.Do(func() {
		h.notifyState = &notifierState{
			slots:     map[int]*airingSlot{},
			refreshed: map[int]time.Time{},
			seen:      map[int]struct{}{},
			sent:      map[string]time.Time{},
		}
	})
	return h.notifyState
}

// notifyEpisode queues a background announcement for an episode that just
// resolved to a playable source. Nothing waits on it: GetServers and Stream
// must stay as fast as the source lookup itself.
func (h *Handlers) notifyEpisode(animeID, episode int) {
	if animeID <= 0 || episode <= 0 || h.cfg.Supabase.URL == "" || h.cfg.Supabase.ServiceKey == "" {
		return
	}
	st := h.notifier()
	key := strconv.Itoa(animeID) + ":" + strconv.Itoa(episode)
	st.mu.Lock()
	if st.sent[key].After(time.Now().Add(-notifierSentRetention)) {
		st.mu.Unlock()
		return
	}
	st.sent[key] = time.Now()
	st.pruneSentLocked()
	st.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := h.sendEpisodeNotifications(ctx, animeID, episode); err != nil {
			// Allow a later resolve to retry; the recipient pre-check keeps
			// retries idempotent.
			st.mu.Lock()
			delete(st.sent, key)
			st.mu.Unlock()
			h.log.Warn().Err(err).Int("animeId", animeID).Int("episode", episode).Msg("episode notification skipped")
		}
	}()
}

// pruneSentLocked drops dedupe keys past their retention once the map gets
// large; called with st.mu held. The database check makes a dropped key a
// cheap re-query, never a duplicate notification.
func (st *notifierState) pruneSentLocked() {
	if len(st.sent) < 20000 {
		return
	}
	cutoff := time.Now().Add(-notifierSentRetention)
	for k, at := range st.sent {
		if at.Before(cutoff) {
			delete(st.sent, k)
		}
	}
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
	h.log.Info().Int("animeId", animeID).Int("episode", episode).Int("recipients", len(rows)).
		Str("title", title).Msg("episode notifications sent")
	return nil
}

type airingMedia struct {
	Status            string `json:"status"`
	Episodes          int    `json:"episodes"`
	NextAiringEpisode *struct {
		Episode  int `json:"episode"`
		AiringAt int `json:"airingAt"`
	} `json:"nextAiringEpisode"`
	Title struct {
		English       string `json:"english"`
		Romaji        string `json:"romaji"`
		UserPreferred string `json:"userPreferred"`
	} `json:"title"`
}

func (h *Handlers) anilistAiringMedia(ctx context.Context, animeID int) (airingMedia, error) {
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { status episodes nextAiringEpisode { episode airingAt } title { english romaji userPreferred } } }`
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

// runSourceNotifier is the background scheduler. It keeps an airing book of
// every bookmarked RELEASING show and, ~75 minutes after each episode's
// airtime, asks Megaplay (only) whether a source exists — retrying every
// 5 minutes for up to 3 hours — then announces to all bookmarked users.
// This complements the on-demand trigger in GetServers/Stream so
// notifications are not limited to users who actually press play.
func (h *Handlers) runSourceNotifier(ctx context.Context) {
	if h.cfg.Supabase.URL == "" || h.cfg.Supabase.ServiceKey == "" {
		h.log.Warn().Msg("source notifier disabled: supabase service key missing")
		return
	}
	st := h.notifier()
	select {
	case <-time.After(30 * time.Second):
	case <-ctx.Done():
		return
	}
	h.refreshBookmarks(ctx, st)
	bookmarkTicker := time.NewTicker(5 * time.Minute)
	refreshTicker := time.NewTicker(scheduleRefreshInterval)
	sweepTicker := time.NewTicker(time.Minute)
	defer bookmarkTicker.Stop()
	defer refreshTicker.Stop()
	defer sweepTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-bookmarkTicker.C:
			h.refreshBookmarks(ctx, st)
		case <-refreshTicker.C:
			h.refreshOneAiring(ctx, st)
		case <-sweepTicker.C:
			h.sweepDueSlots(ctx, st)
		}
	}
}

// refreshBookmarks pulls every bookmarked anime ID (no cap) and reconciles
// the airing book: new bookmarks join the refresh queue, removed ones are
// pruned. One paged Supabase scan; cheap and local.
func (h *Handlers) refreshBookmarks(ctx context.Context, st *notifierState) {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	ids, err := h.bookmarkedAnimeIDs(ctx)
	if err != nil {
		h.log.Warn().Err(err).Msg("source notifier: failed to load bookmarks")
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	seen := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		seen[id] = struct{}{}
		if _, known := st.refreshed[id]; !known {
			delete(st.refreshed, id) // no-op, keeps the map tidy
		}
		if _, has := st.slots[id]; !has {
			if _, wasQueued := st.refreshed[id]; !wasQueued {
				st.refreshed[id] = time.Time{} // queued, never refreshed
			}
		}
	}
	st.seen = seen
	for id := range st.slots {
		if _, keep := seen[id]; !keep {
			delete(st.slots, id)
			delete(st.refreshed, id)
		}
	}
	for id := range st.refreshed {
		if _, keep := seen[id]; !keep {
			delete(st.refreshed, id)
		}
	}
}

// refreshOneAiring re-queries AniList for the next stale book entry (one
// call per tick, paced by scheduleRefreshInterval, so a 10k-show book
// cycles in ~3.3 hours without touching AniList's rate limit).
func (h *Handlers) refreshOneAiring(ctx context.Context, st *notifierState) {
	st.mu.Lock()
	var target int
	targetAt := time.Now().Add(scheduleStaleAfter)
	for id, at := range st.refreshed {
		if at.Before(targetAt) {
			target, targetAt = id, at
		}
	}
	if target == 0 {
		st.mu.Unlock()
		return
	}
	st.refreshed[target] = time.Now()
	st.mu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	media, err := h.anilistAiringMedia(rctx, target)
	if err != nil {
		// Requeue: clear the stamp so the next tick retries this show.
		st.mu.Lock()
		st.refreshed[target] = time.Time{}
		st.mu.Unlock()
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if media.Status != "RELEASING" || media.NextAiringEpisode == nil {
		// Finished, upcoming or unknown schedule: no slot, the on-demand
		// trigger covers it.
		delete(st.slots, target)
		return
	}
	ep := media.NextAiringEpisode.Episode
	air := time.Unix(int64(media.NextAiringEpisode.AiringAt), 0)
	if ep <= 0 {
		delete(st.slots, target)
		return
	}
	slot, ok := st.slots[target]
	if !ok || slot.episode != ep {
		st.slots[target] = &airingSlot{
			episode:     ep,
			nextAir:     air,
			nextAttempt: air.Add(notifierFirstCheckDelay),
		}
		return
	}
	slot.nextAir = air
}

// sweepDueSlots checks every slot whose airtime is at least
// notifierFirstCheckDelay old and whose retry cadence has come due: one
// Megaplay call each. Source found -> announce; not found -> 5-minute
// retry; window expired -> dead. Past-due slots are handled sequentially
// here (bounded concurrency, ~40s worst case per probe), and the sweep
// never overlaps itself.
func (h *Handlers) sweepDueSlots(ctx context.Context, st *notifierState) {
	if !h.sweepBusy.CompareAndSwap(false, true) {
		return
	}
	defer h.sweepBusy.Store(false)

	type due struct {
		id      int
		episode int
	}
	st.mu.Lock()
	now := time.Now()
	var dueSlots []due
	for id, slot := range st.slots {
		if slot.dead || slot.notified || now.Before(slot.nextAttempt) {
			continue
		}
		dueSlots = append(dueSlots, due{id: id, episode: slot.episode})
	}
	for _, d := range dueSlots {
		if slot, ok := st.slots[d.id]; ok {
			slot.nextAttempt = now.Add(notifierRetryDelay)
			slot.attempts++
		}
	}
	st.mu.Unlock()

	for _, d := range dueSlots {
		if ctx.Err() != nil {
			return
		}
		st.mu.Lock()
		slot, ok := st.slots[d.id]
		if !ok || slot.episode != d.episode {
			st.mu.Unlock()
			continue
		}
		if slot.attempts > int(notifierGiveUpAfter/notifierRetryDelay) {
			slot.dead = true
			st.mu.Unlock()
			h.log.Info().Int("animeId", d.id).Int("episode", d.episode).
				Msg("source notifier: gave up on episode window")
			continue
		}
		st.mu.Unlock()

		pctx, cancel := context.WithTimeout(ctx, 40*time.Second)
		found := h.stream.MegaVidSourceAvailable(pctx, d.id, d.episode, "sub")
		cancel()

		st.mu.Lock()
		slot, ok = st.slots[d.id]
		if !ok || slot.episode != d.episode {
			st.mu.Unlock()
			continue
		}
		if found {
			slot.notified = true
			st.mu.Unlock()
			h.notifyEpisode(d.id, d.episode)
			continue
		}
		st.mu.Unlock()
	}
}

// bookmarkedAnimeIDs pages through every bookmarked anime ID.
func (h *Handlers) bookmarkedAnimeIDs(ctx context.Context) ([]int, error) {
	const pageSize = 1000
	seen := map[int]struct{}{}
	var ids []int
	for offset := 0; offset < 100*pageSize; offset += pageSize {
		path := fmt.Sprintf("/rest/v1/bookmarks?select=anime_id&order=anime_id&limit=%d&offset=%d", pageSize, offset)
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
			AnimeID int `json:"anime_id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		for _, r := range rows {
			if r.AnimeID > 0 {
				if _, ok := seen[r.AnimeID]; !ok {
					seen[r.AnimeID] = struct{}{}
					ids = append(ids, r.AnimeID)
				}
			}
		}
		if len(rows) < pageSize {
			break
		}
	}
	return ids, nil
}
