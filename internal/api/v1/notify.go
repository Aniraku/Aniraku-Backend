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

// Notification pipeline (2026-11 redesign, v2).
//
// Two triggers feed the same writer:
//  1. On-demand: a user resolves sources for an episode (GetServers/Stream)
//     and notifyEpisode announces it to every bookmarked user. Freshness is
//     gated by episodeIsFresh (only the latest episode), exactly as before.
//  2. Scheduled: an airing book tracks every bookmarked RELEASING show via
//     AniList's nextAiringEpisode. Each AIRED episode gets
//     its own check keyed by episode number: the first Megaplay source probe
//     happens ~75 minutes after ITS airtime, retrying every 5 minutes. If
//     Megaplay misses, the check falls back to the full multi-provider
//     manager resolution — an episode playable in the app is a notification,
//     no matter which provider carries it. Checks are created for episodes
//     aired within the last 6 hours (restart catch-up) and given up at
//     airedAt+6h; pruned after 30h.
//
// v1 post-mortem: the first design tracked only each show's "next upcoming"
// episode in one slot. The refresher replaced that slot as soon as AniList
// noticed the airing (≤30 min), which is always before the +75 min check
// came due — so the freshly aired episode was structurally never checked
// and notifications only fired through the play trigger. Keying checks by
// episode number with their own airtime removes that race entirely.

const (
	// notifierFirstCheckDelay is how long after an episode's airtime the
	// first Megaplay source probe happens. Subs typically land 30-120
	// minutes after airing; 60 minutes catches most episodes on the first
	// attempt and the 5-minute retries sweep up the rest.
	notifierFirstCheckDelay = 60 * time.Minute
	// notifierRetryDelay is the cadence of follow-up probes while no
	// source has been found yet.
	notifierRetryDelay = 5 * time.Minute
	// notifierGiveUpAfter bounds probing: past airedAt+6h the episode is
	// marked dead and only the on-demand play trigger can announce it.
	notifierGiveUpAfter = 6 * time.Hour
	// scheduleAiringWindow limits catch-up: episodes aired longer ago than
	// this never enter the book (their announcement would be stale news).
	scheduleAiringWindow = 6 * time.Hour
	// scheduleStaleAfter is how old a book entry's AniList data may get
	// before the refresher re-queries it.
	scheduleStaleAfter = 30 * time.Minute
	// scheduleRefreshInterval paces AniList lookups: one bookmarked show
	// per tick keeps a 10k-show book cycling in ~3.3 hours, far under
	// AniList's rate limit, and airtimes rarely change faster.
	scheduleRefreshInterval = 1200 * time.Millisecond
	// notifierSentRetention bounds the in-memory dedupe: sent keys older
	// than this are pruned (the database notifiedUsers check is the
	// authoritative dedupe; the map only prevents repeated upstream work).
	notifierSentRetention = 24 * time.Hour
)

// episodeCheck tracks the notification lifecycle of ONE aired episode.
type episodeCheck struct {
	airedAt     time.Time
	nextAttempt time.Time
	attempts    int
	notified    bool
	dead        bool
}

// showBook is one bookmarked show: its last AniList refresh, airing status,
// the nextAiringEpisode transition state (lastSeenNext is the episode we
// last observed as "next" — when it advances, that episode has aired within
// the observation window), and the per-episode checks in flight.
type showBook struct {
	refreshed    time.Time
	status       string
	lastSeenNext int
	lastSeenAt   time.Time
	checks       map[int]*episodeCheck // keyed by episode number
}

// notifierState is the scheduler's bounded memory: the airing book, the
// bookmark set it must match, and the sent-key dedupe (animeID:episode).
type notifierState struct {
	mu    sync.Mutex
	book  map[int]*showBook
	seen  map[int]struct{}
	sent  map[string]time.Time
	cycle int // incrementing stamp for refresher fairness
}

func (h *Handlers) notifier() *notifierState {
	h.notifyOnce.Do(func() {
		h.notifyState = &notifierState{
			book: map[int]*showBook{},
			seen: map[int]struct{}{},
			sent: map[string]time.Time{},
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
		if err := h.sendEpisodeNotifications(ctx, animeID, episode, false); err != nil {
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

func (h *Handlers) sendEpisodeNotifications(ctx context.Context, animeID, episode int, scheduled bool) error {
	media, err := h.anilistAiringMedia(ctx, animeID)
	if err != nil {
		return err
	}
	if scheduled {
		// The scheduler already gated freshness by the episode's own airtime
		// (its check would not exist otherwise); only require the show to
		// still be airing so a finished or renamed entry never announces.
		if media.Status != "RELEASING" {
			return nil
		}
	} else {
		next := 0
		if media.NextAiringEpisode != nil {
			next = media.NextAiringEpisode.Episode
		}
		if !episodeIsFresh(media.Status, media.Episodes, next, episode) {
			return nil
		}
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
		Str("title", title).Bool("scheduled", scheduled).Msg("episode notifications sent")
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
	// nextAiringEpisode drives everything: its episode + airingAt schedule
	// the future check, and its ADVANCEMENT between refreshes is the proof
	// that the previously-observed episode aired (transition detection).
	// airingSchedule was tried and dropped: the self-hosted mirror ignores
	// the sort argument and always pages ascending from episode 1, so the
	// most recent airing is unreachable through it.
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

// episodeIsFresh reports whether an episode is recent enough to announce
// through the ON-DEMAND trigger. Only airing shows produce new episodes:
// the latest aired one is nextAiringEpisode-1, and nextAiringEpisode itself
// lets an early source for the upcoming episode through without announcing
// older backlog. Scheduled checks gate by their own airtime instead (the
// check would not exist otherwise).
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
// every bookmarked RELEASING show, gives each AIRED episode its own check,
// and ~75 minutes after that episode's airtime probes Megaplay (one call)
// for a source — falling back to the full multi-provider resolution when
// Megaplay misses — retrying every 5 minutes for up to 6 hours. This
// complements the on-demand trigger in GetServers/Stream so notifications
// are not limited to users who actually press play.
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
			h.sweepDueChecks(ctx, st)
		}
	}
}

// refreshBookmarks pulls every bookmarked anime ID (no cap) and reconciles
// the airing book: new bookmarks join as immediately-stale entries (first
// refresh on the next tick), removed ones are pruned.
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
		if _, known := st.book[id]; !known {
			st.book[id] = &showBook{checks: map[int]*episodeCheck{}}
		}
	}
	st.seen = seen
	for id := range st.book {
		if _, keep := seen[id]; !keep {
			delete(st.book, id)
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
	for id, entry := range st.book {
		if entry.refreshed.Before(targetAt) {
			target, targetAt = id, entry.refreshed
		}
	}
	if target == 0 {
		st.mu.Unlock()
		return
	}
	st.mu.Unlock()

	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	media, err := h.anilistAiringMedia(rctx, target)
	if err != nil {
		// Requeue: clear the stamp so the next tick retries this show.
		st.mu.Lock()
		if entry, ok := st.book[target]; ok {
			entry.refreshed = time.Time{}
		}
		st.mu.Unlock()
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	entry, ok := st.book[target]
	if !ok {
		return // bookmark vanished mid-refresh
	}
	entry.refreshed = time.Now()
	entry.status = media.Status
	now := time.Now()

	addCheck := func(ep int, at time.Time) {
		if ep <= 0 {
			return
		}
		if at.Add(scheduleAiringWindow).Before(now) {
			return // too old to announce
		}
		if existing, ok := entry.checks[ep]; ok {
			// Keep the existing lifecycle; only correct the airtime if
			// AniList moved it.
			if !existing.airedAt.Equal(at) && at.After(now.Add(-scheduleAiringWindow)) {
				existing.airedAt = at
			}
			return
		}
		first := at.Add(notifierFirstCheckDelay)
		if first.Before(now) {
			first = now // catch-up: probe on the next sweep
		}
		entry.checks[ep] = &episodeCheck{airedAt: at, nextAttempt: first}
	}

	// RELEASING only: every branch below exists to announce episodes.
	if media.Status != "RELEASING" || media.NextAiringEpisode == nil {
		return
	}
	next := media.NextAiringEpisode.Episode
	air := time.Unix(int64(media.NextAiringEpisode.AiringAt), 0)
	if next <= 0 {
		return
	}

	// The upcoming episode: schedule its check at ITS airtime + 75 minutes,
	// straight from AniList (no observation lag).
	if air.After(now) {
		addCheck(next, air)
	} else {
		addCheck(next, now)
	}

	// Transition detection: we last observed episode `lastSeenNext` as the
	// upcoming one at `lastSeenAt`; AniList now names a LATER episode, so
	// the observed one (and anything between) aired inside that window.
	// This is the check the v1 slot-advance bug lost. AiredAt is bounded by
	// the observation window, so late restarts (>48h) skip instead of
	// announcing ancient episodes.
	if entry.lastSeenNext > 0 && next > entry.lastSeenNext && now.Sub(entry.lastSeenAt) <= 48*time.Hour {
		for ep := entry.lastSeenNext; ep < next; ep++ {
			before := len(entry.checks)
			addCheck(ep, entry.lastSeenAt)
			if len(entry.checks) > before {
				h.log.Info().Int("animeId", target).Int("episode", ep).
					Time("airedAt", entry.lastSeenAt).
					Msg("source notifier: aired episode detected, probe scheduled")
			}
		}
	}
	entry.lastSeenNext = next
	entry.lastSeenAt = now

	// Prune checks past their announcement window.
	for ep, c := range entry.checks {
		if c.airedAt.Add(scheduleAiringWindow + notifierGiveUpAfter).Before(now) {
			delete(entry.checks, ep)
		}
	}
}

// sweepDueChecks probes every check whose retry cadence has come due:
// Megaplay first (one call), and when Megaplay misses the full
// multi-provider resolution — the same availability signal a user's play
// produces. Source found -> announce; not found -> 5-minute retry;
// airedAt+6h -> give up. Probes run sequentially and the sweep never
// overlaps itself.
func (h *Handlers) sweepDueChecks(ctx context.Context, st *notifierState) {
	if !h.sweepBusy.CompareAndSwap(false, true) {
		return
	}
	defer h.sweepBusy.Store(false)

	type dueCheck struct {
		id      int
		episode int
	}
	st.mu.Lock()
	now := time.Now()
	var due []dueCheck
	for id, entry := range st.book {
		if entry.status != "RELEASING" {
			continue
		}
		for ep, c := range entry.checks {
			if c.dead || c.notified || now.Before(c.nextAttempt) {
				continue
			}
			due = append(due, dueCheck{id: id, episode: ep})
		}
	}
	for _, d := range due {
		if c, ok := st.book[d.id].checks[d.episode]; ok {
			c.nextAttempt = now.Add(notifierRetryDelay)
			c.attempts++
		}
	}
	st.mu.Unlock()
	if len(due) == 0 {
		return
	}

	attempted, found, gaveUp := 0, 0, 0
	for _, d := range due {
		if ctx.Err() != nil {
			return
		}
		st.mu.Lock()
		entry, ok := st.book[d.id]
		if !ok {
			st.mu.Unlock()
			continue
		}
		c, ok := entry.checks[d.episode]
		if !ok || c.dead || c.notified {
			st.mu.Unlock()
			continue
		}
		if c.airedAt.Add(notifierGiveUpAfter).Before(time.Now()) {
			c.dead = true
			gaveUp++
			st.mu.Unlock()
			continue
		}
		st.mu.Unlock()

		// Probe 1: Megaplay only — one upstream call.
		pctx, cancel := context.WithTimeout(ctx, 40*time.Second)
		hasSource := h.stream.MegaVidSourceAvailable(pctx, d.id, d.episode, "sub")
		cancel()
		// Probe 2: the real multi-provider resolution — the same signal a
		// user's play produces. Megaplay misses plenty of episodes other
		// providers carry, and "available in the app" is what the
		// notification means.
		if !hasSource {
			fctx, fcancel := context.WithTimeout(ctx, 45*time.Second)
			servers := h.stream.FindAllServers(fctx, d.id, d.episode, "sub", nil)
			hasSource = hasSources(servers)
			fcancel()
		}
		attempted++

		st.mu.Lock()
		entry, ok = st.book[d.id]
		if !ok {
			st.mu.Unlock()
			continue
		}
		c, ok = entry.checks[d.episode]
		if !ok {
			st.mu.Unlock()
			continue
		}
		if hasSource {
			c.notified = true
			st.mu.Unlock()
			found++
			go func() {
				sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer scancel()
				if err := h.sendEpisodeNotifications(sctx, d.id, d.episode, true); err != nil {
					h.log.Warn().Err(err).Int("animeId", d.id).Int("episode", d.episode).
						Msg("scheduled episode notification failed")
				}
			}()
			continue
		}
		st.mu.Unlock()
	}
	h.log.Info().Int("attempted", attempted).Int("found", found).Int("gaveUp", gaveUp).
		Msg("source notifier: sweep complete")
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
