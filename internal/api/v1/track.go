package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// ani.pm exposes per-episode sub/dub availability on its (undocumented)
// title endpoint. We report the LAST RELEASED episode number per track, not
// the track's episode count: dubs can have gaps (One Piece reports dubCount
// 1154 but its latest dub episode is 1157).
//
// Cloudflare bans this host's IPv6 range (403 "error code: 1106") while IPv4
// stays clean, so the client is pinned to tcp4. A browser User-Agent is
// required; Accept and Referer are not, but are sent anyway.
//
// If ani.pm ever blocks IPv4 too, fall back to a static dataset keyed by
// AniList ID (derived from AniSchedule release feeds, refreshed daily). That
// dataset only has counts, so it cannot preserve the latest-episode semantics.
const (
	trackUserAgent     = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	trackCacheTTL      = 30 * time.Minute
	trackNegativeTTL   = 2 * time.Minute
	trackFallbackTTL   = 24 * time.Hour
	trackFallbackRetry = 5 * time.Minute
)

// Vars so tests can point at httptest servers.
var (
	trackAPIBase     = "https://ani.pm"
	trackFallbackURL = "https://raw.githubusercontent.com/thaware12/ani-dub-sub-list/main/data/progress.json"
)

var errTrackNotFound = errors.New("anime not found in track source")

// trackCounts is both the internal value and the response body: {"sub": n, "dub": n}.
type trackCounts struct {
	Sub int `json:"sub"`
	Dub int `json:"dub"`
}

type trackCacheEntry struct {
	counts trackCounts
	at     time.Time
	miss   bool
}

var (
	trackCache sync.Map // AniList ID -> trackCacheEntry

	trackClient = &http.Client{
		Timeout: 12 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
				return trackDialer.DialContext(ctx, "tcp4", addr)
			},
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		},
		CheckRedirect: netguard.NoRedirects,
	}

	// Same resolver pinning as the rest of the server: /etc/hosts cannot
	// steer outbound requests, and netguard.Control rejects private IPs.
	trackDialer = &net.Dialer{
		Timeout: 10 * time.Second,
		Resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "udp", "8.8.8.8:53")
			},
		},
		Control: netguard.Control,
	}
)

func (h *Handlers) GetAnimeTrack(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil || id <= 0 {
		h.respondError(w, http.StatusBadRequest, "invalid anime ID")
		return
	}

	counts, err := h.trackCounts(r.Context(), id)
	switch {
	case errors.Is(err, errTrackNotFound):
		h.respondError(w, http.StatusNotFound, "anime not found")
	case err != nil:
		h.log.Warn().Err(err).Int("anilist_id", id).Msg("track: all sources failed")
		h.respondError(w, http.StatusBadGateway, "failed to fetch track counts")
	default:
		h.respondJSON(w, http.StatusOK, counts)
	}
}

func (h *Handlers) trackCounts(ctx context.Context, id int) (trackCounts, error) {
	if v, ok := trackCache.Load(id); ok {
		e := v.(trackCacheEntry)
		ttl := trackCacheTTL
		if e.miss {
			ttl = trackNegativeTTL
		}
		if time.Since(e.at) < ttl {
			if e.miss {
				return trackCounts{}, errTrackNotFound
			}
			return e.counts, nil
		}
	}

	counts, found, err := fetchAniPMTrack(ctx, trackClient, id)
	if err != nil || !found {
		// ani.pm is the fresher source, but the static mirror covers its
		// outages and its catalog gaps.
		if fb, ok := fallbackTrack(ctx, trackClient, id); ok {
			trackCache.Store(id, trackCacheEntry{counts: fb, at: time.Now()})
			return fb, nil
		}
		if err != nil {
			return trackCounts{}, err
		}
		trackCache.Store(id, trackCacheEntry{miss: true, at: time.Now()})
		return trackCounts{}, errTrackNotFound
	}

	trackCache.Store(id, trackCacheEntry{counts: counts, at: time.Now()})
	return counts, nil
}

func fetchAniPMTrack(ctx context.Context, client *http.Client, id int) (trackCounts, bool, error) {
	u := fmt.Sprintf("%s/api/anime/ani/%d?phase=core", trackAPIBase, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return trackCounts{}, false, err
	}
	req.Header.Set("User-Agent", trackUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Referer", trackAPIBase+"/")

	resp, err := client.Do(req)
	if err != nil {
		return trackCounts{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return trackCounts{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return trackCounts{}, false, fmt.Errorf("ani.pm: status %d", resp.StatusCode)
	}

	var body struct {
		SubCount *int `json:"subCount"`
		DubCount *int `json:"dubCount"`
		Episodes []struct {
			Number int  `json:"number"`
			Sub    bool `json:"sub"`
			Dub    bool `json:"dub"`
		} `json:"episodes"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&body); err != nil {
		return trackCounts{}, false, err
	}
	if body.SubCount == nil && body.DubCount == nil && len(body.Episodes) == 0 {
		return trackCounts{}, false, nil
	}

	c := trackCounts{}
	for _, ep := range body.Episodes {
		if ep.Number <= 0 {
			continue
		}
		if ep.Sub && ep.Number > c.Sub {
			c.Sub = ep.Number
		}
		if ep.Dub && ep.Number > c.Dub {
			c.Dub = ep.Number
		}
	}
	// Empty episode arrays (movies, announcements) still carry counts.
	if len(body.Episodes) == 0 {
		if body.SubCount != nil {
			c.Sub = *body.SubCount
		}
		if body.DubCount != nil {
			c.Dub = *body.DubCount
		}
	}
	return c, true, nil
}

var trackFallback struct {
	mu      sync.Mutex
	data    map[string]trackCounts
	at      time.Time
	nextTry time.Time
}

func fallbackTrack(ctx context.Context, client *http.Client, id int) (trackCounts, bool) {
	trackFallback.mu.Lock()
	defer trackFallback.mu.Unlock()

	now := time.Now()
	if trackFallback.data == nil || now.Sub(trackFallback.at) > trackFallbackTTL {
		if now.Before(trackFallback.nextTry) {
			c, ok := trackFallback.data[strconv.Itoa(id)]
			return c, ok
		}
		if err := loadTrackFallback(ctx, client); err != nil {
			trackFallback.nextTry = now.Add(trackFallbackRetry)
		} else {
			trackFallback.at, trackFallback.nextTry = now, time.Time{}
		}
	}
	c, ok := trackFallback.data[strconv.Itoa(id)]
	return c, ok
}

func loadTrackFallback(ctx context.Context, client *http.Client) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, trackFallbackURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", trackUserAgent)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("track fallback: status %d", resp.StatusCode)
	}
	var data map[string]trackCounts
	// 8 MiB is ~6x the current 1.3 MB dataset; bounds a hostile response.
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&data); err != nil {
		return err
	}
	trackFallback.data = data
	return nil
}
