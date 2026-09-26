package streaming

import (
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
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
)

// Kiwi download links come from ZokoAnime's download API (the same backend
// that powers its /download pages): a short-lived token plus a link list
// per title/episode/track, including per-quality entries
// (360p/720p/1080p where available). The pahe short links resolve (in the
// user's browser, whose egress passes kwik.cx Cloudflare) to the actual
// files — the backend only ever handles the short links, never the files.
//
// This fetcher is deliberately independent of the Zoko streaming provider
// (and its pause flag): the download API endpoints proved stable even while
// streams flapped.
type kiwiTokenEntry struct {
	token string
	exp   time.Time
}

var (
	kiwiTokenMu sync.Mutex
	kiwiTokens  = map[string]kiwiTokenEntry{}
)

// kiwiTokenTTLMargin refreshes tokens this far before their server-stated
// expiry (tokens live ~600s).
const kiwiTokenTTLMargin = 60 * time.Second

// fetchKiwiDownloads returns download links for an episode, or nil when the
// title/track has none. AniList-keyed first (no MAL lookup needed —
// verified the ani form serves the same entries); MAL-keyed fallback for
// titles only indexed by MAL ID (observed: ani/113417 empty while
// mal/40746 serves 3 qualities).
func fetchKiwiDownloads(ctx context.Context, client *http.Client, anilistID string, episode int, track string) []core.DownloadLink {
	if track != "dub" {
		track = "sub"
	}
	if links := fetchKiwiDownloadsKey(ctx, client, "ani", anilistID, episode, track); len(links) > 0 {
		return links
	}
	id, err := strconv.Atoi(anilistID)
	if err != nil || id <= 0 {
		return nil
	}
	malID := tmdb.FetchMalID(ctx, client, id)
	if malID <= 0 {
		malID = fetchAniListMALID(ctx, client, id)
	}
	return fetchKiwiWithMAL(ctx, client, anilistID, malID, episode, track)
}

// fetchKiwiWithMAL tries the AniList-keyed list, falling back to the given
// MAL ID (0 skips the MAL attempt). Split out so the fallback is
// unit-testable without the network MAL-ID lookups.
func fetchKiwiWithMAL(ctx context.Context, client *http.Client, anilistID string, malID, episode int, track string) []core.DownloadLink {
	if links := fetchKiwiDownloadsKey(ctx, client, "ani", anilistID, episode, track); len(links) > 0 {
		return links
	}
	if malID <= 0 {
		return nil
	}
	return fetchKiwiDownloadsKey(ctx, client, "mal", strconv.Itoa(malID), episode, track)
}

func fetchKiwiDownloadsKey(ctx context.Context, client *http.Client, source, id string, episode int, track string) []core.DownloadLink {
	token, ok := kiwiToken(ctx, client, source, id, episode, track)
	if !ok {
		return nil
	}
	u := fmt.Sprintf("%s/api/downloads/%s/%s/%d/%s?t=%s",
		zokoBase, url.PathEscape(source), url.PathEscape(id), episode, url.QueryEscape(track), url.QueryEscape(token))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Referer", zokoBase+"/")
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil
	}
	var raw struct {
		Downloads []struct {
			Label string `json:"label"`
			URL   string `json:"url"`
		} `json:"downloads"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	var out []core.DownloadLink
	seen := map[string]bool{}
	for _, d := range raw.Downloads {
		if strings.TrimSpace(d.URL) == "" || seen[d.URL] {
			continue
		}
		seen[d.URL] = true
		label := strings.TrimSpace(d.Label)
		if label == "" {
			label = "Download"
		}
		out = append(out, core.DownloadLink{URL: d.URL, Label: label})
	}
	return out
}

// kiwiToken returns a cached download token, fetching a fresh one when
// missing or near expiry. The cache key includes the source namespace
// (ani vs mal IDs differ per namespace).
func kiwiToken(ctx context.Context, client *http.Client, source, id string, episode int, track string) (string, bool) {
	key := source + "/" + id + "/" + fmt.Sprintf("%d", episode) + "/" + track
	kiwiTokenMu.Lock()
	if e, ok := kiwiTokens[key]; ok && time.Now().Add(kiwiTokenTTLMargin).Before(e.exp) {
		tok := e.token
		kiwiTokenMu.Unlock()
		return tok, true
	}
	kiwiTokenMu.Unlock()

	u := fmt.Sprintf("%s/api/downloads/token/%s/%s/%d/%s",
		zokoBase, url.PathEscape(source), url.PathEscape(id), episode, url.QueryEscape(track))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", false
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Referer", zokoBase+"/")
	resp, err := client.Do(req)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return "", false
	}
	var raw struct {
		Token string `json:"token"`
		Exp   int64  `json:"exp"`
	}
	if err := json.Unmarshal(body, &raw); err != nil || raw.Token == "" {
		return "", false
	}
	exp := time.Unix(raw.Exp, 0)
	if exp.IsZero() {
		exp = time.Now().Add(10 * time.Minute)
	}
	kiwiTokenMu.Lock()
	kiwiTokens[key] = kiwiTokenEntry{token: raw.Token, exp: exp}
	kiwiTokenMu.Unlock()
	return raw.Token, true
}

// attachKiwiDownloads merges download links into every non-embed server.
// Embed servers (FlixCloud Yuta, hentai Miru-embed) play inside embedded
// players and take no file links — they are skipped, everything else gets
// the links (deduped by URL, existing entries first).
func attachKiwiDownloads(servers []core.Server, links []core.DownloadLink) []core.Server {
	if len(links) == 0 {
		return servers
	}
	for i := range servers {
		playable := false
		for _, s := range servers[i].Sources {
			if strings.ToLower(s.Type) != "embed" {
				playable = true
				break
			}
		}
		if !playable {
			continue
		}
		servers[i].Downloads = mergeDownloadLinks(servers[i].Downloads, links)
	}
	return servers
}
