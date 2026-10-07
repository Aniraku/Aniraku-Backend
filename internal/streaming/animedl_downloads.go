package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// AnimeDL (animedl.to) download links: a token-less API addressed by
// AniList ID + episode, shaped release group -> sub/dub -> quality ->
// short-page URL. Its short pages (/download/<code>, then /go/<code>)
// resolve in the user's browser to the original file host (pahe.win ->
// kwik.cx — the AnimePahe releases), so the backend ships links only and
// never touches files.
//
// Adoption measured 2026-10-07 against the Zoko/Kiwi fetcher it replaces:
// latest episodes return full 360p/720p/1080p ladders in one call where
// the Kiwi list endpoint returned zero links, sub and dub arrive together,
// and no auth/token dance is needed. Their upstream resolver behind a 2h
// server cache intermittently errors on first-hit lookups ("release source
// could not be reached") — a miss is cached briefly and served as "no
// downloads"; it is never retried inside a request (list speed first).
var animeDlBase = "https://animedl.to"

const (
	// animeDlCacheTTL reuses a parsed response for half their server-side
	// cache window (their cache is 2h) so repeated /servers calls for the
	// same episode cost no upstream latency.
	animeDlCacheTTL = 60 * time.Minute
	// animeDlMissTTL shields their flapping upstream from a hot page
	// hammering it: a failed lookup resolves to empty for a minute.
	animeDlMissTTL = 60 * time.Second
)

// animeDlLink is one quality entry before it becomes a core.DownloadLink:
// track and release group select/order entries; the user-facing label is
// the published quality key alone ("1080p", "1080p mirror").
type animeDlLink struct {
	track string // "sub" | "dub"
	group string // release group ("MTBB", "SubsPlease", ...) — ordering only
	label string // quality key as published
	url   string
}

type animeDlEntry struct {
	links []animeDlLink
	exp   time.Time
}

var (
	animeDlMu    sync.Mutex
	animeDlCache = map[string]animeDlEntry{}
)

// resetAnimeDlCache clears the process-wide cache so tests never share
// entries (mirrors the old kiwi token-cache reset).
func resetAnimeDlCache() {
	animeDlMu.Lock()
	animeDlCache = map[string]animeDlEntry{}
	animeDlMu.Unlock()
}

// fetchAnimeDlDownloads returns the requested track's download links for an
// episode, or nil when the title/episode/track has none — including when
// their upstream is unreachable (graceful empty, decided 2026-10-07: links
// appear on a later request once their cache warms).
func fetchAnimeDlDownloads(ctx context.Context, client *http.Client, anilistID string, episode int, track string) []core.DownloadLink {
	if strings.TrimSpace(anilistID) == "" {
		return nil
	}
	if track != "dub" {
		track = "sub"
	}
	key := anilistID + "/" + strconv.Itoa(episode)

	if links, ok := loadAnimeDlCache(key); ok {
		return filterAnimeDlTrack(links, track)
	}
	links := fetchAnimeDlEntries(ctx, client, anilistID, episode)
	storeAnimeDlCache(key, links)
	return filterAnimeDlTrack(links, track)
}

// fetchAnimeDlEntries performs one upstream GET for both tracks (sub and
// dub share a response) and returns the sorted, URL-deduped entries, or nil
// on any failure.
func fetchAnimeDlEntries(ctx context.Context, client *http.Client, anilistID string, episode int) []animeDlLink {
	u := fmt.Sprintf("%s/api/anilist/%s/%d", animeDlBase, url.PathEscape(anilistID), episode)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || len(body) == 0 {
		return nil
	}
	// Every top-level key except "status" (and "error" on 404s) is a
	// release group.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	delete(raw, "status")
	delete(raw, "error")

	out := make([]animeDlLink, 0, 8)
	for group, msg := range raw {
		var tracks struct {
			Sub *animeDlTrack `json:"sub"`
			Dub *animeDlTrack `json:"dub"`
		}
		if err := json.Unmarshal(msg, &tracks); err != nil {
			continue
		}
		for trackName, tr := range map[string]*animeDlTrack{"sub": tracks.Sub, "dub": tracks.Dub} {
			if tr == nil {
				continue
			}
			for label, link := range tr.Download {
				if strings.TrimSpace(link) == "" {
					continue
				}
				out = append(out, animeDlLink{
					track: trackName,
					group: group,
					label: strings.TrimSpace(label),
					url:   link,
				})
			}
		}
	}
	if len(out) == 0 {
		return nil
	}

	// Deterministic order: quality descending (non-numeric labels last),
	// then label, group, url for total stability.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if an, bn := qualityNum(a.label), qualityNum(b.label); an != bn {
			return an > bn
		}
		if a.label != b.label {
			return a.label < b.label
		}
		if a.group != b.group {
			return a.group < b.group
		}
		return a.url < b.url
	})
	seen := make(map[string]bool, len(out))
	deduped := out[:0]
	for _, l := range out {
		if seen[l.url] {
			continue
		}
		seen[l.url] = true
		deduped = append(deduped, l)
	}
	return deduped
}

// animeDlTrack is one lane's download map (quality key -> short-page URL).
type animeDlTrack struct {
	Download map[string]string `json:"download"`
}

// qualityNum extracts the leading number of a quality label ("1080p" ->
// 1080, "1080p mirror" -> 1080); labels without one sort last.
func qualityNum(label string) int {
	n, found := 0, false
	for _, r := range label {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
		found = true
		if n > 1000000 {
			break
		}
	}
	if !found {
		return -1
	}
	return n
}

// filterAnimeDlTrack maps cached entries onto the requested lane only
// (sub requests never see dub links and vice versa), labels quality-only.
func filterAnimeDlTrack(entries []animeDlLink, track string) []core.DownloadLink {
	var out []core.DownloadLink
	for _, l := range entries {
		if l.track != track {
			continue
		}
		out = append(out, core.DownloadLink{URL: l.url, Label: l.label})
	}
	return out
}

func loadAnimeDlCache(key string) ([]animeDlLink, bool) {
	animeDlMu.Lock()
	defer animeDlMu.Unlock()
	e, ok := animeDlCache[key]
	if !ok || !time.Now().Before(e.exp) {
		return nil, false
	}
	return e.links, true
}

// storeAnimeDlCache remembers successes for animeDlCacheTTL and misses for
// animeDlMissTTL; expired entries are swept once the map grows past a
// modest size so a busy catalog cannot grow it without bound.
func storeAnimeDlCache(key string, links []animeDlLink) {
	ttl := animeDlMissTTL
	if len(links) > 0 {
		ttl = animeDlCacheTTL
	}
	animeDlMu.Lock()
	if len(animeDlCache) > 256 {
		now := time.Now()
		for k, e := range animeDlCache {
			if !now.Before(e.exp) {
				delete(animeDlCache, k)
			}
		}
	}
	animeDlCache[key] = animeDlEntry{links: links, exp: time.Now().Add(ttl)}
	animeDlMu.Unlock()
}

// attachDownloadLinks merges AnimeDL links into every server except
// FlixCloud's: "all sources without the FlixCloud" — the embed player takes
// no file links, every other server (embeds included) receives them.
// Existing provider-owned links stay first; new ones append deduped by URL.
func attachDownloadLinks(servers []core.Server, links []core.DownloadLink) []core.Server {
	if len(links) == 0 {
		return servers
	}
	for i := range servers {
		if strings.EqualFold(servers[i].Provider, "flixcloud") {
			continue
		}
		servers[i].Downloads = mergeDownloadLinks(servers[i].Downloads, links)
	}
	return servers
}
