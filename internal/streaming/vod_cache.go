package streaming

import (
	"bytes"
	"sync"
	"time"
)

// VOD playlist body cache — the playback-startup fast path.
//
// Every HLS session begins with a serial chain before the first frame:
// master → media playlist → segments. For relay-backed catalogs (Supaplay)
// the master and media hops alone cost 1.1-1.2s EACH because the relay
// fetches upstream server-side — while the collector's honesty probe had
// ALREADY fetched both playlists moments earlier when the server list was
// built, then threw the bodies away. This cache keeps those bodies (and any
// playlist the media proxy fetches) for a short TTL so the player's playlist
// hops resolve in ~1ms instead of 1-2s. Segment bodies are never cached.
//
// Deliberately different from the removed serverList snapshot cache (which
// poisoned proxied URLs and couldn't be invalidated around tokenized URLs):
//
//   - It stores RAW upstream playlist bytes, never wrapped/proxied URLs —
//     there is no origin/base URL to poison between requests. The proxy
//     rewrites every response per request (its own proxyBase, the al audio
//     strip parameter, and the rn nonce are applied fresh on every hit).
//   - The key is the exact upstream URL including any query string, so a
//     rotated token is a new key that misses and refetches; a 45s TTL stays
//     far inside any observed token lifetime.
//   - Only static playlist bodies are stored (isCacheablePlaylist): live
//     playlists without ENDLIST/VOD markers are never cached.
//   - Bounded: 256 entries × 256KiB max body, oldest-first eviction.
const (
	vodCacheTTL     = 45 * time.Second
	vodCacheMaxEnt  = 256
	vodCacheMaxBody = 256 * 1024
)

type vodCacheEntry struct {
	body []byte
	at   time.Time
}

var (
	vodCacheMu sync.Mutex
	vodCache   = map[string]vodCacheEntry{}
)

// isCacheablePlaylist reports whether a fetched body is a static playlist
// safe to serve again for the cache TTL: a valid m3u8 that either ends/
// declares itself VOD (media playlists) or declares variants/renditions
// (master playlists are static per episode and carry no ENDLIST marker).
// Live playlists carry none of these markers and are never stored.
func isCacheablePlaylist(body []byte) bool {
	if !bytes.Contains(body, []byte("#EXTM3U")) {
		return false
	}
	if bytes.Contains(body, []byte("#EXT-X-ENDLIST")) ||
		bytes.Contains(body, []byte("#EXT-X-PLAYLIST-TYPE:VOD")) {
		return true
	}
	return bytes.Contains(body, []byte("#EXT-X-STREAM-INF")) ||
		bytes.Contains(body, []byte("#EXT-X-MEDIA:"))
}

// VODCacheGet returns a copy of the raw cached playlist body previously
// stored under exactURL. The caller owns the bytes and may transform them —
// the proxy still runs its own rewrite (proxyBase, al strip, rn nonce) on
// every hit; the map's buffer stays pristine for the next request.
func VODCacheGet(exactURL string) ([]byte, bool) {
	if exactURL == "" {
		return nil, false
	}
	vodCacheMu.Lock()
	defer vodCacheMu.Unlock()
	e, ok := vodCache[exactURL]
	if !ok {
		return nil, false
	}
	if time.Since(e.at) > vodCacheTTL {
		delete(vodCache, exactURL)
		return nil, false
	}
	cp := make([]byte, len(e.body))
	copy(cp, e.body)
	return cp, true
}

// VODCacheSet stores a raw playlist body under exactURL when it passes the
// static-playlist gate. Oversized bodies and live playlists are dropped.
func VODCacheSet(exactURL string, body []byte) {
	if exactURL == "" || len(body) == 0 || len(body) > vodCacheMaxBody || !isCacheablePlaylist(body) {
		return
	}
	vodCacheMu.Lock()
	defer vodCacheMu.Unlock()
	if len(vodCache) >= vodCacheMaxEnt {
		var (
			oldestKey string
			oldestAt  time.Time
		)
		for k, e := range vodCache {
			if oldestKey == "" || e.at.Before(oldestAt) {
				oldestKey, oldestAt = k, e.at
			}
		}
		delete(vodCache, oldestKey)
	}
	cp := make([]byte, len(body))
	copy(cp, body)
	vodCache[exactURL] = vodCacheEntry{body: cp, at: time.Now()}
}
