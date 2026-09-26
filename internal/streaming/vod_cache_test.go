package streaming

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

const (
	testMasterPL = "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",NAME=\"ja\",URI=\"ja.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=1000000,Audio=\"a\"\nvariant.m3u8\n"
	testMediaPL  = "#EXTM3U\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:4\n#EXTINF:4.0,\nseg0.ts\n#EXT-X-ENDLIST\n"
	testLivePL   = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4.0,\nseg0.ts\n"
)

func TestIsCacheablePlaylist(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"master with stream-inf", testMasterPL, true},
		{"master with media only", "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,URI=\"a.m3u8\"\n", true},
		{"vod media with endlist", testMediaPL, true},
		{"vod media with playlist-type", "#EXTM3U\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:4\n", true},
		{"live playlist is never cached", testLivePL, false},
		{"rate-limit garbage body", `{"error":"rate limit exceeded","reason":"Slow down motherfucker!!!"}`, false},
		{"html error page", "<html><body>502 Bad Gateway</body></html>", false},
		{"empty body", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCacheablePlaylist([]byte(tc.body)); got != tc.want {
				t.Errorf("isCacheablePlaylist(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestVODCacheRoundTrip(t *testing.T) {
	const url = "https://vodcache-test.example/master.m3u8"
	if _, ok := VODCacheGet(url); ok {
		t.Fatal("cache hit before store")
	}
	if _, ok := VODCacheGet(""); ok {
		t.Fatal("empty URL must miss")
	}

	VODCacheSet(url, []byte(testMasterPL))
	got, ok := VODCacheGet(url)
	if !ok {
		t.Fatal("expected cache hit after store")
	}
	if string(got) != testMasterPL {
		t.Errorf("cached body mismatch:\n got: %q\nwant: %q", got, testMasterPL)
	}

	// The stored bytes must be a copy: callers rewrite bodies in place, and
	// a shared buffer would corrupt the next hit.
	got[0] = 'X'
	again, ok := VODCacheGet(url)
	if !ok {
		t.Fatal("expected second hit")
	}
	if again[0] == 'X' {
		t.Error("cache returned a aliased buffer instead of a copy")
	}
}

func TestVODCacheSetRejectsNonPlaylists(t *testing.T) {
	const url = "https://vodcache-test.example/seg.ts"
	VODCacheSet(url, []byte("not a playlist"))
	if _, ok := VODCacheGet(url); ok {
		t.Error("non-playlist body must not be stored")
	}

	const bigURL = "https://vodcache-test.example/big.m3u8"
	VODCacheSet(bigURL, bytes.Repeat([]byte("a"), vodCacheMaxBody+1))
	if _, ok := VODCacheGet(bigURL); ok {
		t.Error("oversized body must not be stored")
	}
}

func TestVODCacheExpiry(t *testing.T) {
	const url = "https://vodcache-test.example/expiring.m3u8"
	VODCacheSet(url, []byte(testMediaPL))

	vodCacheMu.Lock()
	e := vodCache[url]
	e.at = time.Now().Add(-vodCacheTTL - time.Second)
	vodCache[url] = e
	vodCacheMu.Unlock()

	if _, ok := VODCacheGet(url); ok {
		t.Error("expired entry must miss")
	}
	vodCacheMu.Lock()
	_, still := vodCache[url]
	vodCacheMu.Unlock()
	if still {
		t.Error("expired entry must be evicted on read")
	}
}

func TestVODCacheBounded(t *testing.T) {
	// Fill past the cap with unique keys; the map must never exceed it.
	for i := 0; i <= vodCacheMaxEnt+5; i++ {
		VODCacheSet(fmt.Sprintf("https://vodcache-test.example/fill/%d.m3u8", i), []byte(testMediaPL))
	}
	vodCacheMu.Lock()
	n := len(vodCache)
	vodCacheMu.Unlock()
	if n > vodCacheMaxEnt {
		t.Errorf("cache holds %d entries, cap is %d", n, vodCacheMaxEnt)
	}
}
