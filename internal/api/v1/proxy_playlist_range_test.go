package v1

import (
	"fmt"
	"net/http"
	"testing"
)

// A ranged playlist fetch (bytes=0-) must be SERVED, not502'd: extension-less
// playlist legs like ReCloud's /p?t= don't match the ".m3u8" Range exclusion,
// so a player that ranges every request upstream-answers its playlists with
// 206. The old code rejected every non-200 and playback died as soon as the
// 45 s VOD cache went cold (observed live 2026-10-07).
func TestClassifyPlaylistResponse(t *testing.T) {
	t.Parallel()

	fullPlaylist := []byte("#EXTM3U\n#EXT-X-TARGETDURATION:13\n#EXTINF:3.629,\nseg1.ts\n#EXT-X-ENDLIST\n")
	binary := []byte{0x47, 0x40, 0x00, 0x10, 0x00, 0x01, 0xc1, 0x00} // TS sync byte
	complete := fmt.Sprintf("bytes 0-%d/%d", len(fullPlaylist)-1, len(fullPlaylist))
	truncated := fmt.Sprintf("bytes 0-500/%d", len(fullPlaylist)+500)

	cases := []struct {
		name         string
		status       int
		contentRange string
		body         []byte
		want         playlistAction
	}{
		{"200 playlist serves", http.StatusOK, "", fullPlaylist, playlistServe},
		{"200 binary is media misfire", http.StatusOK, "", binary, playlistMedia},
		{"206 playlist covering whole file serves", http.StatusPartialContent, complete, fullPlaylist, playlistServe},
		{"206 truncated playlist rejects", http.StatusPartialContent, truncated, fullPlaylist, playlistReject},
		{"206 playlist without Content-Range rejects", http.StatusPartialContent, "", fullPlaylist, playlistReject},
		{"206 binary passes through as media", http.StatusPartialContent, "bytes 0-7/999", binary, playlistMedia},
		{"404 rejects", http.StatusNotFound, "", fullPlaylist, playlistReject},
		{"403 rejects", http.StatusForbidden, "", fullPlaylist, playlistReject},
		{"500 rejects", http.StatusInternalServerError, "", binary, playlistReject},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyPlaylistResponse(tc.status, tc.contentRange, tc.body); got != tc.want {
				t.Errorf("classifyPlaylistResponse(%d, %q, %d bytes) = %v, want %v",
					tc.status, tc.contentRange, len(tc.body), got, tc.want)
			}
		})
	}
}

func TestPartialCoversWhole(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		contentRange string
		n            int
		want         bool
	}{
		{"complete open range", "bytes 0-1088/1089", 1089, true},
		{"complete small file", "bytes 0-0/1", 1, true},
		{"total beyond body", "bytes 0-1088/2000", 1089, false},
		{"body beyond total", "bytes 0-1088/1089", 500, false},
		{"nonzero start", "bytes 10-1088/1089", 1089, false},
		{"end short of total", "bytes 0-1087/1089", 1089, false},
		{"missing header", "", 1089, false},
		{"unknown total", "bytes 0-1088/*", 1089, false},
		{"no total", "bytes 0-1088", 1089, false},
		{"no dash", "bytes 1089/1089", 1089, false},
		{"not a byte range", "garbage", 1089, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := partialCoversWhole(tc.contentRange, tc.n); got != tc.want {
				t.Errorf("partialCoversWhole(%q, %d) = %v, want %v", tc.contentRange, tc.n, got, tc.want)
			}
		})
	}
}
