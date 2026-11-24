package streaming

import (
	"bytes"
	"encoding/base64"
	"testing"
)

// flixcloudWrapForTest mirrors the CDN wrapper: b64(XOR(plain, pk)).
func flixcloudWrapForTest(plain string, pk []byte) []byte {
	wrapped := make([]byte, len(plain))
	for i := range plain {
		wrapped[i] = plain[i] ^ pk[i%len(pk)]
	}
	return []byte(base64.StdEncoding.EncodeToString(wrapped))
}

func resetFlixcloudDecodeState(t *testing.T) {
	t.Helper()
	reset := func() {
		flixcloudPKMu.Lock()
		flixcloudPKSet = map[string][]byte{flixcloudPKFallbackHex: flixcloudPKFallback}
		flixcloudPKOrder = []string{flixcloudPKFallbackHex}
		flixcloudPKMu.Unlock()

		flixcloudURLMu.Lock()
		flixcloudURLKeys = map[string][]byte{}
		flixcloudURLOrder = nil
		flixcloudURLMu.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

// The key is minted per embed-page render, so a body can arrive wrapped with
// a generation this process never resolved. The stable playlist opening must
// recover all 32 key bytes without any prior state.
func TestDecodeFlixcloudPlaylistRecoversUnseenKey(t *testing.T) {
	resetFlixcloudDecodeState(t)

	pk := make([]byte, 32)
	for i := range pk {
		pk[i] = byte(0x40 + i*3)
	}
	plain := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\"," +
		"LANGUAGE=\"jpn\",NAME=\"Native\",DEFAULT=YES,AUTOSELECT=YES," +
		"URI=\"../abc/audio/audio.m3u8\"\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=5184,RESOLUTION=1920x1080,AUDIO=\"audio\"," +
		"CODECS=\"avc1.64001f,mp4a.40.2\"\n../abc/video.m3u8\n"

	if got := DecodeFlixcloudPlaylist(flixcloudWrapForTest(plain, pk)); string(got) != plain {
		t.Fatalf("unseen-generation decode failed:\n got %q\nwant %q", got, plain)
	}
}

// A master binds its key to the variant URLs it names. Even after the global
// candidate set loses the key (and even for a variant opening the recovery
// table does not know), the variant must decode through the URL binding.
func TestDecodeFlixcloudPlaylistForURLBindsVariant(t *testing.T) {
	resetFlixcloudDecodeState(t)

	masterURL := "https://fetch10.flixcloud.cc/_v7/abc/master.m3u8?token=first"
	variantURL := "https://fetch10.flixcloud.cc/_v7/abc/video.m3u8"
	pk := bytes.Repeat([]byte{0x7c}, 32)

	master := "#EXTM3U\n#EXT-X-VERSION:3\n" +
		"#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",URI=\"video.m3u8\"\n" +
		"#EXT-X-STREAM-INF:BANDWIDTH=1\nvideo.m3u8\n"
	if got := DecodeFlixcloudPlaylistForURL(masterURL, flixcloudWrapForTest(master, pk)); string(got) != master {
		t.Fatalf("master decode failed: %q", got)
	}

	// Simulate eviction of the key from the global generation cache.
	flixcloudPKMu.Lock()
	flixcloudPKSet = map[string][]byte{flixcloudPKFallbackHex: flixcloudPKFallback}
	flixcloudPKOrder = []string{flixcloudPKFallbackHex}
	flixcloudPKMu.Unlock()

	// Opening deliberately absent from flixcloudPlaylistOpenings.
	variant := "#EXTM3U\n#EXT-X-CUSTOM:probe\n#EXTINF:6.0,\nseg-0.png\n#EXT-X-ENDLIST\n"
	if got := DecodeFlixcloudPlaylistForURL(variantURL, flixcloudWrapForTest(variant, pk)); string(got) != variant {
		t.Fatalf("variant decode via URL binding failed: %q", got)
	}
}

// Variant playlists can start with the key tag directly (no VERSION line).
// That opening was missing from the recovery table, so a body whose URL
// binding was absent stayed wrapped forever.
func TestDecodeFlixcloudPlaylistRecoversKeyFirstVariant(t *testing.T) {
	resetFlixcloudDecodeState(t)

	pk := make([]byte, 32)
	for i := range pk {
		pk[i] = byte(0x11 + i*7)
	}
	plain := "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x0fc0f6959de0a47ff34b7fb43b2b7e79\n" +
		"#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n" +
		"#EXTINF:6.006000,\nseg-0-f1-v1-a0.webp\n#EXT-X-ENDLIST\n"

	if got := DecodeFlixcloudPlaylist(flixcloudWrapForTest(plain, pk)); string(got) != plain {
		t.Fatalf("key-first variant decode failed:\n got %q\nwant %q", got, plain)
	}
}

func TestFlixcloudURLKeyIgnoresTokenQuery(t *testing.T) {
	resetFlixcloudDecodeState(t)

	pk := bytes.Repeat([]byte{0x5a}, 32)
	flixcloudBindURLKey("https://fetch9.flixcloud.cc/_v7/id/master.m3u8?token=one", pk)
	got := flixcloudURLKey("https://fetch9.flixcloud.cc/_v7/id/master.m3u8?token=two")
	if !bytes.Equal(got, pk) {
		t.Fatalf("token rotation lost the bound key: got %x", got)
	}
}
