package streaming

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
)

// Fixtures captured while reverse-engineering the flixcloud player
// (flixcloud.cc/e/{id}, 2026-11-16). Both wasm payloads are real per-page
// w_payload modules: the code structure AND constants randomize per render,
// so the tests pin one sample of each shape and the outputs the browser
// produced for them.
const (
	fcWasmP1 = "AGFzbQEAAAABEQNgAX8AYAV/f39/fwBgAAF/AwQDAAECBQMBAAEGBgF/AUEACwcZBAZtZW1vcnkCAAJfcwAAAl9yAAECX2MAAgrOAQMGACAAJAALhgEBA39BACEFA0ACQCAFIARPDQAgACAFai0AACABIAVqLQAAcyACIAVqLQAAcyEGIAZBwAFqQf8BcSEGIAZB5gFzIQYgBkGcAXMhBiAGQQd2IAZBAXRB/wFxciEGIAVBGmwjAGpB/wFxIQcgBiAHcyEGIAMgBWogBjoAACAFQQFqIQUMAQsLCz0BAX9BACEAA0ACQCAAQSBPDQBBkBAgAGpB0A8gAGotAABB8A8gAGotAABzOgAAIABBAWohAAwBCwtBkBALC0cBAEHQDwtAta0sh4/Md9gUwUejogMvsCHiNBD2XvX1eIMQqLFQ5KMYKUub3FYpSY4n1IzCTAWm846nxaIAZQIdm0a84pID5w=="
	fcWasmP2 = "AGFzbQEAAAABEQNgAX8AYAV/f39/fwBgAAF/AwQDAAECBQMBAAEGBgF/AUEACwcZBAZtZW1vcnkCAAJfcwAAAl9yAAECX2MAAgrGAQMGACAAJAALfwEDf0EAIQUDQAJAIAUgBE8NACAAIAVqLQAAIAEgBWotAABzIAIgBWotAABzIQYgBkEHdEH/AXEgBkEBdnIhBiAGQbIBakH/AXEhBiAGQckBcyEGIAVByQBsIwBqQf8BcSEHIAYgB3MhBiADIAVqIAY6AAAgBUEBaiEFDAELCws9AQF/QQAhAANAAkAgAEEgTw0AQZAQIABqQdAPIABqLQAAQfAPIABqLQAAczoAACAAQQFqIQAMAQsLQZAQCwtHAQBB0A8LQOZl0Isw5KrS46VyFaSdDzDRfa6lLqIQ7Y4d2F2Q2vRdS+G3l2N+9EN5Q+E6xNIlJgMRPXB6/IAa6wWOScMYExk="
)

// Fragment inputs captured from a live decrypt attempt (identical inputs
// fed to both modules below).
var (
	fcFrag1, _ = hex.DecodeString("57a90343a9e74b2fa76c1d4259f8cfdb23a08b2e9978626cbde4e0bfabed09c7")
	fcFrag2, _ = hex.DecodeString("97f951cf9a72369c23f8b9559bf2e1009e2c6a30747ab9c4280701817bb5eaf4")
	fcFrag3, _ = hex.DecodeString("b873e4e9c017a4130f7df944b12eab1c789e5d6fcfbfdc2d20b42e5092a66051")
	fcSeedInt  = uint32(2789513915) // 0xa6449abb
)

func mustWasm(t *testing.T, b64 string) []byte {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("fixture decode: %v", err)
	}
	return raw
}

// The two sampled modules produce DIFFERENT mixes for identical inputs —
// pinning both proves we execute the shipped module instead of reimplementing
// one frozen transform.
func TestFlixcloudMixMatchesBrowser(t *testing.T) {
	cases := []struct {
		name string
		b64  string
		want string
	}{
		{"P1 (+192,^110,rotl1,mul26)", fcWasmP1, "3fe6f7b7b04d9045e902710b613059baa424833ff2d38c6f359fb4d16324b551"},
		{"P2 (rotl7,+178,^201,mul73)", fcWasmP2, "9c8e893bbd122671bd233c4c857d045e16e6048565e1fdf7d648754eadf8f3b8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mod, err := flixcloudInstantiate(mustWasm(t, tc.b64))
			if err != nil {
				t.Fatalf("instantiate: %v", err)
			}
			got, err := flixcloudMix(mod, fcFrag1, fcFrag2, fcFrag3, fcSeedInt)
			if err != nil {
				t.Fatalf("mix: %v", err)
			}
			if hex.EncodeToString(got) != tc.want {
				t.Fatalf("mix mismatch:\n got %x\nwant %s", got, tc.want)
			}
		})
	}
}

// _c's output is the static playlist XOR key; both sampled modules embed
// the same data-section key material.
func TestFlixcloudPlaylistKey(t *testing.T) {
	want := "ad84671c539a5e919ae6932f604f2a16d26c93d5545e90f76518561453c2e744"
	for _, b64 := range []string{fcWasmP1, fcWasmP2} {
		mod, err := flixcloudInstantiate(mustWasm(t, b64))
		if err != nil {
			t.Fatalf("instantiate: %v", err)
		}
		pk, err := flixcloudPlaylistKey(mod)
		if err != nil {
			t.Fatalf("playlist key: %v", err)
		}
		if hex.EncodeToString(pk) != want {
			t.Fatalf("playlist key mismatch: got %x want %s", pk, want)
		}
	}
}

// Field names must reproduce the player's ar() chain exactly — one wrong
// substring and every lookup silently misses.
func TestFlixcloudFieldNames(t *testing.T) {
	keyField, ivField, container, array, object, tokenField, keyFrag2 := flixcloudFieldNames("42b30df494e14d9a")
	if keyField != "kf_59975635" || ivField != "ivf_d25851b2" ||
		container != "cd_8c5b4277" || array != "ad_5aa86e15" || object != "od_026ea8a0" ||
		tokenField != "0691d551ece8ab26_ece8ab26" {
		t.Fatalf("field names mismatch: kf=%s iv=%s cd=%s ad=%s od=%s token=%s",
			keyField, ivField, container, array, object, tokenField)
	}
	// keyFrag2 from the SECOND chain; its value was present on the sampled
	// page under this exact key.
	if keyFrag2 == "" || !strings.Contains(keyFrag2, "_") {
		t.Fatalf("keyFrag2 field name malformed: %q", keyFrag2)
	}
}

// The playlist wrapper round-trips: encode = b64(XOR(plain, pk)).
func TestDecodeFlixcloudPlaylist(t *testing.T) {
	flixcloudPKMu.RLock()
	pk := flixcloudPKFallback
	flixcloudPKMu.RUnlock()

	plain := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\nseg-0.png\n"
	wrapped := make([]byte, len(plain))
	for i := 0; i < len(plain); i++ {
		wrapped[i] = plain[i] ^ pk[i%len(pk)]
	}
	enc := []byte(base64.StdEncoding.EncodeToString(wrapped))

	if got := DecodeFlixcloudPlaylist(enc); string(got) != plain {
		t.Fatalf("decode mismatch:\n got %q\nwant %q", got, plain)
	}
	// Plaintext bodies pass through untouched.
	if got := DecodeFlixcloudPlaylist([]byte(plain)); string(got) != plain {
		t.Fatalf("passthrough mismatch: %q", got)
	}
	// Garbage returns as-is (proxy classification handles rejection).
	if got := DecodeFlixcloudPlaylist([]byte("not base64!!!")); string(got) != "not base64!!!" {
		t.Fatalf("garbage passthrough mismatch: %q", got)
	}
}

// A resolve landing on a cached page of a DIFFERENT build embeds a stale
// key; storing it must never break decodes of the other generation's
// playlists (the fallback always stays in the candidate set).
func TestDecodeFlixcloudPlaylistKeyGenerations(t *testing.T) {
	flixcloudPKMu.RLock()
	fallback := flixcloudPKFallback
	flixcloudPKMu.RUnlock()

	plain := "#EXTM3U\n#EXT-X-VERSION:3\nseg-0.png\n"
	wrapped := make([]byte, len(plain))
	for i := 0; i < len(plain); i++ {
		wrapped[i] = plain[i] ^ fallback[i%len(fallback)]
	}
	enc := []byte(base64.StdEncoding.EncodeToString(wrapped))

	// Simulate a resolve that saw a different build's key.
	flixcloudStorePlaylistKey([]byte("stale-key-from-an-older-build-xxxxxx"))
	if got := DecodeFlixcloudPlaylist(enc); string(got) != plain {
		t.Fatalf("decode broke after storing a stale key: got %q", got[:40])
	}
}
