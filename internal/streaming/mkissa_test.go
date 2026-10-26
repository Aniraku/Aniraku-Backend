package streaming

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func mkissaTestLaneKey() []byte {
	return bytes.Repeat([]byte{0x42}, 32)
}

func mkissaTestMask() []byte {
	return mkissaDeriveMask(mkissaBuildID, mkissaMaskParts)
}

func mkissaEncryptTobeparsed(laneKey []byte, v any) string {
	b, _ := json.Marshal(v)
	iv := bytes.Repeat([]byte{0x07}, 12)
	block, _ := aes.NewCipher(laneKey)
	gcm, _ := cipher.NewGCM(block)
	ct := gcm.Seal(nil, iv, b, nil)
	out := append([]byte{1}, iv...)
	out = append(out, ct...)
	return base64.StdEncoding.EncodeToString(out)
}

func newMkissaFixture(t *testing.T) (*httptest.Server, *httptest.Server, *httptest.Server, *MkissaProvider) {
	t.Helper()
	// media server for probes
	mmux := http.NewServeMux()
	mmux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100,RESOLUTION=640x360\nq/playlist.m3u8\n")
	})
	mmux.HandleFunc("/q/playlist.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg.ts\n")
	})
	mmux.HandleFunc("/q/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.Write(append([]byte{0x47}, make([]byte, 100)...))
	})
	media := httptest.NewServer(mmux)
	t.Cleanup(func() { media.Close() })

	// anilist server
	amux := http.NewServeMux()
	amux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"Media":{"title":{"romaji":"One Piece","english":"One Piece"}}}}`)
	})
	anilist := httptest.NewServer(amux)
	t.Cleanup(func() { anilist.Close() })

	laneKey := mkissaTestLaneKey()
	mask := mkissaTestMask()
	partB := make([]byte, 32)
	for i := 0; i < 32; i++ {
		partB[i] = laneKey[i] ^ mask[i]
	}
	partBStr := base64.StdEncoding.EncodeToString(partB)

	nowMs := timeNowMs()
	epoch := nowMs / 604800000
	if epoch > 0 && nowMs-epoch*604800000 < 86400000 {
		epoch--
	}

	// api server
	showID := "ReooPAxPMsHM4KPMY"
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/client-crypto/v1/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"epoch":%d,"partB":%q,"k":"k7"}`, epoch, partBStr)
	})
	apiMux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		b, _ := io.ReadAll(r.Body)
		raw := string(b)
		if strings.Contains(raw, "shows(search") {
			fmt.Fprintf(w, `{"data":{"shows":{"edges":[{"_id":%q,"name":"ONE PIECE","englishName":"One Piece","nativeName":"","availableEpisodes":{"sub":1181},"availableEpisodesDetail":{"sub":["1","2"],"dub":["1"]},"aniListId":"21"}]}}}`, showID)
			return
		}
		// episode
		dec := mkissaDecrypted{}
		dec.Episode.SourceUrls = []mkissaSource{{
			Name:         "default",
			ExtractedURL: media.URL + "/master.m3u8",
			Type:         "player",
			Priority:     10,
		}}
		tb := mkissaEncryptTobeparsed(laneKey, dec)
		fmt.Fprintf(w, `{"data":{"tobeparsed":%q}}`, tb)
	})
	api := httptest.NewServer(apiMux)
	t.Cleanup(func() { api.Close() })

	os.Setenv("MKISSA_API", api.URL)
	t.Cleanup(func() { os.Unsetenv("MKISSA_API") })

	mkissaAPIGap = 0
	p := NewMkissaProvider(zerolog.Nop())
	p.apiGap = 0
	p.anilistURL = anilist.URL
	// httptest servers are loopback: bypass the SSRF guard in tests.
	p.client = &http.Client{Timeout: 30 * time.Second}
	t.Cleanup(func() { mkissaAPIGap = 4 * time.Second })
	return api, anilist, media, p
}

func timeNowMs() int64 {
	return 1700000000000 // fixed; epoch calc
}

func TestMkissaFindEpisodeSource(t *testing.T) {
	_, _, _, p := newMkissaFixture(t)
	sr, err := p.FindEpisodeSource(context.Background(), "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) == 0 {
		t.Fatal("expected sources")
	}
	if sr.ServerNames[0] != "Chuu" {
		t.Fatalf("server name = %q, want Chuu", sr.ServerNames[0])
	}
	if !strings.Contains(sr.Sources[0].URL, "master.m3u8") {
		t.Fatalf("url = %q", sr.Sources[0].URL)
	}
}

func TestMkissaDubWorks(t *testing.T) {
	// anilist returns one piece; api search dub returns empty detail
	// Reuse fixture but override? Simpler: expect no panic.
	_, _, _, p := newMkissaFixture(t)
	// force dub by calling; fixture has dub ["1"], so should resolve
	sr, err := p.FindEpisodeSource(context.Background(), "21", 1, "dub")
	if err != nil {
		t.Fatalf("dub: %v", err)
	}
	if sr == nil {
		t.Fatal("expected dub source")
	}
}

func TestMkissaCryptoRoundTrip(t *testing.T) {
	laneKey := mkissaTestLaneKey()
	mask := mkissaTestMask()
	// verify mask salt
	if len(mask) != 32 {
		t.Fatal("mask len")
	}
	// boot token deterministic
	tok := mkissaBootToken(mask, mkissaBuildID, 1234)
	if len(tok) != 64 {
		t.Fatalf("boot token len %d", len(tok))
	}
	// aaReq round trip
	aa, err := mkissaAaReq(laneKey, 1234, mkissaBuildID, mkissaEpisodeQueryHash, 300000)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.StdEncoding.DecodeString(aa)
	if raw[0] != 1 {
		t.Fatal("version byte")
	}
	// decrypt tobeparsed
	v := map[string]any{"hello": "world"}
	tb := mkissaEncryptTobeparsed(laneKey, v)
	pt, err := mkissaDecryptTobeparsed(laneKey, tb)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(pt, []byte("hello")) {
		t.Fatal("decrypted missing")
	}
}
