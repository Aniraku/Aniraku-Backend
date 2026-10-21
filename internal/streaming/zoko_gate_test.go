package streaming

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestZokoManifestGatePinsTheFanOut pins the gate's reason: the CDN
// blackholes zoko manifests from this egress, and the old flow (45s probe
// timeout + MAL retry with a second 45s probe) pinned the /servers fan-out
// at the 85000ms context cap. The fixed flow must (1) judge a blackholed
// manifest within one short probe budget, (2) skip the MAL fallback (same
// edge, same egress, the retry can only reproduce the block), and
// (3) return immediately for the rest of the backoff window, then recover
// automatically once the manifest answers again.
func TestZokoManifestGatePinsTheFanOut(t *testing.T) {
	oldBudget, oldBackoff, oldBase := zokoProbeBudget, zokoManifestGateBackoff, zokoBase
	t.Cleanup(func() {
		zokoProbeBudget, zokoManifestGateBackoff, zokoBase = oldBudget, oldBackoff, oldBase
	})
	zokoProbeBudget = 300 * time.Millisecond
	zokoManifestGateBackoff = 1 * time.Second

	var embedHits, malHits, m3u8Hits atomic.Int64
	var m3u8Fast atomic.Bool

	mux := http.NewServeMux()
	embedPage := func(w http.ResponseWriter, _ *http.Request) {
		embedHits.Add(1)
		payload := zokoPayload{Src: zokoBase + "/manifest.m3u8"}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("payload marshal: %v", err)
		}
		key := []byte(zokoObfKey)
		blob := make([]byte, len(raw))
		for i := range raw {
			blob[i] = raw[i] ^ key[i%len(key)]
		}
		fmt.Fprintf(w, "window.__P=\"%s\"", base64.StdEncoding.EncodeToString(blob))
	}
	mux.HandleFunc("/stream/ani/", embedPage)
	mux.HandleFunc("/stream/mal/", func(w http.ResponseWriter, r *http.Request) {
		malHits.Add(1)
		embedPage(w, r)
	})
	mux.HandleFunc("/manifest.m3u8", func(w http.ResponseWriter, r *http.Request) {
		m3u8Hits.Add(1)
		if m3u8Fast.Load() {
			w.Write([]byte("#EXTM3U\n#EXT-X-VERSION:3\n"))
			return
		}
		// the classic egress-blocked edge: accepts the connection, never
		// answers before the probe's context gives up
		select {
		case <-time.After(60 * time.Second):
		case <-r.Context().Done():
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	zokoBase = server.URL

	p := NewZokoProvider(zerolog.Nop())
	p.client = server.Client()
	ctx := context.Background()

	// Round 1: one embed + one budgeted probe; MAL fallback must be skipped.
	start := time.Now()
	sr, err := p.FindEpisodeSource(ctx, "185874", 9, "sub")
	round1 := time.Since(start)
	if err != nil || sr != nil {
		t.Fatalf("round 1 = (%v, %v), want blocked drop (nil, nil)", sr, err)
	}
	if round1 > 2*time.Second {
		t.Fatalf("round 1 took %v, want the ~300ms probe budget, not the 45s client timeout", round1.Round(100*time.Millisecond))
	}
	if malHits.Load() != 0 {
		t.Fatalf("MAL fallback ran %d times, want skipped (same CDN edge)", malHits.Load())
	}
	// gate armed: until is in the future
	if until := p.manifestBlocked.Load(); until <= time.Now().UnixNano() {
		t.Fatal("gate not armed after probe failure")
	}

	// Round 2 (inside the window): no upstream call at all.
	start = time.Now()
	sr2, err2 := p.FindEpisodeSource(ctx, "185874", 9, "sub")
	round2 := time.Since(start)
	if err2 != nil || sr2 != nil {
		t.Fatalf("round 2 = (%v, %v), want gated drop", sr2, err2)
	}
	if round2 > 300*time.Millisecond {
		t.Fatalf("round 2 took %v, want near-zero (gate skip, no upstream)", round2.Round(10*time.Millisecond))
	}
	if embedHits.Load() != 1 {
		t.Fatalf("round 2 embed hits = %d, want still 1 (gate must skip before any fetch)", embedHits.Load())
	}

	// Round 3 (window expired, manifest healthy again): recovery.
	time.Sleep(1100 * time.Millisecond)
	m3u8Fast.Store(true)
	sr3, err3 := p.FindEpisodeSource(ctx, "185874", 9, "sub")
	if err3 != nil {
		t.Fatalf("round 3 error: %v", err3)
	}
	if sr3 == nil || len(sr3.Sources) != 1 || sr3.Sources[0].URL != zokoBase+"/manifest.m3u8" {
		t.Fatalf("round 3 = %+v, want the recovered source", sr3)
	}
	if until := p.manifestBlocked.Load(); until != 0 {
		t.Fatal("successful probe must clear the gate")
	}
}
