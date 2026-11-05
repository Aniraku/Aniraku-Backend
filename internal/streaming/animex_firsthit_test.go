package streaming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestAnimeXFirstHitStopsAtFirstServer pins the /stream fast path:
// sub-providers resolve sequentially in plyr priority order and the first
// success stops before any other sub-provider's sources-API call. The
// player path must spend one API hit per request, not the whole fingerprint
// budget — that budget is what answered 429 "fingerprint source limit" in
// cycles on the VPS.
func TestAnimeXFirstHitStopsAtFirstServer(t *testing.T) {
	var mu sync.Mutex
	apiCalls := map[string]int{}

	plyrBase := animexPlyrBase
	t.Cleanup(func() { animexPlyrBase = plyrBase })

	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/rest/api/sources"):
			provider := r.URL.Query().Get("providerId")
			mu.Lock()
			apiCalls[provider]++
			mu.Unlock()
			if provider != "beep" {
				http.Error(w, "only beep should be queried", http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, `{"sources":[{"url":"%s/m/master.m3u8","quality":"auto","type":"hls"}],"headers":{},"tracks":[],"chapters":[]}`, serverURL)
		case r.URL.Path == "/m/master.m3u8":
			// Two-level playlist: master -> variant -> segment.
			fmt.Fprintf(w, "#EXTM3U\n%s/m/variant.m3u8?x=1\n", serverURL)
		case strings.HasPrefix(r.URL.Path, "/m/variant.m3u8"):
			fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\n%s/m/seg.ts\n#EXT-X-ENDLIST", serverURL)
		case r.URL.Path == "/m/seg.ts":
			w.Write([]byte("segmentbytes"))
		case r.URL.Path == "/e/1/1":
			w.Write([]byte(`servers:{subProviders:[{id:"beep",default:true},{id:"yuki"}],dubProviders:[{id:"yuki"}]},id:"bleach-fixture-x",anilistId:21,title:"Bleach"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverURL = server.URL
	animexPlyrBase = server.URL

	p := NewAnimeXProvider(zerolog.Nop(), server.URL)
	// httptest's default client has no cookie jar; ensureSession reads it.
	jar, _ := cookiejar.New(nil)
	c := *server.Client()
	c.Jar = jar
	p.client = &c
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want exactly one source, got %+v", sr)
	}
	if sr.ServerName != "Lumi" {
		t.Fatalf("server name = %q, want Lumi (beep)", sr.ServerName)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(apiCalls) != 1 || apiCalls["beep"] != 1 {
		t.Fatalf("first-hit must call only beep once, got %v", apiCalls)
	}
}

// TestAnimeXCooldownEscalation pins the throttle ladder: a 429 inside an
// already-open window is the same throttle episode (no escalation), a 429
// after the window expired is a new cycle and doubles the window up to the
// cap, and a healthy answer (resetThrottle) restarts the ladder.
func TestAnimeXCooldownEscalation(t *testing.T) {
	p := &AnimeXProvider{}
	window := func() time.Duration {
		p.limitMu.Lock()
		defer p.limitMu.Unlock()
		return time.Until(p.limitUntil)
	}
	// expire the window manually, the cycle that follows is a new impact.
	expire := func() {
		p.limitMu.Lock()
		p.limitUntil = time.Now().Add(-time.Second)
		p.limitMu.Unlock()
	}

	p.setCooldown(30 * time.Second)
	if w := window(); w < 25*time.Second || w > 31*time.Second {
		t.Fatalf("first 429 must open the base 30s window, got %s", w)
	}
	p.setCooldown(30 * time.Second)
	if w := window(); w < 25*time.Second || w > 31*time.Second {
		t.Fatalf("in-window 429 is the same episode, must not escalate, got %s", w)
	}
	expire()
	p.setCooldown(30 * time.Second)
	if w := window(); w < 55*time.Second || w > 61*time.Second {
		t.Fatalf("second throttle cycle must double to 60s, got %s", w)
	}
	expire()
	p.setCooldown(30 * time.Second)
	if w := window(); w < 115*time.Second {
		t.Fatalf("third throttle cycle must escalate, got %s", w)
	}
	if w := window(); w > 121*time.Second {
		t.Fatalf("third throttle cycle must sit at the 2m cap, got %s", w)
	}
	p.resetThrottle()
	if p.coolingDown() {
		t.Fatal("healthy answer must clear the cooldown")
	}
	p.setCooldown(30 * time.Second)
	if w := window(); w < 25*time.Second || w > 31*time.Second {
		t.Fatalf("after reset the ladder restarts at the base window, got %s", w)
	}
}
