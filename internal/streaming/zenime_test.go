package streaming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// armsFixture serves info + watch JSON plus a playable master->media->seg
// chain, with counters proving the info cache works.
type armsFixture struct {
	server   *httptest.Server
	infoHits atomic.Int32
}

func newArmsFixture(t *testing.T) *armsFixture {
	t.Helper()
	f := &armsFixture{}
	var mux http.ServeMux
	episodes := func(prefix string, n int) string {
		var b strings.Builder
		b.WriteString(`{"episodes":[`)
		for i := 1; i <= n; i++ {
			if i > 1 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"id":"%s-%d","number":%d,"title":"Ep %d"}`, prefix, i, i, i)
		}
		b.WriteString("]}")
		return b.String()
	}
	mux.HandleFunc("/meta/anilist/info/", func(w http.ResponseWriter, r *http.Request) {
		f.infoHits.Add(1)
		prov := r.URL.Query().Get("provider")
		switch prov {
		case "xanime":
			_, _ = w.Write([]byte(episodes("x", 3)))
		case "animeparadise":
			_, _ = w.Write([]byte(episodes("p", 3)))
		case "hentaimama":
			_, _ = w.Write([]byte(episodes("h", 8)))
		default:
			_, _ = w.Write([]byte(`{"episodes":[]}`))
		}
	})
	mux.HandleFunc("/meta/anilist/watch", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		prov := q.Get("provider")
		id := q.Get("episodeId")
		_ = id
		ref := map[string]string{"xanime": "https://xanime.me/", "animeparadise": "https://stream.animeparadise.moe/", "hentaimama": "https://hentaimama.io/"}[prov]
		if ref == "" {
			ref = "https://example.com/"
		}
		host := "http://" + r.Host
		var servers string
		if prov == "xanime" {
			servers = `[{"name":"Sub Vidplay","url":"` + host + `/m-sub.m3u8","type":"sub"},{"name":"Dub Vidplay","url":"` + host + `/m-dub.m3u8","type":"dub"}]`
		} else {
			servers = `[{"name":"HD","url":"` + host + `/m.m3u8","type":"sub"}]`
		}
		fmt.Fprintf(w, `{"servers":%s,"sources":[{"url":"%s/m-sub.m3u8","quality":"auto","isM3U8":true}],"subtitles":[{"url":"%s/subs/en.vtt","lang":"en","label":"English"}],"headers":{"Referer":%q}}`,
			servers, host, host, ref)
	})
	chain := func(master, media, seg, segBody string) {
		mux.HandleFunc(master, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n" + media + "\n"))
		})
		mux.HandleFunc(media, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1\n" + seg + "\n"))
		})
		mux.HandleFunc(seg, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(segBody))
		})
	}
	chain("/m-sub.m3u8", "/m-media.m3u8", "/m-seg.ts", "G@videobytes-ok")
	chain("/m-dub.m3u8", "/m-dub-media.m3u8", "/m-dub-seg.ts", "G@videobytes-ok")
	chain("/m.m3u8", "/p-media.m3u8", "/p-seg.ts", "G@videobytes-ok")
	f.server = httptest.NewServer(&mux)
	t.Cleanup(f.server.Close)
	return f
}

func newZenimeTestProvider(t *testing.T, f *armsFixture) *ZenimeProvider {
	t.Helper()
	t.Setenv("ANIRAKU_ARMS_BASE", f.server.URL)
	p := NewZenimeProvider(zerolog.Nop())
	// Plain client: the netguard transport SSRF-blocks the 127.0.0.1
	// fixture server (same override pattern as the other provider tests).
	p.client = f.server.Client()
	return p
}

func TestZenimeSlotsAndSubs(t *testing.T) {
	f := newArmsFixture(t)
	p := newZenimeTestProvider(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil {
		t.Fatal("FindEpisodeSource returned nil")
	}
	if len(sr.Sources) != 1 {
		t.Fatalf("sources = %d, want 1 (Miru only — animeparadise dropped)", len(sr.Sources))
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Miru" {
		t.Fatalf("ServerNames = %v, want [Miru]", sr.ServerNames)
	}
	if len(sr.Sources[0].Subtitles) != 1 || sr.Sources[0].Subtitles[0].Lang != "en" {
		t.Fatalf("subs not mapped: %+v", sr.Sources[0].Subtitles)
	}
}

func TestZenimeDubSelect(t *testing.T) {
	f := newArmsFixture(t)
	p := newZenimeTestProvider(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "dub")
	if err != nil {
		t.Fatalf("dub FindEpisodeSource: %v", err)
	}
	if len(sr.Sources) == 0 {
		t.Fatal("dub: no sources")
	}
	if !strings.Contains(sr.Sources[0].URL, "m-dub.m3u8") {
		t.Fatalf("dub must pick the dub variant, got %s", sr.Sources[0].URL)
	}
	sr, err = p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("sub FindEpisodeSource: %v", err)
	}
	if len(sr.Sources) == 0 || !strings.Contains(sr.Sources[0].URL, "m-sub.m3u8") {
		t.Fatalf("sub must pick the sub variant, got %+v", sr)
	}
}

func TestZenimeHentai(t *testing.T) {
	f := newArmsFixture(t)
	p := newZenimeTestProvider(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindHentaiEpisodeSource(ctx, "113417", 1, "sub")
	if err != nil {
		t.Fatalf("hentai FindEpisodeSource: %v", err)
	}
	if len(sr.Sources) != 1 || len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Miru" {
		t.Fatalf("hentai = %+v names=%v, want 1 source named Miru", sr.Sources, sr.ServerNames)
	}
}

func TestIsEmbedPage(t *testing.T) {
	t.Parallel()

	// Gateway/proxy media URLs carry no extension but serve playlists —
	// misclassifying these hid Robin (paradise /m3u8?url= token URLs).
	media := []string{
		"https://x.example/a-master.m3u8?x=1",
		"https://x.example/manifest/abc/master.m3u8",
		"https://x.example/v/1.mp4",
		"https://stream.animeparadise.moe/m3u8?url=Ce3SCeOsXXVQufwnfTvT9EQRivsTGNr8x3Tx9ImuAJrxCr4",
		"https://xanivsrc10.org/media/s3v/412/573214/a-master.m3u8?slug=/media/s3v/412/573214&e=1790379706",
	}
	for _, u := range media {
		if isEmbedPage(u) {
			t.Fatalf("isEmbedPage(%s) = true, want false", u)
		}
	}
	embeds := []string{
		"https://hentaimama.io/?dt_embed=hls&p=abc&ep=1",
		"https://x.example/embed/abc",
		"https://x.example/watch/abc",
		"https://x.example/player/abc",
		"%", // unparseable: treated as embed (dropped unless allowed)
	}
	for _, u := range embeds {
		if !isEmbedPage(u) {
			t.Fatalf("isEmbedPage(%s) = false, want true", u)
		}
	}
}

func TestZenimeInfoCached(t *testing.T) {
	f := newArmsFixture(t)
	p := newZenimeTestProvider(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := p.FindEpisodeSource(ctx, "21", 1, "sub"); err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	if _, err := p.FindEpisodeSource(ctx, "21", 2, "sub"); err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	// 1 provider x 1 info fetch; episode-2 reuses the cached info.
	if got := f.infoHits.Load(); got != 1 {
		t.Fatalf("info hits = %d, want 1 (cached on second resolve)", got)
	}
}

// TestZenimeEmbedPassthrough pins the hentaimama path: embed player pages
// (no direct media URL) ship as type:"embed" for the client's embedded
// player instead of going through segment probing.
func TestZenimeEmbedPassthrough(t *testing.T) {
	var mux http.ServeMux
	mux.HandleFunc("/meta/anilist/watch", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"servers":[{"name":"HD","url":"https://hentaimama.io/?dt_embed=hls&p=abc&ep=1","type":"sub"}],"sources":[],"subtitles":[],"headers":{"Referer":"https://hentaimama.io/"}}`))
	})
	server := httptest.NewServer(&mux)
	defer server.Close()
	t.Setenv("ANIRAKU_ARMS_BASE", server.URL)
	p := NewZenimeProvider(zerolog.Nop())
	p.client = server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.resolveWatch(ctx, "h-1", "hentaimama", "sub", true)
	if err != nil {
		t.Fatalf("embed resolve: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want 1 embed source, got %+v", sr)
	}
	if got := sr.Sources[0].Type; got != "embed" {
		t.Fatalf("source type = %q, want embed", got)
	}
	if got := sr.Sources[0].Verification; got != "embed" {
		t.Fatalf("verification = %q, want embed", got)
	}
}

// TestZenimeSFWNeverEmbeds pins the product rule: regular titles resolve
// to HLS or nothing — embed pages must never list for SFW, even when they
// are the only candidates the API returns.
func TestZenimeSFWNeverEmbeds(t *testing.T) {
	var mux http.ServeMux
	mux.HandleFunc("/meta/anilist/watch", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"servers":[{"name":"HD","url":"https://hentaimama.io/?dt_embed=hls&p=abc&ep=1","type":"sub"}],"sources":[],"subtitles":[],"headers":{"Referer":"https://hentaimama.io/"}}`))
	})
	server := httptest.NewServer(&mux)
	defer server.Close()
	t.Setenv("ANIRAKU_ARMS_BASE", server.URL)
	p := NewZenimeProvider(zerolog.Nop())
	p.client = server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.resolveWatch(ctx, "h-1", "hentaimama", "sub", false)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if sr != nil {
		t.Fatalf("SFW must never return embeds, got %+v", sr)
	}
}

// TestZenimeInfoRetry pins the flap guard: a failing info fetch is
// retried once, converting arms per-title 500s into hits.
func TestZenimeInfoRetry(t *testing.T) {
	var infoHits atomic.Int32
	var mux http.ServeMux
	mux.HandleFunc("/meta/anilist/info/", func(w http.ResponseWriter, r *http.Request) {
		if infoHits.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"flap"}`))
			return
		}
		_, _ = w.Write([]byte(`{"episodes":[{"id":"x-1","number":1}]}`))
	})
	mux.HandleFunc("/meta/anilist/watch", func(w http.ResponseWriter, r *http.Request) {
		host := "http://" + r.Host
		fmt.Fprintf(w, `{"servers":[{"name":"HD","url":"%s/m.m3u8","type":"sub"}],"sources":[],"subtitles":[],"headers":{"Referer":%q}}`, host, host+"/")
	})
	mux.HandleFunc("/m.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nmedia.m3u8\n"))
	})
	mux.HandleFunc("/media.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1\nseg.ts\n"))
	})
	mux.HandleFunc("/seg.ts", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(append([]byte{0x47}, []byte("video-bytes")...))
	})
	server := httptest.NewServer(&mux)
	defer server.Close()
	t.Setenv("ANIRAKU_ARMS_BASE", server.URL)
	p := NewZenimeProvider(zerolog.Nop())
	p.client = server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// xanime only: fresh provider, first info 500s, retry must succeed.
	p.infoMu.Lock()
	p.info = make(map[string]*zenimeInfoEntry)
	p.infoMu.Unlock()
	eps, err := p.fetchInfo(ctx, "21", "xanime")
	if err != nil {
		t.Fatalf("info after retry: %v", err)
	}
	if len(eps) != 1 || eps[0].id != "x-1" {
		t.Fatalf("episodes = %+v", eps)
	}
	if got := infoHits.Load(); got != 2 {
		t.Fatalf("info hits = %d, want 2 (fail + retry)", got)
	}
}

// TestZenimeEmptyNotPoisoned pins the flap guard: an empty episode list
// (arms per-title flakiness) must not poison the slot for the full TTL —
// it expires within minutes so recovery is automatic.
func TestZenimeEmptyNotPoisoned(t *testing.T) {
	var mux http.ServeMux
	mux.HandleFunc("/meta/anilist/info/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"episodes":[]}`))
	})
	server := httptest.NewServer(&mux)
	defer server.Close()
	t.Setenv("ANIRAKU_ARMS_BASE", server.URL)
	p := NewZenimeProvider(zerolog.Nop())
	p.client = server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := p.fetchInfo(ctx, "21", "xanime"); err != nil {
		t.Fatalf("fetchInfo: %v", err)
	}
	p.infoMu.Lock()
	e, ok := p.info["21\x00xanime"]
	p.infoMu.Unlock()
	if !ok {
		t.Fatal("empty list should still be cached (briefly)")
	}
	if ttl := zenimeInfoTTL - time.Since(e.fetched); ttl > zenimeEmptyTTL+time.Minute {
		t.Fatalf("empty entry lives %v, want ~%v", ttl, zenimeEmptyTTL)
	}
}

func TestZenimeBlockedDropped(t *testing.T) {
	var mux http.ServeMux
	mux.HandleFunc("/meta/anilist/info/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"episodes":[{"id":"e1","number":1}]}`))
	})
	mux.HandleFunc("/meta/anilist/watch", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"servers":[],"sources":[],"subtitles":[],"headers":{}}`))
	})
	server := httptest.NewServer(&mux)
	defer server.Close()
	t.Setenv("ANIRAKU_ARMS_BASE", server.URL)
	p := NewZenimeProvider(zerolog.Nop())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("empty watch must not error, got %v", err)
	}
	if sr != nil {
		t.Fatalf("empty watch must yield nil, got %+v", sr)
	}
}
