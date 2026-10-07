package streaming

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type mkissaFixture struct {
	t     *testing.T
	api   *httptest.Server
	media *httptest.Server
	dubEp []string
	// Optional overrides set by tests before the first request: raw edge
	// list (JSON) replacing the default show, and the AniList titles the
	// provider will search with.
	edges   string
	romaji  string
	english string
}

const mkissaTestShowID = "ReooPAxPMsHM4KPMY"

// Canned engine output: one direct Default m3u8, one Uni (uns.bio) m3u8,
// one mp4upload mp4, one pure embed (skipped), one unknown kind (skipped).
// Priorities are fractional exactly like the real engine — an int-typed
// Priority field makes Go reject the whole response line and hang until
// the call timeout, so the fixture must keep them fractional.
func mkissaStubOutput(mediaBase string) string {
	return fmt.Sprintf(`{"id":0,"showId":%q,"audio":"sub","results":[{"episode":"1","sources":[
{"name":"Default","url":"https://mkissa.to/e/x","extractedUrl":%q,"type":"player","priority":10},
{"name":"Uni","url":"https://watchanime.uns.bio/#x","extractedUrl":%q,"type":"file","priority":5.2},
{"name":"Mp4","url":"https://mp4upload.com/embed-abc.html","extractedUrl":"%s/v.mp4","type":"file","priority":4.5},
{"name":"Ss-Hls","url":"https://streamsb.net/e/abc.html","type":"embed","priority":3.5},
{"name":"Weird","url":"https://x.example/e","extractedUrl":"https://x.example/e","type":"file","priority":1}
]}]}`,
		mkissaTestShowID, mediaBase+"/master.m3u8", mediaBase+"/uni.m3u8", mediaBase)
}

func newMkissaFixture(t *testing.T, dubEps []string, stubOut string) *mkissaFixture {
	t.Helper()
	f := &mkissaFixture{t: t, dubEp: dubEps, romaji: "ONE PIECE", english: "One Piece"}
	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if f.edges != "" {
			fmt.Fprintf(w, `{"data":{"shows":{"edges":[%s]}}}`, f.edges)
			return
		}
		var dubList string
		if f.dubEp == nil {
			dubList = `[]`
		} else {
			q := make([]string, 0, len(f.dubEp))
			for _, e := range f.dubEp {
				q = append(q, fmt.Sprintf("%q", e))
			}
			dubList = `[` + strings.Join(q, ",") + `]`
		}
		fmt.Fprintf(w, `{"data":{"shows":{"edges":[{"_id":%q,"name":"ONE PIECE","englishName":"One Piece","nativeName":"","availableEpisodes":{"sub":1181},"availableEpisodesDetail":{"sub":["1","2"],"dub":%s},"aniListId":"20"}]}}}`,
			mkissaTestShowID, dubList)
	})
	mux.HandleFunc("/anilist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":{"Media":{"title":{"romaji":%q,"english":%q}}}}`, f.romaji, f.english)
	})
	f.api = httptest.NewServer(mux)
	mmux := http.NewServeMux()
	mmux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100,RESOLUTION=640x360\nq/playlist.m3u8\n")
	})
	mmux.HandleFunc("/uni.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100,RESOLUTION=640x360\nq/playlist.m3u8\n")
	})
	mmux.HandleFunc("/q/playlist.m3u8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg.ts\n")
	})
	mmux.HandleFunc("/q/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		w.Write(append([]byte{0x47}, bytes1k...))
	})
	mmux.HandleFunc("/v.mp4", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.Write(bytes1k)
	})
	f.media = httptest.NewServer(mmux)
	if stubOut == "" {
		stubOut = mkissaStubOutput(f.media.URL)
	} else {
		// {{media}} placeholder lets a test script multi-line canned
		// output (throttle-then-succeed) before the media URL exists.
		stubOut = strings.ReplaceAll(stubOut, "{{media}}", f.media.URL)
	}
	dir := t.TempDir()
	out := filepath.Join(dir, "out.json")
	if err := os.WriteFile(out, []byte(stubOut), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MKISSA_STUB_OUT", out)
	t.Cleanup(func() { f.api.Close(); f.media.Close() })
	return f
}

// TestMkissaDaemonStub is NOT a real test: the fixture re-executes the
// test binary with this name to serve canned daemon responses over the
// line protocol (shell text tools buffer pipes, so sh/awk/sed stubs hang;
// a Go helper writes unbuffered). Canned output carries "id":0, replaced
// with the request id per line. Canned files are pretty-printed, so every
// segment is collapsed to one line (the parent frames on newlines);
// segments separated by a "===NEXT===" marker line answer successive
// requests (the last repeats) so a test can script e.g. throttle-then-
// succeed sequences.
func TestMkissaDaemonStub(t *testing.T) {
	if os.Getenv("GO_MKISSA_STUB") != "1" {
		return
	}
	raw, err := os.ReadFile(os.Getenv("MKISSA_STUB_OUT"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "stub: read canned:", err)
		os.Exit(2)
	}
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	segs := strings.Split(string(raw), "\n===NEXT===\n")
	for i, seg := range segs {
		segs[i] = strings.ReplaceAll(seg, "\n", "")
	}
	var served int
	for sc.Scan() {
		var req struct {
			ID uint64 `json:"id"`
		}
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			continue
		}
		idx := served
		if idx >= len(segs) {
			idx = len(segs) - 1
		}
		served++
		resp := strings.Replace(segs[idx], `"id":0`, fmt.Sprintf(`"id":%d`, req.ID), 1)
		fmt.Fprintln(w, resp)
		w.Flush()
	}
}

var bytes1k = make([]byte, 1024)

func newMkissaTestProvider(f *mkissaFixture) *MkissaProvider {
	p := NewMkissaProvider(zerolog.Nop())
	p.apiBase = f.api.URL + "/api"
	p.anilistURL = f.api.URL + "/anilist"
	p.client = &http.Client{Timeout: 30 * time.Second}
	// Re-execute the test binary as the daemon (see TestMkissaDaemonStub).
	self, err := os.Executable()
	if err != nil {
		f.t.Fatal(err)
	}
	p.engineBin = self
	p.engineScript = "-test.run=TestMkissaDaemonStub"
	f.t.Setenv("GO_MKISSA_STUB", "1")
	return p
}

func mkissaTestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// Full sub chain through the stub engine: direct m3u8 + Uni m3u8 + mp4
// kept with cute names, embeds and unknown kinds skipped.
func TestMkissaFindEpisodeSource(t *testing.T) {
	f := newMkissaFixture(t, []string{"1"}, "")
	p := newMkissaTestProvider(f)
	sr, err := p.FindEpisodeSource(mkissaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if len(sr.Sources) != 3 {
		t.Fatalf("sources = %d, want 3 (Chuu + Umi + Kissy)", len(sr.Sources))
	}
	if sr.Sources[0].Type != "hls" || sr.ServerNames[0] != "Chuu" {
		t.Fatalf("source[0] = %+v name %q, want hls/Chuu", sr.Sources[0], sr.ServerNames[0])
	}
	if sr.Sources[1].Type != "hls" || sr.ServerNames[1] != "Umi" {
		t.Fatalf("source[1] = %+v name %q, want hls/Umi", sr.Sources[1], sr.ServerNames[1])
	}
	if sr.Sources[2].Type != "mp4" || sr.ServerNames[2] != "Kissy" {
		t.Fatalf("source[2] = %+v name %q, want mp4/Kissy", sr.Sources[2], sr.ServerNames[2])
	}
	if sr.Headers["Referer"] != mkissaReferer {
		t.Fatalf("headers = %v, want mkissa Referer", sr.Headers)
	}
	for _, s := range sr.Sources {
		if strings.Contains(s.URL, "streamsb") || strings.Contains(s.URL, "x.example") {
			t.Fatalf("embed/unknown source leaked: %q", s.URL)
		}
	}
}

// Strict per-lang: no dub episode entry means no dub resolve. The stub
// engine ignores stdin and always returns sources, so a dub failure
// proves the listing check fires before any engine spawn (strengthened
// by pointing the binary at /bin/false).
func TestMkissaStrictDubMissing(t *testing.T) {
	f := newMkissaFixture(t, nil, "")
	p := newMkissaTestProvider(f)
	p.engineBin = "/bin/false"
	if _, err := p.FindEpisodeSource(mkissaTestCtx(t), "20", 1, "dub"); err == nil {
		t.Fatal("expected dub to fail when the dub episode list is empty")
	}
	p2 := newMkissaTestProvider(f)
	if _, err := p2.FindEpisodeSource(mkissaTestCtx(t), "20", 1, "sub"); err != nil {
		t.Fatalf("sub must still resolve: %v", err)
	}
}

// Cached resolve results are isolated copies.
func TestMkissaResolveCacheIsolation(t *testing.T) {
	f := newMkissaFixture(t, []string{"1"}, "")
	p := newMkissaTestProvider(f)
	sr, err := p.FindEpisodeSource(mkissaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("sub: %v", err)
	}
	sr.Sources[0].URL = "MUTATED"
	again, err := p.FindEpisodeSource(mkissaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("sub again: %v", err)
	}
	if again.Sources[0].URL == "MUTATED" {
		t.Fatal("cache poisoned through the previously returned slice")
	}
}

func TestMkissaServerNames(t *testing.T) {
	for kind, want := range map[string]string{
		"default": "Chuu", "uv-mp4": "Mua", "mp4": "Kissy",
		"ss-hls": "Smooch", "sl-mp4": "Peck", "ok": "Xoxo",
		"uni": "Umi",
	} {
		if got := mkissaServerNames[kind]; got != want {
			t.Fatalf("kind %q = %q, want %q", kind, got, want)
		}
	}
}

func TestMkissaProbeReferer(t *testing.T) {
	for url, want := range map[string]string{
		"https://a6.mp4upload.com:183/d/x/video.mp4": "https://mp4upload.com/",
		"https://www.mp4upload.com/embed-abc.html":   "https://mp4upload.com/",
		"https://repackager.wixmp.com/master.m3u8":   mkissaReferer,
		"https://94.131.217.174/v4/x/master.m3u8":    mkissaReferer,
		"://bad-url": mkissaReferer,
	} {
		if got := mkissaProbeReferer(url); got != want {
			t.Fatalf("referer(%q) = %q, want %q", url, got, want)
		}
	}
}

func TestMkissaAniListID(t *testing.T) {
	if got := mkissaAniListID("21"); got != "21" {
		t.Fatalf("string id = %q", got)
	}
	if got := mkissaAniListID(float64(21)); got != "21" {
		t.Fatalf("numeric id = %q", got)
	}
	if got := mkissaAniListID(nil); got != "" {
		t.Fatalf("nil id = %q", got)
	}
}

// TestMkissaRejectsUnrelatedShowMatch replays the live mismatch trap:
// mkissa's dub-mode search for a sub-only title answers with unrelated
// shows (observed for Big X → "Boonie Bears: The Big Top Secret" with a
// null aniListId, plus "Pleasant Goat…" with a different one). Nothing in
// that result may win — neither a weak token-overlap hit (Boonie scores
// 35 on the shared word "big") nor an EXACT title that records a
// different aniListId (the old code took it at score 100). A clean
// resolve failure is the only correct outcome: no mkissa server beats
// the wrong anime on the list. The same fixture with the right aniListId
// proves the guard costs no coverage.
// TestMkissaRealEnginePayloadDecodes pins the decode contract against a
// REAL daemon response (testdata/mkissa_engine_response.json, captured
// from mkissa_daemon.mjs with URLs genericized). The engine emits
// fractional priorities, null extracted URLs and headers/downloads
// objects; when Priority was typed int, Go rejected the entire line as
// malformed JSON, no reply matched a pending call, and every prod resolve
// burned the 40s engine timeout — invisible to the canned stub, which
// used integer priorities.
func TestMkissaRealEnginePayloadDecodes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "mkissa_engine_response.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var out mkissaEngineOutput
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("real engine payload must decode: %v", err)
	}
	if out.ID != 42 || out.ShowID != "GtAbp5JCDWofKrGXj" {
		t.Fatalf("id/show = %d/%q, want 42/GtAbp5JCDWofKrGXj", out.ID, out.ShowID)
	}
	if len(out.Results) != 1 || out.Results[0].Episode != "1" {
		t.Fatalf("results = %+v, want one episode 1", out.Results)
	}
	srcs := out.Results[0].Sources
	if len(srcs) != 4 {
		t.Fatalf("sources = %d, want 4 (real payload)", len(srcs))
	}
	byName := map[string]mkissaEngineSource{}
	for _, s := range srcs {
		byName[s.Name] = s
	}
	if p := byName["Ok"].Priority; p != 3.5 {
		t.Fatalf("ok.ru priority = %v, want 3.5 (fractional)", p)
	}
	if byName["Ok"].ExtractedURL == "" {
		t.Fatal("ok.ru source must carry its direct m3u8")
	}
	if byName["Mp4"].ExtractedURL != "" {
		t.Fatalf("null extractedUrl must decode as empty, got %q", byName["Mp4"].ExtractedURL)
	}
	if byName["Ss-Hls"].ExtractedURL != "" || byName["Ss-Hls"].URL == "" {
		t.Fatal("pure embed must keep its page URL and no extracted URL")
	}
}

func TestMkissaRejectsUnrelatedShowMatch(t *testing.T) {
	const (
		bigXID = 9613
		bigXEp = `,"availableEpisodes":{"sub":59},"availableEpisodesDetail":{"sub":["1"],"dub":["1"]}`
	)
	unrelated := strings.Join([]string{
		`{"_id":"BOONIE","name":"Boonie Bears: The Big Top Secret","englishName":"Boonie Bears: The Big Top Secret","nativeName":""` + bigXEp + `,"aniListId":null}`,
		`{"_id":"PLEASANT","name":"Pleasant Goat and Big Big Wolf: Joys of Seasons","englishName":"Pleasant Goat and Big Big Wolf: Joys of Seasons","nativeName":""` + bigXEp + `,"aniListId":"131569"}`,
		`{"_id":"WRONGX","name":"Big X","englishName":"Big X","nativeName":""` + bigXEp + `,"aniListId":"999999"}`,
	}, ",")

	f := newMkissaFixture(t, []string{"1"}, "")
	f.edges, f.romaji, f.english = unrelated, "Big X", "Big X"
	p := newMkissaTestProvider(f)
	p.engineBin = "/bin/false" // a rejected match must never reach the engine
	if _, _, err := p.resolveShow(mkissaTestCtx(t), bigXID, "dub"); err == nil {
		t.Fatal("resolveShow accepted an unrelated show — that is an anime mismatch")
	}

	// Positive control: the identical edge set carrying the right
	// aniListId resolves, so the guard rejects wrongly-labelled shows,
	// not the title itself.
	f2 := newMkissaFixture(t, []string{"1"}, "")
	f2.edges = `{"_id":"BIGX","name":"Big X","englishName":"Big X","nativeName":""` + bigXEp + `,"aniListId":"9613"}`
	f2.romaji, f2.english = "Big X", "Big X"
	p2 := newMkissaTestProvider(f2)
	p2.engineBin = "/bin/false"
	showID, title, err := p2.resolveShow(mkissaTestCtx(t), bigXID, "dub")
	if err != nil {
		t.Fatalf("exact aniListId match must resolve: %v", err)
	}
	if showID != "BIGX" || title != "Big X" {
		t.Fatalf("resolved show = %q (%q), want BIGX / Big X", showID, title)
	}
}

// The engine/search slot is the only thing standing between a hung call
// and every caller behind it, so it must fail on the WAITER's context —
// the old mutex had no deadline at all and one stuck run burned the whole
// fan-out budget for the rest (observed 2026-10-04: mkissa collectors
// always consumed the full 30-45s budget).
func TestMkissaEngineSlotHonoursContext(t *testing.T) {
	if err := acquireEngine(context.Background()); err != nil {
		t.Fatalf("free slot must be acquirable: %v", err)
	}
	defer func() {
		// Hand the slot back without leaving a gap behind: the gap is
		// production pacing, not test state.
		mkissaEngineStateMu.Lock()
		mkissaEngineFreeAt = time.Time{}
		mkissaEngineStateMu.Unlock()
		<-mkissaEngineSlot
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := acquireEngine(ctx); err == nil {
		t.Fatal("busy slot must return the caller's context error instead of blocking")
	}
}

func TestMkissaRelayEnvOverride(t *testing.T) {
	t.Setenv("MKISSA_API", "https://relay.example")
	p := NewMkissaProvider(zerolog.Nop())
	if p.apiBase != "https://relay.example/api" {
		t.Fatalf("apiBase = %q, want relay host + /api", p.apiBase)
	}
}

// Relay key plumbing: with MKISSA_API pointed at a worker, doJSON carries
// X-Relay-Key; direct mode never sends it even when ANIRAKU_RELAY_KEY set
// (the key must not leak to api.mkissa.net or file hosts).
func TestMkissaRelayKeyHeader(t *testing.T) {
	var gotKey, gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Relay-Key")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Setenv("MKISSA_API", srv.URL)
	t.Setenv("ANIRAKU_RELAY_KEY", "k-relay")
	p := NewMkissaProvider(zerolog.Nop())
	p.client = srv.Client()
	if p.apiBase != srv.URL+"/api" {
		t.Fatalf("apiBase = %q, want relay + /api", p.apiBase)
	}
	var out map[string]any
	if err := p.doJSON(mkissaTestCtx(t), map[string]any{"q": 1}, &out); err != nil {
		t.Fatalf("doJSON via relay: %v", err)
	}
	if gotKey != "k-relay" || gotPath != "/api" {
		t.Fatalf("key/path = %q/%q, want k-relay + /api", gotKey, gotPath)
	}
	// Direct mode: no key even with ANIRAKU_RELAY_KEY set.
	t.Setenv("MKISSA_API", "")
	gotKey = "unset-check"
	p2 := NewMkissaProvider(zerolog.Nop())
	p2.client = srv.Client()
	p2.apiBase = srv.URL + "/api"
	if err := p2.doJSON(mkissaTestCtx(t), map[string]any{"q": 1}, &out); err != nil {
		t.Fatalf("doJSON direct: %v", err)
	}
	if gotKey != "" {
		t.Fatalf("key %q leaked to direct endpoint", gotKey)
	}
}

func TestMkissaThrottleBreaker(t *testing.T) {
	b := &mkissaThrottleBreaker{cooldown: time.Hour, tripAfter: 2}
	if b.blocked() {
		t.Fatal("fresh breaker must not block")
	}
	b.record(false, true)
	if b.blocked() {
		t.Fatal("one throttle must not trip yet")
	}
	b.record(false, false) // unrelated failure resets the streak
	b.record(false, true)
	if b.blocked() {
		t.Fatal("non-consecutive throttle must not trip")
	}
	b.record(false, true)
	if !b.blocked() {
		t.Fatal("two consecutive throttles must trip")
	}
	b.record(true, false)
	if b.blocked() {
		t.Fatal("success must reset the breaker")
	}
	if !mkissaThrottleErr(fmt.Errorf("mkissa: episode 1: Too many requests, please try again in 2 seconds.")) {
		t.Fatal("rate message must classify as throttle")
	}
	if !mkissaThrottleErr(fmt.Errorf("mkissa: episode 1: NEED_CAPTCHA")) {
		t.Fatal("captcha must classify as throttle")
	}
	if mkissaThrottleErr(fmt.Errorf("mkissa: no show match")) {
		t.Fatal("resolve failure must not classify as throttle")
	}
}
