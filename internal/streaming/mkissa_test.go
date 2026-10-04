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

// Canned engine output: one direct Default m3u8, one mp4upload mp4,
// one pure embed (skipped), one unknown kind (skipped).
func mkissaStubOutput(mediaBase string) string {
	return fmt.Sprintf(`{"id":0,"showId":%q,"audio":"sub","results":[{"episode":"1","sources":[
{"name":"Default","url":"https://mkissa.to/e/x","extractedUrl":%q,"type":"player","priority":10},
{"name":"Mp4","url":"https://mp4upload.com/embed-abc.html","extractedUrl":"%s/v.mp4","type":"file","priority":5},
{"name":"Ss-Hls","url":"https://streamsb.net/e/abc.html","type":"embed","priority":4},
{"name":"Weird","url":"https://x.example/e","extractedUrl":"https://x.example/e","type":"file","priority":3}
]}]}`,
		mkissaTestShowID, mediaBase+"/master.m3u8", mediaBase)
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
// with the request id per line.
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
	// Single line per response: the parent frames on newlines, so any
	// pretty-printing in the canned file must go.
	oneLine := strings.ReplaceAll(string(raw), "\n", "")
	for sc.Scan() {
		var req struct {
			ID uint64 `json:"id"`
		}
		if err := json.Unmarshal(sc.Bytes(), &req); err != nil {
			continue
		}
		resp := strings.Replace(oneLine, `"id":0`, fmt.Sprintf(`"id":%d`, req.ID), 1)
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

// Full sub chain through the stub engine: direct m3u8 + mp4 kept with
// cute names, embeds and unknown kinds skipped.
func TestMkissaFindEpisodeSource(t *testing.T) {
	f := newMkissaFixture(t, []string{"1"}, "")
	p := newMkissaTestProvider(f)
	sr, err := p.FindEpisodeSource(mkissaTestCtx(t), "20", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if len(sr.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 (Chuu + Kissy)", len(sr.Sources))
	}
	if sr.Sources[0].Type != "hls" || sr.ServerNames[0] != "Chuu" {
		t.Fatalf("source[0] = %+v name %q, want hls/Chuu", sr.Sources[0], sr.ServerNames[0])
	}
	if sr.Sources[1].Type != "mp4" || sr.ServerNames[1] != "Kissy" {
		t.Fatalf("source[1] = %+v name %q, want mp4/Kissy", sr.Sources[1], sr.ServerNames[1])
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
	} {
		if got := mkissaServerNames[kind]; got != want {
			t.Fatalf("kind %q = %q, want %q", kind, got, want)
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
