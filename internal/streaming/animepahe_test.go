package streaming

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// paheFixture serves the whole animepahe surface from one httptest server:
// /api (search + release), /anilist, /play/…, kwik /e/… pages, and the
// uwu-style media chain (master -> playlist -> segment). The media hops
// refuse any request without the kwik referer — measured on the live CDN
// (segment 200 with Referer https://kwik.cx/, 403 without it or with an
// animepahe referer) — so the probe wiring is asserted, not assumed.
type paheFixture struct {
	t   *testing.T
	srv *httptest.Server

	// Overrides, set before the first provider request.
	searchData   string // JSON array for /api?m=search data
	releaseRows  string // JSON array for /api?m=release data
	romaji       string
	english      string
	acceptUA     string            // when set, /api 403s any other UA
	kwikInline   map[string]string // kwik id -> inline m3u8 URL ("" = eval page)
	kwikReject   bool              // serve a player script the safety filter must drop
	learnedHosts []string
}

const (
	paheTestSearchData = `[{"id":1571,"title":"Naruto","type":"TV","episodes":220,"year":2002,"score":8.1,"session":"anime-sess-1"}]`

	paheTestReleaseRows = `[
 {"episode":1,"episode2":0,"edition":"","session":"ep1-jpn","anime_session":"anime-sess-1","audio":"jpn"},
 {"episode":1,"episode2":0,"edition":"","session":"ep1-eng","anime_session":"anime-sess-1","audio":"eng"},
 {"episode":2,"episode2":0,"edition":"","session":"ep2-jpn","anime_session":"anime-sess-1","audio":"jpn"}
]`

	// Production attribute order on kw720 (data-src first, as served);
	// kw360 deliberately reverses it to pin order-independence.
	paheTestPlayHTML = `<html><body><div class="player">
<button type="button" data-src="%[1]s/e/kw720" data-resolution="720" data-audio="jpn">720p</button>
<button type="button" data-audio="jpn" data-resolution="360" data-src="%[1]s/e/kw360">360p</button>
<button type="button" data-src="%[1]s/e/kw720d" data-resolution="720" data-audio="eng">720p dub</button>
</div></body></html>`
)

func newPaheFixture(t *testing.T) *paheFixture {
	t.Helper()
	f := &paheFixture{t: t, romaji: "Naruto", english: "Naruto"}
	mux := http.NewServeMux()

	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		if f.acceptUA != "" && r.Header.Get("User-Agent") != f.acceptUA {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("m") {
		case "search":
			data := f.searchData
			if data == "" {
				data = paheTestSearchData
			}
			fmt.Fprintf(w, `{"total":1,"per_page":8,"current_page":1,"last_page":1,"data":%s}`, data)
		case "release":
			rows := f.releaseRows
			if rows == "" {
				rows = paheTestReleaseRows
			}
			fmt.Fprintf(w, `{"total":3,"per_page":30,"current_page":1,"last_page":1,"data":%s}`, rows)
		default:
			http.Error(w, "bad query", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("/anilist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":{"Media":{"title":{"romaji":%q,"english":%q}}}}`, f.romaji, f.english)
	})
	mux.HandleFunc("/play/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprintf(w, paheTestPlayHTML, f.srv.URL)
	})
	mux.HandleFunc("/e/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/e/")
		w.Header().Set("Content-Type", "text/html")
		if target, ok := f.kwikInline[id]; ok && target != "" {
			// No eval anywhere on this page: only the inline fast path
			// can produce a source (engineBin=/bin/false proves it).
			fmt.Fprintf(w, `<html><head><title>inline</title></head><body><script>var cfg={src:"%s"};</script></body></html>`, target)
			return
		}
		if f.kwikReject {
			fmt.Fprint(w, `<html><body><script>eval(atob("dmFycyA9IFtdOw=="));require("child_process");</script></body></html>`)
			return
		}
		// The real shape: one obfuscated eval script, no inline URL.
		fmt.Fprint(w, `<html><head><title>kwik player</title></head><body><div id="player"></div><script>eval(atob("dmFyIHBhcmFtcyA9IHsxOiA5fTs="));</script></body></html>`)
	})
	// Media hops: the referer gate mirrors the live uwucdn behaviour.
	gated := func(body []byte, ctype string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Referer") != paheKwikReferer {
				http.Error(w, "missing referer", http.StatusForbidden)
				return
			}
			w.Header().Set("Content-Type", ctype)
			w.Write(body)
		}
	}
	serveMaster := gated([]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100,RESOLUTION=640x360\nq/playlist.m3u8\n"),
		"application/vnd.apple.mpegurl")
	mux.HandleFunc("/master.m3u8", serveMaster)
	mux.HandleFunc("/eval.m3u8", serveMaster)
	mux.HandleFunc("/q/playlist.m3u8", gated([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg.ts\n"),
		"application/vnd.apple.mpegurl"))
	mux.HandleFunc("/q/seg.ts", gated(append([]byte{0x47}, bytes1k...), "video/mp2t"))

	f.srv = httptest.NewServer(mux)
	f.kwikInline = map[string]string{"kw360": f.srv.URL + "/master.m3u8"}
	t.Cleanup(f.srv.Close)
	return f
}

// TestAnimepaheSolverStub is NOT a real test: FindEpisodeSource's provider
// re-executes the test binary with this name to emit a canned clearance
// (see TestMkissaDaemonStub for why sh/awk stubs do not work here).
func TestAnimepaheSolverStub(t *testing.T) {
	if os.Getenv("GO_PAHE_SOLVER_STUB") != "1" {
		return
	}
	if p := os.Getenv("GO_PAHE_STUB_COUNT"); p != "" {
		if fh, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			fh.WriteString("x")
			fh.Close()
		}
	}
	ua := os.Getenv("GO_PAHE_SOLVER_UA")
	if ua == "" {
		ua = "ua-solved"
	}
	io.Copy(io.Discard, os.Stdin)
	fmt.Printf("PAHE_SOLVE_RESULT {\"cookies\":{\"cf_clearance\":%q,\"animepahe_session\":\"sess\"},\"ua\":%q}\n",
		"clr-"+ua, ua)
}

// TestAnimepaheEngineStub is NOT a real test: the provider re-executes the
// test binary with this name to answer the kwik eval spawn (stdin = the
// player script, stdout = captured m3u8).
func TestAnimepaheEngineStub(t *testing.T) {
	if os.Getenv("GO_PAHE_ENGINE_STUB") != "1" {
		return
	}
	io.Copy(io.Discard, os.Stdin)
	fmt.Println(os.Getenv("GO_PAHE_STUB_M3U8"))
}

func newPaheTestProvider(t *testing.T, f *paheFixture) *AnimepaheProvider {
	t.Helper()
	t.Setenv(paheSolverEnv, "")
	t.Setenv(paheBaseEnv, "")
	p := NewAnimepaheProvider(zerolog.Nop())
	p.base = f.srv.URL
	p.anilistURL = f.srv.URL + "/anilist"
	// Plain client: netguard blocks loopback fixtures (by design).
	p.client = &http.Client{Timeout: 30 * time.Second}
	p.clearanceFile = filepath.Join(t.TempDir(), "clearance.json")
	p.kwikPattern = regexp.MustCompile("^" + regexp.QuoteMeta(f.srv.URL) + "/e/[A-Za-z0-9]+$")
	p.SetHostLearner(func(host string) { f.learnedHosts = append(f.learnedHosts, host) })

	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p.solverBin = self
	p.solverScript = "-test.run=TestAnimepaheSolverStub"
	t.Setenv("GO_PAHE_SOLVER_STUB", "1")
	t.Setenv("GO_PAHE_SOLVER_UA", "ua-solved")
	t.Setenv("GO_PAHE_STUB_COUNT", filepath.Join(t.TempDir(), "solver-count"))

	p.engineBin = self
	p.engineScript = "-test.run=TestAnimepaheEngineStub"
	t.Setenv("GO_PAHE_ENGINE_STUB", "1")
	t.Setenv("GO_PAHE_STUB_M3U8", f.srv.URL+"/eval.m3u8")
	return p
}

func paheTestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// Full sub chain: solve stub -> search -> release -> play -> kwik (eval
// for kw720, inline fast path for kw360) -> probe -> sources. The eval
// button must resolve to the stub's m3u8 and the inline button to the URL
// embedded in its own page — either path silently swapping would fail the
// URL assertions.
func TestAnimepaheFindEpisodeSource(t *testing.T) {
	f := newPaheFixture(t)
	p := newPaheTestProvider(t, f)
	sr, err := p.FindEpisodeSource(paheTestCtx(t), "1", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if len(sr.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 (720 + 360 jpn); got %+v", len(sr.Sources), sr.Sources)
	}
	if sr.Sources[0].Quality != "720p" || sr.Sources[1].Quality != "360p" {
		t.Fatalf("qualities = %q/%q, want 720p/360p (best first)", sr.Sources[0].Quality, sr.Sources[1].Quality)
	}
	if want := f.srv.URL + "/eval.m3u8"; sr.Sources[0].URL != want {
		t.Fatalf("eval path source = %q, want %q", sr.Sources[0].URL, want)
	}
	if want := f.srv.URL + "/master.m3u8"; sr.Sources[1].URL != want {
		t.Fatalf("inline path source = %q, want %q", sr.Sources[1].URL, want)
	}
	if sr.ServerNames[0] != "Pocky" || sr.ServerNames[1] != "Linda" {
		t.Fatalf("server names = %v, want Pocky/Linda", sr.ServerNames)
	}
	if sr.Headers["Referer"] != paheKwikReferer {
		t.Fatalf("headers = %v, want kwik referer", sr.Headers)
	}
	for _, s := range sr.Sources {
		if s.Type != "hls" || s.Verification != "proxy" {
			t.Fatalf("source %+v: want hls/proxy", s)
		}
	}
	// The dub button on the same play page must never leak into sub.
	for _, s := range sr.Sources {
		if strings.Contains(s.URL, "kw720d") {
			t.Fatalf("dub kwik leaked into sub: %q", s.URL)
		}
	}
	if len(f.learnedHosts) == 0 || f.learnedHosts[0] != "127.0.0.1" {
		t.Fatalf("learned hosts = %v, want 127.0.0.1 (proxy allowlist)", f.learnedHosts)
	}
}

// Inline-only pages must resolve without ever spawning the engine.
func TestAnimepaheInlineKwikNeedsNoEngine(t *testing.T) {
	f := newPaheFixture(t)
	p := newPaheTestProvider(t, f)
	// kw720 falls back to an inline page too, so BOTH sources come from
	// the fast path; /bin/false would fail any spawn attempt.
	f.kwikInline["kw720"] = f.srv.URL + "/master.m3u8"
	p.engineBin = "/bin/false"
	sr, err := p.FindEpisodeSource(paheTestCtx(t), "1", 1, "sub")
	if err != nil {
		t.Fatalf("inline-only resolve: %v", err)
	}
	if len(sr.Sources) != 2 {
		t.Fatalf("sources = %d, want 2 inline-only", len(sr.Sources))
	}
}

// Strict per-lang: rows without eng audio mean no dub server, and the
// failure fires before any engine spawn (/bin/false proves it).
func TestAnimepaheStrictDubMissing(t *testing.T) {
	f := newPaheFixture(t)
	f.releaseRows = `[
 {"episode":1,"episode2":0,"edition":"","session":"ep1-jpn","anime_session":"anime-sess-1","audio":"jpn"},
 {"episode":2,"episode2":0,"edition":"","session":"ep2-jpn","anime_session":"anime-sess-1","audio":"jpn"}
]`
	p := newPaheTestProvider(t, f)
	p.engineBin = "/bin/false"
	if _, err := p.FindEpisodeSource(paheTestCtx(t), "1", 1, "dub"); err == nil {
		t.Fatal("expected dub to fail when no eng release exists")
	}
	p2 := newPaheTestProvider(t, f)
	if _, err := p2.FindEpisodeSource(paheTestCtx(t), "1", 1, "sub"); err != nil {
		t.Fatalf("sub must still resolve: %v", err)
	}
}

// The scoring floor is what keeps "Naruto Shippuden" from satisfying a
// "Naruto" request: candidates that contain the wanted title but ADD words
// stay below paheFuzzyMin, while exact/equal-token/clean-contain match.
func TestAnimepaheScoreHit(t *testing.T) {
	cases := []struct {
		name       string
		candidate  string
		want       []string
		atLeast    float64
		belowFloor bool
	}{
		{"exact", "Naruto", []string{"Naruto"}, 100, false},
		{"english exact first", "Naruto", []string{"Naruto", "Naruto"}, 100, false},
		{"season superset rejected", "Naruto Shippuden", []string{"Naruto"}, 0, true},
		{"fate final season superset rejected", "Attack on Titan Final Season", []string{"Attack on Titan"}, 0, true},
		{"punctuation equal tokens", "Re:Zero", []string{"Re Zero"}, 90, false},
		{"clean contain passes", "Naruto Movie", []string{"Naruto Movie"}, 100, false},
		{"token overlap only", "Boonie Bears: The Big Top Secret", []string{"Big X"}, 0, true},
		{"unrelated", "One Piece", []string{"Naruto"}, 0, true},
	}
	for _, c := range cases {
		got := paheScoreHit(c.candidate, c.want)
		if c.belowFloor && got >= paheFuzzyMin {
			t.Errorf("%s: %q vs %v scored %v, must stay below %d", c.name, c.candidate, c.want, got, paheFuzzyMin)
		}
		if !c.belowFloor && got < c.atLeast {
			t.Errorf("%s: %q vs %v scored %v, want >= %v", c.name, c.candidate, c.want, got, c.atLeast)
		}
	}
}

// A catalog with only wrong/superset titles must resolve to NOTHING — no
// match beats a wrong show — while the exact-title control still works.
func TestAnimepaheRejectsMismatchedTitles(t *testing.T) {
	f := newPaheFixture(t)
	f.searchData = `[
 {"id":9001,"title":"Naruto Shippuden","type":"TV","episodes":500,"year":2007,"session":"sess-shippuden"},
 {"id":9002,"title":"Boruto: Naruto Next Generations","type":"TV","episodes":0,"year":2018,"session":"sess-boruto"}
]`
	p := newPaheTestProvider(t, f)
	if _, _, err := p.resolveShow(paheTestCtx(t), 1); err == nil {
		t.Fatal("resolveShow accepted supersets of the wanted title — that is an anime mismatch")
	}

	// Positive control: the exact title in the same fixture shape wins.
	f2 := newPaheFixture(t)
	f2.searchData = `[{"id":1571,"title":"Naruto","type":"TV","session":"anime-sess-1"}]`
	p2 := newPaheTestProvider(t, f2)
	session, title, err := p2.resolveShow(paheTestCtx(t), 1)
	if err != nil {
		t.Fatalf("exact title must resolve: %v", err)
	}
	if session != "anime-sess-1" || title != "Naruto" {
		t.Fatalf("resolved %q (%q), want anime-sess-1 / Naruto", session, title)
	}
}

// A 403 mid-flight (expired/blocked clearance) invalidates the session and
// retries exactly once with a fresh solve; a persistent 403 surfaces as an
// upstream-gated error (fan-out skips silently) and a later good solve
// recovers. The solver stub's invocation count pins the retry budget.
func TestAnimepahe403TriggersFreshSolve(t *testing.T) {
	f := newPaheFixture(t)
	f.acceptUA = "ua-good"
	p := newPaheTestProvider(t, f)
	countFile := os.Getenv("GO_PAHE_STUB_COUNT")

	t.Setenv("GO_PAHE_SOLVER_UA", "ua-bad")
	_, err := p.search(paheTestCtx(t), "Naruto")
	if err == nil {
		t.Fatal("expected persistent 403 to surface as an error")
	}
	if !isUpstreamGated(err) {
		t.Fatalf("error %q must classify as upstream-gated (HTTP 403)", err)
	}
	if n := solverSolveCount(t, countFile); n != 2 {
		t.Fatalf("solver spawned %d times for one fetch, want 2 (initial + one re-solve)", n)
	}

	t.Setenv("GO_PAHE_SOLVER_UA", "ua-good")
	hits, err := p.search(paheTestCtx(t), "Naruto")
	if err != nil {
		t.Fatalf("fresh solve must recover: %v", err)
	}
	if len(hits) != 1 || hits[0].Session != "anime-sess-1" {
		t.Fatalf("hits = %+v, want the fixture show", hits)
	}
	if n := solverSolveCount(t, countFile); n != 3 {
		t.Fatalf("solver count = %d, want 3 after recovery", n)
	}
}

func solverSolveCount(t *testing.T, path string) int {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return len(body)
}

// Cached resolves are deep copies: mutating a returned result must never
// poison the cache.
func TestAnimepaheResolveCacheIsolation(t *testing.T) {
	f := newPaheFixture(t)
	p := newPaheTestProvider(t, f)
	sr, err := p.FindEpisodeSource(paheTestCtx(t), "1", 1, "sub")
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	sr.Sources[0].URL = "MUTATED"
	again, err := p.FindEpisodeSource(paheTestCtx(t), "1", 1, "sub")
	if err != nil {
		t.Fatalf("cached resolve: %v", err)
	}
	if again.Sources[0].URL == "MUTATED" {
		t.Fatal("cache poisoned through the previously returned slice")
	}
}

// The player script is executed by bun, so anything that could reach the
// host or the network must be rejected BEFORE the spawn — proven absent
// from the live kwik player, and fatal here with engineBin=/bin/false.
func TestAnimepaheRejectsDangerousKwikScript(t *testing.T) {
	f := newPaheFixture(t)
	f.kwikReject = true
	p := newPaheTestProvider(t, f)
	p.engineBin = "/bin/false"
	_, err := p.resolveKwik(paheTestCtx(t), f.srv.URL+"/e/kw720")
	if err == nil {
		t.Fatal("dangerous player script must be rejected")
	}
	if !strings.Contains(err.Error(), "child_process") {
		t.Fatalf("error %q must name the rejected token", err)
	}
}

// parsePahePlaySources must survive attribute order variations and never
// bleed sibling attributes across tag boundaries.
func TestAnimepaheParsePlaySources(t *testing.T) {
	html := `
<button data-src="https://kwik.cx/e/aaa" data-resolution="720" data-audio="jpn" type="button">x</button>
<button type="button" data-audio="eng" data-resolution="1080" data-src="https://kwik.cx/e/bbb">y</button>
<div data-resolution="360" data-src="https://kwik.cx/e/ccc" data-audio="jpn"></div>
<span data-src="https://kwik.cx/e/nothing">no res</span>
`
	got := parsePahePlaySources(html)
	if len(got) != 4 {
		t.Fatalf("parsed %d buttons, want 4: %+v", len(got), got)
	}
	want := []pahePlaySource{
		{Src: "https://kwik.cx/e/aaa", Resolution: 720, Audio: "jpn"},
		{Src: "https://kwik.cx/e/bbb", Resolution: 1080, Audio: "eng"},
		{Src: "https://kwik.cx/e/ccc", Resolution: 360, Audio: "jpn"},
		{Src: "https://kwik.cx/e/nothing", Resolution: 0, Audio: ""},
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("button %d = %+v, want %+v", i, got[i], w)
		}
	}
}

// Release-row preference: main cut over edition, part 0 over later parts,
// and audio stays exact (jpn row can never satisfy eng).
func TestAnimepahePickRelease(t *testing.T) {
	rows := []paheRelease{
		{Episode: 1, Episode2: 1, Session: "part2", Audio: "jpn"},
		{Episode: 1, Episode2: 0, Edition: "Director's Cut", Session: "dc", Audio: "jpn"},
		{Episode: 1, Episode2: 0, Session: "main", Audio: "jpn"},
		{Episode: 1, Episode2: 0, Session: "eng1", Audio: "eng"},
	}
	if got, ok := pickPaheRelease(rows, 1, "jpn"); !ok || got.Session != "main" {
		t.Fatalf("jpn pick = %+v (ok=%v), want the clean main cut", got, ok)
	}
	if got, ok := pickPaheRelease(rows, 1, "eng"); !ok || got.Session != "eng1" {
		t.Fatalf("eng pick = %+v (ok=%v), want eng1", got, ok)
	}
	if _, ok := pickPaheRelease(rows, 2, "jpn"); ok {
		t.Fatal("missing episode must not pick anything")
	}
}

// FindEpisodes dedupes multi-row episodes (sub+dub rows share a number)
// and skips fractional episodes that can never match an int request.
func TestAnimepaheFindEpisodes(t *testing.T) {
	f := newPaheFixture(t)
	f.releaseRows = `[
 {"episode":1,"episode2":0,"edition":"","session":"a","anime_session":"s","audio":"jpn"},
 {"episode":1,"episode2":0,"edition":"","session":"b","anime_session":"s","audio":"eng"},
 {"episode":1.5,"episode2":0,"edition":"","session":"c","anime_session":"s","audio":"jpn"},
 {"episode":2,"episode2":0,"edition":"","session":"d","anime_session":"s","audio":"jpn"}
]`
	p := newPaheTestProvider(t, f)
	eps, err := p.FindEpisodes(paheTestCtx(t), "anime-sess-1")
	if err != nil {
		t.Fatalf("FindEpisodes: %v", err)
	}
	if len(eps) != 2 || eps[0].Number != 1 || eps[1].Number != 2 {
		t.Fatalf("episodes = %+v, want [1 2] (deduped, no 1.5)", eps)
	}
}

// Without the baked solver the provider must report unconfigured and fail
// before any network — the fan-out's zero-cost skip depends on it.
func TestAnimepaheDisabledWithoutSolver(t *testing.T) {
	t.Setenv(paheSolverEnv, "0")
	p := NewAnimepaheProvider(zerolog.Nop())
	if p.Configured() {
		t.Fatal("ANIRAKU_PAHE_SOLVER=0 must disable the provider")
	}
	_, err := p.FindEpisodeSource(paheTestCtx(t), "1", 1, "sub")
	if err == nil || !strings.Contains(err.Error(), "solver not configured") {
		t.Fatalf("error = %v, want solver-not-configured before any network", err)
	}
}

// Alias normalization: the provider family and every cute server name must
// route to animepahe, while vidnest keeps its own aliases — "animepahe"
// used to alias VidNest before the real provider shipped.
func TestAnimepaheAliasNormalization(t *testing.T) {
	t.Setenv(paheSolverEnv, "0")
	m := NewManager(zerolog.Nop())
	// Seed the hentai cache so the gate never reaches the network.
	m.hentaiCache[1] = hentaiEntry{isHentai: false, expires: time.Now().Add(time.Hour)}
	ctx := paheTestCtx(t)

	for _, prov := range []string{"animepahe", "pahe", "Pocky", "Linda", "Minto", "Yuzu"} {
		_, err := m.GetSourcesForProviderWithSlug(ctx, 1, prov, "sub", "auto", 1, "")
		if err == nil {
			t.Fatalf("provider %q: expected the unconfigured-solver error", prov)
		}
		if !strings.Contains(err.Error(), "animepahe") {
			t.Fatalf("provider %q routed elsewhere: %v (want animepahe)", prov, err)
		}
	}
	// VidNest keeps its own aliases. It resolves live (baked default
	// relay), so this asserts the ROUTING only: whatever happens, the
	// request must never surface the animepahe family — an unconfigured
	// animepahe cannot return success, and a vidnest failure names
	// vidnest, not us.
	for _, prov := range []string{"vidnest", "nest"} {
		// A deliberately tiny deadline: routing happens before any I/O,
		// so the local error surfaces without waiting on the live relay.
		rctx, rcancel := context.WithTimeout(ctx, 250*time.Millisecond)
		_, err := m.GetSourcesForProviderWithSlug(rctx, 1, prov, "sub", "auto", 1, "")
		rcancel()
		if err != nil && strings.Contains(err.Error(), "animepahe") {
			t.Fatalf("provider %q must stay on vidnest, got: %v", prov, err)
		}
	}
}
