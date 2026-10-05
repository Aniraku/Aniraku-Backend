package streaming

// Animepahe provider — official domains only (animepahe.pw canonical; the
// .com/.org mirrors redirect there; every mirror/clone domain is out of
// scope by operator rule).
//
// Pipeline (proven end-to-end 2026-10-05: search -> release -> play ->
// kwik -> m3u8 -> segment 200):
//  1. Cloudflare clearance: animepahe sits behind a managed challenge that
//     rejected every non-browser transport we tried (curl, python requests,
//     cloudscraper, wreq browser-TLS, translate.goog, CORS proxies — all
//     hard 403). The only pass was a real browser. Production split:
//     Go execs third_party/pahe-solver/solve_once.py on demand (camoufox
//     spawns inside the API container, extracts {cookies, ua}, exits —
//     zero resident cost: steady-state +0 MB, ~450 MB only for the seconds
//     a solve takes, single-flight so a burst costs one spawn). The
//     clearance is replayed through a Firefox-impersonated client
//     (bogdanfinn/tls-client firefox profile + the EXACT UA the clearance
//     was issued under — measured 200 where plain TLS with the same cookie
//     got 403, so the clearance is bound to UA+TLS fingerprint). A 403
//     mid-flight invalidates the session and triggers exactly one
//     re-solve, so the clearance TTL never matters and a solve only
//     happens when Cloudflare actually expires it. A solve that outlives
//     the caller's deadline still finishes on disk (PAHE_CLEARANCE_FILE),
//     so the next request reads it instead of paying for the browser
//     again.
//  2. AniList ID -> titles (graphql.aniraku.tech) -> /api?m=search with
//     strict title scoring — exact string, equal token set, or the
//     candidate containing the whole wanted title WITHOUT adding words
//     (a "Naruto Shippuden" candidate can never win for "Naruto": season
//     supersets score below the floor). No match beats a wrong show.
//  3. /api?m=release (paginated) -> episode rows. Strict audio like
//     kaa/mkissa: sub = jpn rows, dub = eng rows; a title with no eng
//     rows has no dub server. Multi-part rows (episode2) and editions
//     prefer the clean main release (no edition, part 0).
//  4. /play/{anime}/{episode} -> per-quality kwik /e/ buttons
//     (data-src / data-resolution / data-audio).
//  5. kwik page -> obfuscated player JS sandbox-evalled by
//     third_party/animepahe-engine/kwik_resolve.js (bun, node fallback)
//     -> uwu.m3u8. Sandbox: fetch/XHR/WebSocket stubbed dead, DOM inert,
//     globalThis.process/require deleted; Go rejects scripts carrying
//     require/import/child_process tokens before spawning (verified: the
//     live kwik player carries none).
//  6. Honesty probe per source (master->media->segment with Referer
//     https://kwik.cx/ — measured: segment 200 with the kwik referer,
//     403 without it or with an animepahe referer) + learnURLHost so the
//     proxy allowlist learns any new vault host.
//
// Caches: show->session 24h, release rows 6h, resolved sources 30min
// (mkissa parity — kwik ids rotate on re-encode, so the resolve TTL
// bounds staleness; the VOD cache warms every probed playlist).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

const (
	// paheDefaultBase is the canonical official domain. .com/.org 301 to
	// .pw, mirrors are out of scope.
	paheDefaultBase = "https://animepahe.pw"

	// Env overrides (documented in .env.example):
	//   ANIRAKU_PAHE_SOLVER   path to solve_once.py (unset: auto-detect
	//                         /app + repo paths). "0" disables the provider.
	//   ANIRAKU_PAHE_PYTHON   interpreter override (unset: the container
	//                         venv, then python3 on PATH).
	//   ANIRAKU_PAHE_BASE     API base (tests/fixtures).
	//   ANIRAKU_PAHE_ENGINE   path override for kwik_resolve.js.
	paheSolverEnv        = "ANIRAKU_PAHE_SOLVER"
	pahePythonEnv        = "ANIRAKU_PAHE_PYTHON"
	paheBaseEnv          = "ANIRAKU_PAHE_BASE"
	paheEngineEnv        = "ANIRAKU_PAHE_ENGINE"
	paheClearanceFileEnv = "ANIRAKU_PAHE_CLEARANCE_FILE"

	paheShowTTL          = 24 * time.Hour
	paheReleaseTTL       = 6 * time.Hour
	paheResolveTTL       = 30 * time.Minute
	paheClearanceFileTTL = 20 * time.Minute // re-read grace; a 403 drops it instantly
	maxPaheEntries       = 500
	pahePagesMax         = 60 // ~1800 episodes; nothing on the site goes higher
	paheSourcesMax       = 4  // one per quality bucket (360..1080)
	paheWorkers          = 3  // concurrent release-page fetches
	// Two solver attempts x 45 s default budget + spawn overhead.
	paheSolveTO     = 100 * time.Second
	paheKwikTO      = 20 * time.Second
	paheEngineTO    = 15 * time.Second
	paheFetchMax    = 2 << 20
	paheFuzzyMin    = 80
	paheProfileName = "firefox_148"
)

// paheKwikReferer: the ONLY referer kwik's media layer accepts (measured:
// segment 200 with it, 403 without / with animepahe). The proxy force-sets
// it for uwucdn/owocdn hosts too — belt and braces.
const paheKwikReferer = "https://kwik.cx/"

// paheKWIKPattern validates kwik embed URLs before fetching. URLs come from
// animepahe's own play pages, but they are still validated explicitly (the
// pahe client bypasses netguard for fingerprint reasons).
var paheKWIKPattern = regexp.MustCompile(`^https://kwik\.(?:cx|si|link)/e/[A-Za-z0-9]+$`)

var (
	paheScriptTagRe = regexp.MustCompile(`(?is)<script[^>]*>([\s\S]*?)</script>`)
	paheInlineM3U8  = regexp.MustCompile(`https?://[^\s'"<>\)]+\.m3u8[^\s'"<>\)]*`)
)

// paheEvalReject: page scripts carrying any of these tokens are never
// spawned (verified absent from the live kwik player). They are the only
// ways the evaluated script could reach the host or the network.
var paheEvalReject = []string{
	"child_process", "process.binding", "process.dlopen",
	"import(", "globalThis.require", "Bun.spawn", "Deno.",
}

// paheServerNames: cute names double as provider aliases (the frontend
// sends the server NAME back as provider), so every name here must also
// appear in manager.go's alias normalization.
var paheServerNames = []string{"Pocky", "Linda", "Minto", "Yuzu"}

func paheServerName(i int) string {
	if i >= 0 && i < len(paheServerNames) {
		return paheServerNames[i]
	}
	return fmt.Sprintf("Pocky-%d", i+1)
}

type AnimepaheProvider struct {
	log        zerolog.Logger
	client     *http.Client // netguard: AniList, kwik pages, probes, media
	base       string
	anilistURL string
	learnHost  func(host string)

	// solverBin/solverScript run solve_once.py (interpreter, script). The
	// constructor auto-detects the container venv + vendored script; an
	// empty pair means "provider disabled" (zero upstream calls). Tests
	// point both at a re-exec stub, mirroring mkissa's daemonCommand.
	solverBin    string
	solverScript string
	// clearanceFile caches the last solve on disk so a solve that outlived
	// its caller's deadline is never paid for twice.
	clearanceFile string

	// engineBin/engineScript override the bun spawn (tests point at a
	// re-exec stub, mirroring mkissa's daemonCommand pattern).
	engineBin    string
	engineScript string

	// kwikPattern validates kwik embed URLs. It is an instance field so
	// tests can widen it to their fixture host; production always uses
	// paheKWIKPattern (official kwik hosts only).
	kwikPattern *regexp.Regexp

	// Clearance session: rebuilt wholesale on every solve so a stale jar
	// can never leak into a new session.
	solveMu  sync.Mutex // serializes solves (double-checked below)
	sessMu   sync.Mutex
	paheHTTP tlsclient.HttpClient
	ua       string

	mu       sync.Mutex
	shows    map[int]*paheShowEntry
	releases map[string]*paheReleaseEntry
	resolved map[paheResolveKey]*paheResolvedEntry
}

type paheShowEntry struct {
	session string
	title   string
	fetched time.Time
}

type paheReleaseEntry struct {
	rows    []paheRelease
	fetched time.Time
}

type paheResolveKey struct {
	session string
	episode int
	lang    string
}

type paheResolvedEntry struct {
	result  *SourceResult
	fetched time.Time
}

type paheSearchHit struct {
	ID       int     `json:"id"`
	Title    string  `json:"title"`
	Type     string  `json:"type"`
	Episodes int     `json:"episodes"`
	Year     int     `json:"year"`
	Score    float64 `json:"score"`
	Session  string  `json:"session"`
}

type paheSearchPage struct {
	Total    int             `json:"total"`
	LastPage int             `json:"last_page"`
	Data     []paheSearchHit `json:"data"`
}

type paheRelease struct {
	Episode      float64 `json:"episode"`
	Episode2     int     `json:"episode2"`
	Edition      string  `json:"edition"`
	Session      string  `json:"session"`
	AnimeSession string  `json:"anime_session"`
	Audio        string  `json:"audio"`
}

type paheReleasePage struct {
	Total    int           `json:"total"`
	PerPage  int           `json:"per_page"`
	LastPage int           `json:"last_page"`
	Data     []paheRelease `json:"data"`
}

type pahePlaySource struct {
	Src        string
	Resolution int
	Audio      string
}

type paheSolveResponse struct {
	Cookies map[string]string `json:"cookies"`
	UA      string            `json:"ua"`
}

func NewAnimepaheProvider(log zerolog.Logger) *AnimepaheProvider {
	base := strings.TrimSpace(os.Getenv(paheBaseEnv))
	if base == "" {
		base = paheDefaultBase
	}
	bin, script := paheResolveSolver()
	clearance := strings.TrimSpace(os.Getenv(paheClearanceFileEnv))
	if clearance == "" {
		clearance = "/tmp/aniraku_pahe_clearance.json"
	}
	return &AnimepaheProvider{
		log:           log,
		client:        &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		base:          strings.TrimRight(base, "/"),
		solverBin:     bin,
		solverScript:  script,
		clearanceFile: clearance,
		kwikPattern:   paheKWIKPattern,
		anilistURL:    "https://graphql.aniraku.tech",
		shows:         make(map[int]*paheShowEntry),
		releases:      make(map[string]*paheReleaseEntry),
		resolved:      make(map[paheResolveKey]*paheResolvedEntry),
	}
}

// paheResolveSolver locates (interpreter, solve_once.py). Only the
// container layout and explicit env count: a bare python3 on a dev box has
// no camoufox, and an interpreter that cannot import it would only fail
// per-request. ANIRAKU_PAHE_SOLVER=0 disables the provider outright.
func paheResolveSolver() (string, string) {
	if v := strings.TrimSpace(os.Getenv(paheSolverEnv)); v != "" {
		if v == "0" || strings.EqualFold(v, "off") || strings.EqualFold(v, "false") {
			return "", ""
		}
		if st, err := os.Stat(v); err == nil && !st.IsDir() {
			return paheResolvePython(), v
		}
		return "", ""
	}
	for _, c := range []string{
		"/app/third_party/pahe-solver/solve_once.py",
		"third_party/pahe-solver/solve_once.py",
	} {
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			if bin := paheResolvePython(); bin != "" {
				return bin, c
			}
			return "", ""
		}
	}
	return "", ""
}

// paheResolvePython: explicit override, then the container venv. Neither a
// system python3 nor PATH heuristics — camoufox lives only in the venv.
func paheResolvePython() string {
	if v := strings.TrimSpace(os.Getenv(pahePythonEnv)); v != "" {
		if st, err := os.Stat(v); err == nil && !st.IsDir() {
			return v
		}
		return ""
	}
	for _, c := range []string{
		"/app/pahe-solver/venv/bin/python",
		"pahe-solver/venv/bin/python",
	} {
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			return c
		}
	}
	return ""
}

func (p *AnimepaheProvider) Name() string { return "animepahe" }

// Configured reports whether the in-container solver is available. The
// fan-out collector and explicit requests both check this first, so a
// deployment without the baked solver costs zero upstream calls (and zero
// latency).
func (p *AnimepaheProvider) Configured() bool {
	return p.solverBin != "" && p.solverScript != ""
}

func (p *AnimepaheProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *AnimepaheProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

// ------------------------------------------------------------ session

// ensureSession guarantees a live clearance session, serializing solves so
// a stampeding fan-out triggers exactly one browser spawn.
func (p *AnimepaheProvider) ensureSession(ctx context.Context) error {
	if !p.Configured() {
		return fmt.Errorf("animepahe: solver not configured (set %s)", paheSolverEnv)
	}
	p.sessMu.Lock()
	ready := p.paheHTTP != nil && p.ua != ""
	p.sessMu.Unlock()
	if ready {
		return nil
	}
	p.solveMu.Lock()
	defer p.solveMu.Unlock()
	p.sessMu.Lock()
	ready = p.paheHTTP != nil && p.ua != ""
	p.sessMu.Unlock()
	if ready {
		return nil // another caller solved while we waited
	}
	return p.solveSessionLocked(ctx)
}

// invalidateSession drops the clearance so the next fetch re-solves. Called
// on every 403: the clearance is bound to UA+TLS fingerprint+IP, so it must
// be replaced as a unit, never patched. The on-disk copy goes too — a file
// that just produced a 403 must never be picked up again.
func (p *AnimepaheProvider) invalidateSession() {
	p.sessMu.Lock()
	p.paheHTTP = nil
	p.ua = ""
	p.sessMu.Unlock()
	if p.clearanceFile != "" {
		_ = os.Remove(p.clearanceFile)
	}
}

// solveSessionLocked installs a clearance session. Order matters:
//   - fresh on-disk result first (a solve that outlived its caller's
//     fan-out deadline finished writing the file — free to reuse);
//   - otherwise exec solve_once.py (single-flight: callers already hold
//     solveMu), which spawns camoufox, clears the challenge and exits.
func (p *AnimepaheProvider) solveSessionLocked(ctx context.Context) error {
	if sr, ok := p.readClearanceFile(); ok {
		if err := p.installSession(sr); err != nil {
			return err
		}
		p.log.Info().Int("cookies", len(sr.Cookies)).Msg("animepahe: clearance reused from disk")
		return nil
	}

	sctx, cancel := context.WithTimeout(ctx, paheSolveTO)
	defer cancel()
	cmd := exec.CommandContext(sctx, p.solverBin, p.solverScript)
	cmd.Env = append(os.Environ(), "PAHE_CLEARANCE_FILE="+p.clearanceFile)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if sctx.Err() != nil {
		// Killed mid-solve (fan-out deadline): the script may still have
		// written the file just before the kill — salvage it below, then
		// report the timeout honestly.
		if sr, ok := p.readClearanceFile(); ok && p.installSession(sr) == nil {
			p.log.Info().Int("cookies", len(sr.Cookies)).
				Msg("animepahe: clearance salvaged after deadline kill")
			return nil
		}
		return fmt.Errorf("animepahe: solver timeout after %s", paheSolveTO)
	}
	// Primary path: the marker line the script prints after persisting.
	if sr, ok := paheParseSolveLine(stdout.String()); ok {
		if ierr := p.installSession(sr); ierr == nil {
			p.log.Info().Int("cookies", len(sr.Cookies)).Msg("animepahe: clearance solved")
			return nil
		}
	}
	// Fallback: the persisted file (survives mangled stdout).
	if sr, ok := p.readClearanceFile(); ok && p.installSession(sr) == nil {
		p.log.Info().Int("cookies", len(sr.Cookies)).Msg("animepahe: clearance solved")
		return nil
	}
	if err != nil {
		tail := strings.TrimSpace(stderr.String())
		if len(tail) > 400 {
			tail = tail[len(tail)-400:]
		}
		return fmt.Errorf("animepahe: solver failed: %w (%s)", err, tail)
	}
	return fmt.Errorf("animepahe: solver produced no clearance")
}

// paheParseSolveLine finds the "PAHE_SOLVE_RESULT {json}" marker in the
// solver's stdout (other lines may interleave from playwright/camoufox).
func paheParseSolveLine(out string) (paheSolveResponse, bool) {
	var sr paheSolveResponse
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "PAHE_SOLVE_RESULT ") {
			continue
		}
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "PAHE_SOLVE_RESULT ")), &sr) == nil &&
			sr.UA != "" && sr.Cookies["cf_clearance"] != "" {
			return sr, true
		}
	}
	return sr, false
}

// readClearanceFile loads a solve_once.py result written within the grace
// window. Owner-only (0600) by construction; the cookie jar travels with
// the UA because the clearance is fingerprint-bound.
func (p *AnimepaheProvider) readClearanceFile() (paheSolveResponse, bool) {
	if p.clearanceFile == "" {
		return paheSolveResponse{}, false
	}
	st, err := os.Stat(p.clearanceFile)
	if err != nil || st.IsDir() {
		return paheSolveResponse{}, false
	}
	if time.Since(st.ModTime()) > paheClearanceFileTTL {
		return paheSolveResponse{}, false
	}
	body, err := os.ReadFile(p.clearanceFile)
	if err != nil {
		return paheSolveResponse{}, false
	}
	var sr paheSolveResponse
	if json.Unmarshal(body, &sr) != nil {
		return paheSolveResponse{}, false
	}
	if sr.UA == "" || sr.Cookies["cf_clearance"] == "" {
		return paheSolveResponse{}, false
	}
	return sr, true
}

// installSession rebuilds the impersonated client wholesale from the
// solver's {cookies, ua} pair.
func (p *AnimepaheProvider) installSession(sr paheSolveResponse) error {
	baseURL, err := url.Parse(p.base)
	if err != nil {
		return err
	}
	client, err := newPaheHTTPClient(baseURL, sr.Cookies, sr.UA)
	if err != nil {
		return err
	}
	p.sessMu.Lock()
	p.paheHTTP = client
	p.ua = sr.UA
	p.sessMu.Unlock()
	return nil
}

// newPaheHTTPClient builds the Firefox-impersonated client. The profile
// matters as much as the cookie: a plain TLS stack (python requests, curl,
// Go stdlib) with a VALID cf_clearance gets 403 — measured. The exact UA
// the browser got the clearance under must be replayed too.
func newPaheHTTPClient(base *url.URL, cookies map[string]string, ua string) (tlsclient.HttpClient, error) {
	jar := tlsclient.NewCookieJar()
	var kvs []*fhttp.Cookie
	for k, v := range cookies {
		kvs = append(kvs, &fhttp.Cookie{Name: k, Value: v, Path: "/"})
	}
	jar.SetCookies(base, kvs)

	var prof profiles.ClientProfile
	for _, name := range []string{paheProfileName, "firefox_147", "firefox_135", "firefox_133"} {
		if cp, ok := profiles.MappedTLSClients[name]; ok && cp.GetClientHelloId().Client != "" {
			prof = cp
			break
		}
	}
	if prof.GetClientHelloId().Client == "" {
		return nil, fmt.Errorf("animepahe: no firefox TLS profile available")
	}
	return tlsclient.NewHttpClient(tlsclient.NewNoopLogger(),
		tlsclient.WithClientProfile(prof),
		tlsclient.WithTimeoutSeconds(35),
		tlsclient.WithCookieJar(jar),
	)
}

// fetchPAHE performs one authenticated GET against the animepahe origin.
// A 403 (expired/invalidated clearance) invalidates the session and is
// retried exactly once with a fresh solve; the final error text always
// contains "HTTP 403" so the fan-out's isUpstreamGated treats an expired
// challenge as a silent skip instead of an error.
func (p *AnimepaheProvider) fetchPAHE(ctx context.Context, target, accept string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := p.ensureSession(ctx); err != nil {
			return nil, err
		}
		p.sessMu.Lock()
		client, ua := p.paheHTTP, p.ua
		p.sessMu.Unlock()
		if client == nil || ua == "" {
			return nil, fmt.Errorf("animepahe: no clearance session")
		}
		req, err := fhttp.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", ua)
		req.Header.Set("Accept", accept)
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		resp, err := client.Do(req)
		if err != nil {
			// Transport-level failure: re-solving cannot help.
			return nil, fmt.Errorf("animepahe: fetch %s: %w", target, err)
		}
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, paheFetchMax))
		resp.Body.Close()
		if rerr != nil {
			return nil, fmt.Errorf("animepahe: read %s: %w", target, rerr)
		}
		if resp.StatusCode == http.StatusForbidden {
			p.invalidateSession()
			lastErr = fmt.Errorf("animepahe: HTTP 403")
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("animepahe: HTTP %d", resp.StatusCode)
		}
		return body, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("animepahe: no clearance")
	}
	return nil, lastErr
}

// ------------------------------------------------------------ anilist

func (p *AnimepaheProvider) anilistTitles(ctx context.Context, id int) ([]string, error) {
	gql := `query($id:Int){Media(id:$id,type:ANIME){title{romaji english}}}`
	var out struct {
		Data struct {
			Media struct {
				Title struct {
					Romaji  string `json:"romaji"`
					English string `json:"english"`
				} `json:"title"`
			} `json:"Media"`
		} `json:"data"`
	}
	body, _ := json.Marshal(map[string]any{"query": gql, "variables": map[string]any{"id": id}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.anilistURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", browserUA)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("animepahe: anilist titles -> HTTP %d", resp.StatusCode)
	}
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, err
	}
	var titles []string
	// English first: it is the title animepahe's catalog mirrors most
	// faithfully, so the exact hit usually lands on the first search.
	if t := strings.TrimSpace(out.Data.Media.Title.English); t != "" {
		titles = append(titles, t)
	}
	if t := strings.TrimSpace(out.Data.Media.Title.Romaji); t != "" {
		titles = append(titles, t)
	}
	if len(titles) == 0 {
		return nil, fmt.Errorf("animepahe: anilist %d has no titles", id)
	}
	return titles, nil
}

// ------------------------------------------------------------ search

func (p *AnimepaheProvider) search(ctx context.Context, q string) ([]paheSearchHit, error) {
	body, err := p.fetchPAHE(ctx,
		p.base+"/api?m=search&q="+url.QueryEscape(q), "application/json")
	if err != nil {
		return nil, err
	}
	var page paheSearchPage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("animepahe: search decode: %w", err)
	}
	return page.Data, nil
}

// paheTokens splits a title into the same token shape mkissa uses.
func paheTokens(t string) map[string]bool {
	tt := map[string]bool{}
	for _, w := range strings.FieldsFunc(t, func(r rune) bool {
		return !(r == '_' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z'))
	}) {
		tt[w] = true
	}
	return tt
}

// paheScoreHit scores a candidate title against the wanted titles. Stricter
// than mkissaScoreHit on purpose (operator: exact title match only):
//
//	100 exact lowercase string
//	 90 same token set (punctuation/word-order differences only)
//	 80 candidate contains the WHOLE wanted title and adds no new words
//	<=60 everything else (token overlap alone never passes the floor)
//
// The "adds no new words" guard is what stops "Naruto Shippuden" from
// satisfying a "Naruto" request (and "Attack on Titan Final Season" from
// satisfying "Attack on Titan"): those candidates contain the wanted title
// but carry extra tokens, so they fall below paheFuzzyMin.
func paheScoreHit(candidate string, want []string) float64 {
	text := strings.ToLower(strings.TrimSpace(candidate))
	if text == "" {
		return 0
	}
	candTokens := paheTokens(text)
	var score float64
	for _, t := range want {
		tl := strings.ToLower(strings.TrimSpace(t))
		if tl == "" {
			continue
		}
		wantTokens := paheTokens(tl)
		switch {
		case tl == text:
			score = max(score, 100)
			continue
		case strings.EqualFold(strings.Join(sortedTokenSet(candTokens), ""),
			strings.Join(sortedTokenSet(wantTokens), "")) && len(candTokens) > 0:
			score = max(score, 90)
		case strings.Contains(text, tl):
			adds := false
			for w := range candTokens {
				if !wantTokens[w] {
					adds = true
					break
				}
			}
			if !adds {
				score = max(score, 80)
			}
		default:
			var overlap float64
			for w := range wantTokens {
				if candTokens[w] {
					overlap++
				}
			}
			if len(wantTokens) > 0 {
				score = max(score, overlap/float64(len(wantTokens))*60)
			}
		}
	}
	return score
}

func sortedTokenSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// resolveShow maps an AniList ID to an animepahe anime session (24h
// cache). Every title is searched (english first) and the best score wins;
// only an exact hit short-circuits, so a fuzzy hit on the english title can
// never mask an exact romaji match. Below-floor candidates are dropped —
// no match beats a wrong show.
func (p *AnimepaheProvider) resolveShow(ctx context.Context, id int) (string, string, error) {
	p.mu.Lock()
	if e, ok := p.shows[id]; ok && time.Since(e.fetched) < paheShowTTL {
		session, title := e.session, e.title
		p.mu.Unlock()
		return session, title, nil
	}
	p.mu.Unlock()

	titles, err := p.anilistTitles(ctx, id)
	if err != nil {
		return "", "", err
	}
	var best *paheSearchHit
	bestScore := -1.0
	exact := false
	for _, t := range titles {
		hits, err := p.search(ctx, t)
		if err != nil || len(hits) == 0 {
			continue
		}
		for i := range hits {
			if hits[i].Session == "" {
				continue
			}
			s := paheScoreHit(hits[i].Title, titles)
			if s < paheFuzzyMin {
				continue
			}
			if s > bestScore {
				h := hits[i]
				best, bestScore = &h, s
			}
			if s == 100 {
				exact = true
			}
		}
		if exact {
			break
		}
	}
	if best == nil {
		return "", "", fmt.Errorf("animepahe: no title match for anilist %d (%s)",
			id, strings.Join(titles, " / "))
	}
	if !exact {
		// Kept loud on purpose: this path is the only way a non-exact
		// show can ever be selected, so it must be visible in prod logs.
		p.log.Warn().Int("anilistId", id).Str("session", best.Session).
			Str("showTitle", best.Title).Float64("score", bestScore).
			Msg("animepahe: matched by fuzzy title score (no exact hit)")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.shows {
		if time.Since(e.fetched) > paheShowTTL {
			delete(p.shows, k)
		}
	}
	if len(p.shows) >= maxPaheEntries {
		for k := range p.shows {
			delete(p.shows, k)
			break
		}
	}
	p.shows[id] = &paheShowEntry{session: best.Session, title: best.Title, fetched: now}
	return best.Session, best.Title, nil
}

// Search maps a title to animepahe sessions (tooling parity), best score
// first.
func (p *AnimepaheProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	hits, err := p.search(ctx, title)
	if err != nil {
		return nil, err
	}
	type scored struct {
		hit   paheSearchHit
		score float64
	}
	var all []scored
	for _, h := range hits {
		if h.Session == "" {
			continue
		}
		all = append(all, scored{hit: h, score: paheScoreHit(h.Title, []string{title})})
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].score > all[j].score })
	out := make([]SearchResult, 0, len(all))
	for _, s := range all {
		out = append(out, SearchResult{ID: s.hit.Session, Title: s.hit.Title})
	}
	return out, nil
}

// ------------------------------------------------------------ releases

// releaseRows fetches EVERY release page for an anime session (6h cache).
// Strict: any page failure fails the whole index — a partial list could map
// a request to the wrong release.
func (p *AnimepaheProvider) releaseRows(ctx context.Context, animeSession string) ([]paheRelease, error) {
	p.mu.Lock()
	if e, ok := p.releases[animeSession]; ok && time.Since(e.fetched) < paheReleaseTTL {
		rows := e.rows
		p.mu.Unlock()
		return rows, nil
	}
	p.mu.Unlock()

	fetchPage := func(n int) (*paheReleasePage, error) {
		u := p.base + "/api?m=release&id=" + url.QueryEscape(animeSession) +
			"&sort=episode_asc&page=" + strconv.Itoa(n)
		body, err := p.fetchPAHE(ctx, u, "application/json")
		if err != nil {
			return nil, err
		}
		var page paheReleasePage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("animepahe: release page %d decode: %w", n, err)
		}
		return &page, nil
	}

	first, err := fetchPage(1)
	if err != nil {
		return nil, err
	}
	last := first.LastPage
	if last < 1 {
		last = 1
	}
	if last > pahePagesMax {
		p.log.Warn().Str("session", animeSession).Int("lastPage", last).
			Msg("animepahe: release page cap hit, list may be truncated")
		last = pahePagesMax
	}
	byPage := make([][]paheRelease, last+1)
	byPage[1] = first.Data

	if last > 1 {
		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			firstErr error
			sem      = make(chan struct{}, paheWorkers)
		)
		for n := 2; n <= last; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				page, err := fetchPage(n)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					return
				}
				byPage[n] = page.Data
			}(n)
		}
		wg.Wait()
		if firstErr != nil {
			return nil, firstErr
		}
	}
	rows := make([]paheRelease, 0, first.Total)
	for n := 1; n <= last; n++ {
		rows = append(rows, byPage[n]...)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.releases {
		if time.Since(e.fetched) > paheReleaseTTL {
			delete(p.releases, k)
		}
	}
	if len(p.releases) >= maxPaheEntries {
		for k := range p.releases {
			delete(p.releases, k)
			break
		}
	}
	p.releases[animeSession] = &paheReleaseEntry{rows: rows, fetched: now}
	return rows, nil
}

// paheRowAudio normalizes a release row's audio tag (missing = sub).
func paheRowAudio(r paheRelease) string {
	if strings.TrimSpace(r.Audio) == "" {
		return "jpn"
	}
	return strings.ToLower(strings.TrimSpace(r.Audio))
}

// pickPaheRelease chooses the release row for an episode + audio track.
// Preference: no edition (main cut), then lowest part number — multi-part
// episodes (episode2 > 0) are separate rows and the main part comes first.
func pickPaheRelease(rows []paheRelease, episode int, audio string) (paheRelease, bool) {
	var best paheRelease
	found := false
	for _, r := range rows {
		if r.Session == "" || r.Episode != float64(episode) {
			continue
		}
		if paheRowAudio(r) != audio {
			continue
		}
		if !found || paheReleasePreferred(r, best) {
			best, found = r, true
		}
	}
	return best, found
}

func editionClean(r paheRelease) bool { return strings.TrimSpace(r.Edition) == "" }

// paheReleasePreferred reports whether a should replace b as the pick.
func paheReleasePreferred(a, b paheRelease) bool {
	ac, bc := editionClean(a), editionClean(b)
	if ac != bc {
		return ac // main cut beats an edition
	}
	if a.Episode2 != b.Episode2 {
		return a.Episode2 < b.Episode2 // part 0 first, then lowest part
	}
	return false
}

// FindEpisodes lists integer episodes for an animepahe session (fractional
// episodes like 14.5 are skipped — they can never match an int request).
func (p *AnimepaheProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	session := strings.TrimSpace(providerID)
	if session == "" {
		return nil, fmt.Errorf("animepahe: empty provider id")
	}
	rows, err := p.releaseRows(ctx, session)
	if err != nil {
		return nil, err
	}
	seen := map[int]bool{}
	var out []Episode
	for _, r := range rows {
		ep := r.Episode
		if ep != float64(int(ep)) || ep <= 0 {
			continue
		}
		if !seen[int(ep)] {
			seen[int(ep)] = true
			out = append(out, Episode{Number: int(ep)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out, nil
}

// ------------------------------------------------------------ play page

// parsePahePlaySources extracts the per-quality source buttons. The page
// renders one <button data-src=... data-resolution=... data-audio=...> per
// (quality, audio); attribute order is not guaranteed, so each data-src is
// expanded to its enclosing tag and the sibling attributes are read there.
func parsePahePlaySources(html string) []pahePlaySource {
	var out []pahePlaySource
	rest := html
	for {
		i := strings.Index(rest, `data-src="`)
		if i < 0 {
			break
		}
		vStart := i + len(`data-src="`)
		vEnd := strings.Index(rest[vStart:], `"`)
		if vEnd < 0 {
			break
		}
		src := strings.TrimSpace(rest[vStart : vStart+vEnd])
		// Enclosing tag bounds: nearest '<' before, nearest '>' after the
		// attribute, so sibling attributes can never bleed across tags.
		tagStart := strings.LastIndex(rest[:i], "<")
		if tagStart < 0 {
			tagStart = 0
		}
		after := rest[vStart+vEnd:]
		tagEnd := strings.Index(after, ">")
		var tag string
		if tagEnd >= 0 {
			tag = rest[tagStart : vStart+vEnd+tagEnd+1]
		} else {
			tag = rest[tagStart:]
		}
		if src != "" {
			res, _ := strconv.Atoi(strings.TrimSpace(paheAttr(tag, "data-resolution")))
			audio := strings.ToLower(strings.TrimSpace(paheAttr(tag, "data-audio")))
			out = append(out, pahePlaySource{Src: src, Resolution: res, Audio: audio})
		}
		rest = rest[vStart+vEnd+1:]
	}
	return out
}

// paheAttr reads name="value" from a single HTML tag.
func paheAttr(tag, name string) string {
	needle := name + `="`
	i := strings.Index(tag, needle)
	if i < 0 {
		return ""
	}
	v := tag[i+len(needle):]
	j := strings.Index(v, `"`)
	if j < 0 {
		return ""
	}
	return v[:j]
}

// ------------------------------------------------------------ kwik

// resolveKwik turns a kwik /e/ page into a direct m3u8 (or returns the URL
// unchanged for pages that already carry an inline media reference).
func (p *AnimepaheProvider) resolveKwik(ctx context.Context, pageURL string) (string, error) {
	if !p.kwikPattern.MatchString(pageURL) {
		return "", fmt.Errorf("animepahe: non-kwik source url rejected: %s", pageURL)
	}
	kctx, cancel := context.WithTimeout(ctx, paheKwikTO)
	defer cancel()
	req, err := http.NewRequestWithContext(kctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return "", err
	}
	// Full browser header set: a bare UA gets Cloudflare 403 on kwik.cx
	// while this exact set returns the app page (measured).
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("sec-ch-ua", `"Chromium";v="124", "Not_A Brand";v="99"`)
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Referer", p.base+"/")
	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("animepahe: kwik fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("animepahe: kwik HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	page := string(body)

	// Fast path: an inline media URL (no eval needed).
	if m := paheInlineM3U8.FindString(page); m != "" {
		return m, nil
	}

	script, err := pahePlayerScript(page)
	if err != nil {
		return "", err
	}
	for _, tok := range paheEvalReject {
		if strings.Contains(script, tok) {
			p.log.Warn().Str("page", pageURL).Str("token", tok).
				Msg("animepahe: kwik player script rejected by safety filter")
			return "", fmt.Errorf("animepahe: kwik player script carries %q", tok)
		}
	}
	return p.evalKwikScript(ctx, script)
}

// pahePlayerScript picks the obfuscated player script: prefer the eval
// script that mentions the player bits, else any eval script (longest).
func pahePlayerScript(page string) (string, error) {
	var evalScripts []string
	for _, m := range paheScriptTagRe.FindAllStringSubmatch(page, -1) {
		if len(m) < 2 {
			continue
		}
		s := strings.TrimSpace(m[1])
		if s == "" || !strings.Contains(s, "eval(") {
			continue
		}
		evalScripts = append(evalScripts, s)
	}
	if len(evalScripts) == 0 {
		return "", fmt.Errorf("animepahe: kwik page has no eval player script")
	}
	for _, s := range evalScripts {
		if strings.Contains(s, ".m3u8") || strings.Contains(s, "Plyr") ||
			strings.Contains(s, "source") || strings.Contains(s, "uwu") {
			return s, nil
		}
	}
	best := evalScripts[0]
	for _, s := range evalScripts[1:] {
		if len(s) > len(best) {
			best = s
		}
	}
	return best, nil
}

// evalKwikScript spawns the sandbox (bun, node fallback) with the player
// script on stdin and returns the first m3u8 it captured. One-shot per
// cache miss — the resolved URL is cached with the episode.
func (p *AnimepaheProvider) evalKwikScript(ctx context.Context, script string) (string, error) {
	bin, engineScript, err := p.kwikEngineCommand()
	if err != nil {
		return "", err
	}
	ectx, cancel := context.WithTimeout(ctx, paheEngineTO)
	defer cancel()
	args := []string{}
	if engineScript != "" {
		args = append(args, engineScript)
	}
	cmd := exec.CommandContext(ectx, bin, args...)
	cmd.Stdin = strings.NewReader(script)
	cmd.Env = os.Environ()
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil && ectx.Err() != nil {
		return "", fmt.Errorf("animepahe: kwik eval timeout: %w", err)
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && strings.Contains(line, ".m3u8") && len(line) < 2048 {
			return line, nil
		}
	}
	tail := strings.TrimSpace(stderr.String())
	if len(tail) > 300 {
		tail = tail[len(tail)-300:]
	}
	return "", fmt.Errorf("animepahe: kwik eval produced no m3u8 (stderr: %s)", tail)
}

// kwikEngineCommand resolves the sandbox runner: explicit override (tests),
// then ANIRAKU_PAHE_ENGINE, then the vendored script with bun (node
// fallback) — mkissa's daemonCommand pattern.
func (p *AnimepaheProvider) kwikEngineCommand() (string, string, error) {
	if p.engineBin != "" {
		return p.engineBin, p.engineScript, nil
	}
	var script string
	if env := strings.TrimSpace(os.Getenv(paheEngineEnv)); env != "" {
		script = env
	} else {
		for _, c := range []string{
			"/app/third_party/animepahe-engine/kwik_resolve.js",
			"third_party/animepahe-engine/kwik_resolve.js",
		} {
			if st, err := os.Stat(c); err == nil && !st.IsDir() {
				script = c
				break
			}
		}
	}
	if script == "" {
		return "", "", fmt.Errorf("animepahe: kwik engine script not found")
	}
	if bin, err := exec.LookPath("bun"); err == nil {
		return bin, script, nil
	}
	if bin, err := exec.LookPath("node"); err == nil {
		return bin, script, nil
	}
	return "", "", fmt.Errorf("animepahe: neither bun nor node on PATH")
}

// ------------------------------------------------------------ resolve

func (p *AnimepaheProvider) loadResolved(session string, episode int, lang string) *SourceResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.resolved[paheResolveKey{session: session, episode: episode, lang: lang}]
	if !ok || time.Since(e.fetched) > paheResolveTTL {
		return nil
	}
	return cloneSourceResult(e.result)
}

func (p *AnimepaheProvider) storeResolved(session string, episode int, lang string, sr *SourceResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, e := range p.resolved {
		if time.Since(e.fetched) > paheResolveTTL {
			delete(p.resolved, k)
		}
	}
	if len(p.resolved) >= maxPaheEntries {
		oldest := paheResolveKey{}
		var oldestAt time.Time
		first := true
		for k, e := range p.resolved {
			if first || e.fetched.Before(oldestAt) {
				oldest, oldestAt, first = k, e.fetched, false
			}
		}
		delete(p.resolved, oldest)
	}
	p.resolved[paheResolveKey{session: session, episode: episode, lang: lang}] =
		&paheResolvedEntry{result: cloneSourceResult(sr), fetched: now}
}

// FindEpisodeSource resolves one episode for exactly the requested lang
// (strict per-lang like kaa/mkissa): a dub request with no eng release
// fails instead of falling back to sub.
func (p *AnimepaheProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	// Guard before ANY network: resolveShow's AniList lookup would fire
	// even with no solver available, and the fan-out must skip us at
	// zero cost instead of paying for a lookup we cannot complete.
	if !p.Configured() {
		return nil, fmt.Errorf("animepahe: solver not configured (set %s)", paheSolverEnv)
	}
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("animepahe: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	wantAudio := "jpn"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
		wantAudio = "eng"
	}
	session, _, err := p.resolveShow(ctx, id)
	if err != nil {
		return nil, err
	}
	if got := p.loadResolved(session, episode, langKey); got != nil {
		return got, nil
	}
	rows, err := p.releaseRows(ctx, session)
	if err != nil {
		return nil, err
	}
	row, ok := pickPaheRelease(rows, episode, wantAudio)
	if !ok {
		return nil, fmt.Errorf("animepahe: episode %d not listed (%s)", episode, langKey)
	}
	playURL := p.base + "/play/" + url.PathEscape(session) + "/" + url.PathEscape(row.Session)
	html, err := p.fetchPAHE(ctx, playURL, "text/html,application/xhtml+xml")
	if err != nil {
		return nil, err
	}
	btns := p.paheSourceButtons(string(html), wantAudio)
	if len(btns) == 0 {
		return nil, fmt.Errorf("animepahe: no %s source buttons for episode %d", langKey, episode)
	}

	// Resolve + probe every button concurrently: the fan-out's latency IS
	// its slowest collector, and kwik eval + segment probe dominate.
	results := make([]paheOutcome, len(btns))
	var wg sync.WaitGroup
	for i := range btns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = p.resolveAndProbe(ctx, btns[i])
		}(i)
	}
	wg.Wait()

	sr := &SourceResult{Headers: map[string]string{"Referer": paheKwikReferer}}
	for _, r := range results {
		if !r.ok {
			continue
		}
		p.learnURLHost(r.url)
		quality := "auto"
		if r.btn.Resolution > 0 {
			quality = strconv.Itoa(r.btn.Resolution) + "p"
		}
		sr.Sources = append(sr.Sources, core.Source{
			URL:          r.url,
			Type:         r.typ,
			Quality:      quality,
			Verification: "proxy",
		})
		sr.ServerNames = append(sr.ServerNames, paheServerName(len(sr.ServerNames)))
	}
	if len(sr.Sources) == 0 {
		return nil, fmt.Errorf("animepahe: no playable source for episode %d (%s)", episode, langKey)
	}
	p.log.Info().Int("animeId", id).Int("episode", episode).Str("lang", langKey).
		Int("sources", len(sr.Sources)).Msg("animepahe resolved")
	p.storeResolved(session, episode, langKey, sr)
	return sr, nil
}

// paheSourceButtons filters the play page's buttons to the requested audio
// track, keeping only kwik embeds or direct media, best quality first.
func (p *AnimepaheProvider) paheSourceButtons(html, wantAudio string) []pahePlaySource {
	all := parsePahePlaySources(html)
	var out []pahePlaySource
	for _, b := range all {
		if b.Audio != "" && b.Audio != wantAudio {
			continue
		}
		// Buttons without an audio tag are only kept for sub (the common
		// japanese track) — an untagged button must never serve dub.
		if b.Audio == "" && wantAudio != "jpn" {
			continue
		}
		if p.kwikPattern.MatchString(b.Src) ||
			strings.Contains(b.Src, ".m3u8") || strings.Contains(b.Src, ".mp4") {
			out = append(out, b)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Resolution > out[j].Resolution })
	if len(out) > paheSourcesMax {
		out = out[:paheSourcesMax]
	}
	return out
}

// paheOutcome is one button's resolved + probed result.
type paheOutcome struct {
	btn pahePlaySource
	url string
	typ string
	ok  bool
}

// resolveAndProbe resolves one button to a direct URL and honesty-probes it
// from this egress (master->media->segment, VOD cache warmed). A button
// whose resolve or probe fails is dropped — the other qualities survive.
func (p *AnimepaheProvider) resolveAndProbe(ctx context.Context, btn pahePlaySource) paheOutcome {
	switch {
	case p.kwikPattern.MatchString(btn.Src):
		m3u8, err := p.resolveKwik(ctx, btn.Src)
		if err != nil {
			p.log.Debug().Err(err).Str("src", btn.Src).Msg("animepahe: kwik resolve failed")
			return paheOutcome{btn: btn}
		}
		if !probeSegmentsLenient(ctx, p.client, m3u8, paheKwikReferer, browserUA) {
			p.log.Debug().Str("m3u8", m3u8).Msg("animepahe: media probe failed")
			return paheOutcome{btn: btn}
		}
		return paheOutcome{btn: btn, url: m3u8, typ: "hls", ok: true}
	case strings.Contains(btn.Src, ".m3u8"):
		if !probeSegmentsLenient(ctx, p.client, btn.Src, p.base+"/", browserUA) {
			return paheOutcome{btn: btn}
		}
		return paheOutcome{btn: btn, url: btn.Src, typ: "hls", ok: true}
	case strings.Contains(btn.Src, ".mp4"):
		if !probeMediaFileLenient(ctx, p.client, btn.Src, p.base+"/", browserUA) {
			return paheOutcome{btn: btn}
		}
		return paheOutcome{btn: btn, url: btn.Src, typ: "mp4", ok: true}
	}
	return paheOutcome{btn: btn}
}
