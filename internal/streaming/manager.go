package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

type contextKey string

const refreshKey contextKey = "refresh_cache"

func WithRefresh(ctx context.Context) context.Context {
	return context.WithValue(ctx, refreshKey, true)
}

func IsRefresh(ctx context.Context) bool {
	v, _ := ctx.Value(refreshKey).(bool)
	return v
}

// PlaybackVerdict ranks how a source can reach a player. Verdicts are soft
// ordering hints, never filters: a "dead" datacenter verdict does not mean
// the source is dead for a residential browser.
type PlaybackVerdict int

const (
	VerdictDead PlaybackVerdict = iota
	VerdictEmbed
	VerdictDirect
	VerdictProxy
)

func (v PlaybackVerdict) String() string {
	switch v {
	case VerdictProxy:
		return "proxy"
	case VerdictDirect:
		return "direct"
	case VerdictEmbed:
		return "embed"
	}
	return "dead"
}

type Manager struct {
	log       zerolog.Logger
	providers []Provider
	// LearnHost, when set, receives hosts the provider chain itself verified
	// (probed stream manifests, subtitle tracks, download links) so the HTTP
	// layer can feed the media-proxy CDN allowlist and provider CDN rotation
	// never 403s at the gate.
	LearnHost func(host string)

	// hentai gate: hentai titles are served by Zoko (MAL-keyed) +
	// FlixCloud only; anikoto/animex must not receive any request
	// for them.
	httpClient  *http.Client
	hentaiMu    sync.Mutex
	hentaiCache map[int]hentaiEntry
}

type hentaiEntry struct {
	isHentai bool
	expires  time.Time
}

// SetHostLearner registers the callback that receives provider-verified hosts.
func (m *Manager) SetHostLearner(fn func(host string)) {
	m.LearnHost = fn
	for _, p := range m.providers {
		if ak, ok := p.(*AnikotoProvider); ok {
			ak.SetHostLearner(fn)
		}
		if zk, ok := p.(*ZokoProvider); ok {
			zk.SetHostLearner(fn)
		}
		if ax, ok := p.(*AnimeXProvider); ok {
			ax.SetHostLearner(fn)
		}
		if nn, ok := p.(*NiNProvider); ok {
			nn.SetHostLearner(fn)
		}
		if ka, ok := p.(*KaaProvider); ok {
			ka.SetHostLearner(fn)
		}
		if ag, ok := p.(*AnimeGGProvider); ok {
			ag.SetHostLearner(fn)
		}
		if aw, ok := p.(*AniWavesProvider); ok {
			aw.SetHostLearner(fn)
		}
		if vn, ok := p.(*VidNestProvider); ok {
			vn.SetHostLearner(fn)
		}
		if le, ok := p.(*LeeProvider); ok {
			le.SetHostLearner(fn)
		}
	}
}

type Provider interface {
	Name() string
	Search(ctx context.Context, title string) ([]SearchResult, error)
	FindEpisodes(ctx context.Context, providerID string) ([]Episode, error)
	FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error)
}

type SearchResult struct {
	ID    string
	Title string
}

type Episode struct {
	Number int
	Title  string
	Filler bool
	Recap  bool
}

type SourceResult struct {
	Sources    []core.Source
	Headers    map[string]string
	Downloads  []core.DownloadLink
	ServerName string
	// ServerNames optionally names each entry of Sources in order (e.g.
	// Anikoto's Niko for the AniList-keyed stream, Momo for the MAL-keyed
	// one). appendNamedServers prefers these when the lengths match and
	// falls back to positional names otherwise, so providers that don't
	// set this behave exactly as before.
	ServerNames []string
	// Intro/Outro are provider skip segments, passed through to the client
	// so it can offer manual skip buttons.
	Intro *core.SkipTimestamp
	Outro *core.SkipTimestamp
}

// NewManager builds the provider set. Anikoto is primary (fully in-process:
// show resolve -> episode data-ids -> servers -> embed decrypt -> verified
// m3u8); AnimeX (plyr API, XOR-decoded direct URLs) is second; Zoko
// (ZokoAnime, AniList-keyed) is third; FlixCloud is the fallback for embed
// playback; AnimeGG (direct mp4, highest quality per mirror) and AniWaves
// (multi-rendition HLS masters) ride next; VidNest (MegaPlay HLS + subs)
// rides last; Lee (ani.pm direct HLS) rides after VidNest. (Zenime removed
// 2026-09-30: arms API unreliable. OGFLix removed: api.anizen.tr challenged
// every request and every resolved edge was blocked — pure fan-out latency
// for nothing.)
//
// NOTE (operator): kaa.lt decrypted stream URLs are dual-language — one
// resolved URL serves both sub (ja-JP) and dub (en-US). A future kaa.lt
// provider must resolve once and share the result across langs, never fetch
// per-lang. Reference scraper: /home/ichigoat/kaa_stream_scraper.py
// (episodes API takes lang=ja-JP|en-US; the decrypted m3u8 covers both).
func NewManager(log zerolog.Logger) *Manager {
	return &Manager{
		log: log,
		providers: []Provider{
			NewAnikotoProvider(log),
			NewAnimeXProvider(log, ""),
			NewZokoProvider(log),
			NewFlixCloudProvider(log),
			NewNiNProvider(log),
			NewKaaProvider(log, "", ""),
			NewAnimeGGProvider(log, "", ""),
			NewAniWavesProvider(log, "", ""),
			NewVidNestProvider(log, ""),
			NewLeeProvider(log, "", ""),
		},
		httpClient:  &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport()},
		hentaiCache: map[int]hentaiEntry{},
	}
}

// isHentaiTitle reports whether the AniList title carries the Hentai genre
// or is flagged isAdult. Results are cached 10 minutes; on lookup failure the
// title is treated as non-hentai so playback never hard-fails on the gate.
func (m *Manager) isHentaiTitle(ctx context.Context, animeID int) bool {
	m.hentaiMu.Lock()
	if e, ok := m.hentaiCache[animeID]; ok && time.Now().Before(e.expires) {
		m.hentaiMu.Unlock()
		return e.isHentai
	}
	m.hentaiMu.Unlock()

	isHentai := false
	query := `{"query":"{ Media(id:` + strconv.Itoa(animeID) + `,type:ANIME){genres isAdult} }"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.aniraku.tech", strings.NewReader(query))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		resp, err := m.httpClient.Do(req)
		if err == nil {
			body, rErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
			resp.Body.Close()
			if rErr == nil {
				var out struct {
					Data struct {
						Media struct {
							Genres  []string `json:"genres"`
							IsAdult bool     `json:"isAdult"`
						} `json:"Media"`
					} `json:"data"`
				}
				if json.Unmarshal(body, &out) == nil && out.Data.Media.Genres != nil {
					isHentai = containsHentai(out.Data.Media.Genres) || out.Data.Media.IsAdult
				}
			}
		}
	}

	m.hentaiMu.Lock()
	m.hentaiCache[animeID] = hentaiEntry{isHentai: isHentai, expires: time.Now().Add(10 * time.Minute)}
	m.hentaiMu.Unlock()
	return isHentai
}

// GetSources resolves sources using the default provider order (anikoto,
// zoko, then flixcloud).
func (m *Manager) GetSources(ctx context.Context, title string, episode int, lang, quality string) (*core.StreamResult, error) {
	return m.GetSourcesForProvider(ctx, episode, "", lang, quality, 0)
}

// GetSourcesForProvider resolves sources for the requested provider/lang.
func (m *Manager) GetSourcesForProvider(ctx context.Context, episode int, provider, lang, quality string, animeID int) (*core.StreamResult, error) {
	return m.GetSourcesForProviderWithSlug(ctx, episode, provider, lang, quality, animeID, "")
}

// zokoPaused kills every ZokoAnime upstream call (server list, stream
// fallback chain, explicit provider requests) while it is unreliable.
// Currently unpaused: streams are back, reliability re-verified.
// Flip back to true if it degrades again; no other change needed.
// NOTE: the Kiwi download fetcher (kiwi_downloads.go) is independent —
// the zokoanime.video download API endpoints proved stable even while
// streams flapped — so it keeps serving regardless of this flag.
const zokoPaused = false

// GetSourcesForProviderWithSlug is the frontend-aware streaming entry point.
// slug is accepted for API compatibility; the Anikoto resolver maps AniList
// IDs itself, so the slug is unused today.
func (m *Manager) GetSourcesForProviderWithSlug(ctx context.Context, episode int, provider, lang, quality string, animeID int, slug string) (*core.StreamResult, error) {
	// Cute server names double as provider aliases (the server list shows
	// "Sunny", the explicit request sends "Sunny"): normalize them to the
	// family before anything else so both forms resolve identically.
	switch strings.ToLower(provider) {
	case "animegg", "sunny", "yolky", "eggy":
		provider = "animegg"
	case "aniwaves", "nami", "coral", "pearl", "wavy", "bubbles", "shelly":
		provider = "aniwaves"
	case "vidnest", "animepahe", "pahe", "nest":
		provider = "vidnest"
	case "lee":
		provider = "lee"
	}
	// Hentai titles are served by Zoko (MAL-keyed) + FlixCloud:
	// explicit requests for anikoto/animex are
	// rejected before any upstream call, and the fallback chain below
	// shrinks accordingly. Zoko only indexes hentai by MAL ID, so its
	// AniList-keyed attempt falls through to the MAL lookup inside
	// the provider.
	hentai := m.isHentaiTitle(ctx, animeID)
	if hentai {
		switch provider {
		case "anikoto", "animex", "yuki", "neko", "zuna", "sora", "nin", "supaplay", "animegg", "aniwaves", "vidnest", "lee":
			return nil, fmt.Errorf("provider %q is not available for this title", provider)
		}
	}
	switch provider {
	case "anikoto":
		if hentai {
			return nil, fmt.Errorf("provider %q is not available for this title", provider)
		}
		result, err := m.tryAnikoto(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("anikoto: no sources for this episode")
	case "animex", "yuki", "neko", "zuna", "sora":
		result, err := m.tryAnimeX(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("animex: no sources for this episode")
	case "zoko", "zokoanime":
		// Frontend sends "zoko"; accept "zokoanime" as an alias. Backend
		// hits ZokoAnime (/stream/ani/{anilistId}/{ep}/{sub|dub}).
		if zokoPaused {
			return nil, fmt.Errorf("zoko is temporarily paused while reliability recovers")
		}
		result, err := m.tryZoko(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("zoko: no sources for this episode")
	case "flixcloud":
		result, err := m.tryFlixCloudWithSlug(ctx, animeID, episode, lang, quality, slug)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("flixcloud: no sources for this episode")
	case "nin", "supaplay":
		// Supaplay (anistream's embed lineup) — SFW titles only: the
		// hentai reject above short-circuits adult titles before any
		// upstream call (Supaplay's API 502s them).
		result, err := m.tryNiN(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("nin: no sources for this episode")
	case "kaa":
		// Explicit kaa requests are allowed for every title: the
		// decrypted krussdomi manifest serves sub and dub alike.
		result, err := m.tryKaa(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("kaa: no sources for this episode")
	case "animegg", "sunny", "yolky", "eggy":
		// Family plus cute aliases (normalization above already maps
		// them; the aliases here cover direct switch hits).
		result, err := m.tryAnimeGG(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("animegg: no sources for this episode")
	case "aniwaves", "nami", "coral", "pearl", "wavy", "bubbles", "shelly":
		result, err := m.tryAniWaves(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("aniwaves: no sources for this episode")
	case "lee", "Lee":
		result, err := m.tryLee(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("lee: no sources for this episode")
	case "vidnest", "animepahe", "pahe", "nest":
		// animepahe pages hit the same VidNest API (verified
		// byte-identical); the alias lands on the same resolve.
		result, err := m.tryVidNest(ctx, animeID, episode, lang, quality)
		if err != nil {
			return nil, err
		}
		if result != nil && len(result.Sources) > 0 {
			return result, nil
		}
		return nil, fmt.Errorf("vidnest: no sources for this episode")
	case "miruro", "hop", "bonk", "bee", "moo", "ally", "pewe", "kiwi", "mimi", "ogflix", "zenime", "mkissa", "tryembed", "astro", "beta", "skye", "zen":
		return nil, fmt.Errorf("provider %q removed - use anikoto, zoko or flixcloud", provider)
	}
	var lastErr error
	// Anikoto direct first, AnimeX (plyr API) second, Zoko third (skipped
	// while paused), FlixCloud embed fourth, kaa.lt fifth, AnimeGG mp4
	// sixth, AniWaves HLS seventh, VidNest eighth, Lee last. Hentai
	// titles
	// only ever reach Zoko (MAL-keyed for hentai, when unpaused) and
	// FlixCloud (Reanime embeds, covers hentai).
	var candidates []func() (*core.StreamResult, error)
	if hentai {
		candidates = []func() (*core.StreamResult, error){}
		if !zokoPaused {
			candidates = append(candidates,
				func() (*core.StreamResult, error) { return m.tryZoko(ctx, animeID, episode, lang, quality) })
		}
		candidates = append(candidates,
			func() (*core.StreamResult, error) {
				return m.tryFlixCloudWithSlug(ctx, animeID, episode, lang, quality, slug)
			},
		)
	} else {
		candidates = []func() (*core.StreamResult, error){
			func() (*core.StreamResult, error) { return m.tryAnikoto(ctx, animeID, episode, lang, quality) },
			func() (*core.StreamResult, error) { return m.tryAnimeX(ctx, animeID, episode, lang, quality) },
		}
		if !zokoPaused {
			candidates = append(candidates,
				func() (*core.StreamResult, error) { return m.tryZoko(ctx, animeID, episode, lang, quality) })
		}
		candidates = append(candidates, func() (*core.StreamResult, error) {
			return m.tryFlixCloudWithSlug(ctx, animeID, episode, lang, quality, slug)
		}, func() (*core.StreamResult, error) {
			return m.tryKaa(ctx, animeID, episode, lang, quality)
		}, func() (*core.StreamResult, error) {
			return m.tryAnimeGG(ctx, animeID, episode, lang, quality)
		}, func() (*core.StreamResult, error) {
			return m.tryAniWaves(ctx, animeID, episode, lang, quality)
		}, func() (*core.StreamResult, error) {
			return m.tryVidNest(ctx, animeID, episode, lang, quality)
		}, func() (*core.StreamResult, error) {
			return m.tryLee(ctx, animeID, episode, lang, quality)
		})
	}
	for _, try := range candidates {
		res, err := try()
		if err == nil && res != nil && len(res.Sources) > 0 {
			return res, nil
		}
		if err != nil {
			lastErr = err
		} else if lastErr == nil {
			lastErr = fmt.Errorf("no sources from provider")
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no streaming source found for this episode")
}

// containsHentai returns true if the genre list contains the Hentai genre.
func containsHentai(genres []string) bool {
	for _, g := range genres {
		if strings.EqualFold(g, "hentai") {
			return true
		}
	}
	return false
}

// FindAllServers lists every selectable server per lang (quality selection
// preserved). Ordering is deterministic: best playback verdict first (proxy >
// direct > embed > dead), then provider order. No server is hidden — dead
// providers simply contribute nothing.
// genres is used to skip providers that should not serve certain content
// (e.g. Anikoto is skipped for Hentai titles).
func (m *Manager) FindAllServers(ctx context.Context, animeID int, episode int, lang string, genres []string) []core.Server {
	if lang == "" {
		lang = "sub"
	}

	// Hard cap for the whole fan-out: one slow provider (dead embed, blocked
	// CDN, hanging search) must not stretch the server list past this.
	fanCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	ctx = fanCtx

	// All providers run AT ONCE (fan-out), then merge in fixed provider
	// order and rank by playback verdict.
	// Hentai gate: when the caller provides genres, trust them; otherwise
	// resolve them once (cached) so hentai titles never reach the gated
	// providers.
	hentai := containsHentai(genres)
	if !hentai && len(genres) == 0 {
		hentai = m.isHentaiTitle(ctx, animeID)
	}
	anilistID := fmt.Sprintf("%d", animeID)

	// No snapshot cache: tokenized URLs (megaplay ?token=, proxied nonces)
	// expire without a reliable invalidation signal, and a cached list also
	// froze whichever request base first wrapped it — a localhost health
	// probe once poisoned every public response for a full TTL with
	// http://127.0.0.1 URLs. Every request computes a fresh, honestly
	// probed list instead; speed comes from the collectors themselves.

	var akServers, axServers, zkServers, fcServers, nnServers, kaServers, agServers, awServers, vnServers, leeServers []core.Server
	var kiwiLinks []core.DownloadLink
	var wg sync.WaitGroup

	// Per-collector wall time, logged after the merge: the fan-out's
	// response latency IS its slowest collector, so every round needs the
	// culprit visible instead of guessed.
	fanStart := time.Now()
	var dmu sync.Mutex
	collectorMs := map[string]int64{}
	run := func(name string, fn func() []core.Server) []core.Server {
		start := time.Now()
		out := fn()
		dmu.Lock()
		collectorMs[name] = time.Since(start).Milliseconds()
		dmu.Unlock()
		return out
	}

	// Fan-out collectors as data: the WaitGroup count derives from the
	// slice, never a hand-maintained number — adding or removing a
	// provider cannot wedge wg.Wait() again (observed 2026-10-02: a stale
	// Add(11) deadlocked every /servers call after a provider removal).
	// Anikoto, AnimeX, NiN, AnimeGG, AniWaves and VidNest are never queried
	// for hentai titles (hentai gate — Supaplay's API 502s them). Zoko and
	// FlixCloud serve them:
	// Zoko only via its MAL-keyed path (its AniList index carries no
	// hentai), FlixCloud via Reanime.
	collectors := []func(){
		func() {
			if !hentai {
				akServers = run("anikoto", func() []core.Server {
					return m.collectAnikotoServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			if !hentai {
				axServers = run("animex", func() []core.Server {
					return m.collectAnimeXServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			if !zokoPaused {
				zkServers = run("zoko", func() []core.Server {
					return m.collectZokoServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			fcServers = run("flixcloud", func() []core.Server {
				return m.collectFlixServers(ctx, anilistID, episode, lang)
			})
		},
		func() {
			if !hentai {
				nnServers = run("nin", func() []core.Server {
					return m.collectNiNServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			if !hentai {
				kaServers = run("kaa", func() []core.Server {
					return m.collectKaaServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			if !hentai {
				agServers = run("animegg", func() []core.Server {
					return m.collectAnimeGGServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			if !hentai {
				awServers = run("aniwaves", func() []core.Server {
					return m.collectAniWavesServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			if !hentai {
				vnServers = run("vidnest", func() []core.Server {
					return m.collectVidNestServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			if !hentai {
				leeServers = run("lee", func() []core.Server {
					return m.collectLeeServers(ctx, anilistID, episode, lang)
				})
			}
		},
		func() {
			// Runs alongside the provider fan-out (not after it): a slow
			// provider must never starve the download fetch of context
			// budget — observed 46s responses when the 45s fan-out cap trips.
			start := time.Now()
			kiwiLinks = fetchKiwiDownloads(ctx, m.httpClient, anilistID, episode, lang)
			dmu.Lock()
			collectorMs["kiwiDownloads"] = time.Since(start).Milliseconds()
			dmu.Unlock()
		},
	}
	wg.Add(len(collectors))
	for _, collect := range collectors {
		go func(fn func()) {
			defer wg.Done()
			fn()
		}(collect)
	}
	wg.Wait()

	// Merged downloads for Zoko: Zoko's own link plus the already-fetched
	// Anikoto download links together — falling back to one downloads-only
	// Anikoto lookup when Anikoto streams were all CDN-blocked (no servers,
	// but downloads may still exist). No extra network call in the common
	// case — akServers was fetched in parallel above. The whole Anikoto
	// merge is skipped for hentai titles: it would send them to a gated
	// provider.
	zkServers = mergeZokoDownloads(ctx, m, zkServers, akServers, anilistID, episode, lang, hentai)
	nnServers = mergeNiNSubtitles(nnServers, akServers, axServers, zkServers, lang)

	// Provider merge order is fixed (direct first, embeds and the newest
	// providers last); playback-verdict ranking below reorders by health.
	allServers := akServers
	for _, pool := range [][]core.Server{axServers, zkServers, fcServers, nnServers, kaServers, agServers, awServers, vnServers, leeServers} {
		allServers = append(allServers, pool...)
	}
	// Kiwi download links (fetched in parallel above): attach to every
	// non-embed server (embed players take no file links). Independent of
	// the Zoko streaming provider and its pause flag.
	if len(kiwiLinks) > 0 {
		allServers = attachKiwiDownloads(allServers, kiwiLinks)
	} else {
		m.log.Debug().Str("anilistId", anilistID).Int("episode", episode).Str("lang", lang).Msg("servers: no kiwi download links")
	}
	sort.SliceStable(allServers, func(i, j int) bool {
		return serverVerdictRank(allServers[i]) > serverVerdictRank(allServers[j])
	})

	m.log.Info().
		Str("anilistId", anilistID).Int("episode", episode).Str("lang", lang).
		Int("servers", len(allServers)).
		Int64("totalMs", time.Since(fanStart).Milliseconds()).
		Interface("collectorMs", collectorMs).
		Msg("servers: fan-out complete")

	return allServers
}

// appendNamedServers maps a provider's source list onto display names
// (Niko/Momo, Yuta/Syota/Mike, ...). When there are more sources than names,
// extras collide on the last name — on a collision the source with the
// richer subtitle track list wins, the other is dropped, so a display name
// never appears twice in the server list.
func appendNamedServers(out []core.Server, serverNames []string, provider, lang string, sr *SourceResult) []core.Server {
	nameIndex := make(map[string]int, len(serverNames))
	for i, src := range sr.Sources {
		name := serverNames[len(serverNames)-1]
		if i < len(serverNames) {
			name = serverNames[i]
		}
		// A result may name its own sources (Anikoto: Niko for the
		// AniList-keyed stream, Momo for the MAL-keyed one) — that wins
		// over positional naming so each key keeps its slot even when
		// the other key is missing.
		if len(sr.ServerNames) == len(sr.Sources) && sr.ServerNames[i] != "" {
			name = sr.ServerNames[i]
		}
		if idx, ok := nameIndex[name]; ok {
			if len(src.Subtitles) > len(out[idx].Sources[0].Subtitles) {
				out[idx].Sources = []core.Source{src}
			}
			continue
		}
		nameIndex[name] = len(out)
		out = append(out, core.Server{
			Name:      name,
			Provider:  provider,
			Lang:      lang,
			Sources:   []core.Source{src},
			Headers:   sr.Headers,
			Downloads: sr.Downloads,
		})
	}
	return out
}

// collectFlixServers maps FlixCloud sources to Yuta/Syota/Mike servers.
func (m *Manager) collectFlixServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		fc, ok := prov.(*FlixCloudProvider)
		if !ok {
			continue
		}
		sr, err := fc.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "flixcloud").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue // the provider logs its own reason (Warn inside flixcloud.go)
		}
		sr = m.withDubSubtitles(ctx, "flixcloud", lang, anilistID, episode, sr)
		out = appendNamedServers(out, []string{"Yuta", "Syota", "Mike"}, "flixcloud", lang, sr)
	}
	return out
}

// collectAnikotoServers maps Anikoto sources to Niko + Momo servers.
func (m *Manager) collectAnikotoServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		ak, ok := prov.(*AnikotoProvider)
		if !ok {
			continue
		}
		sr, err := ak.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "anikoto").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue // the provider logs why (dropped servers / CDN block)
		}
		sr = m.withDubSubtitles(ctx, "anikoto", lang, anilistID, episode, sr)
		out = appendNamedServers(out, anikotoServers[:], "anikoto", lang, sr)
	}
	return out
}

// collectZokoServers maps ZokoAnime sources to the single "Zoko" server.
// The provider stays probe-verified + proxied (never direct): ZokoProvider
// drops CDN-blocked manifests before they can surface here.
func (m *Manager) collectZokoServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		zk, ok := prov.(*ZokoProvider)
		if !ok {
			continue
		}
		sr, err := zk.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "zoko").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue // CDN-blocked or missing episode (logged inside zoko.go)
		}
		sr = m.withDubSubtitles(ctx, "zoko", lang, anilistID, episode, sr)
		out = appendNamedServers(out, []string{zokoServerName}, "zoko", lang, sr)
	}
	return out
}

// collectNiNServers maps Supaplay (NiN) sources to the single "NiN" server.
// The shipped URL is Supaplay's authless hls-proxy relay, probe-verified
// before it can surface (Zoko-style honesty: a relay the probe cannot reach
// would only produce 502s through the media proxy).
func (m *Manager) collectNiNServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		nn, ok := prov.(*NiNProvider)
		if !ok {
			continue
		}
		sr, err := nn.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "nin").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue // blocked relay or missing episode (logged inside nin.go)
		}
		sr = m.withDubSubtitles(ctx, "nin", lang, anilistID, episode, sr)
		out = appendNamedServers(out, []string{ninServerName}, "nin", lang, sr)
	}
	return out
}

// mergeZokoDownloads merges the Anikoto download links into the Zoko
// servers alongside Zoko's own link (deduped by URL, Zoko's own first).
// Stream sources are never mixed — only Downloads. Anikoto direct-only
// resolution carries no download links, so in practice this merges nothing
// today; the loop stays for any future provider-side downloads. hentai
// titles skip the merge entirely: Anikoto is gated for them, so only
// Zoko's own links survive.
func mergeZokoDownloads(ctx context.Context, m *Manager, zkServers, akServers []core.Server, anilistID string, episode int, lang string, hentai bool) []core.Server {
	if len(zkServers) == 0 || hentai {
		return zkServers
	}
	var links []core.DownloadLink
	for _, s := range akServers {
		links = mergeDownloadLinks(links, s.Downloads)
	}
	for i := range zkServers {
		zkServers[i].Downloads = mergeDownloadLinks(zkServers[i].Downloads, links)
	}
	return zkServers
}

// mergeNiNSubtitles attaches verified subtitle tracks to NiN's servers from
// the sibling collectors of the same episode, in order Niko -> animex (Mochi
// et al) -> Zoko. Supaplay's own tracks are unusable from every path we
// have (rawFile hosts TLS-blocked from this egress, its subtitle-proxy 404s,
// the frontend's proxy hits the same walls), while the borrowed tracks point
// at hosts the frontend already proxies fine — same episode, same timings.
// Without a donor NiN simply ships no subs; it never resurrects its own.
func mergeNiNSubtitles(nnServers, akServers, axServers, zkServers []core.Server, lang string) []core.Server {
	if len(nnServers) == 0 {
		return nnServers
	}
	// Dub listings carry their default subtitles, except Sora (animex),
	// whose sources already received the nico files via withDubSubtitles —
	// never overwrite those with provider dub files; only fill sources
	// that somehow have none (nico unavailable for the title).
	fillEmptyOnly := strings.EqualFold(lang, "dub")
	var donor []core.Subtitle
	for _, pool := range [][]core.Server{akServers, axServers, zkServers} {
		for _, s := range pool {
			for _, src := range s.Sources {
				if len(src.Subtitles) > 0 {
					donor = src.Subtitles
					break
				}
			}
			if donor != nil {
				break
			}
		}
		if donor != nil {
			break
		}
	}
	if donor == nil {
		return nnServers
	}
	for i := range nnServers {
		for j := range nnServers[i].Sources {
			if fillEmptyOnly && len(nnServers[i].Sources[j].Subtitles) > 0 {
				continue
			}
			subs := make([]core.Subtitle, len(donor))
			copy(subs, donor)
			nnServers[i].Sources[j].Subtitles = subs
		}
	}
	return nnServers
}

// mergeDownloadLinks dedupes download links by URL, keeping primary order.
func mergeDownloadLinks(primary, fallback []core.DownloadLink) []core.DownloadLink {
	seen := make(map[string]bool, len(primary)+len(fallback))
	out := make([]core.DownloadLink, 0, len(primary)+len(fallback))
	for _, d := range primary {
		if d.URL == "" || seen[d.URL] {
			continue
		}
		seen[d.URL] = true
		out = append(out, d)
	}
	for _, d := range fallback {
		if d.URL == "" || seen[d.URL] {
			continue
		}
		seen[d.URL] = true
		out = append(out, d)
	}
	return out
}

// serverVerdictRank maps a server's best per-source verification tag to a
// comparable rank (proxy > direct > embed > dead).
func serverVerdictRank(s core.Server) int {
	best := 0
	for _, src := range s.Sources {
		switch src.Verification {
		case "proxy":
			return 3
		case "direct":
			if best < 2 {
				best = 2
			}
		case "embed":
			if best < 1 {
				best = 1
			}
		}
	}
	return best
}

func (m *Manager) getFlixCloudProvider() *FlixCloudProvider {
	for _, p := range m.providers {
		if fc, ok := p.(*FlixCloudProvider); ok {
			return fc
		}
	}
	return nil
}

func (m *Manager) tryFlixCloudWithSlug(ctx context.Context, animeID int, episode int, lang, quality, slug string) (*core.StreamResult, error) {
	fc := m.getFlixCloudProvider()
	if fc == nil {
		return nil, fmt.Errorf("flixcloud provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying flixcloud")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := fc.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("flixcloud failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	source = m.withDubSubtitles(ctx, "flixcloud", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

func (m *Manager) getAnikotoProvider() *AnikotoProvider {
	for _, p := range m.providers {
		if ak, ok := p.(*AnikotoProvider); ok {
			return ak
		}
	}
	return nil
}

func (m *Manager) tryAnikoto(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	ak := m.getAnikotoProvider()
	if ak == nil {
		return nil, fmt.Errorf("anikoto provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying anikoto")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := ak.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("anikoto failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	source = m.withDubSubtitles(ctx, "anikoto", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

func (m *Manager) getZokoProvider() *ZokoProvider {
	for _, p := range m.providers {
		if zk, ok := p.(*ZokoProvider); ok {
			return zk
		}
	}
	return nil
}

func (m *Manager) getNiNProvider() *NiNProvider {
	for _, p := range m.providers {
		if nn, ok := p.(*NiNProvider); ok {
			return nn
		}
	}
	return nil
}

func (m *Manager) getAnimeXProvider() *AnimeXProvider {
	for _, p := range m.providers {
		if ax, ok := p.(*AnimeXProvider); ok {
			return ax
		}
	}
	return nil
}

// tryAnimeX resolves an AnimeX stream (plyr API with XOR-decoded direct URLs).
func (m *Manager) tryAnimeX(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	ax := m.getAnimeXProvider()
	if ax == nil {
		return nil, fmt.Errorf("animex provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying animex")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := ax.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("animex failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	source = m.withDubSubtitles(ctx, "animex", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

// collectAnimeXServers maps AnimeX sources to servers. Every sub-provider
// the plyr page lists (Mochi/yuki, Chibi/neko, Kira/zuna, Lumi/beep, Anzu/loli,
// Sora, ...) that resolves becomes its own named server — first source wins
// if a sub-provider returns several. Results keep their per-provider
// Referer/UA headers (result-level), so they must map 1:1 onto servers.
func (m *Manager) collectAnimeXServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		ax, ok := prov.(*AnimeXProvider)
		if !ok {
			continue
		}
		results, err := ax.FindAllEpisodeSources(ctx, anilistID, episode, lang)
		if err != nil && len(results) == 0 {
			m.log.Warn().Err(err).Str("provider", "animex").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		for _, sr := range results {
			if sr == nil || len(sr.Sources) == 0 {
				continue // silent skip
			}
			name := sr.ServerName
			if name == "" {
				name = "Hana"
			}
			sr = m.withDubSubtitles(ctx, "animex", lang, anilistID, episode, sr)
			out = appendNamedServers(out, []string{name}, "animex", lang, sr)
		}
	}
	return out
}

// tryZoko resolves a ZokoAnime stream (frontend "zoko" -> backend ZokoAnime
// /stream/ani/{anilistId}/{ep}/{sub|dub}). The stream itself always comes
// from ZokoAnime, probe-verified + proxied — never Anikoto direct.
// Downloads are Zoko's own links only (Anikoto direct-only resolution
// carries no download links).
func (m *Manager) tryZoko(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	zk := m.getZokoProvider()
	if zk == nil {
		return nil, fmt.Errorf("zoko provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying zoko")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := zk.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("zoko failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	source = m.withDubSubtitles(ctx, "zoko", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

// tryNiN resolves a Supaplay stream (anistream embed lineup; provider "nin").
// Probe-verified inside the provider; hentai titles never reach it (the
// explicit switch rejects them — Supaplay 502s its API for hentai).
func (m *Manager) tryNiN(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	nn := m.getNiNProvider()
	if nn == nil {
		return nil, fmt.Errorf("nin provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying nin")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := nn.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("nin failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	source = m.withDubSubtitles(ctx, "nin", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

// tryKaa resolves a kaa.lt stream (krussdomi HLS master) for exactly the
// requested lang — strict per-lang: no dub server is listed when the en-US
// page has no players, even if sub resolves.
func (m *Manager) tryKaa(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	ka := m.getKaaProvider()
	if ka == nil {
		return nil, fmt.Errorf("kaa provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying kaa")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := ka.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("kaa failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	// Dub subtitle files: Sora (animex) dub uses the nico files
	// (withDubSubtitles); every other provider keeps its defaults.
	source = m.withDubSubtitles(ctx, "kaa", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

func (m *Manager) getKaaProvider() *KaaProvider {
	for _, p := range m.providers {
		if ka, ok := p.(*KaaProvider); ok {
			return ka
		}
	}
	return nil
}

// withDubSubtitles enforces the operator rule: Sora (animex) dub sources
// carry the Nico (kaa) subtitle files. Sora serves krussdomi streams, so
// Nico subs are timing-compatible; every other provider's dub keeps its
// default subtitles. kaa is fetched only for Sora dub requests; when kaa
// is unconfigured, has no match, or carries no subtitles, the dub result
// is kept as-is (best effort: subtitles must never fail playback). The
// input is never mutated: provider caches may share the pointed-to result.
func (m *Manager) withDubSubtitles(ctx context.Context, provider, lang, anilistID string, episode int, sr *SourceResult) *SourceResult {
	if !strings.EqualFold(lang, "dub") || sr == nil || len(sr.Sources) == 0 {
		return sr
	}
	if provider != "animex" || !strings.EqualFold(sr.ServerName, "sora") {
		return sr
	}
	ka := m.getKaaProvider()
	if ka == nil {
		return sr
	}
	ksub, err := ka.FindEpisodeSource(ctx, anilistID, episode, "sub")
	if err != nil || ksub == nil {
		m.log.Warn().Err(err).Str("provider", provider).Str("anilistId", anilistID).
			Int("episode", episode).Msg("dub subtitles: Nico resolve unavailable, keeping dub as-is")
		return sr
	}
	// Nico is kaa's first positional server; prefer the source actually
	// named Nico, else the first source carrying subtitle files.
	var nico []core.Subtitle
	for i := range ksub.Sources {
		if len(ksub.Sources[i].Subtitles) == 0 {
			continue
		}
		if len(nico) == 0 {
			nico = ksub.Sources[i].Subtitles
		}
		if i < len(ksub.ServerNames) && ksub.ServerNames[i] == "Nico" {
			nico = ksub.Sources[i].Subtitles
			break
		}
	}
	if len(nico) == 0 {
		return sr
	}
	out := *sr
	out.Sources = append([]core.Source(nil), sr.Sources...)
	changed := false
	for i := range out.Sources {
		if !sameSubtitleURLs(out.Sources[i].Subtitles, nico) {
			out.Sources[i].Subtitles = nico
			changed = true
		}
	}
	if !changed {
		return sr
	}
	return &out
}

// sameSubtitleURLs reports whether two subtitle lists carry the same files
// in order (tags may differ — only the file identity matters for the
// dub-uses-sub-files rule).
func sameSubtitleURLs(a, b []core.Subtitle) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].URL != b[i].URL {
			return false
		}
	}
	return true
}

// collectKaaServers maps kaa.lt sources to per-player servers (VidStreaming,
// ...). Each source already carries its player display name.
func (m *Manager) collectKaaServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		ka, ok := prov.(*KaaProvider)
		if !ok {
			continue
		}
		sr, err := ka.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "kaa").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue // the provider logs why (no episode match / unplayable)
		}
		sr = m.withDubSubtitles(ctx, "kaa", lang, anilistID, episode, sr)
		out = appendNamedServers(out, []string{"Kaa"}, "kaa", lang, sr)
	}
	return out
}

func (m *Manager) getAnimeGGProvider() *AnimeGGProvider {
	for _, p := range m.providers {
		if ag, ok := p.(*AnimeGGProvider); ok {
			return ag
		}
	}
	return nil
}

func (m *Manager) getAniWavesProvider() *AniWavesProvider {
	for _, p := range m.providers {
		if aw, ok := p.(*AniWavesProvider); ok {
			return aw
		}
	}
	return nil
}

// tryAnimeGG resolves an AnimeGG direct mp4 (highest quality per mirror;
// dub below 720p never resolves — the provider hides it).
func (m *Manager) tryAnimeGG(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	ag := m.getAnimeGGProvider()
	if ag == nil {
		return nil, fmt.Errorf("animegg provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying animegg")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := ag.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("animegg failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	// Dub keeps AnimeGG defaults (only Sora dub takes nico files).
	source = m.withDubSubtitles(ctx, "animegg", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

// tryAniWaves resolves an AniWaves direct stream (multi-rendition HLS
// masters first, highest savedly mp4 as spare).
func (m *Manager) tryAniWaves(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	aw := m.getAniWavesProvider()
	if aw == nil {
		return nil, fmt.Errorf("aniwaves provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying aniwaves")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := aw.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("aniwaves failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	// Dub keeps AniWaves defaults (only Sora dub takes nico files).
	source = m.withDubSubtitles(ctx, "aniwaves", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

// collectAnimeGGServers maps AnimeGG sources to Sunny/Yolky/Eggy servers.
// The provider names its own sources positionally; the fallback guards the
// append contract and is never hit in practice.
func (m *Manager) collectAnimeGGServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		ag, ok := prov.(*AnimeGGProvider)
		if !ok {
			continue
		}
		sr, err := ag.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "animegg").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue
		}
		sr = m.withDubSubtitles(ctx, "animegg", lang, anilistID, episode, sr)
		out = appendNamedServers(out, []string{"Sunny"}, "animegg", lang, sr)
	}
	return out
}

// collectAniWavesServers maps AniWaves sources to Nami/Coral/Pearl/... servers.
func (m *Manager) collectAniWavesServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		aw, ok := prov.(*AniWavesProvider)
		if !ok {
			continue
		}
		sr, err := aw.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "aniwaves").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue
		}
		sr = m.withDubSubtitles(ctx, "aniwaves", lang, anilistID, episode, sr)
		out = appendNamedServers(out, []string{"Nami"}, "aniwaves", lang, sr)
	}
	return out
}

func (m *Manager) getVidNestProvider() *VidNestProvider {
	for _, p := range m.providers {
		if vn, ok := p.(*VidNestProvider); ok {
			return vn
		}
	}
	return nil
}

// tryVidNest resolves a VidNest MegaPlay HLS master (fresh per call, subs
// and skip chapters included).
func (m *Manager) tryVidNest(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	vn := m.getVidNestProvider()
	if vn == nil {
		return nil, fmt.Errorf("vidnest provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying vidnest")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := vn.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("vidnest failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	// Dub keeps VidNest defaults (only Sora dub takes nico files).
	source = m.withDubSubtitles(ctx, "vidnest", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

// collectVidNestServers maps VidNest sources to the single "Nest" server.
func (m *Manager) collectVidNestServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		vn, ok := prov.(*VidNestProvider)
		if !ok {
			continue
		}
		sr, err := vn.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "vidnest").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue
		}
		sr = m.withDubSubtitles(ctx, "vidnest", lang, anilistID, episode, sr)
		out = appendNamedServers(out, []string{vidnestServerName}, "vidnest", lang, sr)
	}
	return out
}

func (m *Manager) getLeeProvider() *LeeProvider {
	for _, p := range m.providers {
		if le, ok := p.(*LeeProvider); ok {
			return le
		}
	}
	return nil
}

// tryLee resolves an ani.pm direct HLS master (fresh token chain per call).
func (m *Manager) tryLee(ctx context.Context, animeID int, episode int, lang, quality string) (*core.StreamResult, error) {
	le := m.getLeeProvider()
	if le == nil {
		return nil, fmt.Errorf("lee provider not configured")
	}

	m.log.Info().Int("animeId", animeID).Int("episode", episode).Str("lang", lang).Msg("trying lee")

	anilistID := fmt.Sprintf("%d", animeID)
	source, err := le.FindEpisodeSource(ctx, anilistID, episode, lang)
	if err != nil {
		return nil, fmt.Errorf("lee failed: %w", err)
	}
	if source == nil || len(source.Sources) == 0 {
		return nil, nil
	}
	// Dub keeps Lee defaults (only Sora dub takes nico files).
	source = m.withDubSubtitles(ctx, "lee", lang, anilistID, episode, source)

	return m.applyQualityFilter(source, quality), nil
}

// collectLeeServers maps Lee sources to the single "Lee" server.
func (m *Manager) collectLeeServers(ctx context.Context, anilistID string, episode int, lang string) []core.Server {
	var out []core.Server
	for _, prov := range m.providers {
		le, ok := prov.(*LeeProvider)
		if !ok {
			continue
		}
		sr, err := le.FindEpisodeSource(ctx, anilistID, episode, lang)
		if err != nil {
			m.log.Warn().Err(err).Str("provider", "lee").Str("anilistId", anilistID).
				Int("episode", episode).Str("lang", lang).Msg("servers: provider failed")
			continue
		}
		if sr == nil || len(sr.Sources) == 0 {
			continue
		}
		sr = m.withDubSubtitles(ctx, "lee", lang, anilistID, episode, sr)
		out = appendNamedServers(out, []string{leeServerName}, "lee", lang, sr)
	}
	return out
}

func (m *Manager) applyQualityFilter(result *SourceResult, quality string) *core.StreamResult {
	qualities := sourceQualities(result.Sources)
	sources := result.Sources
	if quality != "auto" && quality != "" {
		filtered := filterByQuality(sources, quality)
		if len(filtered) > 0 {
			sources = filtered
		}
	}

	return &core.StreamResult{
		Sources:   sources,
		Headers:   result.Headers,
		Qualities: qualities,
		Downloads: result.Downloads,
		Intro:     result.Intro,
		Outro:     result.Outro,
	}
}

// sourceQualities reports only provider-returned labels. Clients must never
// invent adaptive renditions: an Auto HLS source without explicit variants
// remains Auto because the native Expo player does not expose a writable level
// selector.
func sourceQualities(sources []core.Source) []string {
	seen := make(map[string]bool)
	qualities := make([]string, 0, len(sources))
	for _, source := range sources {
		quality := strings.TrimSpace(source.Quality)
		if quality == "" || seen[strings.ToLower(quality)] {
			continue
		}
		seen[strings.ToLower(quality)] = true
		qualities = append(qualities, quality)
	}
	return qualities
}

func filterByQuality(sources []core.Source, quality string) []core.Source {
	var filtered []core.Source
	q := strings.ToLower(quality)
	for _, s := range sources {
		if strings.Contains(strings.ToLower(s.Quality), q) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
