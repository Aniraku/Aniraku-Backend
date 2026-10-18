package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// AnV (anv.to) resolves direct HLS through the site's own first-party edge.
// Verified end-to-end from VPS egress 2026-10-18:
//
//	GET  /anime/<slug>            -> Set-Cookie shiro_watch (the session;
//	                                 HttpOnly, months-long validity)
//	POST /api/episode             -> {"status":"ready","variants":[...]} keyed by
//	                                 {anilistId, episode} — malId is optional
//	                                 (measured: null malId resolves fine)
//	GET  <stream-path>            -> master m3u8, session cookie REQUIRED
//	                                 (403 without the jar, 200 with it)
//
// variants[0] = sub, variants[1] = dub; each carries three labeled sources
// (the site labels them Orange / Pear / Banana — mapped here to Aniraku's
// fruit theme, Mikan/Nashi/Bana, keeping each <provider-family> name theme:
// eggs for AnimeGG, sea words for AniWaves, pets for AnimeX, sky words for
// Tensho). Pear and Banana point at the same tokenized edge path and stay
// separate servers for honesty, exactly like the site itself lists them.
// The stream path is
// relative to anv.to and every child URL under /stream/ needs the same
// session cookie, so playback MUST go through the media proxy with the
// cookie replayed from SourceResult.Headers — that is why probes run with
// the cookie-jarred client (identical to the playback shape) and the
// verification verdict is "proxy".
//
// The AniList-keyed API makes show matching unnecessary: no title search,
// no episode-list fetch — one session mint (the cookie persists in the
// provider's jar across calls) and one POST per /servers call.
const (
	anvBase       = "https://anv.to"
	anvCookieName = "shiro_watch"
)

// anvServerNames names the source slots in order (Aniraku-style fruit
// theme: mikan = mandarin orange, nashi = Japanese pear, bana = banana —
// the site's own Orange/Pear/Banana slots).
var anvServerNames = [...]string{"Mikan", "Nashi", "Bana"}

// anvEpisodeHrefRe finds the first episode/play detail link on the home
// page — the mint target (only /anime/ pages set the session cookie).
var anvEpisodeHrefRe = regexp.MustCompile(`href="(/anime/[^"']{2,80})"`)

type AnvProvider struct {
	log    zerolog.Logger
	client *http.Client // fixed cookie-jarred client; jar churn is done by name inside anvJar
}

// anvJar is a tiny cookie jar whose cookies can be dropped by name without
// replacing the whole client (probes and resolve calls share the client
// concurrently; swapping *http.Client under load would race).
type anvJar struct {
	mu   sync.Mutex
	jar  http.CookieJar
	urls map[string][]*http.Cookie // url string -> cookies
}

// newAnvJar starts with an empty in-memory cookie store.
func newAnvJar() *anvJar {
	return &anvJar{urls: map[string][]*http.Cookie{}}
}

func (j *anvJar) SetCookies(u *url.URL, cookies []*http.Cookie) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.urls[u.Host] = cookies
}

func (j *anvJar) Cookies(u *url.URL) []*http.Cookie {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.urls[u.Host]
}

// clear drops every stored cookie for the host (forces re-mint on retry).
func (j *anvJar) clear(host string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.urls, host)
}

// sessionCookie returns the stored shiro_watch value plus its expiry, ok=false
// when missing.
func (j *anvJar) sessionCookie(host string) (string, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	now := time.Now()
	for _, c := range j.urls[host] {
		if c.Name != anvCookieName {
			continue
		}
		if !c.Expires.IsZero() && !c.Expires.After(now) {
			return "", false // expired session
		}
		return c.Value, true
	}
	return "", false
}

func NewAnvProvider(log zerolog.Logger) *AnvProvider {
	return &AnvProvider{
		log:    log,
		client: &http.Client{Timeout: 45 * time.Second, Transport: netguard.NewTransport(), Jar: newAnvJar()},
	}
}

func (p *AnvProvider) Name() string { return "anv" }

// Search: AniList-keyed provider; catalog search is not part of playback.
func (p *AnvProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, nil
}

// FindEpisodes: the same logic — episode lists come from AniList upstream.
func (p *AnvProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, nil
}

// ensureSession mints a shiro_watch cookie when the jar has none. Only
// /anime/<slug> pages mint it (measured: /browse, /online and /recents set
// nothing — the watch-page routing is where the mint happens), so the flow
// fetches the home page, takes its first live /anime/ href and GETs that
// detail page. That self-heals: whichever episode currently airs always
// exists, no hardcoded slug to rot. The mint costs an extra roundtrip only
// on the cold path (no cookie yet, e.g. after a restart); warm calls skip
// it entirely.
func (p *AnvProvider) ensureSession(ctx context.Context) error {
	if _, ok := p.jar().sessionCookie(anvBaseHost); ok {
		return nil
	}

	anvPage := func(rawURL string) (string, bool) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return "", false
		}
		req.Header.Set("User-Agent", browserUA)
		resp, err := p.client.Do(req)
		if err != nil {
			return "", false
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
		return string(body), err == nil
	}

	home, ok := anvPage(anvBase + "/")
	if ok {
		if m := anvEpisodeHrefRe.FindStringSubmatch(home); len(m) > 1 {
			if detail, ok := anvPage(anvBase + m[1]); ok && len(detail) > 1024 {
				if _, has := p.jar().sessionCookie(anvBaseHost); has {
					return nil
				}
			}
		}
	}
	return fmt.Errorf("anv: could not mint %s session cookie", anvCookieName)
}

// retrySessionOn403 invalidates the stored session and mints a fresh one.
func (p *AnvProvider) retrySessionOn403() {
	p.jar().clear(anvBaseHost)
	_ = p.ensureSession(context.Background())
}

// anvBaseHost is the cookie-jar key (host part of anvBase).
const anvBaseHost = "anv.to"

func (p *AnvProvider) jar() *anvJar {
	j, _ := p.client.Jar.(*anvJar)
	return j
}

// anvEpisodeResponse is /api/episode's shape.
type anvEpisodeResponse struct {
	Status   string       `json:"status"`
	Variants []anvVariant `json:"variants"`
}

type anvVariant struct {
	ID      string   `json:"id"` // "sub" | "dub"
	Label   string   `json:"label"`
	Sources []anvSrc `json:"sources"`
}

type anvSrc struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	URL   string `json:"url"`
	Type  string `json:"type"`
	// Tracks are the platform's soft-subtitle files (WEBVTT under the same
	// session-gated /stream/ paths): English by default plus whatever
	// locales the episode carries (pt-BR, es, es-ES observed). The same
	// tokens repeat across every source of a variant — shared subtitle
	// pool, per-episode.
	Tracks []anvTrack `json:"tracks,omitempty"`
}

type anvTrack struct {
	ID       string `json:"id"`
	Src      string `json:"src"`
	Type     string `json:"type"` // "vtt"
	Label    string `json:"label"`
	Language string `json:"language"`
	Default  bool   `json:"default"`
}

func (p *AnvProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	if err := p.ensureSession(ctx); err != nil {
		return nil, err
	}

	anilistIDInt, _ := strconv.Atoi(anilistID)
	body, _ := json.Marshal(map[string]any{"anilistId": anilistIDInt, "episode": episode})
	sig := anvBase + "/api/episode"
	var data anvEpisodeResponse
	postErr := p.postJSON(ctx, sig, body, &data)
	if postErr != nil && strings.Contains(strings.ToLower(postErr.Error()), "status 403") {
		// Session gone upstream: remint once and retry.
		p.retrySessionOn403()
		data = anvEpisodeResponse{}
		postErr = p.postJSON(ctx, sig, body, &data)
	}
	if postErr != nil {
		return nil, postErr
	}
	if data.Status != "ready" {
		return nil, fmt.Errorf("anv: episode status %q", data.Status)
	}

	var chosen *anvVariant
	for i := range data.Variants {
		if strings.EqualFold(data.Variants[i].ID, langKey) {
			chosen = &data.Variants[i]
			break
		}
	}
	if chosen == nil {
		// Logical miss (no runtime listing yet) — mirrors tensho's
		// "not in lang" path and never pauses anything.
		p.log.Info().Str("anilistId", anilistID).Int("episode", episode).Str("lang", langKey).
			Msg("anv: no variant for lang")
		return nil, nil
	}

	// Sources resolve in parallel (each opens a master + one segment probe).
	type srcHit struct {
		url string
		ok  bool
	}
	hits := make([]srcHit, len(chosen.Sources))
	var sWg sync.WaitGroup
	sSem := make(chan struct{}, 4)
	for i, s := range chosen.Sources {
		if ctx.Err() != nil {
			break
		}
		sWg.Add(1)
		go func(i int, s anvSrc) {
			defer sWg.Done()
			select {
			case sSem <- struct{}{}:
				defer func() { <-sSem }()
			case <-ctx.Done():
				return
			}
			sCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			streamURL := anvAbs(s.URL)
			if streamURL == "" {
				return
			}
			// Probe in playback shape: the same jarred client the resolve
			// used, no Referer needed (children carry the cookie via the
			// proxy replay, not via referer).
			if !probeSegmentsLenient(sCtx, p.client, streamURL, anvBase+"/", browserUA) {
				p.log.Info().Str("source", s.Label).Str("url", streamURL).
					Msg("anv: stream blocked from this egress, dropping source")
				return
			}
			hits[i] = srcHit{url: streamURL, ok: true}
		}(i, s)
	}
	sWg.Wait()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	sources := make([]core.Source, 0, len(hits))
	names := make([]string, 0, len(hits))
	cookieVal, ok := p.jar().sessionCookie(anvBaseHost)
	if !ok {
		return nil, fmt.Errorf("anv: session vanished before shipping")
	}
	for i := range chosen.Sources {
		if !hits[i].ok {
			continue
		}
		s := chosen.Sources[i]
		// Soft subtitles: every /stream/ VTT lives under the same session
		// cookie; the media proxy client-side wrap replays Server.Headers
		// on the raw URLs (same rule tensho's flixera tracks follow).
		subs := make([]core.Subtitle, 0, len(s.Tracks))
		for _, tr := range s.Tracks {
			subURL := anvAbs(tr.Src)
			if subURL == "" || (tr.Type != "" && !strings.EqualFold(tr.Type, "vtt")) {
				continue
			}
			subs = append(subs, buildSubtitle(subURL, tr.Language, tr.Label, ""))
		}
		sources = append(sources, core.Source{
			URL:          hits[i].url,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		})
		// Slot-positional Aniraku names (Mikan/Nashi/Bana = the site's
		// Orange/Pear/Banana): a dropped source keeps its own name.
		names = append(names, anvServerNames[i%len(anvServerNames)])
	}
	if len(sources) == 0 {
		p.log.Info().Str("anilistId", anilistID).Int("episode", episode).Str("lang", langKey).
			Int("tried", len(chosen.Sources)).Msg("anv: no resolvable stream")
		return nil, nil
	}

	p.log.Info().Str("anilistId", anilistID).Int("episode", episode).Str("lang", langKey).
		Int("streams", len(sources)).Msg("anv: resolved")
	return &SourceResult{
		Sources: sources,
		Headers: map[string]string{
			// The stream playlist and every segment under /stream/ need the
			// session cookie; the media proxy replays these headers on the
			// master and on every rewritten child alike.
			"Cookie":     anvCookieName + "=" + cookieVal,
			"Referer":    anvBase + "/",
			"User-Agent": browserUA,
		},
		ServerName:  names[0],
		ServerNames: names,
	}, nil
}

// postJSON posts a JSON body and decodes the response with the site's
// session cookie attached by the jar; status handling is surfaced as an
// error string containing "status NNN" so the 403 remint path can trigger.
func (p *AnvProvider) postJSON(ctx context.Context, rawURL string, body []byte, out *anvEpisodeResponse) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Origin", anvBase)
	req.Header.Set("Referer", anvBase+"/")
	req.Header.Set("User-Agent", browserUA)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("anv: api/episode returned status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// anvAbs absolutizes the relative /stream/ paths the API returns.
func anvAbs(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	if !strings.HasPrefix(path, "/") {
		return anvBase + "/" + path
	}
	return anvBase + path
}
