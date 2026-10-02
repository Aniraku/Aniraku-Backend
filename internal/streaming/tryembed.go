package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// TryEmbedProvider resolves direct streams through tryembed.us.cc
// (ticket page + bootstrap nonce + per-server stream_data + signed /s/
// file URLs). Verified live 2026-10-02 from the production egress.
//
// PLAYER FLOW (replicated 1:1 from the watch page + player bundle):
//  1. GET /embed/anime/{id}/{ep}/{lang} -> BOOTSTRAP_TICKET + session
//     cookies (tryembed_auth, tryembed_session_*).
//  2. POST /api/bootstrap (Origin, Referer, X-TryEmbed-Bootstrap,
//     Sec-Fetch-* headers, session cookies) -> embedNonce.
//  3. GET /api/stream_data?id=&episode=&audio=&player=jw&server=&nonce=
//     (Referer, X-Embed-Nonce, cookies) -> provider block with quality
//     tokens. Repeat per mirror.
//  4. GET /s/{token}.m3u8|.mp4 (cookies) -> 302 to the signed file URL
//     (god.anixx.cloud proxy with exp/sig, or direct mp4).
//
// MIRRORS (operator cute names): astra->Astro (HLS), beta->Beta (HLS),
// sora->Skye (mp4), zen->Zen (title-dependent; skipped when the API does
// not offer it). "Sora" is deliberately NOT used: animex already owns a
// Sora server with nico-subtitle rules that must never touch these files.
//
// FRESHNESS (operator): no resolve cache — signed URLs expire (~4h), so
// every lookup mints a new ticket/session/nonce/token chain.
//
// EGRESS (observed 2026-10-02): /api/stream_data serves Cloudflare 403 to
// datacenter egress (page + bootstrap pass, residential works). A 403 aborts
// the resolve into a silent skip — no per-mirror retry storm, no log spam —
// and the provider lights up on its own if the block lifts.
const (
	tryembedDefaultBase = "https://tryembed.us.cc"
)

var tryembedServers = []struct {
	id   string
	name string
}{
	{"astra", "Astro"},
	{"beta", "Beta"},
	{"sora", "Skye"},
	{"zen", "Zen"},
}

func tryembedCuteName(id string) string {
	for _, s := range tryembedServers {
		if s.id == id {
			return s.name
		}
	}
	return ""
}

type TryEmbedProvider struct {
	log       zerolog.Logger
	transport http.RoundTripper
	base      string
	relay     *RelayClient
	learnHost func(host string)
}

func NewTryEmbedProvider(log zerolog.Logger, base string) *TryEmbedProvider {
	if strings.TrimSpace(base) == "" {
		base = tryembedDefaultBase
	}
	return &TryEmbedProvider{
		log:       log,
		transport: netguard.NewTransport(),
		base:      strings.TrimRight(base, "/"),
		relay:     NewRelayClient(),
	}
}

func (p *TryEmbedProvider) Name() string { return "tryembed" }

func (p *TryEmbedProvider) SetHostLearner(fn func(host string)) { p.learnHost = fn }

func (p *TryEmbedProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func (p *TryEmbedProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("tryembed search not implemented")
}

func (p *TryEmbedProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("tryembed episode listing not implemented")
}

var tryembedTicketRe = regexp.MustCompile(`window\.BOOTSTRAP_TICKET="([^"]+)"`)

// tryembedSession holds one ticket/bootstrap/nonce chain: page cookies in
// the jar, Sec-Fetch headers on every call (the API 403s "Invalid Request
// Signature" without them).
type tryembedSession struct {
	client *http.Client
	base   string
	nonce  string
}

func (p *TryEmbedProvider) newSession() (*tryembedSession, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	return &tryembedSession{
		client: &http.Client{Timeout: 45 * time.Second, Transport: p.transport, Jar: jar},
		base:   p.base,
	}, nil
}

// timeoutCtx bounds one upstream hop far below the client timeout so a
// tarpitted block page can never eat the fan-out budget.
func timeoutCtx(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

func (s *tryembedSession) setFetchHeaders(req *http.Request) {
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
}

func (s *tryembedSession) getText(ctx context.Context, rawURL, referer string, limit int64) (string, error) {
	actx, cancel := timeoutCtx(ctx, 12*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	s.setFetchHeaders(req)
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tryembed: GET %s -> HTTP %d", rawURL, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (s *tryembedSession) getJSON(ctx context.Context, rawURL, referer, nonce string, out any) error {
	actx, cancel := timeoutCtx(ctx, 12*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	s.setFetchHeaders(req)
	if referer != "" {
		req.Header.Set("Referer", referer)
	}
	if nonce != "" {
		req.Header.Set("X-Embed-Nonce", nonce)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("tryembed: GET %s -> HTTP %d", rawURL, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

// bootstrap runs the ticket exchange: watch page -> ticket -> nonce.
func (p *TryEmbedProvider) bootstrap(ctx context.Context, s *tryembedSession, id, episode int, lang string) error {
	pageURL := fmt.Sprintf("%s/embed/anime/%d/%d/%s", s.base, id, episode, lang)
	page, err := s.getText(ctx, pageURL, s.base+"/", 1<<20)
	if err != nil {
		return err
	}
	m := tryembedTicketRe.FindStringSubmatch(page)
	if m == nil {
		return fmt.Errorf("tryembed: no bootstrap ticket on watch page")
	}
	rawURL := s.base + "/api/bootstrap"
	var lastErr error
	for attempt := 1; attempt <= 2; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
		actx, cancel := timeoutCtx(ctx, 12*time.Second)
		req, err := http.NewRequestWithContext(actx, http.MethodPost, rawURL, bytes.NewReader(nil))
		if err != nil {
			cancel()
			return err
		}
		s.setFetchHeaders(req)
		req.Header.Set("Origin", s.base)
		req.Header.Set("Referer", pageURL)
		req.Header.Set("X-TryEmbed-Bootstrap", m[1])
		resp, err := s.client.Do(req)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		status := resp.StatusCode
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("tryembed: bootstrap HTTP %d", status)
			continue
		}
		var out struct {
			EmbedNonce string `json:"embedNonce"`
		}
		if err := json.Unmarshal(body, &out); err != nil || out.EmbedNonce == "" {
			lastErr = fmt.Errorf("tryembed: bootstrap gave no nonce")
			continue
		}
		s.nonce = out.EmbedNonce
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("tryembed: bootstrap failed")
	}
	return lastErr
}

type tryembedQuality struct {
	Name          string `json:"name"`
	Height        *int   `json:"height"`
	Token         string `json:"token"`
	FallbackToken string `json:"fallbackToken"`
	DirectURL     string `json:"directUrl"`
	JwDirectURL   string `json:"jwDirectUrl"`
}

type tryembedCaption struct {
	Label  string `json:"label"`
	Lang   string `json:"lang"`
	URL    string `json:"url"`
	Format string `json:"format"`
}

type tryembedProviderBlock struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Status    string            `json:"status"`
	Qualities []tryembedQuality `json:"qualities"`
	Captions  []tryembedCaption `json:"captions"`
}

type tryembedStreamData struct {
	Providers []tryembedProviderBlock `json:"providers"`
	Intro     *core.SkipTimestamp     `json:"intro"`
	Outro     *core.SkipTimestamp     `json:"outro"`
}

// pickQuality prefers the Auto track, else the tallest rendition, else the
// first tokenized track — always the single best file per mirror.
func tryembedPickQuality(qs []tryembedQuality) *tryembedQuality {
	var best *tryembedQuality
	bestHeight := -1
	for i := range qs {
		if strings.TrimSpace(qs[i].Token) == "" {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(qs[i].Name), "auto") {
			return &qs[i]
		}
		h := 0
		if qs[i].Height != nil {
			h = *qs[i].Height
		}
		if best == nil || h > bestHeight {
			best, bestHeight = &qs[i], h
		}
	}
	return best
}

// resolveMirror fetches stream_data for one mirror and follows its signed
// file URL to the final playable target. A 403 from stream_data means the
// egress is gated (not a missing mirror) and is returned as an error so the
// caller can abort the whole resolve silently instead of retry-storming
// every mirror; all other failures yield empty results (try next mirror).
func (p *TryEmbedProvider) resolveMirror(ctx context.Context, s *tryembedSession, id, episode int, lang, mirror, pageURL string) (file, ext string, block *tryembedProviderBlock, intro, outro *core.SkipTimestamp, err error) {
	q := url.Values{
		"id":      {strconv.Itoa(id)},
		"episode": {strconv.Itoa(episode)},
		"audio":   {lang},
		"player":  {"jw"},
		"server":  {mirror},
		"nonce":   {s.nonce},
	}
	var data tryembedStreamData
	if err := s.getJSON(ctx, s.base+"/api/stream_data?"+q.Encode(), pageURL, s.nonce, &data); err != nil {
		return "", "", nil, nil, nil, err
	}
	var found *tryembedProviderBlock
	for i := range data.Providers {
		if data.Providers[i].ID == mirror {
			found = &data.Providers[i]
			break
		}
	}
	if found == nil {
		return "", "", nil, nil, nil, nil
	}
	pick := tryembedPickQuality(found.Qualities)
	if pick == nil {
		return "", "", nil, nil, nil, nil
	}
	fileExt := "m3u8"
	if strings.EqualFold(strings.TrimSpace(found.Type), "mp4") {
		fileExt = "mp4"
	}
	fileURL := fmt.Sprintf("%s/s/%s.%s", s.base, pick.Token, fileExt)
	actx, cancel := timeoutCtx(ctx, 12*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodGet, fileURL, nil)
	if err != nil {
		return "", "", nil, nil, nil, nil
	}
	s.setFetchHeaders(req)
	req.Header.Set("Referer", pageURL)
	resp, err := s.client.Do(req)
	if err != nil {
		return "", "", nil, nil, nil, nil
	}
	defer resp.Body.Close()
	final := fileURL
	if resp.Request != nil && resp.Request.URL != nil {
		final = resp.Request.URL.String()
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return "", "", nil, nil, nil, nil
	}
	// Drain a small head to confirm media bytes (302 targets serve the
	// playlist/file directly here).
	head, err := io.ReadAll(io.LimitReader(resp.Body, 32768))
	if err != nil || len(head) == 0 {
		return "", "", nil, nil, nil, nil
	}
	lower := strings.ToLower(string(head))
	if strings.Contains(lower, "<html") {
		return "", "", nil, nil, nil, nil
	}
	if fileExt == "m3u8" && !strings.Contains(string(head), "#EXTM3U") {
		return "", "", nil, nil, nil, nil
	}
	return final, fileExt, found, data.Intro, data.Outro, nil
}

// FindEpisodeSource resolves one episode for exactly the requested lang.
// Fresh ticket/session/nonce chain per call (operator rule): signed file
// URLs expire, so nothing is cached — every lookup mints new URLs.
func (p *TryEmbedProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	id, err := strconv.Atoi(strings.TrimSpace(anilistID))
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("tryembed: bad anilist id %q", anilistID)
	}
	langKey := "sub"
	if strings.EqualFold(lang, "dub") {
		langKey = "dub"
	}
	pageURL := fmt.Sprintf("%s/embed/anime/%d/%d/%s", p.base, id, episode, langKey)
	// Relay first when configured: the relay mints the mirrors (direct
	// stream_data is egress-gated); verification still runs here.
	if p.relay != nil {
		return p.findViaRelay(ctx, id, episode, langKey, pageURL)
	}
	sess, err := p.newSession()
	if err != nil {
		return nil, err
	}
	if err := p.bootstrap(ctx, sess, id, episode, langKey); err != nil {
		return nil, err
	}
	// One result per mirror that yields a verified file; mirrors the API
	// does not offer for the title are skipped silently.
	probeClient := &http.Client{Timeout: 45 * time.Second, Transport: p.transport}
	var intro, outro *core.SkipTimestamp
	sr := &SourceResult{Headers: map[string]string{"Referer": sess.base + "/"}}
	for _, m := range tryembedServers {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		file, ext, block, in, out, merr := p.resolveMirror(ctx, sess, id, episode, langKey, m.id, pageURL)
		if merr != nil {
			if isUpstreamGated(merr) {
				// Egress gated, not a missing mirror: abort silently.
				return nil, nil
			}
			continue
		}
		if file == "" || block == nil {
			continue
		}
		if intro == nil {
			intro = in
		}
		if outro == nil {
			outro = out
		}
		src, ok := p.buildSource(ctx, probeClient, pageURL, m.id, ext, file, block.Captions)
		if !ok {
			continue
		}
		sr.Sources = append(sr.Sources, *src)
		sr.ServerNames = append(sr.ServerNames, m.name)
	}
	if len(sr.Sources) == 0 {
		return nil, fmt.Errorf("tryembed: no playable mirror for episode %d", episode)
	}
	sr.Intro, sr.Outro = intro, outro
	if len(sr.ServerNames) > 0 {
		sr.ServerName = sr.ServerNames[0]
	}
	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", langKey).Int("mirrors", len(sr.Sources)).Msg("tryembed resolved")
	return sr, nil
}

// buildSource verifies one mirror file from this egress and assembles its
// server source (shared by the direct and relayed resolve paths).
func (p *TryEmbedProvider) buildSource(ctx context.Context, probeClient *http.Client, pageURL, mirror, ext, file string, caps []tryembedCaption) (*core.Source, bool) {
	typ := "hls"
	if ext == "mp4" {
		typ = "mp4"
	}
	var ok bool
	if typ == "hls" {
		ok = probePlaylistsLenient(ctx, probeClient, file, pageURL, browserUA)
	} else {
		ok = probeMediaFileLenient(ctx, probeClient, file, pageURL, browserUA)
	}
	if !ok {
		p.log.Info().Str("mirror", mirror).Msg("tryembed: probe failed, trying next mirror")
		return nil, false
	}
	p.learnURLHost(file)
	var subs []core.Subtitle
	for _, c := range caps {
		if strings.TrimSpace(c.URL) == "" {
			continue
		}
		p.learnURLHost(c.URL)
		code := strings.TrimSpace(c.Lang)
		if code == "" {
			code = mapSubtitleLang(c.Label)
		}
		label := strings.TrimSpace(c.Label)
		if label == "" {
			label = code
		}
		subs = append(subs, core.Subtitle{URL: c.URL, Lang: code, Label: label})
	}
	return &core.Source{
		URL:          file,
		Type:         typ,
		Quality:      "auto",
		Subtitles:    subs,
		Verification: "proxy",
	}, true
}

// findViaRelay resolves through the operator relay (direct stream_data is
// egress-gated). Minting happens relay-side; verification still runs here
// from this egress before anything lists.
func (p *TryEmbedProvider) findViaRelay(ctx context.Context, id, episode int, lang, pageURL string) (*SourceResult, error) {
	mirrors, intro, outro, err := p.relay.TryEmbedResolve(ctx, id, episode, lang)
	if err != nil {
		return nil, err
	}
	probeClient := &http.Client{Timeout: 45 * time.Second, Transport: p.transport}
	sr := &SourceResult{
		Headers: map[string]string{"Referer": p.base + "/"},
		Intro:   intro,
		Outro:   outro,
	}
	for _, m := range mirrors {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		name := tryembedCuteName(m.Server)
		if name == "" || strings.TrimSpace(m.URL) == "" {
			continue
		}
		src, ok := p.buildSource(ctx, probeClient, pageURL, m.Server, m.Type, m.URL, m.Captions)
		if !ok {
			continue
		}
		sr.Sources = append(sr.Sources, *src)
		sr.ServerNames = append(sr.ServerNames, name)
	}
	if len(sr.Sources) == 0 {
		return nil, nil
	}
	if len(sr.ServerNames) > 0 {
		sr.ServerName = sr.ServerNames[0]
	}
	p.log.Info().Int("animeId", id).Int("episode", episode).
		Str("lang", lang).Int("mirrors", len(sr.Sources)).Msg("tryembed resolved via relay")
	return sr, nil
}
