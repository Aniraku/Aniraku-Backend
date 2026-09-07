package streaming

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
)

const (
	anikotoBase             = "https://anikototv.to"
	anikotoEmbedURLTemplate = "https://anivexa-api-tu4a.onrender.com/watch/anikoto/%s/%s/anikoto-%d"
)

// anikotoServers maps API server names to display names.
// The first server from the API becomes Niko, second becomes Momo.
var anikotoServers = [2]string{"Niko", "Momo"}

// AnikotoProvider scrapes anikototv.to for streaming sources.
// Two servers per episode: Niko (1st) and Momo (2nd).
// Supports sub and dub via separate data-ids per language.
type AnikotoProvider struct {
	client *http.Client
	log    zerolog.Logger
	// learnHost, when set, is called with hosts this provider itself verified:
	// probed stream manifests, subtitle tracks, and download links. The HTTP
	// layer registers it to feed the media-proxy CDN allowlist so rotated CDN
	// hostnames are allowed the moment they surface instead of 403ing.
	learnHost func(host string)
}

// SetHostLearner registers the verified-host callback.
func (p *AnikotoProvider) SetHostLearner(fn func(host string)) {
	p.learnHost = fn
}

// learnURLHost parses raw and, if it is a usable http(s) URL, hands its host
// to the registered learner.
func (p *AnikotoProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" ||
		(u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func NewAnikotoProvider(log zerolog.Logger) *AnikotoProvider {
	// Session cookie jar is REQUIRED: ajax/server?get= rejects cookieless
	// requests. All chain calls share this client, hence one session. The
	// transport is SSRF-guarded: upstream-controlled embed URLs cannot steer
	// this server at private addresses.
	jar, _ := cookiejar.New(nil)
	return &AnikotoProvider{
		client: &http.Client{Timeout: 15 * time.Second, Jar: jar, Transport: netguard.NewTransport()},
		log:    log,
	}
}

func (p *AnikotoProvider) Name() string { return "anikoto" }

func (p *AnikotoProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("anikoto search not implemented")
}

func (p *AnikotoProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("anikoto episode listing not implemented")
}

// FindEpisodeSource resolves AnikotoTV streams directly (Anivexa anikototv
// method, in-process — no Render hop): show resolve -> episode data-ids ->
// server list -> server?get embed -> MegaPlay decrypt -> verified m3u8.
// Same return contract as before (Quality "auto", Verification "proxy").
func (p *AnikotoProvider) FindEpisodeSource(ctx context.Context, providerID string, episode int, lang string) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}

	slug, showID, err := p.resolveShow(ctx, providerID)
	if err != nil {
		p.log.Info().Err(err).Str("anilistId", providerID).Msg("anikoto: resolveShow failed, trying megaplayDirect")
		// Megaplay is directly AniList-keyed — resolve without anikoto's
		// show catalog when the show itself cannot be matched.
		if mp, mpErr := p.megaplayDirect(ctx, providerID, episode, lang); mpErr == nil {
			return mp, nil
		} else {
			p.log.Info().Err(mpErr).Str("anilistId", providerID).Msg("anikoto: megaplayDirect also failed")
		}
		return nil, err
	}
	_ = slug

	dataIDs, epMeta, err := p.fetchEpisodeDataIDs(ctx, showID, episode)
	if err != nil {
		return nil, err
	}
	entries, err := p.fetchServers(ctx, dataIDs, "")
	if err != nil {
		return nil, err
	}
	// Nekostream mapper extras (Anivexa parity): extra servers + downloads.
	entries = append(entries, p.fetchMapperServers(ctx, epMeta, lang)...)

	var sources []core.Source
	var variants []string
	var downloads []core.DownloadLink
	var intro, outro *core.SkipTimestamp
	referer := ""
	seenName := map[string]bool{}
	seenDL := map[string]bool{}

	for _, e := range entries {
		// Anivexa parity: dedupe by server NAME, not file URL — the
		// same file behind two servers (Vidstream/HD) lists twice,
		// and ?s=tcdn variants may resolve differently per fetch.
		if seenName[e.name] {
			continue
		}
		lowerName := strings.ToLower(e.name)
		isDL := e.serverType == "dl" || strings.Contains(lowerName, "download") ||
			strings.Contains(lowerName, "kiwi")
		var embedURL string
		var skip map[string][]float64
		if strings.HasPrefix(e.linkID, "http") {
			// Mapper-provided direct embed URL (Anivexa parity).
			embedURL = e.linkID
		} else {
			var err error
			embedURL, skip, err = p.fetchVideoURL(ctx, e.linkID)
			if err != nil || strings.TrimSpace(embedURL) == "" {
				p.log.Debug().Err(err).Str("server", e.name).Str("linkId", e.linkID).Msg("anikoto: embed url fetch failed")
				continue
			}
		}
		if isDL {
			if !seenDL[embedURL] {
				seenDL[embedURL] = true
				label := strings.TrimSpace(e.name)
				if label == "" {
					label = "Download"
				}
				downloads = append(downloads, core.DownloadLink{URL: embedURL, Label: label})
				// Vouch the download host for the proxy allowlist: the URL
				// came from the provider's own server/mapper chain.
				p.learnURLHost(embedURL)
			}
			continue
		}
		if e.serverType != lang {
			p.log.Debug().Str("server", e.name).Str("serverType", e.serverType).Str("want", lang).Msg("anikoto: server language mismatch")
			continue
		}
		// Anivexa extractEmbedSource parity, hardened: try the #aHR0c base64
		// fragment first, then the MegaPlay decrypt chain (getSourcesNew,
		// then legacy getSources + AES enc decrypt). When decryption yields a
		// file we serve direct HLS; when it does not, the embed URL itself
		// ships as a type:"embed" stream (Anivexa priority-4 fallback) so the
		// client's embedded player can still play it — a source is ALWAYS
		// returned for the episode, never dropped.
		hlsURL := ""
		var tracks []megaplayTrack
		var inTs, outTs *core.SkipTimestamp
		origin := embedURL
		if i := strings.Index(embedURL, "#aHR0c"); i != -1 {
			if raw, e := base64.StdEncoding.DecodeString(embedURL[i+1:]); e == nil {
				if s := strings.TrimSpace(string(raw)); strings.Contains(s, ".m3u8") {
					hlsURL = s
				}
			}
		}
		if hlsURL == "" {
			file, tr, in, out, orig, rErr := p.resolveEmbed(ctx, embedURL)
			if rErr == nil && file != "" {
				hlsURL = file
				tracks = tr
				inTs, outTs = in, out
				origin = orig
			} else {
				p.log.Debug().Err(rErr).Str("server", e.name).Str("embed", embedURL).Msg("anikoto: embed decrypt failed, serving embed url")
				if o, e2 := url.Parse(embedURL); e2 == nil && o.Host != "" {
					origin = o.Scheme + "://" + o.Host
				}
			}
		}
		if strings.HasPrefix(embedURL, "http") {
			if o, e := url.Parse(embedURL); e == nil && o.Host != "" {
				if origin == embedURL {
					origin = o.Scheme + "://" + o.Host
				}
			}
		}
		seenName[e.name] = true
		if referer == "" {
			referer = strings.TrimSuffix(origin, "/") + "/"
		}
		if intro == nil {
			if inTs != nil {
				intro = &core.SkipTimestamp{Start: inTs.Start, End: inTs.End}
			} else if len(skip["intro"]) == 2 {
				intro = &core.SkipTimestamp{Start: skip["intro"][0], End: skip["intro"][1]}
			}
		}
		if outro == nil {
			if outTs != nil {
				outro = &core.SkipTimestamp{Start: outTs.Start, End: outTs.End}
			} else if len(skip["outro"]) == 2 {
				outro = &core.SkipTimestamp{Start: skip["outro"][0], End: skip["outro"][1]}
			}
		}

		if hlsURL != "" {
			// Decrypted: vouch the manifest and subtitle hosts for the proxy
			// allowlist — the auto-learn path for Anikoto CDN rotation.
			p.learnURLHost(hlsURL)
			var subs []core.Subtitle
			for _, t := range tracks {
				if strings.TrimSpace(t.URL) == "" {
					continue
				}
				p.learnURLHost(t.URL)
				subs = append(subs, core.Subtitle{
					URL:   t.URL,
					Lang:  mapSubtitleLang(t.Label),
					Label: t.Label,
				})
			}
			sources = append(sources, core.Source{
				URL:          hlsURL,
				Type:         "hls",
				Quality:      "auto",
				Subtitles:    subs,
				Verification: "proxy",
			})
			variants = append(variants, embedVariant(embedURL))
		} else {
			// Anivexa priority-4 fallback: play through the embed itself.
			sources = append(sources, core.Source{
				URL:          embedURL,
				Type:         "embed",
				Quality:      "auto",
				Verification: "embed",
			})
			variants = append(variants, embedVariant(embedURL))
		}
		// No cap (Anivexa parity): every server lists.
	}
	sources = dedupeSourcesByURL(sources, variants)
	if len(sources) == 0 {
		// The anikoto ajax/embed chain produced nothing usable (dead embeds,
		// blocked getSources, probe failures). Megaplay hosts the same files
		// keyed directly by AniList ID — try it before giving up.
		if mp, mpErr := p.megaplayDirect(ctx, providerID, episode, lang); mpErr == nil {
			return mp, nil
		}
		return nil, nil
	}
	if referer == "" {
		referer = "https://megaplay.buzz/"
	}
	return &SourceResult{
		Sources:   sources,
		Headers:   map[string]string{"Referer": referer},
		Downloads: downloads,
		Intro:     intro,
		Outro:     outro,
	}, nil
}

// megaplayDirect resolves streams straight from megaplay.buzz, which is
// AniList-keyed: /stream/ani/{anilistId}/{episode}/{lang}. It bypasses the
// anikoto catalog entirely and reuses the same MegaPlay decrypt chain.
func (p *AnikotoProvider) megaplayDirect(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}
	embedURL := fmt.Sprintf("https://megaplay.buzz/stream/ani/%s/%d/%s", anilistID, episode, lang)
	p.log.Info().Str("url", embedURL).Msg("megaplayDirect: trying")
	file, tracks, inTs, outTs, origin, err := p.resolveEmbed(ctx, embedURL)
	if err != nil || file == "" {
		p.log.Info().Err(err).Str("url", embedURL).Msg("megaplayDirect: resolveEmbed failed")
		return nil, fmt.Errorf("megaplay direct: %w", err)
	}
	p.log.Info().Str("file", file).Str("origin", origin).Msg("megaplayDirect: resolved")
	// Probe is best-effort: CDN edges (imgnex, norami, akirax) reject
	// datacenter IPs with 403 — the client's HLS proxy handles real
	// playback. Don't let a probe failure drop a valid stream.
	if !p.probeHLS(ctx, file, origin) {
		p.log.Debug().Str("file", file).Msg("megaplay direct: manifest probe failed (non-fatal)")
	}
	p.learnURLHost(file)
	var subs []core.Subtitle
	for _, t := range tracks {
		if strings.TrimSpace(t.URL) == "" {
			continue
		}
		p.learnURLHost(t.URL)
		subs = append(subs, core.Subtitle{
			URL:   t.URL,
			Lang:  mapSubtitleLang(t.Label),
			Label: t.Label,
		})
	}
	return &SourceResult{
		Sources: []core.Source{{
			URL:          file,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		}},
		Headers: map[string]string{"Referer": strings.TrimSuffix(origin, "/") + "/"},
		Intro:   inTs,
		Outro:   outTs,
	}, nil
}

// embedVariant tags which anikoto server-list slot an embed came from.
// HD-1's "?s=tcdn" variant can resolve through a different CDN edge per
// fetch, so it is treated as a distinct server (Niko/Momo) instead of being
// collapsed with the base embed.
func embedVariant(embedURL string) string {
	if strings.Contains(embedURL, "s=tcdn") {
		return "tcdn"
	}
	return "base"
}

// dedupeSourcesByURL collapses sources that resolved to the same file from
// the same embed variant, keeping the copy with the richer subtitle track
// list so the server list never shows the same stream twice. Different
// variants (base vs ?s=tcdn) stay distinct so both server slots fill.
func dedupeSourcesByURL(sources []core.Source, variants []string) []core.Source {
	best := make(map[string]int, len(sources))
	out := make([]core.Source, 0, len(sources))
	for i, s := range sources {
		variant := "base"
		if i < len(variants) {
			variant = variants[i]
		}
		key := s.URL + "|" + variant
		if idx, ok := best[key]; ok {
			if len(s.Subtitles) > len(out[idx].Subtitles) {
				out[idx] = s
			}
			continue
		}
		best[key] = len(out)
		out = append(out, s)
	}
	return out
}

// megaplayTrack is one subtitle/caption entry from /stream/getSources.
type megaplayTrack struct {
	URL   string
	Label string
}

// MegaPlay encrypts the legacy getSources payload with a static AES-256-CBC
// key/IV embedded in its own player bundle (lib/newclient.min.js, TRUST_AES_
// KEY/TRUST_AES_IV, zero-padded to the block size). The ciphertext is
// base64url, plaintext is {"file": "..."}.
const (
	megaPlayEncKey = "i?LMTAx0Q6,:}50U"
	megaPlayEncIv  = "W0;27ToaUpl_P%'c"
)

func decryptMegaPlayEnc(enc string) (string, error) {
	t := strings.NewReplacer("-", "+", "_", "/").Replace(enc)
	if pad := len(t) % 4; pad != 0 {
		t += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.StdEncoding.DecodeString(t)
	if err != nil {
		return "", err
	}
	if len(raw) < 16 || len(raw)%aes.BlockSize != 0 {
		return "", fmt.Errorf("enc payload length %d is not AES-CBC sized", len(raw))
	}
	key := []byte(megaPlayEncKey)
	key = append(key, make([]byte, 32-len(key))...)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	pt := make([]byte, len(raw))
	cipher.NewCBCDecrypter(block, []byte(megaPlayEncIv)).CryptBlocks(pt, raw)
	pad := int(pt[len(pt)-1])
	if pad > 0 && pad <= aes.BlockSize && pad <= len(pt) {
		pt = pt[:len(pt)-pad]
	}
	var out struct {
		File string `json:"file"`
	}
	if err := json.Unmarshal(pt, &out); err != nil {
		return "", err
	}
	if out.File == "" {
		return "", fmt.Errorf("decrypted enc has no file")
	}
	return out.File, nil
}

// resolveEmbed decrypts a MegaPlay-style embed URL to a direct file URL.
// Handles #aHR0c... base64 embeds and data-id -> /stream/getSources embeds.
// Anivexa extractEmbedSource parity: spoofed Referer (hianimes.re) when
// fetching the embed page, then getSources API call for the HLS manifest.
func (p *AnikotoProvider) resolveEmbed(ctx context.Context, embedURL string) (file string, tracks []megaplayTrack, intro, outro *core.SkipTimestamp, origin string, err error) {
	origin = embedURL
	if i := strings.Index(embedURL, "/stream/"); i != -1 {
		origin = embedURL[:i]
	} else if u, e := url.Parse(embedURL); e == nil && u.Host != "" {
		origin = u.Scheme + "://" + u.Host
	}
	if i := strings.Index(embedURL, "#aHR0c"); i != -1 {
		if raw, e := base64.StdEncoding.DecodeString(embedURL[i+1:]); e == nil {
			if s := strings.TrimSpace(string(raw)); strings.Contains(s, ".m3u8") {
				return s, nil, nil, nil, origin, nil
			}
		}
	}
	// Anivexa parity: fetch embed page with spoofed Referer (hianimes.re)
	// and Chrome 124 UA — the embed servers validate referer and reject
	// requests that come from anikototv.to or the embed origin itself.
	embedReq, err := http.NewRequestWithContext(ctx, http.MethodGet, embedURL, nil)
	if err != nil {
		return "", nil, nil, nil, origin, err
	}
	embedReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	embedReq.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	embedReq.Header.Set("Accept-Language", "en-US,en;q=0.9")
	embedReq.Header.Set("Referer", "https://hianimes.re/")
	embedResp, err := p.client.Do(embedReq)
	if err != nil {
		return "", nil, nil, nil, origin, fmt.Errorf("embed page fetch failed: %w", err)
	}
	defer embedResp.Body.Close()
	if embedResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(embedResp.Body, 4096))
		return "", nil, nil, nil, origin, fmt.Errorf("embed page returned HTTP %d: %s", embedResp.StatusCode, string(body[:min(len(body), 200)]))
	}
	pageBytes, err := io.ReadAll(io.LimitReader(embedResp.Body, 512*1024))
	if err != nil {
		return "", nil, nil, nil, origin, err
	}
	page := string(pageBytes)
	m := regexp.MustCompile(`data-id="([^"]+)"`).FindStringSubmatch(page)
	if len(m) < 2 || m[1] == "" {
		return "", nil, nil, nil, origin, fmt.Errorf("embed file id not found")
	}
	// The player rewrites stream/getSources -> stream/getSourcesNew (plain
	// JSON); the legacy endpoint returns an AES-encrypted "enc" blob instead
	// of sources.file. Try New first, fall back to legacy + decrypt.
	fetchSources := func(endpoint string) ([]byte, error) {
		srcURL := fmt.Sprintf("%s/%s?id=%s&id=%s",
			strings.TrimSuffix(origin, "/"), endpoint, url.QueryEscape(m[1]), url.QueryEscape(m[1]))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srcURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
		req.Header.Set("Referer", strings.TrimSuffix(origin, "/")+"/")
		resp, err := p.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	}
	var data struct {
		Sources struct {
			File string `json:"file"`
		} `json:"sources"`
		Tracks []struct {
			File  string `json:"file"`
			Label string `json:"label"`
		} `json:"tracks"`
		Intro *core.SkipTimestamp `json:"intro"`
		Outro *core.SkipTimestamp `json:"outro"`
		Enc   string              `json:"enc"`
	}
	if body, err := fetchSources("stream/getSourcesNew"); err == nil {
		json.Unmarshal(body, &data)
	}
	if data.Sources.File == "" {
		body, err := fetchSources("stream/getSources")
		if err != nil {
			return "", nil, nil, nil, origin, err
		}
		if err := json.Unmarshal(body, &data); err != nil {
			return "", nil, nil, nil, origin, err
		}
		if data.Sources.File == "" && data.Enc != "" {
			file, decErr := decryptMegaPlayEnc(data.Enc)
			if decErr != nil {
				return "", nil, nil, nil, origin, fmt.Errorf("embed enc decrypt failed: %w", decErr)
			}
			data.Sources.File = file
		}
	}
	if data.Sources.File == "" {
		return "", nil, nil, nil, origin, fmt.Errorf("embed returned no file")
	}
	for _, t := range data.Tracks {
		label := strings.TrimSpace(t.Label)
		if label == "" {
			label = "English"
		}
		tracks = append(tracks, megaplayTrack{URL: t.File, Label: label})
	}
	return data.Sources.File, tracks, data.Intro, data.Outro, origin, nil
}

// probeHLS verifies a manifest URL serves a real playlist right now.
func (p *AnikotoProvider) probeHLS(ctx context.Context, fileURL, origin string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	req.Header.Set("Referer", strings.TrimSuffix(origin, "/")+"/")
	resp, err := p.client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return false
	}
	defer resp.Body.Close()
	head, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return false
	}
	return strings.Contains(string(head), "#EXTM3U")
}

// mapSubtitleLang maps a track label to a two-letter language code.
func mapSubtitleLang(label string) string {
	first := strings.ToLower(strings.Split(strings.TrimSpace(label), " ")[0])
	if len(first) == 2 {
		ok := true
		for _, r := range first {
			if r < 'a' || r > 'z' {
				ok = false
			}
		}
		if ok {
			return first
		}
	}
	switch first {
	case "english", "en":
		return "en"
	case "spanish":
		return "es"
	case "french":
		return "fr"
	case "german":
		return "de"
	case "portuguese":
		return "pt"
	case "arabic":
		return "ar"
	case "hindi":
		return "hi"
	default:
		return "en"
	}
}

// anilistMeta carries the titles Anivexa searches (english, romaji, synonyms).
type anilistMeta struct {
	english  string
	romaji   string
	synonyms []string
	episodes int
	format   string
}

func (m anilistMeta) keywords() []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range append([]string{m.english, m.romaji}, m.synonyms...) {
		if t = strings.TrimSpace(t); t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
		if len(out) >= 5 {
			break
		}
	}
	return out
}

// showModifiers are the title words that mark an OVA/movie/spin-off entry;
// a candidate carrying one the target title lacks is heavily penalized
// (Anivexa scoreCandidate parity).
var showModifiers = []string{
	"ova", "movie", "special", "specials", "tales", "journal", "part", "season", "kanwa", "spin-off", "spinoff", "theatre",
}

// normTitle lowercases and trims a title for fuzzy comparison.
func normTitle(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// absInt returns the absolute value of an integer.
func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// scoreShowCandidate is the Anivexa additive scorer, ported exactly: exact
// title matches dominate (+1000/+900/+800), partial matches add smaller
// bonuses, mismatched modifiers subtract heavily, and length difference is
// a mild tiebreaker. No threshold — the top score wins.
func scoreShowCandidate(cand titleCand, meta anilistMeta) float64 {
	score := 0.0
	candName := normTitle(cand.name)
	candJp := normTitle(cand.jp)
	candSlug := normTitle(cand.slug)
	normEn := normTitle(meta.english)
	normRom := normTitle(meta.romaji)

	if normEn != "" && candName == normEn {
		score += 1000
	}
	if normRom != "" && candName == normRom {
		score += 900
	}
	if normRom != "" && candJp == normRom {
		score += 800
	}

	targetText := strings.ToLower(meta.english + " " + meta.romaji + " " + strings.Join(meta.synonyms, " "))
	for _, mod := range showModifiers {
		candHas := strings.Contains(candName, mod) || strings.Contains(candSlug, mod)
		targetHas := strings.Contains(targetText, mod)
		if candHas && !targetHas {
			score -= 300
		}
	}

	titles := append([]string{meta.english, meta.romaji}, meta.synonyms...)
	for _, t := range titles {
		normT := normTitle(t)
		if len(normT) < 3 {
			continue
		}
		switch {
		case candName == normT:
			score += 200
		case strings.HasPrefix(candName, normT) || strings.HasPrefix(normT, candName):
			score += 80
		case strings.Contains(candName, normT) || strings.Contains(normT, candName):
			score += 40
		}
		if candJp != "" && candJp == normT {
			score += 100
		}
	}

	refLen := normEn
	if refLen == "" {
		refLen = normRom
	}
	score -= float64(absInt(len(candName)-len(refLen))) * 2
	return score
}

// resolveShow finds the AnikotoTV show slug and ID from an AniList ID.
// Anivexa findAnikotoShow parity: static mapping fast path, then search the
// full keywords (english, romaji, synonyms — no word splitting), score every
// candidate additively, and take the top one with NO threshold and no extra
// verification round. The old dice/0.5-threshold port failed shows like
// AniList 8 that Anivexa resolves fine.
func (p *AnikotoProvider) resolveShow(ctx context.Context, anilistID string) (slug string, showID string, err error) {
	// Fast path: check static mapping
	if entry := GetAnikotoMapping(anilistID); entry != nil {
		p.log.Info().Str("anilistId", anilistID).Str("slug", entry.Slug).Str("showId", entry.ShowID).Msg("anikoto: resolved from mapping")
		return entry.Slug, entry.ShowID, nil
	}

	meta, err := p.fetchAniListMeta(ctx, anilistID)
	if err != nil {
		return "", "", fmt.Errorf("anilist title fetch failed: %w", err)
	}
	queries := map[string]bool{}
	for _, t := range meta.keywords() {
		queries[t] = true
	}
	seen := map[string]*titleCand{}
	var order []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	for q := range queries {
		wg.Add(1)
		go func(q string) {
			defer wg.Done()
			html, err := p.searchPage(ctx, q)
			if err != nil {
				return
			}
			local := parseTitleAnchors(html)
			mu.Lock()
			for _, c := range local {
				if _, ok := seen[c.slug]; !ok {
					cp := c
					seen[c.slug] = &cp
					order = append(order, c.slug)
				}
			}
			mu.Unlock()
		}(q)
	}
	wg.Wait()
	scored := make([]titleCand, 0, len(order))
	for _, slug := range order {
		c := *seen[slug]
		c.score = scoreShowCandidate(c, meta)
		scored = append(scored, c)
	}
	if len(scored) == 0 {
		return "", "", fmt.Errorf("no search results on anikoto for anilistId=%s", anilistID)
	}
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].score > scored[j].score })

	// Anivexa parity: take the top-scored candidate and read its show id
	// straight off the watch page — no threshold, no verification rounds.
	for _, c := range scored {
		watchHTML, err := p.fetchPage(ctx, anikotoBase+"/watch/"+c.slug)
		if err != nil {
			continue
		}
		m := regexp.MustCompile(`data-id="(\d+)"`).FindStringSubmatch(watchHTML)
		if len(m) >= 2 && m[1] != "" {
			p.log.Info().Str("anilistId", anilistID).Str("slug", c.slug).Str("showId", m[1]).Float64("score", c.score).Msg("anikoto: resolved from search")
			return c.slug, m[1], nil
		}
	}
	return "", "", fmt.Errorf("no matching show found for anilistId=%s", anilistID)
}

// searchPage fetches one Anikoto filter-search page, retrying the site's
// intermittent backend 500s ("An Internal Error Has Occurred").
func (p *AnikotoProvider) searchPage(ctx context.Context, q string) (string, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(attempt) * time.Second):
			}
		}
		html, err := p.searchPageOnce(ctx, q)
		if err == nil && looksLikeFilterPage(html) {
			return html, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("filter page failed validation")
		}
	}
	return "", lastErr
}

func looksLikeFilterPage(html string) bool {
	return len(html) > 20000 && strings.Contains(strings.ToLower(html), "<html")
}

func (p *AnikotoProvider) searchPageOnce(ctx context.Context, q string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/filter?keyword=%s", anikotoBase, url.QueryEscape(q)), nil)
	if err != nil {
		return "", err
	}
	p.setAjaxHeaders(req)
	req.Header.Del("X-Requested-With")
	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return "", err
	}
	return string(body), nil
}

type titleCand struct {
	slug  string
	name  string
	jp    string
	score float64
}

// parseTitleAnchors extracts title anchors (real names), falling back to
// generic watch links when the page variant lacks them.
func parseTitleAnchors(html string) []titleCand {
	seen := map[string]bool{}
	var out []titleCand
	add := func(re *regexp.Regexp, withJp bool) {
		for _, m := range re.FindAllStringSubmatch(html, -1) {
			slug := m[1]
			if seen[slug] || strings.HasPrefix(slug, "genre") || strings.HasPrefix(slug, "filter") {
				continue
			}
			seen[slug] = true
			name, jp := "", ""
			if withJp {
				jp = strings.TrimSpace(m[2])
				name = strings.TrimSpace(stripHTMLTags(m[3]))
			} else {
				name = strings.TrimSpace(stripHTMLTags(m[2]))
			}
			name = strings.ReplaceAll(name, "&amp;", "&")
			if name == "" {
				name = slug
			}
			out = append(out, titleCand{slug: slug, name: name, jp: jp})
		}
	}
	add(regexp.MustCompile(`<a\s+class="name d-title"\s+href="(?:https?://anikototv\.to)?/watch/([^"/]+?)(?:/ep-\d+)?"[^>]*data-jp="([^"]*)"[^>]*>(.*?)</a>`), true)
	if len(out) == 0 {
		add(regexp.MustCompile(`<a[^>]*href="(?:https?://anikototv\.to)?/watch/([^"/]+?)(?:/ep-\d+)?"[^>]*>(.*?)</a>`), false)
	}
	return out
}

// verifyCandidate checks the watch page (AniList banner proof, else exact
// title fallback), caches the mapping, and returns slug + show ID.
func (p *AnikotoProvider) verifyCandidate(ctx context.Context, anilistID, title, slug string, score float64, trust bool) (string, string, error) {
	baseSlug := regexp.MustCompile(`/ep-\d+$`).ReplaceAllString(slug, "")
	pageURL := fmt.Sprintf("%s/watch/%s", anikotoBase, slug)
	pageHTML, err := p.fetchPage(ctx, pageURL)
	if err != nil {
		return "", "", err
	}
	showID := extractShowID(pageHTML)
	if showID == "" {
		showID = tipNearSlug(pageHTML, baseSlug)
	}
	if showID == "" {
		return "", "", fmt.Errorf("no show id")
	}
	pat := regexp.MustCompile(`anilist\.co/file/anilistcdn/media/anime/(?:banner|poster)/` + anilistID + `-`)
	if pat.MatchString(pageHTML) {
		p.log.Info().Str("slug", baseSlug).Str("showId", showID).Msg("anikoto: resolved from search")
		AddAnikotoMapping(anilistID, AnikotoMapping{ShowID: showID, Slug: baseSlug, Title: title})
		return baseSlug, showID, nil
	}
	// Trusted winners (episode-count validated) and exact title matches
	// are accepted without banner proof; the episode list validates after.
	if trust || score >= 0.9 {
		p.log.Info().Str("slug", baseSlug).Str("showId", showID).Msg("anikoto: resolved by exact title (no banner proof)")
		AddAnikotoMapping(anilistID, AnikotoMapping{ShowID: showID, Slug: baseSlug, Title: title})
		return baseSlug, showID, nil
	}
	return "", "", fmt.Errorf("no banner proof for %s", slug)
}

// selectSeries validates top candidates by episode count (Anivexa parity):
// score = title*0.7 + count*0.3, min 0.65. Returns the winner.
func (p *AnikotoProvider) selectSeries(ctx context.Context, scored []titleCand, meta anilistMeta) (titleCand, bool) {
	var zero titleCand
	if meta.episodes <= 0 || len(scored) == 0 {
		return zero, false
	}
	type res struct {
		idx   int
		count int
		total int
	}
	n := len(scored)
	if n > 3 {
		n = 3
	}
	outs := make([]res, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			nums := p.fetchEpisodeNumbers(ctx, scored[i].slug)
			inRange := 0
			for _, num := range nums {
				if num >= 1 && num <= meta.episodes {
					inRange++
				}
			}
			outs[i] = res{idx: i, count: inRange, total: len(nums)}
		}(i)
	}
	wg.Wait()
	best := -1.0
	bestIdx := -1
	for _, r := range outs {
		if r.total == 0 {
			continue
		}
		need := r.count
		want := meta.episodes
		if want >= 6 {
			wantNeed := want - 3
			if wantNeed < 1 {
				wantNeed = 1
			}
			countScore := 1.0
			if need < wantNeed {
				countScore = float64(need) / float64(wantNeed)
			}
			final := scored[r.idx].score*0.7 + countScore*0.3
			if final >= 0.65 && final > best {
				best, bestIdx = final, r.idx
			}
		} else if float64(need) > best {
			best, bestIdx = float64(need), r.idx
		}
	}
	if bestIdx == -1 {
		return zero, false
	}
	return scored[bestIdx], true
}

// fetchEpisodeNumbers returns all episode numbers for a slug via its show ID.
func (p *AnikotoProvider) fetchEpisodeNumbers(ctx context.Context, slug string) []int {
	pageHTML, err := p.fetchPage(ctx, fmt.Sprintf("%s/watch/%s", anikotoBase, slug))
	if err != nil {
		return nil
	}
	showID := extractShowID(pageHTML)
	if showID == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/ajax/episode/list/%s", anikotoBase, showID),
		strings.NewReader("style=&vrf="))
	if err != nil {
		return nil
	}
	p.setAjaxHeaders(req)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil
	}
	var data struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil
	}
	var nums []int
	for _, m := range regexp.MustCompile(`data-num="(\d+)"`).FindAllStringSubmatch(data.Result, -1) {
		var n int
		fmt.Sscanf(m[1], "%d", &n)
		if n > 0 {
			nums = append(nums, n)
		}
	}
	return nums
}

// ---- Anivexa-parity search/scoring utils (best-ever method) ----

func normDice(s string) string {
	return regexp.MustCompile(`[^a-z0-9]`).ReplaceAllString(strings.ToLower(s), "")
}

func diceCoeff(a, b string) float64 {
	na, nb := normDice(a), normDice(b)
	if na == nb {
		return 1
	}
	if len(na) < 2 || len(nb) < 2 {
		return 0
	}
	bg := map[string]int{}
	for i := 0; i+1 < len(na); i++ {
		bg[na[i:i+2]]++
	}
	hits := 0
	for i := 0; i+1 < len(nb); i++ {
		if bg[nb[i:i+2]] > 0 {
			hits++
			bg[nb[i:i+2]]--
		}
	}
	return 2 * float64(hits) / float64(len(na)+len(nb)-2)
}

// titleScoreDice mirrors Anivexa titleScore: dice base with number,
// movie-asymmetry and length penalties.
func titleScoreDice(query, candidate, slug string) float64 {
	slugSp := strings.ReplaceAll(slug, "-", " ")
	base := diceCoeff(query, candidate)
	if s := diceCoeff(query, slugSp); s > base {
		base = s
	}
	// Trailing hash segments (fc8mq, 752db) are site IDs, not sequel
	// numbers: exclude them from the digit/length penalties.
	slugCore := regexp.MustCompile(`-[a-z0-9]*[0-9][a-z0-9]*$`).ReplaceAllString(slug, "")
	if slugCore == "" {
		slugCore = slug
	}
	slug = slugCore
	numRe := regexp.MustCompile(`\d+`)
	qn := numRe.FindString(normDice(query))
	sn := numRe.FindString(slug)
	if qn != "" && sn != "" && qn != sn {
		return base * 0.65
	}
	if qn != "" && sn == "" {
		return base * 0.65
	}
	if qn == "" && sn != "" {
		var n int
		fmt.Sscanf(sn, "%d", &n)
		if n > 1 && n < 1900 {
			return base * (1 - 0.06*float64(n-1))
		}
	}
	lq := strings.ToLower(query)
	movieQ := strings.Contains(lq, "movie") || strings.Contains(lq, "film")
	movieM := strings.Contains(strings.ToLower(candidate), "movie") ||
		strings.Contains(strings.ToLower(candidate), "film") ||
		strings.Contains(slug, "movie") || strings.Contains(slug, "film")
	if movieQ && !movieM {
		return base * 0.4
	}
	ql, sl := len(normDice(query)), len(normDice(slugSp))
	if float64(sl) > float64(ql)*1.6+4 {
		return base * 0.8
	}
	return base
}

// buildSearchQueries mirrors Anivexa: full title, first-4, first-3 words,
// season/part/ordinal-stripped variant.
func buildSearchQueries(title string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(q string) {
		q = strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(q, " "))
		if len(q) >= 3 && !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
	}
	add(title)
	words := strings.Fields(title)
	if len(words) > 4 {
		add(strings.Join(words[:4], " "))
	}
	if len(words) > 3 {
		add(strings.Join(words[:3], " "))
	}
	stripped := regexp.MustCompile(`(?i)\bseason\s*\d+\b|\bpart\s*\d+\b|\b\d+(rd|th|st|nd)\b`).ReplaceAllString(title, "")
	if strings.TrimSpace(stripped) != strings.TrimSpace(title) {
		add(stripped)
	}
	return out
}

// normShowTitle normalizes a title for fuzzy comparison.
func normShowTitle(s string) string {
	return regexp.MustCompile(`[^a-z0-9]`).ReplaceAllString(strings.ToLower(s), "")
}

// stripHTMLTags removes tags from a snippet.
func stripHTMLTags(s string) string {
	return regexp.MustCompile(`<[^>]+>`).ReplaceAllString(s, "")
}

// titleScore ranks a candidate slug/name against the wanted title.
func titleScore(cand, want string) int {
	return titleScoreEx(cand, "", want)
}

// titleScoreEx scores name + Japanese name against the wanted title.
func titleScoreEx(cand, candJp, want string) int {
	if want == "" {
		return 0
	}
	score := 0
	if candJp != "" && candJp == want {
		score = 800
	}
	switch {
	case cand == "" && score == 0:
		return 0
	case cand == want:
		score = 1000
	case strings.HasPrefix(cand, want) || strings.HasPrefix(want, cand):
		if score < 80 {
			score = 80
		}
	case strings.Contains(cand, want) || strings.Contains(want, cand):
		if score < 40 {
			score = 40
		}
	default:
		if score == 0 {
			score = 1
		}
	}
	lowerWant := strings.ToLower(want)
	lowerCand := strings.ToLower(cand)
	for _, mod := range showModifiers {
		if strings.Contains(lowerCand, mod) && !strings.Contains(lowerWant, mod) {
			score -= 300
		}
	}
	return score
}

// tipNearSlug finds the closest data-tip show ID before a slug occurrence.
func tipNearSlug(html, slug string) string {
	idx := strings.Index(html, "/watch/"+slug)
	if idx == -1 {
		return ""
	}
	window := html[maxInt(0, idx-3000):idx]
	re := regexp.MustCompile(`data-tip="(\d+)"`)
	matches := re.FindAllStringSubmatch(window, -1)
	if len(matches) == 0 {
		return ""
	}
	return matches[len(matches)-1][1]
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// extractShowID pulls the data-id from the watch-main div.
func extractShowID(html string) string {
	re := regexp.MustCompile(`id="watch-main"[^>]*data-id="(\d+)"`)
	m := re.FindStringSubmatch(html)
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}

// fetchAniListTitle queries AniList GraphQL for the English or romaji title.
func (p *AnikotoProvider) fetchAniListTitle(ctx context.Context, anilistID string) (string, error) {
	m, err := p.fetchAniListMeta(ctx, anilistID)
	if err != nil {
		return "", err
	}
	if m.english != "" {
		return m.english, nil
	}
	if m.romaji != "" {
		return m.romaji, nil
	}
	return "", fmt.Errorf("no title found for anilistId=%s", anilistID)
}

// fetchAniListMeta resolves titles + synonyms (Anivexa searches english,
// romaji and synonyms as separate keywords). Primary source is AniList
// GraphQL; when AniList is down — it has recurring global outages (403
// "temporarily disabled", 429 rate limits) — it falls back to AniZip, which
// mirrors the same mappings keyed by AniList ID.
func (p *AnikotoProvider) fetchAniListMeta(ctx context.Context, anilistID string) (anilistMeta, error) {
	meta, err := p.fetchAniListMetaUpstream(ctx, anilistID)
	if err == nil {
		return meta, nil
	}
	id, idErr := strconv.Atoi(anilistID)
	if idErr != nil {
		return meta, fmt.Errorf("anilist meta failed (%v); anizip fallback skipped (invalid id)", err)
	}
	az, azErr := tmdb.FetchAniZipMediaMeta(ctx, p.client, id)
	if azErr != nil {
		return meta, fmt.Errorf("anilist meta failed (%v) and anizip fallback failed (%v)", err, azErr)
	}
	return anilistMeta{
		english:  az.English,
		romaji:   az.Romaji,
		synonyms: az.Synonyms,
		episodes: az.EpisodeCount,
	}, nil
}

// fetchAniListMetaUpstream is the direct AniList GraphQL lookup. It reports
// real upstream failures (HTTP status, GraphQL error payload) instead of
// misreporting them as "no title".
func (p *AnikotoProvider) fetchAniListMetaUpstream(ctx context.Context, anilistID string) (anilistMeta, error) {
	var out anilistMeta
	query := `{"query":"{ Media(id:` + anilistID + `,type:ANIME){title{english romaji} synonyms episodes format} }"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://graphql.anilist.co",
		strings.NewReader(query))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024))
	if err != nil {
		return out, err
	}

	var result struct {
		Errors []struct {
			Message string `json:"message"`
			Status  int    `json:"status"`
		} `json:"errors"`
		Data struct {
			Media struct {
				Title struct {
					English *string `json:"english"`
					Romaji  *string `json:"romaji"`
				} `json:"title"`
				Synonyms []string `json:"synonyms"`
				Episodes *int     `json:"episodes"`
				Format   *string  `json:"format"`
			} `json:"Media"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return out, fmt.Errorf("anilist returned undecodable body (HTTP %d): %w", resp.StatusCode, err)
	}
	if len(result.Errors) > 0 {
		return out, fmt.Errorf("anilist graphql error (HTTP %d): %s", resp.StatusCode, result.Errors[0].Message)
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("anilist returned HTTP %d", resp.StatusCode)
	}

	if result.Data.Media.Title.English != nil {
		out.english = strings.TrimSpace(*result.Data.Media.Title.English)
	}
	if result.Data.Media.Title.Romaji != nil {
		out.romaji = strings.TrimSpace(*result.Data.Media.Title.Romaji)
	}
	for _, s := range result.Data.Media.Synonyms {
		if s = strings.TrimSpace(s); s != "" {
			out.synonyms = append(out.synonyms, s)
		}
	}
	if result.Data.Media.Episodes != nil {
		out.episodes = *result.Data.Media.Episodes
	}
	if result.Data.Media.Format != nil {
		out.format = *result.Data.Media.Format
	}
	if out.english == "" && out.romaji == "" {
		return out, fmt.Errorf("no title found for anilistId=%s", anilistID)
	}
	return out, nil
}

// anikotoEpisode represents an episode entry from the HTML.
type anikotoEpisode struct {
	slug    string
	dataIDs string
	number  int
}

// anikotoEpMeta carries episode-list attributes needed downstream
// (mapper lookup needs mal/slug/timestamp).
type anikotoEpMeta struct {
	mal       string
	slug      string
	timestamp string
}

// fetchMapperServers queries the nekostream mapper for extra servers and
// download links (Anivexa parity). Entries with http link IDs are direct
// embed URLs; name suffixes are trimmed like Anivexa.
func (p *AnikotoProvider) fetchMapperServers(ctx context.Context, meta anikotoEpMeta, lang string) []anikotoServerEntry {
	if meta.mal == "" || meta.slug == "" || meta.timestamp == "" {
		return nil
	}
	u := fmt.Sprintf("https://mapper.nekostream.site/api/mal/%s/%s/%s",
		url.PathEscape(meta.mal), url.PathEscape(meta.slug), url.PathEscape(meta.timestamp))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	req.Header.Set("Referer", anikotoBase+"/")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil
	}
	// Generic decode: keys are server names with trailing -/_ trimmed.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	var out []anikotoServerEntry
	for key, val := range raw {
		if key == "status" {
			continue
		}
		name := strings.Trim(strings.Trim(key, "-_"), " ")
		var s struct {
			URL      string            `json:"url"`
			Download map[string]string `json:"download"`
		}
		// pick audio branch
		var branch map[string]json.RawMessage
		if err := json.Unmarshal(val, &branch); err != nil {
			continue
		}
		ab, ok := branch[lang]
		if !ok {
			continue
		}
		if err := json.Unmarshal(ab, &s); err != nil {
			continue
		}
		if s.URL != "" {
			out = append(out, anikotoServerEntry{linkID: s.URL, name: name, serverType: lang})
		}
		for label, durl := range s.Download {
			if durl != "" {
				out = append(out, anikotoServerEntry{linkID: durl, name: name + " " + label, serverType: "dl"})
			}
		}
	}
	return out
}

// fetchEpisodeDataIDs fetches the episode list and returns the data-ids plus
// mapper attributes for the target episode.
func (p *AnikotoProvider) fetchEpisodeDataIDs(ctx context.Context, showID string, episode int) (string, anikotoEpMeta, error) {
	// POST with empty style/vrf — the site requires this body
	episodeURL := fmt.Sprintf("%s/ajax/episode/list/%s", anikotoBase, showID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, episodeURL, strings.NewReader("style=&vrf="))
	if err != nil {
		return "", anikotoEpMeta{}, err
	}
	p.setAjaxHeaders(req)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", anikotoEpMeta{}, fmt.Errorf("episode list request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return "", anikotoEpMeta{}, err
	}

	var result struct {
		Status int    `json:"status"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", anikotoEpMeta{}, err
	}
	if result.Status != 200 {
		return "", anikotoEpMeta{}, fmt.Errorf("episode list returned status %d", result.Status)
	}

	// Parse episode links: <a ... data-num="6" data-ids="..." ...>.
	// data-num is authoritative (data-slug is an internal id on some pages).
	episodeStr := strconv.Itoa(episode)
	for _, pat := range []string{
		`<a[^>]*?data-num="(\d+)"[^>]*?>`,
		`<a[^>]*?data-slug="(\d+)"[^>]*?>`,
	} {
		re := regexp.MustCompile(pat)
		for _, idx := range re.FindAllStringSubmatchIndex(result.Result, -1) {
			if result.Result[idx[2]:idx[3]] != episodeStr {
				continue
			}
			// Grab the whole tag for attribute extraction.
			end := idx[1]
			if j := indexOf(result.Result[end:], ">"); j >= 0 {
				end += j
			}
			tag := result.Result[idx[0]:end]
			ids := attrValue(tag, "data-ids")
			if ids == "" {
				continue
			}
			return ids, anikotoEpMeta{
				mal:       attrValue(tag, "data-mal"),
				slug:      attrValue(tag, "data-slug"),
				timestamp: attrValue(tag, "data-timestamp"),
			}, nil
		}
	}

	return "", anikotoEpMeta{}, fmt.Errorf("episode %d not found in list", episode)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// anikotoServerEntry represents a server from the API.
type anikotoServerEntry struct {
	linkID     string
	svID       string
	name       string
	serverType string
}

// fetchServers fetches the server list for a given data-ids and language.
func (p *AnikotoProvider) fetchServers(ctx context.Context, dataIDs string, lang string) ([]anikotoServerEntry, error) {
	serverURL := fmt.Sprintf("%s/ajax/server/list?servers=%s", anikotoBase, url.QueryEscape(dataIDs))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL, nil)
	if err != nil {
		return nil, err
	}
	p.setAjaxHeaders(req)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("server list request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return nil, err
	}

	var result struct {
		Status int    `json:"status"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if result.Status != 200 {
		return nil, fmt.Errorf("server list returned status %d", result.Status)
	}

	// Parse server HTML. Type blocks are bounded (each block ends where the
	// next data-type block starts) so entries never leak across languages.
	// <li> attributes are extracted independently — attribute ORDER varies.
	var entries []anikotoServerEntry
	typeRe := regexp.MustCompile(`<div[^>]*?class="type"[^>]*?data-type="([^"]+)"`)
	locs := typeRe.FindAllStringSubmatchIndex(result.Result, -1)
	stripTags := regexp.MustCompile(`<[^>]+>`)
	for i, loc := range locs {
		serverType := result.Result[loc[2]:loc[3]]
		blockEnd := len(result.Result)
		if i+1 < len(locs) {
			blockEnd = locs[i+1][0]
		}
		block := result.Result[loc[1]:blockEnd]
		for _, li := range regexp.MustCompile(`(?s)<li\b(.*?)>(.*?)</li>`).FindAllStringSubmatch(block, -1) {
			attrs, inner := li[1], li[2]
			linkID := attrValue(attrs, "data-link-id")
			if linkID == "" {
				continue
			}
			name := strings.TrimSpace(stripTags.ReplaceAllString(inner, ""))
			entries = append(entries, anikotoServerEntry{
				linkID:     linkID,
				svID:       attrValue(attrs, "data-sv-id"),
				name:       name,
				serverType: serverType,
			})
		}
		_ = lang
	}

	return entries, nil
}

// attrValue extracts one HTML attribute value from a tag-attribute string.
func attrValue(attrs, name string) string {
	m := regexp.MustCompile(regexp.QuoteMeta(name) + `="([^"]*)"`).FindStringSubmatch(attrs)
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}

// fetchVideoURL gets the actual video iframe URL for a server link.
func (p *AnikotoProvider) fetchVideoURL(ctx context.Context, linkID string) (string, map[string][]float64, error) {
	serverURL := fmt.Sprintf("%s/ajax/server?get=%s", anikotoBase, url.QueryEscape(linkID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL, nil)
	if err != nil {
		return "", nil, err
	}
	p.setAjaxHeaders(req)

	resp, err := p.client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("video URL request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return "", nil, err
	}

	var result struct {
		Status int `json:"status"`
		Result struct {
			URL      string               `json:"url"`
			SkipData map[string][]float64 `json:"skip_data"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", nil, err
	}
	if result.Status != 200 {
		return "", nil, fmt.Errorf("video URL returned status %d", result.Status)
	}

	return result.Result.URL, result.Result.SkipData, nil
}

// setAjaxHeaders sets the standard headers for anikoto AJAX requests.
func (p *AnikotoProvider) setAjaxHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("Referer", anikotoBase+"/")
}

// fetchPage does a simple GET with browser UA and returns the response body as string.
func (p *AnikotoProvider) fetchPage(ctx context.Context, pageURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	req.Header.Set("Referer", anikotoBase+"/")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
	if err != nil {
		return "", err
	}
	return string(body), nil
}
