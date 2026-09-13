package streaming

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
	"github.com/Aniraku/Aniraku-Backend/internal/tmdb"
)

const (
	zokoBase       = "https://zokoanime.video"
	zokoObfKey     = "otaku-embed-v1"
	zokoPlayerUA   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
	zokoServerName = "Zoko"
)

// zokoPayload matches the deobfuscated window.__P blob the ZokoAnime embed
// page ships. The player (zokoanime2.pages.dev/core/obfuscate.js) decodes it
// as base64(XOR(json, "otaku-embed-v1")).
type zokoPayload struct {
	Src      string `json:"src"`
	Poster   string `json:"poster"`
	VideoID  int64  `json:"video_id"`
	Download string `json:"download_url"`
	Skip     *struct {
		Intro *core.SkipTimestamp `json:"intro"`
		Outro *core.SkipTimestamp `json:"outro"`
	} `json:"skip"`
	Subtitles []struct {
		Lang    string `json:"lang"`
		Label   string `json:"label"`
		Default bool   `json:"default"`
		Src     string `json:"src"`
	} `json:"subtitles"`
}

var zokoPayloadRe = regexp.MustCompile(`window\.__P="([^"]+)"`)

// ZokoProvider scrapes zokoanime.video, which is AniList-keyed:
// /stream/ani/{anilistId}/{episode}/{sub|dub}. The embed page carries the
// direct HLS manifest + subtitle tracks inside an XOR-obfuscated window.__P
// blob; this provider decodes it in-process. The CDN (aniwatchtv.uk family)
// is referer-gated, so every returned source carries the ZokoAnime referer.
type ZokoProvider struct {
	client    *http.Client
	log       zerolog.Logger
	learnHost func(host string)
}

func (p *ZokoProvider) SetHostLearner(fn func(host string)) {
	p.learnHost = fn
}

func (p *ZokoProvider) learnURLHost(raw string) {
	if p.learnHost == nil {
		return
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	p.learnHost(u.Hostname())
}

func NewZokoProvider(log zerolog.Logger) *ZokoProvider {
	return &ZokoProvider{
		client: &http.Client{Timeout: 15 * time.Second, Transport: netguard.NewTransport()},
		log:    log,
	}
}

func (p *ZokoProvider) Name() string { return "zoko" }

func (p *ZokoProvider) Search(ctx context.Context, title string) ([]SearchResult, error) {
	return nil, fmt.Errorf("zoko search not implemented")
}

func (p *ZokoProvider) FindEpisodes(ctx context.Context, providerID string) ([]Episode, error) {
	return nil, fmt.Errorf("zoko episode listing not implemented")
}

// deobfuscateZokoPayload reverses base64(XOR(json, key)). The browser uses
// escape/unescape around the XOR (UTF-8 percent-encoding); for the JSON
// payloads Zoko ships (ASCII) a straight XOR is equivalent.
func deobfuscateZokoPayload(blob string) (*zokoPayload, error) {
	raw, err := base64.StdEncoding.DecodeString(blob)
	if err != nil {
		return nil, fmt.Errorf("zoko payload base64: %w", err)
	}
	key := []byte(zokoObfKey)
	for i := range raw {
		raw[i] ^= key[i%len(key)]
	}
	var payload zokoPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("zoko payload json: %w", err)
	}
	return &payload, nil
}

// probeManifest verifies the stream manifest is actually reachable with the
// ZokoAnime referer (the CDN 403s without it). A CDN-blocked manifest must
// not surface as a playable server.
func (p *ZokoProvider) probeManifest(ctx context.Context, manifestURL string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", zokoPlayerUA)
	req.Header.Set("Referer", zokoBase+"/")
	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	head, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return err == nil && strings.Contains(string(head), "#EXTM3U")
}

// zokoFetchEmbed fetches a Zoko embed page and extracts the payload.
func (p *ZokoProvider) zokoFetchEmbed(ctx context.Context, embedURL string) (*zokoPayload, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, embedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", zokoPlayerUA)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("zoko embed fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("zoko embed returned HTTP %d", resp.StatusCode)
	}
	page, err := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	if err != nil {
		return nil, err
	}
	m := zokoPayloadRe.FindSubmatch(page)
	if len(m) < 2 {
		return nil, fmt.Errorf("zoko payload not found on embed page")
	}
	return deobfuscateZokoPayload(string(m[1]))
}

// zokoBuildSourceResult converts a Zoko payload into a SourceResult.
func (p *ZokoProvider) zokoBuildSourceResult(ctx context.Context, payload *zokoPayload, source string) (*SourceResult, error) {
	if payload.Src == "" || !strings.Contains(payload.Src, ".m3u8") {
		return nil, fmt.Errorf("zoko payload has no m3u8 src")
	}
	if !p.probeManifest(ctx, payload.Src) {
		p.log.Info().Str("source", source).Msg("zoko: manifest probe failed (CDN blocked), dropping source")
		return nil, nil
	}
	p.learnURLHost(payload.Src)

	var subs []core.Subtitle
	for _, s := range payload.Subtitles {
		if strings.TrimSpace(s.Src) == "" {
			continue
		}
		p.learnURLHost(s.Src)
		langCode := strings.TrimSpace(s.Lang)
		if langCode == "" {
			langCode = mapSubtitleLang(s.Label)
		}
		subs = append(subs, core.Subtitle{
			URL:   s.Src,
			Lang:  langCode,
			Label: s.Label,
		})
	}

	var downloads []core.DownloadLink
	if d := strings.TrimSpace(payload.Download); d != "" {
		dlURL := d
		if !strings.HasPrefix(dlURL, "http") {
			dlURL = zokoBase + "/" + strings.TrimPrefix(dlURL, "/")
		}
		p.learnURLHost(dlURL)
		downloads = append(downloads, core.DownloadLink{URL: dlURL, Label: "Zoko"})
	}

	var intro, outro *core.SkipTimestamp
	if payload.Skip != nil {
		intro = payload.Skip.Intro
		outro = payload.Skip.Outro
	}

	return &SourceResult{
		Sources: []core.Source{{
			URL:          payload.Src,
			Type:         "hls",
			Quality:      "auto",
			Subtitles:    subs,
			Verification: "proxy",
		}},
		Headers:   map[string]string{"Referer": zokoBase + "/"},
		Downloads: downloads,
		Intro:     intro,
		Outro:     outro,
	}, nil
}

// FindEpisodeSource resolves the direct HLS stream for an episode.
// Tries AniList-keyed URL first; when that returns no payload, looks up the
// MAL ID via AniZip mappings and retries with /stream/mal/:malId/:ep/:lang.
func (p *ZokoProvider) FindEpisodeSource(ctx context.Context, anilistID string, episode int, lang string) (*SourceResult, error) {
	if lang != "dub" {
		lang = "sub"
	}

	// --- attempt 1: AniList-keyed URL ---
	anilistURL := fmt.Sprintf("%s/stream/ani/%s/%d/%s", zokoBase, url.PathEscape(anilistID), episode, lang)
	payload, err := p.zokoFetchEmbed(ctx, anilistURL)
	if err == nil {
		result, err2 := p.zokoBuildSourceResult(ctx, payload, "anilist")
		if err2 != nil || result != nil {
			return result, err2
		}
		// manifest probe failed — fall through to MAL
	}

	// --- attempt 2: MAL-keyed URL ---
	anilistInt, _ := strconv.Atoi(anilistID)
	if anilistInt <= 0 {
		return nil, fmt.Errorf("zoko: anilist ID %q is not a valid integer, cannot look up MAL", anilistID)
	}
	malID := tmdb.FetchMalID(ctx, p.client, anilistInt)
	if malID <= 0 {
		return nil, fmt.Errorf("zoko: no MAL ID found for anilist %s", anilistID)
	}
	malURL := fmt.Sprintf("%s/stream/mal/%d/%d/%s", zokoBase, malID, episode, lang)
	payload, err = p.zokoFetchEmbed(ctx, malURL)
	if err != nil {
		return nil, fmt.Errorf("zoko MAL fallback (mal=%d): %w", malID, err)
	}
	return p.zokoBuildSourceResult(ctx, payload, fmt.Sprintf("mal=%d", malID))
}
