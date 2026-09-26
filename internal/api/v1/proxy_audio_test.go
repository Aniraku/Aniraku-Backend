package v1

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// The Sora/krussdomi dual-audio master (anilist 130298): upstream serves the
// identical manifest for type=sub and type=dub, carrying both the English
// (dub) and Japanese (sub, DEFAULT=YES) renditions. The proxy must hand the
// player a single-rendition master matching the requested language.
const dualAudioMaster = `#EXTM3U
#EXT-X-INDEPENDENT-SEGMENTS
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="stereo",NAME="English",LANGUAGE="en",CHANNELS="2",URI="64af319752218f22753ede58/playlist.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="stereo",NAME="Japanese",DEFAULT=YES,LANGUAGE="ja",CHANNELS="2",URI="64af3197b4a3c92d0ac00fc9/playlist.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1129808,CODECS="avc1.4d401f,mp4a.40.2",RESOLUTION=1280x720,AUDIO="stereo"
64af319744c6d04c12f57b6b/playlist.m3u8
`

func countAudio(lines, needle string) int {
	n := 0
	for _, l := range strings.Split(lines, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "#EXT-X-MEDIA:") &&
			strings.Contains(l, "TYPE=AUDIO") && strings.Contains(l, needle) {
			n++
		}
	}
	return n
}

func TestStripAudioRenditionsSubKeepsJapanese(t *testing.T) {
	out := stripAudioRenditions(dualAudioMaster, "sub")
	if countAudio(out, "English") != 0 {
		t.Errorf("sub strip left the English (dub) rendition:\n%s", out)
	}
	if countAudio(out, "Japanese") != 1 {
		t.Fatalf("sub strip dropped the Japanese rendition:\n%s", out)
	}
	if !strings.Contains(out, `NAME="Japanese",LANGUAGE="ja",CHANNELS="2"`) && !strings.Contains(out, "DEFAULT=YES") {
		t.Errorf("kept rendition lost attrs or DEFAULT:\n%s", out)
	}
	if strings.Count(out, "DEFAULT=YES") != 1 {
		t.Errorf("want exactly one DEFAULT=YES, got:\n%s", out)
	}
	if !strings.Contains(out, "#EXT-X-STREAM-INF:BANDWIDTH=1129808") {
		t.Errorf("variant line lost:\n%s", out)
	}
}

func TestStripAudioRenditionsDubKeepsEnglish(t *testing.T) {
	out := stripAudioRenditions(dualAudioMaster, "dub")
	if countAudio(out, "Japanese") != 0 {
		t.Errorf("dub strip left the Japanese (sub) rendition:\n%s", out)
	}
	if countAudio(out, "English") != 1 {
		t.Fatalf("dub strip dropped the English rendition:\n%s", out)
	}
	// The surviving English rendition had no DEFAULT flag — the strip must
	// promote it so a compliant player lands on it.
	if strings.Count(out, "DEFAULT=YES") != 1 || !strings.Contains(out, `NAME="English"`) {
		t.Errorf("surviving English rendition not promoted to DEFAULT=YES:\n%s", out)
	}
}

func TestStripAudioRenditionsNoLangUnchanged(t *testing.T) {
	if got := stripAudioRenditions(dualAudioMaster, ""); got != dualAudioMaster {
		t.Errorf("al empty must return content byte-identical")
	}
	if got := stripAudioRenditions(dualAudioMaster, "weird"); got != dualAudioMaster {
		t.Errorf("unknown al must return content byte-identical")
	}
}

func TestStripAudioRenditionsNoMatchUnchanged(t *testing.T) {
	// al=sub but the group has no Japanese track: nothing correct to keep,
	// so behavior must be preserved (no strip, no DEFAULT rewrite).
	master := `#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="English",LANGUAGE="en",URI="en.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="Thai",LANGUAGE="th",URI="th.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO="a"
v.m3u8
`
	if got := stripAudioRenditions(master, "sub"); got != master {
		t.Errorf("no matching rendition must leave the playlist unchanged:\n%s", got)
	}
}

func TestStripAudioRenditionsSingleRenditionUnchanged(t *testing.T) {
	master := `#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="Japanese",DEFAULT=YES,LANGUAGE="ja",URI="ja.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO="a"
v.m3u8
`
	if got := stripAudioRenditions(master, "dub"); got != master {
		t.Errorf("single rendition must not be stripped even when unmatched:\n%s", got)
	}
}

func TestStripAudioRenditionsKeepsSubtitleRenditions(t *testing.T) {
	master := `#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="stereo",NAME="English",LANGUAGE="en",URI="en.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="stereo",NAME="Japanese",DEFAULT=YES,LANGUAGE="ja",URI="ja.m3u8"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="English",LANGUAGE="en",DEFAULT=YES,URI="eng.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO="stereo",SUBTITLES="subs"
v.m3u8
`
	out := stripAudioRenditions(master, "dub")
	if countAudio(out, "Japanese") != 0 || countAudio(out, "English") != 1 {
		t.Errorf("audio strip wrong:\n%s", out)
	}
	if !strings.Contains(out, "TYPE=SUBTITLES") {
		t.Errorf("subtitle rendition must never be touched:\n%s", out)
	}
	if strings.Count(out, "DEFAULT=YES") != 2 {
		t.Errorf("subtitle DEFAULT and promoted audio DEFAULT must both survive:\n%s", out)
	}
}

func TestStripAudioRenditionsQuotedCommaName(t *testing.T) {
	master := `#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="English (US, CC)",LANGUAGE="en",URI="en.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a",NAME="Japanese",LANGUAGE="ja",URI="ja.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO="a"
v.m3u8
`
	out := stripAudioRenditions(master, "sub")
	if countAudio(out, "Japanese") != 1 || countAudio(out, "English") != 0 {
		t.Errorf("quoted comma broke attribute parsing:\n%s", out)
	}
}

// proxySources must stamp the request lang onto every wrapped HLS URL so the
// proxy knows which track language to keep.
func TestProxySourcesAddsAudioLangParam(t *testing.T) {
	for _, tc := range []struct {
		lang    string
		wantAl  string
		wantNoA bool
	}{
		{lang: "sub", wantAl: "&al=sub"},
		{lang: "dub", wantAl: "&al=dub"},
		{lang: "", wantNoA: true},
	} {
		req := httptest.NewRequest("GET", "http://api.test/api/v1/servers?lang="+tc.lang, nil)
		srcs := []core.Source{{URL: "https://cdn.example/master.m3u8", Type: "hls"}}
		proxySources(req, srcs, map[string]string{"Referer": "https://x/"}, tc.lang)
		u := srcs[0].URL
		if !strings.Contains(u, "/api/v1/proxy?url=") {
			t.Fatalf("lang=%q: source not wrapped: %s", tc.lang, u)
		}
		if tc.wantNoA {
			if strings.Contains(u, "&al=") {
				t.Errorf("lang=%q: al param must be absent: %s", tc.lang, u)
			}
		} else if !strings.Contains(u, tc.wantAl) {
			t.Errorf("lang=%q: missing %s in %s", tc.lang, tc.wantAl, u)
		}
	}
}

// The rewrite must propagate al onto child URIs so nested playlist fetches
// keep the strip context, and must strip before wrapping.
func TestRewritePropagatesAudioLang(t *testing.T) {
	h := &Handlers{}
	in := `#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="stereo",NAME="English",LANGUAGE="en",URI="en/playlist.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="stereo",NAME="Japanese",DEFAULT=YES,LANGUAGE="ja",URI="ja/playlist.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1000000,AUDIO="stereo"
720p.m3u8
`
	out := h.rewriteHLSPlaylist(in, "https://hls.krussdomi.com/manifest/x/master.m3u8", `{"Referer":"https://krussdomi.com/"}`, "https://api.test", "sub")
	if strings.Contains(out, "English") {
		t.Errorf("sub rewrite must drop the English rendition:\n%s", out)
	}
	if !strings.Contains(out, "&al=sub") {
		t.Errorf("child URIs must carry &al=sub:\n%s", out)
	}
	// No al → full passthrough of both renditions (other languages/unknown).
	out = h.rewriteHLSPlaylist(in, "https://hls.krussdomi.com/manifest/x/master.m3u8", `{"Referer":"https://krussdomi.com/"}`, "https://api.test", "")
	if !strings.Contains(out, "English") || !strings.Contains(out, "Japanese") {
		t.Errorf("without al both renditions must survive:\n%s", out)
	}
}
