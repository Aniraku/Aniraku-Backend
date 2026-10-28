package streaming

import (
	"testing"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

func TestCanonicalSubtitleLang(t *testing.T) {
	cases := map[string]string{
		// Production cases from api/v1/servers logs
		"English":                         "en",
		"english":                         "en",
		"eng":                             "en",
		"EN":                              "en",
		"English 2":                       "en",
		"Indonesian (- Bahasa Indonesia)": "id",
		"indonesian (- bahasa indonesia)": "id",
		"Malay (- Bahasa Melayu)":         "ms",
		"malay (- bahasa melayu)":         "ms",
		"Thai (- ภาษาไทย)":                "th",
		"thai (- ภาษาไทย)":                "th",
		"Vietnamese (- Tiếng Việt)":       "vi",
		"vietnamese (- tiếng việt)":       "vi",
		"Bahasa Indonesia":                "id",
		"Bahasa Melayu":                   "ms",
		"ภาษาไทย":                         "th",
		"Tiếng Việt":                      "vi",
		"ind":                             "id",
		"may":                             "ms",
		"msa":                             "ms",
		"tha":                             "th",
		"vie":                             "vi",
		"zh-hans":                         "zh-hans",
		"zh-Hans":                         "zh-hans",
		"en-us":                           "en",
		"es-419":                          "es",
		"pt-br":                           "pt-br",
		"Spanish":                         "es",
		"French":                          "fr",
		"German":                          "de",
		"Portuguese":                      "pt",
		"Arabic":                          "ar",
		"Hindi":                           "hi",
		"Japanese":                        "ja",
		"Korean":                          "ko",
		// Unknown or non-language hints stay empty, never "en"
		"":       "",
		"Signs":  "",
		"Forced": "",
		"sub":    "",
		"dub":    "",
		"CC":     "",
		"xx":     "",
	}
	for in, want := range cases {
		if got := canonicalSubtitleLang(in); got != want {
			t.Errorf("canonicalSubtitleLang(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildSubtitle(t *testing.T) {
	for _, tc := range []struct {
		name         string
		url          string
		explicitLang string
		label        string
		hint         string
		wantLang     string
		wantLabel    string
	}{
		// Explicit lang field wins and is normalized (animex cases)
		{"animex english", "https://x/eng-2.vtt", "english", "English", "", "en", "English"},
		{"animex eng", "https://x/f.vtt", "eng", "English", "", "en", "English"},
		{"animex lowercased label in lang", "https://x/ind-5.vtt", "indonesian (- bahasa indonesia)", "Indonesian (- Bahasa Indonesia)", "", "id (- Bahasa Indonesia)", "Indonesian (- Bahasa Indonesia)"},
		{"animex thai", "https://x/tha-3.vtt", "thai (- ภาษาไทย)", "Thai (- ภาษาไทย)", "", "th (- ภาษาไทย)", "Thai (- ภาษาไทย)"},
		{"animex vietnamese", "https://x/vie-4.vtt", "vietnamese (- tiếng việt)", "Vietnamese (- Tiếng Việt)", "", "vi (- Tiếng Việt)", "Vietnamese (- Tiếng Việt)"},
		// Decorated labels keep their distinguishing info so distinct tracks
		// don't collapse into the same label
		{"decorated indonesian", "https://6a8y6.broforgotsave.online/anime/a/b/subtitles/ind-5.vtt", "", "Indonesian (- Bahasa Indonesia)", "", "id (- Bahasa Indonesia)", "Indonesian (- Bahasa Indonesia)"},
		{"decorated malay", "https://x/subtitles/may-6.vtt", "", "Malay (- Bahasa Melayu)", "", "ms (- Bahasa Melayu)", "Malay (- Bahasa Melayu)"},
		{"decorated thai", "https://x/subtitles/tha-3.vtt", "", "Thai (- ภาษาไทย)", "", "th (- ภาษาไทย)", "Thai (- ภาษาไทย)"},
		{"decorated vietnamese", "https://x/subtitles/vie-4.vtt", "", "Vietnamese (- Tiếng Việt)", "", "vi (- Tiếng Việt)", "Vietnamese (- Tiếng Việt)"},
		// Native-script labels are kept as-is (distinct from each other)
		{"native thai", "https://x/tha-3.vtt", "", "ภาษาไทย", "", "th", "ภาษาไทย"},
		{"native vietnamese", "https://x/vie-4.vtt", "", "Tiếng Việt", "", "vi", "Tiếng Việt"},
		{"native indonesian", "https://x/ind-5.vtt", "", "Bahasa Indonesia", "", "id", "Bahasa Indonesia"},
		// Filename fallback when no lang/label signal
		{"filename eng", "https://x/eng-2.vtt", "", "", "", "en", "English"},
		{"filename ind", "https://x/ind-5.vtt", "", "", "", "id", "Indonesian"},
		{"filename th", "https://x/60596_th.srt", "", "", "", "th", "Thai"},
		{"filename zh-hans", "https://x/278072_zh-Hans.srt", "", "", "", "zh-hans", "Chinese"},
		// Distinct tracks keep distinct names
		{"english 2 preserved", "https://x/en2.vtt", "en", "English 2", "", "en", "English 2"},
		{"signs preserved", "https://x/eng-2.vtt", "", "Signs", "", "en", "Signs"},
		// Truly unknown stays empty
		{"unknown", "https://x/64b0e386970810335d81b379.vtt", "", "", "", "", ""},
		// Kaa hint for unnamed krussdomi tracks
		{"kaa unnamed", "https://subst.krussdomi.com/abc/64b0e386970810335d81b379.vtt", "", "", "en", "en", "English"},
	} {
		got := buildSubtitle(tc.url, tc.explicitLang, tc.label, tc.hint)
		if got.URL != tc.url || got.Lang != tc.wantLang || got.Label != tc.wantLabel {
			t.Errorf("%s: buildSubtitle = %+v, want lang=%q label=%q", tc.name, got, tc.wantLang, tc.wantLabel)
		}
	}
}

func TestLangCodeFromSubURL(t *testing.T) {
	cases := map[string]string{
		"https://subbl.krussdomi.com/abc/60596_th.srt":                 "th",
		"https://x/eng-2.vtt":                                          "en",
		"https://x/subtitles/ind-5.vtt":                                "id",
		"https://x/subtitles/may-6.vtt":                                "ms",
		"https://x/subtitles/tha-3.vtt":                                "th",
		"https://x/subtitles/vie-4.vtt":                                "vi",
		"https://x/278072_zh-Hans.srt":                                 "zh-hans",
		"https://x/eng-2.vtt?token=abc":                                "en",
		"https://subst.krussdomi.com/abc/64b0e386970810335d81b379.vtt": "",
		"https://hls.dramahot.top/p/subs/0zhvt79ncggnvom4.vtt":         "",
	}
	for in, want := range cases {
		if got := langCodeFromSubURL(in); got != want {
			t.Errorf("langCodeFromSubURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDisambiguateSubtitleLangsAppendsLabelSuffix(t *testing.T) {
	in := []core.Subtitle{
		{URL: "u1", Lang: "en", Label: "English"},
		{URL: "u2", Lang: "en", Label: "English Full Subtitles (Restyled By Simmsy)"},
		{URL: "u3", Lang: "en", Label: "English CR"},
		{URL: "u4", Lang: "en", Label: "English CR"},
		{URL: "u5", Lang: "es", Label: "Spanish"},
	}
	out := disambiguateSubtitleLangs(in)
	want := []string{
		"en",
		"en (Full Subtitles (Restyled By Simmsy))",
		"en (CR)",
		"en (CR 2)",
		"es",
	}
	for i, w := range want {
		if out[i].Lang != w {
			t.Fatalf("track %d: got lang %q, want %q", i, out[i].Lang, w)
		}
	}
}
