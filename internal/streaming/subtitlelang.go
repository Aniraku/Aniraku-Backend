package streaming

import (
	"regexp"
	"strings"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// Subtitle language resolution is shared by every provider because each
// upstream names its tracks differently: AnimeX echoes its own lang field
// verbatim ("english", "indonesian (- bahasa indonesia)", "eng"), the
// megaplay family ships decorated labels ("Thai (- ภาษาไทย)"), krussdomi
// encodes the code in the filename (60596_th.srt) and some files are
// content-hashed with no signal at all.
//
// Two defects motivated this file. mapSubtitleLang used to return "en" for
// any label outside an eight-entry switch, so a Thai track fetched with the
// label "Thai (- ภาษาไทย)" was advertised as lang:"en" — every provider that
// shared it (anikoto, vidnest, megavid, tensho, zoko, lee) reported English
// for Indonesian, Malay, Thai and Vietnamese alike. And kaa carried the
// request language (a track *kind*, "sub") into Subtitle.Lang for unnamed
// files, producing lang:"sub" label:"sub", which is neither a language nor
// a usable display name.
//
// Resolution now has a fixed priority — explicit lang field, then label,
// then filename, then a provider-supplied hint — and an unresolvable
// language stays empty instead of being claimed as English.

// langAliases maps a lowercased language hint onto a canonical ISO 639-1
// code. It accepts English and native language names, ISO 639-1/639-2/639-3
// codes, and the decorated labels providers send.
var langAliases = map[string]string{
	// English names
	"english": "en", "spanish": "es", "castilian": "es", "french": "fr",
	"german": "de", "italian": "it", "portuguese": "pt", "arabic": "ar",
	"hindi": "hi", "indonesian": "id", "malay": "ms", "thai": "th",
	"vietnamese": "vi", "japanese": "ja", "korean": "ko", "chinese": "zh",
	"dutch": "nl", "russian": "ru", "turkish": "tr", "polish": "pl",
	"ukrainian": "uk", "romanian": "ro", "hungarian": "hu", "czech": "cs",
	"greek": "el", "hebrew": "he", "swedish": "sv", "danish": "da",
	"finnish": "fi", "norwegian": "no", "bengali": "bn", "filipino": "tl",
	"tagalog": "tl", "persian": "fa", "farsi": "fa", "tamil": "ta",
	"burmese": "my", "croatian": "hr", "serbian": "sr", "bulgarian": "bg",
	"slovak": "sk", "slovenian": "sl", "lithuanian": "lt", "latvian": "lv",
	"estonian": "ee", "catalan": "ca", "basque": "eu", "galician": "gl",
	"icelandic": "is", "irish": "ga", "welsh": "cy", "afrikaans": "af",
	"albanian": "sq", "armenian": "hy", "azerbaijani": "az", "belarusian": "be",
	"georgian": "ka", "kazakh": "kk", "macedonian": "mk", "mongolian": "mn",
	"nepali": "ne", "sinhala": "si", "swahili": "sw", "urdu": "ur",
	"punjabi": "pa", "marathi": "mr", "gujarati": "gu", "kannada": "kn",
	"malayalam": "ml", "telugu": "te", "oriya": "or", "assamese": "as",

	// Native names (labels sometimes arrive untranslated)
	"español": "es", "espanol": "es", "português": "pt", "portugues": "pt",
	"français": "fr", "francais": "fr", "deutsch": "de", "italiano": "it",
	"русский": "ru", "日本語": "ja", "한국어": "ko", "中文": "zh",
	"ไทย": "th", "tiếng việt": "vi", "tieng viet": "vi",
	"bahasa indonesia": "id", "bahasa melayu": "ms", "العربية": "ar",
	"فارسی": "fa", "বাংলা": "bn", "தமிழ்": "ta", "नेपाली": "ne",
	"nederlands": "nl", "türkçe": "tr", "turkce": "tr", "polski": "pl",
	"čeština": "cs", "cestina": "cs", "ελληνικά": "el", "עברית": "he",
	"svenska": "sv", "dansk": "da", "suomi": "fi", "norsk": "no",
	"magyar": "hu", "română": "ro", "romana": "ro", "українська": "uk",
	"简体中文": "zh", "繁體中文": "zh", "简体": "zh", "繁體": "zh",
	"ภาษาไทย": "th",

	// ISO 639-2/639-3 codes -> 639-1
	"eng": "en", "spa": "es", "fra": "fr", "fre": "fr", "deu": "de",
	"ger": "de", "ita": "it", "por": "pt", "ara": "ar", "hin": "hi",
	"ind": "id", "may": "ms", "msa": "ms", "tha": "th", "vie": "vi",
	"jpn": "ja", "kor": "ko", "zho": "zh", "chi": "zh", "nld": "nl",
	"dut": "nl", "rus": "ru", "tur": "tr", "pol": "pl", "ukr": "uk",
	"ron": "ro", "rum": "ro", "hun": "hu", "ces": "cs", "cze": "cs",
	"ell": "el", "gre": "el", "heb": "he", "swe": "sv", "dan": "da",
	"fin": "fi", "nor": "no", "ben": "bn", "fil": "tl", "tgl": "tl",
	"per": "fa", "fas": "fa", "tam": "ta", "bur": "my", "mya": "my",
	"hrv": "hr", "srp": "sr", "bul": "bg", "slk": "sk", "slo": "sk",
	"slv": "sl", "lit": "lt", "lav": "lv", "est": "ee", "cat": "ca",
	"eus": "eu", "glg": "gl", "isl": "is", "gle": "ga", "cym": "cy",
	"afr": "af", "sqi": "sq", "alb": "sq", "hye": "hy", "arm": "hy",
	"aze": "az", "bel": "be", "kat": "ka", "geo": "ka", "kaz": "kk",
	"mkd": "mk", "mon": "mn", "nep": "ne", "sin": "si", "swa": "sw",
	"urd": "ur", "pan": "pa", "mar": "mr", "guj": "gu", "kan": "kn",
	"mal": "ml", "tel": "te", "ori": "or", "asm": "as",

	// Canonical codes map to themselves
	"en": "en", "es": "es", "fr": "fr", "de": "de", "it": "it",
	"pt": "pt", "ar": "ar", "hi": "hi", "id": "id", "ms": "ms",
	"th": "th", "vi": "vi", "ja": "ja", "ko": "ko", "zh": "zh",
	"nl": "nl", "ru": "ru", "tr": "tr", "pl": "pl", "uk": "uk",
	"ro": "ro", "hu": "hu", "cs": "cs", "el": "el", "he": "he",
	"sv": "sv", "da": "da", "fi": "fi", "no": "no", "bn": "bn",
	"tl": "tl", "fa": "fa", "ta": "ta", "my": "my", "hr": "hr",
	"sr": "sr", "bg": "bg", "sk": "sk", "sl": "sl", "lt": "lt",
	"lv": "lv", "et": "et", "ca": "ca", "eu": "eu", "gl": "gl",
	"is": "is", "ga": "ga", "cy": "cy", "af": "af", "sq": "sq",
	"hy": "hy", "az": "az", "be": "be", "ka": "ka", "kk": "kk",
	"mk": "mk", "mn": "mn", "ne": "ne", "si": "si", "sw": "sw",
	"ur": "ur", "pa": "pa", "mr": "mr", "gu": "gu", "kn": "kn",
	"ml": "ml", "te": "te", "or": "or", "as": "as",

	// Script/region tags the client already understands
	"zh-hans": "zh-hans", "zh-hant": "zh-hant", "pt-br": "pt-br",
}

// langDisplayNames maps a canonical code to the English display name used
// when a provider ships no label of its own. Codes without an entry fall
// back to the code itself.
var langDisplayNames = map[string]string{
	"en": "English", "es": "Spanish", "fr": "French", "de": "German",
	"it": "Italian", "pt": "Portuguese", "ar": "Arabic", "hi": "Hindi",
	"id": "Indonesian", "ms": "Malay", "th": "Thai", "vi": "Vietnamese",
	"ja": "Japanese", "ko": "Korean", "zh": "Chinese", "nl": "Dutch",
	"ru": "Russian", "tr": "Turkish", "pl": "Polish", "uk": "Ukrainian",
	"ro": "Romanian", "hu": "Hungarian", "cs": "Czech", "el": "Greek",
	"he": "Hebrew", "sv": "Swedish", "da": "Danish", "fi": "Finnish",
	"no": "Norwegian", "bn": "Bengali", "tl": "Filipino", "fa": "Persian",
	"ta": "Tamil", "my": "Burmese", "hr": "Croatian", "sr": "Serbian",
	"bg": "Bulgarian", "sk": "Slovak", "sl": "Slovenian", "lt": "Lithuanian",
	"lv": "Latvian", "et": "Estonian", "ca": "Catalan", "eu": "Basque",
	"gl": "Galician", "is": "Icelandic", "ga": "Irish", "cy": "Welsh",
	"af": "Afrikaans", "sq": "Albanian", "hy": "Armenian", "az": "Azerbaijani",
	"be": "Belarusian", "ka": "Georgian", "kk": "Kazakh", "mk": "Macedonian",
	"mn": "Mongolian", "ne": "Nepali", "si": "Sinhala", "sw": "Swahili",
	"ur": "Urdu", "pa": "Punjabi", "mr": "Marathi", "gu": "Gujarati",
	"kn": "Kannada", "ml": "Malayalam", "te": "Telugu", "or": "Odia",
	"as": "Assamese", "zh-hans": "Chinese",
	"zh-hant": "Chinese Traditional", "pt-br": "Portuguese (Brazil)",
}

// subFileCodeRe pulls a language code out of a subtitle filename. The first
// group covers krussdomi's style (60596_th.srt, 278072_zh-Hans.srt); the
// second covers the megaplay family (eng-2.vtt, ind-5.vtt).
var (
	subFileCodeRe    = regexp.MustCompile(`[_-]([A-Za-z]{2,8}(?:-[A-Za-z0-9]+)?)\.(?:srt|vtt)$`)
	subFileNumCodeRe = regexp.MustCompile(`[/_.-]([A-Za-z]{2,4})[-_]?\d*\.(?:srt|vtt)$`)
)

// canonicalSubtitleLang maps any upstream hint — an explicit lang field, a
// decorated label, a native-script name or a bare ISO code — onto a
// language tag. It returns "" when the hint carries no language
// information, and never guesses "en": an empty value tells the client the
// language is unknown, which is strictly better than claiming a Thai track
// is English.
func canonicalSubtitleLang(hint string) string {
	h := strings.ToLower(strings.TrimSpace(hint))
	if h == "" {
		return ""
	}
	// Strip decorations: "Indonesian (- Bahasa Indonesia)", "English [CC]".
	if i := strings.IndexAny(h, "(["); i > 0 {
		h = strings.TrimSpace(h[:i])
	}
	if h == "" {
		return ""
	}
	if c, ok := langAliases[h]; ok {
		return c
	}
	// First word: "vietnamese (- tiếng việt)" is already stripped above, but
	// labels also arrive as "Thai, English" or "English 2".
	if tok, _, _ := strings.Cut(h, " "); tok != h {
		if c, ok := langAliases[tok]; ok {
			return c
		}
	}
	// "zh-hans", "zh-hant", "pt-br": keep tags the client understands.
	// "en-us", "es-419": collapse region tags to the base 639-1 code.
	if strings.Contains(h, "-") {
		if whole, ok := langAliases[h]; ok {
			return whole
		}
		if base, _, _ := strings.Cut(h, "-"); base != "" {
			if c, ok := langAliases[base]; ok {
				return c
			}
		}
		return ""
	}
	return ""
}

// langDisplay returns the English display name for a canonical code. An
// empty code yields an empty name so callers can tell "unknown" apart from
// a real label.
func langDisplay(code string) string {
	if code == "" {
		return ""
	}
	if n, ok := langDisplayNames[code]; ok {
		return n
	}
	return code
}

// langCodeFromSubURL recovers a language code from a subtitle URL, ignoring
// query strings and fragments so signed URLs still parse.
func langCodeFromSubURL(rawURL string) string {
	base := rawURL
	if i := strings.IndexAny(base, "?#"); i >= 0 {
		base = base[:i]
	}
	if m := subFileCodeRe.FindStringSubmatch(base); m != nil {
		if c := canonicalSubtitleLang(m[1]); c != "" {
			return c
		}
	}
	if m := subFileNumCodeRe.FindStringSubmatch(base); m != nil {
		if c := canonicalSubtitleLang(m[1]); c != "" {
			return c
		}
	}
	return ""
}

// buildSubtitle resolves one track. Priority: the provider's explicit lang
// field, then its label, then the filename, then hint — which is reserved
// for providers that know their upstream's convention (kaa's unnamed
// krussdomi track is English) rather than a blanket default.
//
// Labels are normalized to Kaa-quality English display names when the
// upstream label is just a language name ("Thai (- ภาษาไทย)" -> "Thai",
// "ภาษาไทย" -> "Thai", "Bahasa Indonesia" -> "Indonesian"). Labels that
// carry extra distinguishing info ("English 2", "Signs") are preserved so
// distinct tracks stay distinct.
func buildSubtitle(rawURL, explicitLang, label, hint string) core.Subtitle {
	lang := canonicalSubtitleLang(explicitLang)
	if lang == "" {
		lang = canonicalSubtitleLang(label)
	}
	if lang == "" {
		lang = langCodeFromSubURL(rawURL)
	}
	if lang == "" {
		lang = canonicalSubtitleLang(hint)
	}

	display := strings.TrimSpace(label)
	if display == "" {
		return core.Subtitle{URL: rawURL, Lang: lang, Label: langDisplay(lang)}
	}
	if lang != "" && canonicalSubtitleLang(display) == lang {
		stripped := display
		if i := strings.IndexAny(stripped, "(["); i > 0 {
			stripped = strings.TrimSpace(stripped[:i])
		}
		_, isLangName := langAliases[strings.ToLower(stripped)]
		hasNonASCII := false
		for _, r := range display {
			if r > 127 {
				hasNonASCII = true
				break
			}
		}
		// Pure language name (possibly decorated or native-script):
		// use the standard English display name.
		// "English 2" is not an alias key and has no decoration,
		// so it survives intact.
		if isLangName || hasNonASCII || stripped != display {
			return core.Subtitle{URL: rawURL, Lang: lang, Label: langDisplay(lang)}
		}
	}
	return core.Subtitle{URL: rawURL, Lang: lang, Label: display}
}
