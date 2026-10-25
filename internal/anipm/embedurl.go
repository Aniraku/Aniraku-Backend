package anipm

import (
	"regexp"
	"strconv"
	"strings"
)

// Options mirrors the object accepted by the SDK's embedUrl(options) and the
// Player constructor. Every field is intentionally `any` so that the exact
// JavaScript coercion rules apply: callers may pass typed Go values (as the
// constructor does) or the raw strings a DOM data-* attribute carries (as
// mountAll does). A nil field is JavaScript `undefined`.
type Options struct {
	Key       any `json:"key"`
	Anime     any `json:"anime"`
	Episode   any `json:"episode"`
	Lang      any `json:"lang"`
	Hardsub   any `json:"hardsub"`
	Start     any `json:"start"`
	Autoplay  any `json:"autoplay"`
	Muted     any `json:"muted"`
	Poster    any `json:"poster"`
	AutoNext  any `json:"autoNext"`
	AutoSkip  any `json:"autoSkip"`
	Theater   any `json:"theater"`
	Episodes  any `json:"episodes"`
	Color     any `json:"color"`
	Subtitles any `json:"subtitles"`
	API       any `json:"api"`
	Origin    any `json:"origin"`
	Adult     any `json:"adult"`
}

var (
	// /^pk_live_[0-9A-Za-z]{24}$/
	publishableKeyRE = regexp.MustCompile(`^pk_live_[0-9A-Za-z]{24}$`)
	// /^(?:[1-9]\d{0,9}|mal-[1-9]\d{0,9})$/
	animeIDRE = regexp.MustCompile(`^(?:[1-9][0-9]{0,9}|mal-[1-9][0-9]{0,9})$`)
	// /^#?[0-9a-fA-F]{6}$/
	colorRE = regexp.MustCompile(`^#?[0-9a-fA-F]{6}$`)
	// /\/+$/
	trailingSlashRE = regexp.MustCompile(`/+$`)
)

type queryParam struct{ key, value string }

// EmbedURL is a faithful port of the SDK's embedUrl(options): it returns the
// iframe URL for the given options, or an *Error whose Code is one of
// "invalid-key", "invalid-anime" or "invalid-episode".
//
// Without a publishable key the URL is the open form
//
//	https://ani.pm/embed/ani/<anilistId>/<episode>/<sub|dub>
//	https://ani.pm/embed/mal/<malId>/<episode>/<sub|dub>
//
// With a key it is the app form
//
//	https://ani.pm/embed/v1/<anilistId>/<episode>?key=pk_live_…
//
// Query parameters are emitted in the same order and with the same encoding as
// the original.
func EmbedURL(o Options) (string, error) {
	// Publishable key: validated only when present and non-empty.
	key := ""
	if o.Key != nil {
		key = JSString(o.Key)
		if key != "" && !publishableKeyRE.MatchString(key) {
			return "", newError("invalid-key",
				"options.key, when given, must be your publishable key (pk_live_…). Leave it out for an open embed.")
		}
	}
	hasKey := key != ""

	// anime: String(o.anime == null ? '' : o.anime).trim()
	anime := strings.TrimSpace(JSString(o.Anime))
	if !animeIDRE.MatchString(anime) {
		return "", newError("invalid-anime",
			"options.anime must be an AniList id (154587) or 'mal-<id>'.")
	}

	// episode: o.episode == null ? 1 : Number(o.episode)
	episode := 1.0
	if o.Episode != nil {
		episode = JSNumber(o.Episode)
	}
	if !IsSafeInteger(episode) || episode < 1 {
		return "", newError("invalid-episode",
			"options.episode must be a positive whole number.")
	}

	// origin: String(o.origin || DEFAULT_ORIGIN).replace(/\/+$/,'')
	origin := DefaultOrigin
	if jsTruthy(o.Origin) {
		origin = trailingSlashRE.ReplaceAllString(JSString(o.Origin), "")
	}

	// lang: o.lang === 'dub' ? 'dub' : 'sub'
	lang := "sub"
	if s, ok := o.Lang.(string); ok && s == "dub" {
		lang = "dub"
	}

	var params []queryParam
	var path string
	if hasKey {
		path = "/embed/v1/" + anime + "/" + strconv.FormatInt(int64(episode), 10)
		params = append(params, queryParam{"key", key})
		if s, ok := o.Lang.(string); ok && (s == "dub" || s == "sub") {
			params = append(params, queryParam{"lang", s})
		}
	} else if strings.HasPrefix(anime, "mal-") {
		path = "/embed/mal/" + anime[4:] + "/" + strconv.FormatInt(int64(episode), 10) + "/" + lang
	} else {
		path = "/embed/ani/" + anime + "/" + strconv.FormatInt(int64(episode), 10) + "/" + lang
	}

	// adult: o.adult === false || o.adult === 'false' || o.adult === '0' || o.adult === 0
	if b, ok := o.Adult.(bool); (ok && !b) ||
		o.Adult == "false" || o.Adult == "0" || numericZero(o.Adult) {
		params = append(params, queryParam{"adult", "0"})
	}

	addBool := func(field any, name string) {
		if v, defined := Bool(field); defined {
			if v {
				params = append(params, queryParam{name, "1"})
			} else {
				params = append(params, queryParam{name, "0"})
			}
		}
	}
	addBool(o.Hardsub, "hardsub")

	if n := JSNumber(o.Start); n > 0 {
		params = append(params, queryParam{"start", strconv.FormatInt(int64(n), 10)})
	}

	addBool(o.Autoplay, "autoplay")
	addBool(o.Muted, "muted")
	addBool(o.Poster, "poster")
	addBool(o.AutoNext, "autonext")
	addBool(o.AutoSkip, "autoskip")
	addBool(o.Theater, "theater")
	addBool(o.Episodes, "episodes")

	if s, ok := o.Color.(string); ok && colorRE.MatchString(s) {
		params = append(params, queryParam{"color", strings.Replace(s, "#", "", 1)})
	}
	if s, ok := o.Subtitles.(string); ok && s != "" {
		params = append(params, queryParam{"subtitles", s})
	}
	if b, ok := o.API.(bool); ok && !b {
		params = append(params, queryParam{"api", "0"})
	}

	var search strings.Builder
	for i, p := range params {
		if i > 0 {
			search.WriteByte('&')
		}
		search.WriteString(p.key)
		search.WriteByte('=')
		search.WriteString(FormEncode(p.value))
	}
	if search.Len() == 0 {
		return origin + path, nil
	}
	return origin + path + "?" + search.String(), nil
}
