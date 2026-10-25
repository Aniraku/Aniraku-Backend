// Package anipm reconstructs, in Go, the portable logic of the ani.pm Player
// SDK v1 — the two-way client that ani.pm ships at
// https://ani.pm/embed/sdk/v1.js and documents at
// https://ani.pm/developers#quick-start.
//
// Provenance (recovered, not guessed):
//
//   - Reference artifact: https://ani.pm/embed/sdk/v1.js, "ani.pm Player SDK
//     v1.4.0", internal VERSION "1.4.1" (fetched 2026-10-25, 20492 bytes).
//     A byte-for-byte copy lives in testdata/sdk-v1.js and is executed by the
//     differential tests in this package.
//   - Type surface: https://ani.pm/embed/sdk/v1.d.ts (testdata/sdk-v1.d.ts).
//   - In-page docs: the Developers route bundle (assets/Developers-*.js),
//     sections "quick-start", "embed-url", "sdk", "events", "postmessage".
//
// What is reconstructed here is the SDK's dependency-free core:
//
//   - EmbedURL — embedUrl(options): the iframe URL builder, including the id
//     and publishable-key validation, boolean coercion and query encoding.
//   - Bool, JSNumber, JSString, FormEncode — the exact JavaScript coercions the
//     builder relies on (=== comparisons, Number(), String(), URLSearchParams).
//   - The postMessage protocol: Namespace/version envelope codec and a
//     transport-agnostic Client that mirrors the SDK's handshake, command
//     queue, pending-command timeouts and event dispatch.
//
// What is deliberately not ported: the DOM/iframe surface (element mounting,
// theater layout, fullscreen). That logic is inseparable from a browser and is
// exercised by the original artifact, not by a server.
//
// The Go implementation is a clean-room reconstruction from the two shipped
// artifacts above; it targets observable equivalence with the original for the
// covered inputs, verified by differential testing (see embedurl_test.go).
package anipm

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Constants mirrored from the SDK's module scope.
const (
	// Version is the SDK's reported version (the VERSION binding).
	Version = "1.4.1"
	// Namespace is the message envelope namespace (NS).
	Namespace = "anipm.player"
	// APIProtocol is the envelope protocol version (API).
	APIProtocol = 1
	// DefaultOrigin is the embed origin used when Options.Origin is unset.
	DefaultOrigin = "https://ani.pm"
	// CommandTimeout is how long a command waits for an answer before it is
	// rejected with a "timeout" error (COMMAND_TIMEOUT_MS).
	CommandTimeout = 15 * time.Second
	// SettingsKey is the localStorage key the SDK uses to mirror the viewer's
	// player settings into the host page (SETTINGS_KEY).
	SettingsKey = "anipm:player-settings:v1"
)

// Events lists every event name the SDK accepts, in its declared order
// (the EVENTS array). "*" additionally receives every event on the client side.
var Events = []string{
	"loaded", "ready", "play", "pause", "playing", "waiting", "seeking",
	"seeked", "timeupdate", "durationchange", "ended", "error",
	"episodechange", "languagechange", "episodes", "volumechange",
	"ratechange", "qualitychange", "subtitlechange", "fullscreenchange",
	"autonextchange", "autoskipchange", "autoplayblocked", "theaterchange",
	"ambientchange", "settings",
}

// KnownEvent reports whether name is one of the SDK's declared events.
func KnownEvent(name string) bool {
	for _, e := range Events {
		if e == name {
			return true
		}
	}
	return false
}

// Error mirrors the SDK's AniPMError: an Error carrying a machine-readable
// code (for example "invalid-anime", "timeout", "destroyed").
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

func newError(code, message string) *Error { return &Error{Code: code, Message: message} }

// Bool reproduces the SDK's bool() helper:
//
//	const bool = (value) => value === undefined || value === null ? undefined
//	  : value === true || value === 'true' || value === '1' || value === 1
//
// defined is false only for JavaScript undefined/null. Any other value is
// "defined": it is true exactly when it is the boolean true, the string "true"
// or "1", or the number 1; everything else (including false, "0", 0, "TRUE")
// is a defined false.
func Bool(value any) (result, defined bool) {
	switch v := value.(type) {
	case nil:
		return false, false
	case bool:
		return v, true
	case string:
		return v == "true" || v == "1", true
	case float64:
		return v == 1, true
	case float32:
		return v == 1, true
	case int:
		return v == 1, true
	case int8:
		return v == 1, true
	case int16:
		return v == 1, true
	case int32:
		return v == 1, true
	case int64:
		return v == 1, true
	case uint:
		return v == 1, true
	case uint8:
		return v == 1, true
	case uint16:
		return v == 1, true
	case uint32:
		return v == 1, true
	case uint64:
		return v == 1, true
	default:
		return false, true
	}
}

// JSString reproduces JavaScript String(value) for the scalar types the embed
// options can carry. Objects, arrays and functions are out of scope and yield
// "".
func JSString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		return numberToString(v)
	case float32:
		return numberToString(float64(v))
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	default:
		return ""
	}
}

// numberToString formats a float64 the way JavaScript's String(number) does for
// the integer range the embed uses (plain decimal digits). Non-integral and
// out-of-range values fall back to a shortest round-trip format.
func numberToString(f float64) string {
	if f == 0 {
		return "0"
	}
	if !math.IsInf(f, 0) && !math.IsNaN(f) && f == math.Trunc(f) && math.Abs(f) < 1e21 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// JSNumber reproduces JavaScript Number(value) for the scalar types the embed
// options can carry: undefined/null yield NaN, booleans yield 1/0, strings are
// trimmed and parsed (empty string is 0, "0x…" is hexadecimal, "Infinity" is
// infinite) and non-numeric strings yield NaN.
func JSNumber(value any) float64 {
	switch v := value.(type) {
	case nil:
		return math.NaN()
	case string:
		return stringToNumber(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	default:
		return math.NaN()
	}
}

func stringToNumber(s string) float64 {
	t := strings.TrimSpace(s)
	if t == "" {
		return 0
	}
	switch t {
	case "Infinity", "+Infinity":
		return math.Inf(1)
	case "-Infinity":
		return math.Inf(-1)
	}
	if len(t) > 2 && (t[0] == '0') && (t[1] == 'x' || t[1] == 'X') {
		if i, err := strconv.ParseInt(t[2:], 16, 64); err == nil {
			return float64(i)
		}
		return math.NaN()
	}
	if f, err := strconv.ParseFloat(t, 64); err == nil {
		return f
	}
	return math.NaN()
}

// IsSafeInteger mirrors Number.isSafeInteger.
func IsSafeInteger(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0) && f == math.Trunc(f) && math.Abs(f) <= 9007199254740991
}

// FormEncode percent-encodes a value exactly as URLSearchParams.toString() does
// under the application/x-www-form-urlencoded serializer: ASCII alphanumerics
// and '*', '-', '.', '_' pass through, a space becomes '+', every other byte
// becomes %XX with uppercase hex. Multi-byte UTF-8 is encoded per byte.
func FormEncode(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == ' ':
			b.WriteByte('+')
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '*':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// numericZero reports whether v is the JavaScript number 0 (any integer type or
// float). Used by the embed builder's strict `=== 0` check for options.adult.
func numericZero(v any) bool {
	switch n := v.(type) {
	case float64:
		return n == 0
	case float32:
		return n == 0
	case int:
		return n == 0
	case int8:
		return n == 0
	case int16:
		return n == 0
	case int32:
		return n == 0
	case int64:
		return n == 0
	case uint:
		return n == 0
	case uint8:
		return n == 0
	case uint16:
		return n == 0
	case uint32:
		return n == 0
	case uint64:
		return n == 0
	default:
		return false
	}
}

// jsTruthy approximates JavaScript truthiness for the `||` fallbacks the
// builder uses (empty string, null/undefined, 0 and false are falsy).
func jsTruthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case string:
		return x != ""
	case bool:
		return x
	default:
		if numericZero(v) {
			return false
		}
		f := JSNumber(v)
		return !math.IsNaN(f)
	}
}
