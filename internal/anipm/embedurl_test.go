package anipm

import "testing"

func mustEmbedURL(t *testing.T, o Options) string {
	t.Helper()
	got, err := EmbedURL(o)
	if err != nil {
		t.Fatalf("EmbedURL(%+v) unexpected error: %v", o, err)
	}
	return got
}

func mustEmbedCode(t *testing.T, o Options) string {
	t.Helper()
	_, err := EmbedURL(o)
	if err == nil {
		t.Fatalf("EmbedURL(%+v) expected error, got none", o)
	}
	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("EmbedURL(%+v) error type = %T, want *Error", o, err)
	}
	return e.Code
}

// TestEmbedURLCases pins behavior to the original SDK. Every expected string
// below was produced by running embedUrl from testdata/sdk-v1.js under Node.
func TestEmbedURLCases(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string
	}{
		{"open basic", Options{Anime: 154587, Episode: 1},
			"https://ani.pm/embed/ani/154587/1/sub"},
		{"mal dub", Options{Anime: "mal-52991", Episode: 12, Lang: "dub"},
			"https://ani.pm/embed/mal/52991/12/dub"},
		{"options ordering", Options{Anime: "154587", Episode: 1, Color: "#A1B2C3",
			Subtitles: "English (US)", Start: "10.9", Autoplay: false, Muted: true, API: false},
			"https://ani.pm/embed/ani/154587/1/sub?start=10&autoplay=0&muted=1&color=A1B2C3&subtitles=English+%28US%29&api=0"},
		{"partner key", Options{Anime: "154587", Episode: 1, Key: "pk_live_" + rep("a", 24)},
			"https://ani.pm/embed/v1/154587/1?key=pk_live_aaaaaaaaaaaaaaaaaaaaaaaa"},
		{"partner key sub", Options{Anime: "154587", Episode: 1, Key: "pk_live_A1b2C3d4E5f6G7h8I9j0K1l2", Lang: "sub"},
			"https://ani.pm/embed/v1/154587/1?key=pk_live_A1b2C3d4E5f6G7h8I9j0K1l2&lang=sub"},
		{"booleans", Options{Anime: 154587, Episode: 1, Adult: false, Hardsub: "1",
			AutoNext: true, AutoSkip: 0, Theater: "true", Episodes: false, Poster: "0"},
			"https://ani.pm/embed/ani/154587/1/sub?adult=0&hardsub=1&poster=0&autonext=1&autoskip=0&theater=1&episodes=0"},
		{"string episode + trailing origin", Options{Anime: 154587, Episode: "3", Origin: "https://cdn.example.com///"},
			"https://cdn.example.com/embed/ani/154587/3/sub"},
		{"whitespace anime", Options{Anime: " 154587 ", Episode: 1},
			"https://ani.pm/embed/ani/154587/1/sub"},
		{"unicode subtitles", Options{Anime: 154587, Episode: 1, Subtitles: "日本語 ~*'()!"},
			"https://ani.pm/embed/ani/154587/1/sub?subtitles=%E6%97%A5%E6%9C%AC%E8%AA%9E+%7E*%27%28%29%21"},
		{"invalid color dropped", Options{Anime: 154587, Episode: 1, Color: "zzzzzz"},
			"https://ani.pm/embed/ani/154587/1/sub"},
		{"empty key is open", Options{Anime: 154587, Episode: 1, Key: ""},
			"https://ani.pm/embed/ani/154587/1/sub"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustEmbedURL(t, tc.opts); got != tc.want {
				t.Errorf("EmbedURL =\n  %q\nwant\n  %q", got, tc.want)
			}
		})
	}

	errCases := []struct {
		name string
		opts Options
		code string
	}{
		{"short key", Options{Anime: "154587", Episode: 1, Key: "pk_live_short"}, "invalid-key"},
		{"mal-0", Options{Anime: "mal-0", Episode: 1}, "invalid-anime"},
		{"bool anime", Options{Anime: true, Episode: 1}, "invalid-anime"},
		{"episode zero", Options{Anime: 154587, Episode: 0}, "invalid-episode"},
		{"episode fraction", Options{Anime: 154587, Episode: 1.5}, "invalid-episode"},
	}
	for _, tc := range errCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mustEmbedCode(t, tc.opts); got != tc.code {
				t.Errorf("error code = %q, want %q", got, tc.code)
			}
		})
	}
}

func rep(s string, n int) string {
	out := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

func TestBool(t *testing.T) {
	cases := []struct {
		in             any
		value, defined bool
	}{
		{nil, false, false},
		{true, true, true},
		{false, false, true},
		{"true", true, true},
		{"1", true, true},
		{"0", false, true},
		{"TRUE", false, true},
		{1, true, true},
		{0, false, true},
		{2, false, true},
		{"yes", false, true},
	}
	for _, tc := range cases {
		v, d := Bool(tc.in)
		if v != tc.value || d != tc.defined {
			t.Errorf("Bool(%#v) = (%v,%v), want (%v,%v)", tc.in, v, d, tc.value, tc.defined)
		}
	}
}

func TestFormEncode(t *testing.T) {
	cases := map[string]string{
		"abc":          "abc",
		"English (US)": "English+%28US%29",
		"~*'()!":       "%7E*%27%28%29%21",
		"-_.*":         "-_.*",
		"日本":           "%E6%97%A5%E6%9C%AC",
	}
	for in, want := range cases {
		if got := FormEncode(in); got != want {
			t.Errorf("FormEncode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJSNumber(t *testing.T) {
	if v := JSNumber(""); v != 0 {
		t.Errorf(`JSNumber("") = %v, want 0`, v)
	}
	if v := JSNumber(" 12 "); v != 12 {
		t.Errorf(`JSNumber(" 12 ") = %v, want 12`, v)
	}
	if v := JSNumber("0x1A"); v != 26 {
		t.Errorf(`JSNumber("0x1A") = %v, want 26`, v)
	}
	if v := JSNumber(true); v != 1 {
		t.Errorf("JSNumber(true) = %v, want 1", v)
	}
	if v := JSNumber("abc"); !isNaN(v) {
		t.Errorf(`JSNumber("abc") = %v, want NaN`, v)
	}
}

func isNaN(f float64) bool { return f != f }
