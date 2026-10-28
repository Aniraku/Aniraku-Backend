package tmdb

import "testing"

func TestPickLogoPathPrefersEnglishThenTextless(t *testing.T) {
	cases := []struct {
		name  string
		logos []any
		want  string
	}{
		{
			name: "english wins over textless and other",
			logos: []any{
				map[string]any{"file_path": "/japanese.svg", "iso_639_1": "ja"},
				map[string]any{"file_path": "/none.png", "iso_639_1": nil},
				map[string]any{"file_path": "/english.svg", "iso_639_1": "en"},
			},
			want: "/english.svg",
		},
		{
			name: "textless wins when no english",
			logos: []any{
				map[string]any{"file_path": "/ja.png", "iso_639_1": "ja"},
				map[string]any{"file_path": "/none.png", "iso_639_1": nil},
			},
			want: "/none.png",
		},
		{
			name:  "any language is last resort",
			logos: []any{map[string]any{"file_path": "/de.png", "iso_639_1": "de"}},
			want:  "/de.png",
		},
		{
			name:  "empty logos",
			logos: nil,
			want:  "",
		},
		{
			name:  "entry without path is skipped",
			logos: []any{map[string]any{"iso_639_1": "en"}},
			want:  "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickLogoPath(tc.logos); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSafeStillUrlAcceptsLogoExtensions(t *testing.T) {
	if got := safeStillUrl("/abc123.svg"); got == "" {
		t.Fatal("svg logo path rejected, want accepted")
	}
	if got := safeStillUrl("/abc123.png"); got == "" {
		t.Fatal("png logo path rejected, want accepted")
	}
	if got := safeStillUrl("/../evil.svg"); got != "" {
		t.Fatalf("path traversal accepted: %q", got)
	}
}
