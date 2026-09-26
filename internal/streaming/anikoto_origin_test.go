package streaming

import (
	"testing"
)

// The /videojs/stream/... player URLs must resolve API calls to the host
// root, not to a phantom /videojs/stream/getSourcesNew (404 on every edge,
// misreported as blocked). Regression pin for the OGFLix re-enable, whose
// server list is full of videojs embeds.
func TestMegaplayOrigin(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "videojs nested path",
			url:  "https://megaplay.buzz/videojs/stream/s-2/12352/sub?autoplay=true",
			want: "https://megaplay.buzz",
		},
		{
			name: "stream page",
			url:  "https://megaplay.buzz/stream/ani/21/1/sub",
			want: "https://megaplay.buzz",
		},
		{
			name: "embed page",
			url:  "https://megaplay.buzz/e/abc123?v=1",
			want: "https://megaplay.buzz",
		},
		{
			name: "other host keeps split",
			url:  "https://cdn.example/videojs/stream/x/sub",
			want: "https://cdn.example/videojs",
		},
		{
			name: "other host plain",
			url:  "https://cdn.example/e/abc",
			want: "https://cdn.example",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := megaplayOrigin(tc.url); got != tc.want {
				t.Fatalf("megaplayOrigin(%q) = %q, want %q", tc.url, got, tc.want)
			}
		})
	}
}
