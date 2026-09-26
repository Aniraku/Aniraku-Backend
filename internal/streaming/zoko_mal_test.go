package streaming

import "testing"

func TestParseAniListMAL(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int
	}{
		{
			name: "valid idMal",
			body: `{"data":{"Media":{"idMal":40750}}}`,
			want: 40750,
		},
		{
			name: "null idMal (unmapped title)",
			body: `{"data":{"Media":{"idMal":null}}}`,
			want: 0,
		},
		{
			name: "missing field",
			body: `{"data":{"Media":{}}}`,
			want: 0,
		},
		{
			name: "graphql error payload",
			body: `{"errors":[{"message":"Not Found."}],"data":null}`,
			want: 0,
		},
		{
			name: "zero idMal",
			body: `{"data":{"Media":{"idMal":0}}}`,
			want: 0,
		},
		{
			name: "negative idMal",
			body: `{"data":{"Media":{"idMal":-1}}}`,
			want: 0,
		},
		{
			name: "html challenge page is not JSON",
			body: `<!DOCTYPE html><html><title>Just a moment...</title></html>`,
			want: 0,
		},
		{
			name: "empty body",
			body: ``,
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseAniListMAL([]byte(tc.body)); got != tc.want {
				t.Fatalf("parseAniListMAL(%s) = %d, want %d", tc.name, got, tc.want)
			}
		})
	}
}
