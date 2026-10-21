package streaming

import (
	"testing"
)

// Tokens observed live on 185874 ep9: base64URL, the alphabet
// carries '-' and '_'. The old single-URL regex (base64 alphabet only) could
// never terminate a match on such tokens, silently yielding zero subtitles.
const anixoLiveTokenEn = "NSwrbztrHFBVWlZQXldcUV4XBBkaHwNLXVxCFE4OTnQ5LjI4MBIGDRcXBBACRgYEBwUDC0ARHxseEVoUFks-Y2loaDtQBwdSVFVXBFAMUwkPXQ1ZRkYWQBEQRlhOOz85bT8-UlBXUFdSV19dX1kKDg4MWUJDSxJCFEMTQHUoKT8qNhQNBxBLERQGCwI1WzMIAAheBwYH"
const anixoLiveTokenDe = "NSwrbztrHFBVWlZQXldcUV4XBBkaHwNLXVxCFE4OTnQ5LjI4MBIGDRcXBBACRgYEBwUDC0ARHxseEVoUFks-Y2loaDtQBwdSVFVXBFAMUwkPXQ1ZRkYWQBEQRlhOOz85bT8-UlBXUFdSV19dX1kKDg4MWUJDSxJCFEMTQHUoKT8qNhQNBxBLERQGCwI1WjMKCx1eBwYH"

func anixoTrackURL(token string) string {
	return "https://anixo.buzz/api/stream/vtt?t=" + token
}

func TestParseAnixoSubtitles(t *testing.T) {
	live := `data.sources=[{"file":"https://anixo.buzz/api/stream/m3u8?t=NSwrbztr"}];` +
		`"sources":["https://anixo.buzz/api/stream/m3u8?t=NSwrbztrHFBVWlZQXldYXlkXBBkaHwNLXVwZEBEWCHQoNDQxLQFPFgwUSgQGWw1SXllbCl8WFENDREQVTx1jOT9tPWlWVgZQAQBWSF4IDg5cDA9dQURBQENETkJOaTo-PjxpUlNbAlIEUwNQRgcKHxkLHV4cQQZM"],` +
		`"subtitles":[{"url":"` + anixoTrackURL(anixoLiveTokenEn) + `","label":"English","default":true,"kind":"captions"},` +
		`{"url":"` + anixoTrackURL(anixoLiveTokenDe) + `","label":"German","default":false,"kind":"captions"}]`

	got := parseAnixoSubtitles(live)
	if len(got) != 2 {
		t.Fatalf("parseAnixoSubtitles(live shape) = %d tracks, want 2", len(got))
	}
	if got[0].URL != anixoTrackURL(anixoLiveTokenEn) {
		t.Fatalf("track[0].URL = %q, want the full base64URL token preserved", got[0].URL)
	}
	if got[0].Label != "English" {
		t.Fatalf("track[0].Label = %q, want English", got[0].Label)
	}
	if got[1].Label != "German" {
		t.Fatalf("track[1].Label = %q, want German", got[1].Label)
	}
}

func TestParseAnixoSubtitlesFallbackBareURL(t *testing.T) {
	// Older page shape: a bare vtt URL, no subtitles[] array, and the
	// token still carries base64URL chars, which the fallback regex must
	// now match.
	bare := `{other:1,"url":"` + anixoTrackURL(anixoLiveTokenEn) + `"}`
	got := parseAnixoSubtitles(bare)
	if len(got) != 1 {
		t.Fatalf("parseAnixoSubtitles(bare URL) = %d tracks, want 1", len(got))
	}
	if got[0].URL != anixoTrackURL(anixoLiveTokenEn) {
		t.Fatalf("fallback URL = %q, want the full base64URL token", got[0].URL)
	}
	if got[0].Label != "English" {
		t.Fatalf("fallback Label = %q, want English default", got[0].Label)
	}
}

func TestParseAnixoSubtitlesNone(t *testing.T) {
	// streamUrl-only page (episode ships no captions) must stay nil, not
	// an empty slice from a stray empty-array match.
	if got := parseAnixoSubtitles(`"streamUrl":"https://anixo.buzz/api/stream/m3u8?t=abc",
		"subtitles":[]`); got != nil {
		t.Fatalf("empty subtitles array = %d tracks, want nil", len(got))
	}
	if got := parseAnixoSubtitles(`<html>Unauthorized Embed Domain</html>`); got != nil {
		t.Fatalf("gate body = %d tracks, want nil", len(got))
	}
}
