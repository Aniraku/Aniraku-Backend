package v1

import (
	"net/http/httptest"
	"testing"
)

// applyProxyQueryHeaders force-sets the hotlink referer per CDN host
// (measured: mp4upload file servers 403 mkissa.to and empty referers,
// 206 with the site referer; kwik CDNs behave the same for kwik.cx).
// Client-supplied headers always win.
func TestApplyProxyQueryHeadersReferer(t *testing.T) {
	for url, want := range map[string]string{
		"https://a6.mp4upload.com:183/d/x/video.mp4":       "https://mp4upload.com/",
		"https://www.mp4upload.com/embed-abc.html":         "https://mp4upload.com/",
		"https://vault-69.aniwatchtv.site/i/x/master.m3u8": "",
	} {
		req := httptest.NewRequest("GET", "/api/v1/proxy?url="+url, nil)
		applyProxyQueryHeaders(req, "")
		if got := req.Header.Get("Referer"); got != want {
			t.Fatalf("referer(%q) = %q, want %q", url, got, want)
		}
	}

	// Client headers take priority over the force-set.
	req := httptest.NewRequest("GET", "/api/v1/proxy?url=https://a6.mp4upload.com/x.mp4", nil)
	req.Header.Set("Referer", "https://custom.example/")
	applyProxyQueryHeaders(req, "")
	if got := req.Header.Get("Referer"); got != "https://custom.example/" {
		t.Fatalf("client referer overridden: %q", got)
	}

	// Kwik branch untouched.
	req = httptest.NewRequest("GET", "/api/v1/proxy?url=https://vault-10.uwucdn.top/x/master.m3u8", nil)
	applyProxyQueryHeaders(req, "")
	if got := req.Header.Get("Referer"); got != "https://kwik.cx/" {
		t.Fatalf("kwik referer = %q, want kwik.cx", got)
	}
}
