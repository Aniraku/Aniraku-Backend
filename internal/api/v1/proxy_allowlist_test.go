package v1

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
)

func TestIsAllowedProxyHost(t *testing.T) {

	allowed := []string{
		"hls.anidb.app",
		"vidtub.akirax.buzz",
		"vidtub.shiora.site",
		"cdn.uwucdn.com",
		"www.animegg.org",
		"video.wixstatic.com",
		"repackager.wixmp.com",
		"storage.googleapis.com",
		"cdn.miruro.tv",
		"185.237.106.79",
		// Observed live 2026-08-05 via the Miruro API
		"tools.fast4speed.rsvp",
		"vault-10.uwucdn.top",
		"morning-credit-3bcc.vibevibe.workers.dev",
		"a1.mp4upload.com",
		"vidtub.kotocdn.site",
		"vivibebe.site",
	}
	blocked := []string{
		"example.com",
		"www.google.com",
		"evil-anidb.app",
		"notanidb.app",
		"anidb.app.evil.com",
		"anidbapp",
		"",
		"8.8.8.8",
		"127.0.0.1",
	}

	for _, host := range allowed {
		if !isAllowedProxyHost(host) {
			t.Errorf("isAllowedProxyHost(%q) = false, want true", host)
		}
	}
	for _, host := range blocked {
		if isAllowedProxyHost(host) {
			t.Errorf("isAllowedProxyHost(%q) = true, want false", host)
		}
	}
}

func TestProxyAllowlistEnvOverride(t *testing.T) {
	os.Setenv("ANIRAKU_PROXY_CDN_ALLOWLIST", "mycdn.example.net,cdn2.example.net")
	t.Cleanup(func() {
		os.Unsetenv("ANIRAKU_PROXY_CDN_ALLOWLIST")
		resetProxyCDNList()
	})
	resetProxyCDNList()

	if !isAllowedProxyHost("media.mycdn.example.net") {
		t.Error("env-added suffix not honored")
	}
	if !isAllowedProxyHost("hls.anidb.app") {
		t.Error("default suffix lost after env override")
	}
}

// TestProxyRejectsNonCDNPublicHost verifies the proxy refuses a public
// host that is not on the CDN allowlist (no longer a general relay).
func TestProxyRejectsNonCDNPublicHost(t *testing.T) {
	h := &Handlers{}
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/proxy?url="+url.QueryEscape("https://example.com/file.m3u8"), nil)
	rec := httptest.NewRecorder()
	h.Proxy(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("Proxy status = %d, want 403 (host not on CDN allowlist)", rec.Code)
	}
}
