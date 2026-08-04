package middleware

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestRealIPIgnoresXFFWithoutTrustedProxies verifies fail-closed behavior:
// with no ANIRAKU_TRUSTED_PROXY_CIDRS configured, X-Forwarded-For is ignored
// entirely and the socket peer is used, so a client cannot forge its identity
// or mint a fresh rate-limit bucket.
func TestRealIPIgnoresXFFWithoutTrustedProxies(t *testing.T) {
	os.Unsetenv("ANIRAKU_TRUSTED_PROXY_CIDRS")
	t.Cleanup(func() { os.Unsetenv("ANIRAKU_TRUSTED_PROXY_CIDRS") })

	cases := []struct {
		name       string
		remoteAddr string
		xff        string
		want       string
	}{
		{"no xff uses peer", "1.2.3.4:5678", "", "1.2.3.4"},
		{"single xff entry ignored", "100.20.1.1:1234", "9.9.9.9", "100.20.1.1"},
		{"spoofed chain ignored", "100.20.1.1:1234", "1.1.1.1, 2.2.2.2, 9.9.9.9", "100.20.1.1"},
		{"malformed entries ignored", "100.20.1.1:1234", "not-an-ip, 9.9.9.9", "100.20.1.1"},
		{"blank entries ignored", "100.20.1.1:1234", " , 9.9.9.9, ", "100.20.1.1"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = c.remoteAddr
			if c.xff != "" {
				req.Header.Set("X-Forwarded-For", c.xff)
			}
			got := ""
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.RemoteAddr
			})
			RealIP(next).ServeHTTP(httptest.NewRecorder(), req)
			if got != c.want+":0" {
				t.Errorf("RealIP RemoteAddr = %q, want %q", got, c.want+":0")
			}
		})
	}
}

func TestRealIPSkipsTrustedProxies(t *testing.T) {
	os.Setenv("ANIRAKU_TRUSTED_PROXY_CIDRS", "100.20.0.0/16,3.210.0.0/16")
	t.Cleanup(func() { os.Unsetenv("ANIRAKU_TRUSTED_PROXY_CIDRS") })

	// Rightmost entry is a trusted proxy; the first untrusted entry left of it
	// is the real client.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "100.20.1.1:1234"
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 3.210.9.9, 100.20.2.2")

	got := ""
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.RemoteAddr
	})
	RealIP(next).ServeHTTP(httptest.NewRecorder(), req)
	if got != "1.2.3.4:0" {
		t.Errorf("RealIP RemoteAddr = %q, want 1.2.3.4:0", got)
	}
}
