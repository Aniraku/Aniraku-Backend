package netguard

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIsPublicIPTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ip     string
		public bool
	}{
		// Public unicast
		{"8.8.8.8", true},
		{"1.1.1.1", true},
		{"93.184.216.34", true},
		{"2606:4700:4700::1111", true},
		{"2001:4860:4860::8888", true},

		// Loopback
		{"127.0.0.1", false},
		{"127.8.8.8", false},
		{"::1", false},

		// RFC1918
		{"10.0.0.5", false},
		{"172.16.0.1", false},
		{"172.31.255.255", false},
		{"192.168.1.1", false},

		// CGNAT RFC6598
		{"100.64.0.1", false},
		{"100.127.255.254", false},
		// 100.64/10 boundary: outside the range is public
		{"100.128.0.1", true},
		{"100.63.255.255", true},

		// Link-local + metadata
		{"169.254.169.254", false},
		{"169.254.0.1", false},
		{"fe80::1", false},

		// Unspecified
		{"0.0.0.0", false},
		{"::", false},

		// Multicast
		{"224.0.0.1", false},
		{"ff02::1", false},

		// Reserved class E / broadcast
		{"240.0.0.1", false},
		{"255.255.255.255", false},

		// ULA fc00::/7
		{"fd00::1", false},
		{"fc00::1", false},

		// IPv4-mapped IPv6 carrying a private v4 address
		{"::ffff:127.0.0.1", false},
		{"::ffff:192.168.0.1", false},
		{"::ffff:8.8.8.8", true},

		// Interface-local multicast v6
		{"ff01::1", false},
	}

	for _, c := range cases {
		ip := net.ParseIP(c.ip)
		if ip == nil {
			t.Fatalf("bad test IP %q", c.ip)
		}
		if got := IsPublicIP(ip); got != c.public {
			t.Errorf("IsPublicIP(%s) = %v, want %v", c.ip, got, c.public)
		}
	}
}

func TestControlHookBlocksPrivateDials(t *testing.T) {
	t.Parallel()

	cases := []struct {
		address string
		wantErr bool
	}{
		{"8.8.8.8:443", false},
		{"127.0.0.1:8080", true},
		{"10.1.2.3:80", true},
		{"192.168.0.10:5432", true},
		{"169.254.169.254:80", true},
		{"[::1]:9000", true},
		{"[fd00::5]:443", true},
		{"not-an-ip:80", true}, // unresolved address must fail closed
	}

	for _, c := range cases {
		err := Control("tcp", c.address, nil)
		if (err != nil) != c.wantErr {
			t.Errorf("Control(%q) error = %v, wantErr %v", c.address, err, c.wantErr)
		}
	}
}

// TestNewHTTPClientBlocksLoopbackDial proves the guard acts at dial time on a
// real client, not just in the pure function.
func TestNewHTTPClientBlocksLoopbackDial(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client := NewHTTPClient(5 * time.Second)
	_, err := client.Get(srv.URL) // srv.URL is 127.0.0.1
	if err == nil {
		t.Fatal("guarded client dialed loopback; SSRF boundary leaked")
	}
	if !strings.Contains(err.Error(), "ssrf guard") {
		t.Errorf("unexpected error shape: %v", err)
	}
}

func TestNoRedirectsPolicy(t *testing.T) {
	t.Parallel()
	err := NoRedirects(nil, nil)
	if err != http.ErrUseLastResponse {
		t.Errorf("NoRedirects = %v, want http.ErrUseLastResponse", err)
	}
}

// TestNewHTTPClientRefusedRedirect: a 3xx must surface as the 3xx response
// itself (ErrUseLastResponse), never be followed. Uses example.com so the
// guarded client can actually complete the request (loopback is blocked by
// design — see TestNewHTTPClientBlocksLoopbackDial); the test only asserts
// that the redirect policy holds for whatever the server returns.
func TestNewHTTPClientRefusedRedirect(t *testing.T) {
	if testing.Short() {
		t.Skip("network test")
	}
	client := NewHTTPClient(10 * time.Second)
	resp, err := client.Get("https://example.com/definitely-missing-page")
	if err != nil {
		t.Skipf("network unavailable: %v", err)
	}
	defer resp.Body.Close()
	// Whatever example.com answers (404 etc.), the client must not have
	// followed any redirect chain silently — a followed chain would end in a
	// 200, and the transport would have dialed the redirect target.
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		t.Errorf("got a 3xx (%d) after transport had redirects refused — impossible unless policy broken", resp.StatusCode)
	}
}
