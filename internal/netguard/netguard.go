// Package netguard provides the network-level SSRF controls used by outbound
// HTTP clients. Validation at dial time makes the protection resilient to DNS
// rebinding because it inspects the concrete address immediately before the
// connection is opened.
package netguard

import (
	"fmt"
	"net"
	"net/http"
	"syscall"
)

// Control is suitable for net.Dialer.Control. It rejects connections to
// non-public IP addresses after hostname resolution and before a socket is
// connected.
func Control(_ string, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("ssrf guard: cannot parse address %q: %w", address, err)
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("ssrf guard: unresolved address %q", host)
	}
	if !isPublicIP(ip) {
		return fmt.Errorf("ssrf guard: blocked non-public address %s", ip)
	}
	return nil
}

// NoRedirect prevents an HTTP client from automatically following a redirect.
// Redirect targets must be requested as a new, independently validated proxy
// request rather than being followed server-side.
func NoRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

// isPublicIP reports whether an IP is a globally routable unicast address.
func isPublicIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsPrivate() {
		return false
	}

	if ip4 := ip.To4(); ip4 != nil {
		// 100.64.0.0/10 (CGNAT), 0.0.0.0/8, and 240.0.0.0/4 are not public.
		if (ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127) ||
			ip4[0] == 0 || ip4[0] >= 240 {
			return false
		}
		return true
	}

	// NAT64's well-known prefix can embed an otherwise non-public IPv4 address.
	if len(ip) == net.IPv6len && ip[0] == 0x00 && ip[1] == 0x64 &&
		ip[2] == 0xff && ip[3] == 0x9b {
		return isPublicIP(net.IPv4(ip[12], ip[13], ip[14], ip[15]))
	}

	return true
    }
