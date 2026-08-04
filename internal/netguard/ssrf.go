package netguard

import (
	"fmt"
	"net"
	"net/http"
	"syscall"
)

// Control is a net.Dialer.Control hook. It runs after DNS resolution,
// immediately before the socket connects, and inspects the concrete IP the OS
// is about to dial. Validating here — rather than on the hostname — is what
// makes it robust: DNS rebinding, HTTP redirects, and alternate IP encodings
// all funnel through this same check, because they all must eventually connect
// to an actual address.
func Control(_ /*network*/ string, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("ssrf guard: cannot parse address %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("ssrf guard: unresolved address %q", host)
	}
	if !IsPublicIP(ip) {
		return fmt.Errorf("ssrf guard: blocked non-public address %s", ip)
	}
	return nil
}

// IsPublicIP reports whether ip is a globally routable unicast address.
// Everything else — loopback, private, link-local, CGNAT, ULA, multicast,
// unspecified, and the cloud metadata address — is rejected.
func IsPublicIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return false
	}
	// Private ranges: 10/8, 172.16/12, 192.168/16, fc00::/7, plus IsPrivate
	// covers RFC1918 and RFC4193.
	if ip.IsPrivate() {
		return false
	}
	if ip4 := ip.To4(); ip4 != nil {
		// 100.64.0.0/10 CGNAT (RFC6598) — not covered by IsPrivate.
		if ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
			return false
		}
		// 169.254.0.0/16 covers the 169.254.169.254 metadata endpoint;
		// IsLinkLocalUnicast already rejects it, but be explicit.
		if ip4[0] == 169 && ip4[1] == 254 {
			return false
		}
		// 0.0.0.0/8 "this host" range.
		if ip4[0] == 0 {
			return false
		}
		// 240.0.0.0/4 — reserved (class E) plus the limited broadcast
		// 255.255.255.255. IsMulticast only matches 224/4, so this range
		// must be rejected explicitly.
		if ip4[0] >= 240 {
			return false
		}
	} else {
         // IPv4-mapped IPv6 (::ffff:a.b.c.d) is already handled by ip.To4() above.
		// NAT64 well-known prefix 64:ff9b::/96 wrapping a private v4.
		if len(ip) == net.IPv6len &&
 			ip[0] == 0x00 && ip[1] == 0x64 && ip[2] == 0xff && ip[3] == 0x9b &&
 			ip[4] == 0x00 && ip[5] == 0x00 && ip[6] == 0x00 && ip[7] == 0x00 &&
 			ip[8] == 0x00 && ip[9] == 0x00 && ip[10] == 0x00 && ip[11] == 0x00 {
			if v4 := net.IPv4(ip[12], ip[13], ip[14], ip[15]); v4 != nil {
				return IsPublicIP(v4)
			}
		}
	}
	return true
}

// NoRedirects rejects every redirect at the client layer. Backing off at the
// first hop means an upstream cannot bounce this server at an arbitrary
// internal target, even one the dialer's IP guard would later permit.
func NoRedirects(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}
