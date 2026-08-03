package v1

import (
	"fmt"
	"net"
	"strings"
	"syscall"
)

// ssrfGuardControl is a net.Dialer.Control hook. It runs after DNS resolution,
// immediately before the socket connects, and inspects the concrete IP the OS
// is about to dial. Validating here — rather than on the hostname — is what
// makes it robust: DNS rebinding, HTTP redirects, and alternate IP encodings
// all funnel through this same check, because they all must eventually connect
// to an actual address.
func ssrfGuardControl(_ /*network*/ string, address string, _ syscall.RawConn) error {
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

// isPublicIP reports whether ip is a globally routable unicast address.
// Everything else — loopback, private, link-local, CGNAT, ULA, multicast,
// unspecified, and the cloud metadata address — is rejected.
func isPublicIP(ip net.IP) bool {
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
		// IPv4-mapped IPv6 (::ffff:a.b.c.d) — re-check as IPv4 so a mapped
		// private address cannot slip through.
		if mapped := ip.To4(); mapped != nil {
			return isPublicIP(mapped)
		}
		// NAT64 well-known prefix 64:ff9b::/96 wrapping a private v4.
		if len(ip) == net.IPv6len && ip[0] == 0x00 && ip[1] == 0x64 &&
			ip[2] == 0xff && ip[3] == 0x9b {
			if v4 := net.IPv4(ip[12], ip[13], ip[14], ip[15]); v4 != nil {
				return isPublicIP(v4)
			}
		}
	}
	return true
}

// validateProxyTarget performs a cheap up-front hostname check so obviously
// bad requests fail fast with a clear message. The dialer Control hook remains
// the authoritative boundary; this is UX, not security.
func validateProxyTarget(host string) error {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return fmt.Errorf("empty host")
	}
	// Trim IPv6 brackets if present.
	host = strings.Trim(host, "[]")
	switch host {
	case "localhost", "metadata.google.internal", "metadata.goog":
		return fmt.Errorf("blocked host")
	}
	if strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".internal") {
		return fmt.Errorf("blocked host")
	}
	// If it parses as an IP, apply the same rule the dialer will enforce,
	// so we can reject with a 4xx instead of a failed dial.
	if ip := net.ParseIP(host); ip != nil && !isPublicIP(ip) {
		return fmt.Errorf("blocked address")
	}
	return nil
}
