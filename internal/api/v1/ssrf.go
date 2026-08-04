package v1

import (
	"fmt"
	"net"
	"strings"

	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// validateProxyTarget performs a cheap up-front hostname check so obviously
// bad requests fail fast with a clear message. The dialer Control hook
// (netguard.Control) remains the authoritative boundary; this is UX, not
// security.
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
	if ip := net.ParseIP(host); ip != nil && !netguard.IsPublicIP(ip) {
		return fmt.Errorf("blocked address")
	}
	return nil
}
