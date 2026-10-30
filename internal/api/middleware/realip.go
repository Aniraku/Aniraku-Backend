package middleware

import (
	"net"
	"net/http"
	"os"
	"strings"
)

// trustedProxyCIDRs parses the ANIRAKU_TRUSTED_PROXY_CIDRS env var
// (comma-separated CIDRs) into a reusable list. Empty when unset.
func trustedProxyCIDRs() []*net.IPNet {
	var out []*net.IPNet
	for _, c := range strings.Split(os.Getenv("ANIRAKU_TRUSTED_PROXY_CIDRS"), ",") {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, n, err := net.ParseCIDR(c); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// RealIP replaces chi's RealIP middleware: chi trusts the leftmost
// X-Forwarded-For entry, which any client can spoof to dodge per-IP rate
// limits. Behind a trusted proxy, the true socket peer is appended to
// X-Forwarded-For, so the client-unforgeable address is the rightmost
// entry that is not a trusted proxy CIDR. With no trusted CIDRs configured,
// X-Forwarded-For is ignored entirely and the socket peer is used.
//
// Security: X-Forwarded-For is only honored when the direct TCP peer itself
// is one of the configured trusted proxies. Otherwise an attacker who can
// reach the origin directly can append a trusted-looking proxy address and
// choose the derived client IP (chi CVE-2026-114437 / GHSA-j6qf-3m9c-vwj3).
func RealIP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			peer = r.RemoteAddr
		}

		if trusted := trustedProxyCIDRs(); len(trusted) > 0 {
			peerIP := net.ParseIP(peer)
			peerTrusted := false
			if peerIP != nil {
				for _, n := range trusted {
					if n.Contains(peerIP) {
						peerTrusted = true
						break
					}
				}
			}
			if peerTrusted {
				if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
					entries := strings.Split(xff, ",")
					for i := len(entries) - 1; i >= 0; i-- {
						ip := strings.TrimSpace(entries[i])
						if ip == "" {
							continue
						}
						parsed := net.ParseIP(ip)
						if parsed == nil {
							continue
						}
						trustedEntry := false
						for _, n := range trusted {
							if n.Contains(parsed) {
								trustedEntry = true
								break
							}
						}
						if !trustedEntry {
							peer = ip
							break
						}
					}
				}
			}
		}

		if peer != "" {
			r.RemoteAddr = net.JoinHostPort(peer, "0")
		}
		next.ServeHTTP(w, r)
	})
}