package streaming

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"

	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// newChromeTLSClient builds an HTTP/2 client whose TLS handshake presents
// Chrome's real ClientHello (uTLS HelloChrome_Auto, default ALPN h2+http/1.1
// - narrowing the ALPN to http/1.1 alone gets blocked where the full hello
// passes). Cloudflare fronts fingerprint JA3 and reject the stock Go
// handshake even with a browser User-Agent: ani.pm tightened theirs on
// 2026-11 (plain Go TLS -> error 1106 on every request, while this chain
// answered 200), so providers facing Cloudflare-hosted APIs resolve through
// it. Egress is tcp4-only with the netguard control hook (the VPS IPv6
// egress is Cloudflare-blocked).
func newChromeTLSClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout: 10 * time.Second,
		Resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "udp", "8.8.8.8:53")
			},
		},
		Control: netguard.Control,
	}
	transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			raw, err := dialer.DialContext(ctx, "tcp4", addr)
			if err != nil {
				return nil, err
			}
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				raw.Close()
				return nil, err
			}
			tlsConn := utls.UClient(raw, &utls.Config{ServerName: host}, utls.HelloChrome_Auto)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				raw.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}
