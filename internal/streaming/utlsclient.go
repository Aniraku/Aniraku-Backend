package streaming

import (
	"context"
	"net"
	"net/http"
	"time"

	utls "github.com/refraction-networking/utls"

	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// newChromeTLSClient builds an HTTP/1.1 client whose TLS handshake presents
// Chrome's real ClientHello (uTLS). Cloudflare fronts fingerprint JA3 and
// reject the stock Go handshake even with a browser User-Agent — ani.pm
// tightened theirs on 2026-11 (plain Go TLS -> error 1106 on every request,
// while the proxy's uTLS chain answered 200), so providers that face
// Cloudflare-hosted APIs resolve through this client.
// Egress is tcp4-only with the netguard control hook, mirroring the
// provider transport guardrails (the VPS IPv6 egress is Cloudflare-blocked).
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
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp4", addr)
		},
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			raw, err := dialer.DialContext(ctx, "tcp4", addr)
			if err != nil {
				return nil, err
			}
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				raw.Close()
				return nil, err
			}
			tlsConn := utls.UClient(raw, &utls.Config{ServerName: host, NextProtos: []string{"http/1.1"}}, utls.HelloChrome_Auto)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				raw.Close()
				return nil, err
			}
			return tlsConn, nil
		},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}
