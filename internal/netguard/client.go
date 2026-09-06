package netguard

import (
	"context"
	"net"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"
)

// allowPrivateEgress opts the process out of private-address blocking. It
// exists for local development against loopback sidecars and for tests; it
// must never be set in production (it disables the SSRF boundary).
func allowPrivateEgress() bool {
	v := os.Getenv("ANIRAKU_ALLOW_PRIVATE_EGRESS")
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	return err == nil && b
}

// control is the dialer hook actually installed on guarded transports: the
// public Control, unless private egress is explicitly allowed.
func control(network, address string, c syscall.RawConn) error {
	if allowPrivateEgress() {
		return nil
	}
	return Control(network, address, c)
}

// guardedDialer resolves through Go's own resolver (so OS /etc/hosts
// overrides cannot steer lookups) and validates every concrete IP in the
// Control hook, after DNS resolution, immediately before the socket connects.
func guardedDialer() *net.Dialer {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "udp", "8.8.8.8:53")
		},
	}
	return &net.Dialer{
		Timeout:  15 * time.Second,
		Resolver: resolver,
		Control:  control,
	}
}

// NewTransport returns an http.Transport whose every dial passes the SSRF
// Control hook: loopback, private, CGNAT, link-local, and metadata addresses
// are rejected after DNS resolution, which defeats rebinding and redirect
// tricks. Connection pooling is tuned for media segment fetching.
func NewTransport() *http.Transport {
	return &http.Transport{
		DialContext:           guardedDialer().DialContext,
		MaxIdleConns:          20,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// NewHTTPClient returns an http.Client on the guarded transport that also
// refuses every redirect. timeout bounds the whole request including body
// read; pass 0 for streaming responses whose lifetime is bound by the
// request context instead.
func NewHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		Transport:     NewTransport(),
		CheckRedirect: NoRedirects,
	}
}
