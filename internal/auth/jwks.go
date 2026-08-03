package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type JWKS struct {
	keys    map[string]crypto.PublicKey
	mu      sync.RWMutex
	url     string
	client  *http.Client
	log     zerolog.Logger
	expires time.Time
	ttl     time.Duration
}

type jwksResponse struct {
	Keys []jwksEntry `json:"keys"`
}

type jwksEntry struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func NewJWKS(url string, log zerolog.Logger) *JWKS {
	return &JWKS{
		keys:   make(map[string]crypto.PublicKey),
		url:    url,
		client: &http.Client{Timeout: 10 * time.Second},
		log:    log,
		ttl:    1 * time.Hour,
	}
}

func (j *JWKS) GetKey(ctx context.Context, kid string) (crypto.PublicKey, error) {
	j.mu.RLock()
	key, ok := j.keys[kid]
	j.mu.RUnlock()

	if ok && time.Now().Before(j.expires) {
		return key, nil
	}

	return j.refresh(ctx, kid)
}

func (j *JWKS) refresh(ctx context.Context, kid string) (crypto.PublicKey, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if key, ok := j.keys[kid]; ok && time.Now().Before(j.expires) {
		return key, nil
	}

	j.log.Info().Str("url", j.url).Msg("refreshing JWKS")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating JWKS request: %w", err)
	}

	resp, err := j.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching JWKS: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading JWKS response: %w", err)
	}

	var jwks jwksResponse
	if err := json.Unmarshal(body, &jwks); err != nil {
		return nil, fmt.Errorf("parsing JWKS: %w", err)
	}

	j.keys = make(map[string]crypto.PublicKey, len(jwks.Keys))
	for _, entry := range jwks.Keys {
		if entry.Kid == "" {
			continue
		}

		switch entry.Kty {
		case "RSA":
			key, err := parseRSAKey(entry.N, entry.E)
			if err != nil {
				j.log.Warn().Err(err).Str("kid", entry.Kid).Msg("failed to parse RSA key")
				continue
			}
			j.keys[entry.Kid] = key

		case "EC":
			key, err := parseECDSAKey(entry.Crv, entry.X, entry.Y)
			if err != nil {
				j.log.Warn().Err(err).Str("kid", entry.Kid).Msg("failed to parse ECDSA key")
				continue
			}
			j.keys[entry.Kid] = key

		default:
			j.log.Warn().Str("kid", entry.Kid).Str("kty", entry.Kty).Msg("unsupported key type, skipping")
		}
	}

	j.expires = time.Now().Add(j.ttl)

	key, ok := j.keys[kid]
	if !ok {
		return nil, fmt.Errorf("key %s not found in JWKS", kid)
	}

	return key, nil
}

func parseRSAKey(nStr, eStr string) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(nStr)
	if err != nil {
		return nil, fmt.Errorf("decode modulus: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(eStr)
	if err != nil {
		return nil, fmt.Errorf("decode exponent: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}

	return &rsa.PublicKey{N: n, E: e}, nil
}

func parseECDSAKey(crvStr, xStr, yStr string) (*ecdsa.PublicKey, error) {
	var crv elliptic.Curve
	switch crvStr {
	case "P-256":
		crv = elliptic.P256()
	case "P-384":
		crv = elliptic.P384()
	case "P-521":
		crv = elliptic.P521()
	default:
		return nil, fmt.Errorf("unsupported curve: %s", crvStr)
	}

	xBytes, err := base64.RawURLEncoding.DecodeString(xStr)
	if err != nil {
		return nil, fmt.Errorf("decode x coordinate: %w", err)
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(yStr)
	if err != nil {
		return nil, fmt.Errorf("decode y coordinate: %w", err)
	}

	x := new(big.Int).SetBytes(xBytes)
	y := new(big.Int).SetBytes(yBytes)

	if !crv.IsOnCurve(x, y) {
		return nil, fmt.Errorf("point not on curve")
	}

	return &ecdsa.PublicKey{Curve: crv, X: x, Y: y}, nil
}
