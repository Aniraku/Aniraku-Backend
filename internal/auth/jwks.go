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

	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// jwksGrace lets verification continue with the previous key set when a
// refresh fails (Supabase blip). Availability tradeoff: during an outage the
// old keys keep validating tokens for at most this window past expiry.
const jwksGrace = 24 * time.Hour

type JWKS struct {
	keys    map[string]crypto.PublicKey
	mu      sync.Mutex
	url     string
	client  *http.Client
	log     zerolog.Logger
	expires time.Time
	ttl     time.Duration
	// refreshing is the single-flight flag: the first GetKey that notices
	// stale keys performs the network refresh; every other caller waits on
	// the condition variable and then re-reads the shared state. Previously
	// refresh ran while holding mu, serializing all token verification behind
	// one upstream HTTP call.
	refreshing bool
	cond       *sync.Cond
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
	j := &JWKS{
		keys:   make(map[string]crypto.PublicKey),
		url:    url,
		client: netguard.NewHTTPClient(10 * time.Second),
		log:    log,
		ttl:    1 * time.Hour,
	}
	j.cond = sync.NewCond(&j.mu)
	return j
}

func (j *JWKS) GetKey(ctx context.Context, kid string) (crypto.PublicKey, error) {
	j.mu.Lock()
	for {
		if key, ok := j.keys[kid]; ok && time.Now().Before(j.expires) {
			j.mu.Unlock()
			return key, nil
		}
		// Fresh keys exist but not for this kid: Supabase may have rotated to
		// a key we have not seen — refresh once before giving up.
		fresh := time.Now().Before(j.expires) && len(j.keys) > 0
		if fresh || j.refreshing {
			if fresh && !j.refreshing {
				j.refreshing = true
				go func() {
					j.doRefresh(ctx)
					j.mu.Lock()
					j.refreshing = false
					j.cond.Broadcast()
					j.mu.Unlock()
				}()
			}
			if j.refreshing {
				j.cond.Wait()
				continue
			}
			continue
		}
		// Stale/empty keys, nobody else refreshing: do it on this goroutine.
		j.refreshing = true
		err := j.doRefreshLocked(ctx)
		j.refreshing = false
		j.cond.Broadcast()
		if err != nil {
			// Grace: fall back to the previous key set (if any) instead of
			// hard-failing every request during a Supabate blip.
			if key, ok := j.keys[kid]; ok {
				j.mu.Unlock()
				j.log.Warn().Err(err).Msg("jwks refresh failed; serving previous keys (grace window)")
				return key, nil
			}
			j.mu.Unlock()
			return nil, err
		}
	}
}

// doRefresh performs the network refresh outside the lock and installs the
// result. Used by the single-flight background path.
func (j *JWKS) doRefresh(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()
	_ = j.doRefreshLocked(ctx)
}

// doRefreshLocked runs the HTTP fetch and key parse while j.mu is held. It is
// only called from the goroutine that claimed the refreshing flag; all other
// callers wait on cond instead of queueing behind this network call.
func (j *JWKS) doRefreshLocked(ctx context.Context) error {
	j.log.Info().Str("url", j.url).Msg("refreshing JWKS")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.url, nil)
	if err != nil {
		return fmt.Errorf("creating JWKS request: %w", err)
	}

	resp, err := j.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetching JWKS: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("reading JWKS response: %w", err)
	}

	var jwks jwksResponse
	if err := json.Unmarshal(body, &jwks); err != nil {
		return fmt.Errorf("parsing JWKS: %w", err)
	}

	newKeys := make(map[string]crypto.PublicKey, len(jwks.Keys))
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
			newKeys[entry.Kid] = key

		case "EC":
			key, err := parseECDSAKey(entry.Crv, entry.X, entry.Y)
			if err != nil {
				j.log.Warn().Err(err).Str("kid", entry.Kid).Msg("failed to parse ECDSA key")
				continue
			}
			newKeys[entry.Kid] = key

		default:
			j.log.Warn().Str("kid", entry.Kid).Str("kty", entry.Kty).Msg("unsupported key type, skipping")
		}
	}

	if len(newKeys) == 0 {
		// Keep the previous key set and old expiry: an empty refresh must
		// never wipe working keys.
		j.log.Warn().Msg("jwks refresh produced no usable keys; keeping previous set")
		return fmt.Errorf("jwks refresh produced no keys")
	}

	j.keys = newKeys
	j.expires = time.Now().Add(j.ttl)

	return nil
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
