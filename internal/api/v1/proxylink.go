package v1

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

// Opaque proxy links: the backend replaces raw provider URLs (and their
// scraper-session headers) with self-contained AES-GCM tokens. The client
// only ever sees /api/v1/proxy?k=<token>; the proxy decrypts the token
// server-side and fetches upstream with the sealed headers. Tokens expire
// (6h) so leaked links die with the session that minted them.
//
// The key is PROXY_LINK_KEY (32-byte hex) — shared with the Cloudflare
// Pages media shield, which mints/resolves the same token shape, so
// backend-minted links pass through the Pages shield untouched.

type proxyLinkPayload struct {
	URL     string            `json:"u"`
	Headers map[string]string `json:"h,omitempty"`
	Exp     int64             `json:"e"`
}

const proxyLinkTTL = 6 * time.Hour

var proxyLinkKeyBytes []byte

// proxyLinkInit decodes PROXY_LINK_KEY (hex, 32 bytes = AES-256). Returns
// false when unset or malformed — the minting layer stays off and the
// legacy raw-URL responses continue.
func proxyLinkInit(keyHex string) bool {
	key, err := hex.DecodeString(keyHex)
	if err != nil || len(key) != 32 {
		return false
	}
	proxyLinkKeyBytes = key
	return true
}

func proxyLinkCipher() (cipher.AEAD, error) {
	if len(proxyLinkKeyBytes) != chacha20poly1305.KeySize {
		return nil, errors.New("proxy link key not configured")
	}
	return chacha20poly1305.New(proxyLinkKeyBytes)
}

// mintProxyLink seals a raw upstream URL + its provider headers into an
// opaque, expiring token. Returns the full /api/v1/proxy?k= URL path.
func mintProxyLink(rawURL string, headers map[string]string) (string, error) {
	aead, err := proxyLinkCipher()
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(proxyLinkPayload{
		URL:     rawURL,
		Headers: headers,
		Exp:     time.Now().Add(proxyLinkTTL).Unix(),
	})
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := aead.Seal(nonce, nonce, payload, nil)
	return "/api/v1/proxy?k=" + base64.RawURLEncoding.EncodeToString(sealed), nil
}

// resolveProxyLink opens a minted token back into its raw URL + headers.
func resolveProxyLink(token string) (string, map[string]string, error) {
	aead, err := proxyLinkCipher()
	if err != nil {
		return "", nil, err
	}
	sealed, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return "", nil, fmt.Errorf("proxy link: bad encoding: %w", err)
	}
	plain, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], nil)
	if err != nil {
		return "", nil, fmt.Errorf("proxy link: %w", err)
	}
	var payload proxyLinkPayload
	if err := json.Unmarshal(plain, &payload); err != nil {
		return "", nil, fmt.Errorf("proxy link: %w", err)
	}
	if payload.Exp > 0 && time.Now().Unix() > payload.Exp {
		return "", nil, errors.New("proxy link expired")
	}
	if payload.URL == "" {
		return "", nil, errors.New("proxy link: empty url")
	}
	return payload.URL, payload.Headers, nil
}
