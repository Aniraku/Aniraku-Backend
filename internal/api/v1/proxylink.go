package v1

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Opaque proxy links: the backend replaces raw provider URLs (and their
// scraper-session headers) with self-contained tokens. The client only ever
// sees /api/v1/proxy?k=<token>; the proxy decrypts the token server-side and
// fetches upstream with the sealed headers. Tokens expire (6h) so leaked
// links die with the session that minted them.
//
// The key is PROXY_LINK_KEY (32-byte hex) — shared with the Cloudflare
// Pages media shield, which mints and resolves the SAME token shape
// (shared/mediaShield.js), so backend-minted links pass through the Pages
// shield untouched and Pages-minted links resolve here.
//
// The wire format is byte-for-byte the JS one, and it is not negotiable:
//
//	token = base64url( iv(12) || AES-256-GCM ciphertext || tag(16) )
//	plain = {"u": <url>, "h": <headers?>, "e": <expiry unix ms>}
//
// This side used to seal with ChaCha20-Poly1305. WebCrypto has no
// ChaCha20-Poly1305, so the Pages shield could never open those tokens: the
// moment it started intercepting ?k= (instead of forwarding it), every
// backend-minted source and subtitle link 404'd at the edge, and tokens
// minted by the shield 404'd here. AES-GCM is the only cipher both runtimes
// share, so both sides now use it.

type proxyLinkPayload struct {
	URL     string            `json:"u"`
	Headers map[string]string `json:"h,omitempty"`
	Exp     int64             `json:"e"` // unix milliseconds, as in mediaShield.js
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
	if len(proxyLinkKeyBytes) != 32 {
		return nil, errors.New("proxy link key not configured")
	}
	block, err := aes.NewCipher(proxyLinkKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("proxy link: %w", err)
	}
	// NewGCM: 12-byte nonce, 16-byte tag appended to the ciphertext — the
	// exact layout bytesToB64url/openK expect on the JS side.
	return cipher.NewGCM(block)
}

// mintProxyLink seals a raw upstream URL + its provider headers into an
// opaque, expiring token. Returns the full /api/v1/proxy?k= URL path.
//
// Collectors return sources already pointing at the LOCAL proxy
// (http://127.0.0.1:43211/api/v1/proxy?url=<raw>&headers=<json>) — the
// nested self-proxy is unwrapped first so the token seals the RAW upstream
// URL + headers. Sealing the nested loopback URL instead makes every
// k-link 403 at the allowlist (proxy target 127.0.0.1).
func mintProxyLink(rawURL string, headers map[string]string) (string, error) {
	if u, err := url.Parse(rawURL); err == nil && u.Query().Get("url") != "" {
		// Nested self-proxy: unwrap the inner url= and merge the inner
		// headers= (the provider session cookies/headers the collector
		// already embedded) over the caller-supplied ones. The al= audio
		// strip context rides on the OUTER URL — proxySources appends it
		// after wrapping — and MUST survive the unwrap onto the sealed
		// inner URL, or sub/dub selections lose their audio-strip context
		// and both play the default rendition.
		if inner, ierr := url.QueryUnescape(u.Query().Get("url")); ierr == nil {
			if al := u.Query().Get("al"); al != "" {
				inner = withAudioLang(inner, al)
			}
			rawURL = inner
		}
		if hj := u.Query().Get("headers"); hj != "" {
			var innerHeaders map[string]string
			if jerr := json.Unmarshal([]byte(hj), &innerHeaders); jerr == nil {
				merged := map[string]string{}
				for k, v := range headers {
					merged[k] = v
				}
				for k, v := range innerHeaders {
					merged[k] = v
				}
				headers = merged
			}
		}
	}
	aead, err := proxyLinkCipher()
	if err != nil {
		return "", err
	}
	// A sealed URL is by definition one of OUR provider-resolved playback
	// targets (source, subtitle, nested unwrap) — vouch its host into the
	// dynamic allowlist right here so the proxy gate accepts it even when
	// the upstream rotates to a CDN the static list has never seen. This
	// is the automated-learning anchor: every host the providers hand out
	// for playback passes through mintProxyLink exactly once per mint.
	if u, perr := url.Parse(rawURL); perr == nil {
		LearnHostFromPlaylist(u.Hostname())
	}
	payload, err := json.Marshal(proxyLinkPayload{
		URL:     rawURL,
		Headers: headers,
		Exp:     time.Now().Add(proxyLinkTTL).UnixMilli(),
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
	if len(sealed) < aead.NonceSize() {
		return "", nil, errors.New("proxy link: truncated")
	}
	plain, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], nil)
	if err != nil {
		return "", nil, fmt.Errorf("proxy link: %w", err)
	}
	var payload proxyLinkPayload
	if err := json.Unmarshal(plain, &payload); err != nil {
		return "", nil, fmt.Errorf("proxy link: %w", err)
	}
	if payload.Exp > 0 && time.Now().UnixMilli() > payload.Exp {
		return "", nil, errors.New("proxy link expired")
	}
	if payload.URL == "" {
		return "", nil, errors.New("proxy link: empty url")
	}
	return payload.URL, payload.Headers, nil
}
