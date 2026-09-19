package api

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/auth"
	"github.com/Aniraku/Aniraku-Backend/internal/config"
)

// testEnv wires a full router against a fake JWKS endpoint and a fake
// Supabase (admin RPC). Tokens are minted with the test RSA key so the
// auth chain is exercised end to end. Private-address egress is allowed for
// the process because the fakes live on loopback httptest servers.
func testEnv(t *testing.T) (*httptest.Server, *rsa.PrivateKey, string) {
	t.Helper()
	t.Setenv("ANIRAKU_ALLOW_PRIVATE_EGRESS", "1")

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	// Fake JWKS serving the test public key.
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pub := key.PublicKey
		n := base64URLEncode(pub.N.Bytes())
		e := base64URLEncode(bigEndian(pub.E))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]string{{
				"kid": "test-kid", "kty": "RSA", "alg": "RS256", "use": "sig",
				"n": n, "e": e,
			}},
		})
	}))
	t.Cleanup(jwksSrv.Close)

	// Fake Supabase: is_admin returns true.
	sbSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rest/v1/rpc/is_admin" {
			w.Write([]byte("true"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(sbSrv.Close)

	cfg := &config.Config{}
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = 0
	cfg.Supabase.URL = sbSrv.URL
	cfg.Supabase.AnonKey = "test-anon-key"
	cfg.Supabase.JWKSURL = jwksSrv.URL
	cfg.Supabase.JWTAud = "authenticated"
	cfg.Logging.Level = "error"

	log := zerolog.Nop()
	srv := httptest.NewServer(NewRouter(cfg, log))
	t.Cleanup(srv.Close)

	return srv, key, sbSrv.URL + "/auth/v1"
}

func TestRouterHealthPublic(t *testing.T) {
	srv, _, _ := testEnv(t)

	resp, err := srv.Client().Get(srv.URL + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", resp.StatusCode)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Errorf("health body = %v", body)
	}
}

func TestRouterAuthRequiredWithoutToken(t *testing.T) {
	srv, _, _ := testEnv(t)

	resp, err := srv.Client().Get(srv.URL + "/api/v1/favorites")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestRouterAuthRequiredWithValidToken(t *testing.T) {
	srv, key, issuer := testEnv(t)
	tok := mintToken(t, key, "11111111-1111-1111-1111-111111111111", issuer)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/favorites", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	// Supabase request would fail (fake has no /rest/v1/favorites with data),
	// but the auth chain itself must pass — a 404/502 from the fake backend
	// proves authentication succeeded.
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		t.Fatal("valid token rejected by auth chain")
	}
}

func TestRouterAdminGateAllowsAdmin(t *testing.T) {
	srv, key, issuer := testEnv(t)
	tok := mintToken(t, key, "22222222-2222-2222-2222-222222222222", issuer)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("metrics status = %d, body = %s", resp.StatusCode, string(b))
	}
	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	if _, ok := body["requests"]; !ok {
		t.Errorf("metrics body missing requests section: %v", body)
	}
}

func TestRouterAdminGateDeniesAnonymous(t *testing.T) {
	srv, _, _ := testEnv(t)

	resp, err := srv.Client().Get(srv.URL + "/api/v1/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("metrics status = %d, want 401", resp.StatusCode)
	}
}

func TestRouterCORSEchoesAllowedOrigin(t *testing.T) {
	srv, _, _ := testEnv(t)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/health", nil)
	req.Header.Set("Origin", "https://www.aniraku.tech")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://www.aniraku.tech" {
		t.Errorf("ACAO = %q, want echoed origin", got)
	}
}

func TestRouterCORSUnknownOriginNotEchoed(t *testing.T) {
	srv, _, _ := testEnv(t)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/health", nil)
	req.Header.Set("Origin", "https://evil.example")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got == "https://evil.example" {
		t.Error("unknown origin must not be echoed")
	}
}

func TestRouterCatalogCacheHeader(t *testing.T) {
	srv, _, _ := testEnv(t)

	// Episodes will fail upstream (fake env) but the Cache-Control header is
	// set by middleware before the handler runs.
	resp, err := srv.Client().Get(srv.URL + "/api/v1/anime/21/episodes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Cache-Control"); got != "public, max-age=300" {
		t.Errorf("Cache-Control = %q, want public, max-age=300", got)
	}
}

func TestRouterCompressionOnJSON(t *testing.T) {
	srv, _, _ := testEnv(t)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/health", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("Content-Encoding = %q, want gzip (body len %d)", got, len(body))
	}
}

func TestRouterProxyHostNotOnAllowlist(t *testing.T) {
	srv, _, _ := testEnv(t)

	resp, err := srv.Client().Get(srv.URL + "/api/v1/proxy?url=" + strings.ReplaceAll("http://random-cdn.example/video.ts", ":", "%3A"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("proxy status = %d, want 403 for non-allowlisted host", resp.StatusCode)
	}
}

func base64URLEncode(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func bigEndian(i int) []byte {
	if i == 0 {
		return []byte{0}
	}
	var out []byte
	for i > 0 {
		out = append([]byte{byte(i & 0xff)}, out...)
		i >>= 8
	}
	return out
}

func mintToken(t *testing.T, key *rsa.PrivateKey, sub, issuer string) string {
	t.Helper()
	claims := auth.Claims{
		Sub: sub,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   sub,
			Issuer:    issuer,
			Audience:  jwt.ClaimStrings{"authenticated"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "test-kid"
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}
