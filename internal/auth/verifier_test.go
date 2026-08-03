package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
)

func newTestVerifier(t *testing.T) (*Verifier, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	v := &Verifier{
		jwks: &JWKS{
			keys: map[string]crypto.PublicKey{
				"test-kid": key.Public(),
			},
			client:  &http.Client{},
			expires: time.Now().Add(time.Hour),
		},
		aud:    "authenticated",
		issuer: "https://sbjdrjaovcgvttfnpfsz.supabase.co/auth/v1",
		log:    zerolog.Nop(),
	}
	return v, key
}

func signTestToken(t *testing.T, key *rsa.PrivateKey, claims jwt.Claims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "test-kid"
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

func baseClaims(sub string) *Claims {
	return &Claims{
		Sub: sub,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   sub,
			Issuer:    "https://sbjdrjaovcgvttfnpfsz.supabase.co/auth/v1",
			Audience:  jwt.ClaimStrings{"authenticated"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
}

func TestVerifyAcceptsValidToken(t *testing.T) {
	t.Parallel()
	v, key := newTestVerifier(t)
	raw := signTestToken(t, key, baseClaims("11111111-1111-1111-1111-111111111111"))
	sub, err := v.Verify(context.Background(), raw)
	if err != nil {
		t.Fatalf("Verify = %v", err)
	}
	if sub != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("sub = %q", sub)
	}
}

func TestVerifyRejectsMissingAudience(t *testing.T) {
	t.Parallel()
	v, key := newTestVerifier(t)
	claims := baseClaims("u1")
	claims.Audience = nil
	raw := signTestToken(t, key, claims)
	if _, err := v.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected error for missing audience")
	}
}

func TestVerifyRejectsWrongAudience(t *testing.T) {
	t.Parallel()
	v, key := newTestVerifier(t)
	claims := baseClaims("u1")
	claims.Audience = jwt.ClaimStrings{"someone-else"}
	raw := signTestToken(t, key, claims)
	if _, err := v.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected error for wrong audience")
	}
}

func TestVerifyRejectsMissingExpiry(t *testing.T) {
	t.Parallel()
	v, key := newTestVerifier(t)
	claims := baseClaims("u1")
	claims.ExpiresAt = nil
	raw := signTestToken(t, key, claims)
	if _, err := v.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected error for missing expiry")
	}
}

func TestVerifyRejectsExpiredToken(t *testing.T) {
	t.Parallel()
	v, key := newTestVerifier(t)
	claims := baseClaims("u1")
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
	raw := signTestToken(t, key, claims)
	if _, err := v.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected error for expired token")
	}
}

func TestVerifyRejectsBlankSubject(t *testing.T) {
	t.Parallel()
	v, key := newTestVerifier(t)
	claims := baseClaims("   ")
	raw := signTestToken(t, key, claims)
	if _, err := v.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected error for blank subject")
	}
}

func TestVerifyRejectsWrongIssuer(t *testing.T) {
	t.Parallel()
	v, key := newTestVerifier(t)
	claims := baseClaims("u1")
	claims.Issuer = "https://evil.example/auth/v1"
	raw := signTestToken(t, key, claims)
	if _, err := v.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected error for wrong issuer")
	}
}

func TestVerifyRejectsNotYetValid(t *testing.T) {
	t.Parallel()
	v, key := newTestVerifier(t)
	claims := baseClaims("u1")
	claims.NotBefore = jwt.NewNumericDate(time.Now().Add(5 * time.Minute))
	raw := signTestToken(t, key, claims)
	if _, err := v.Verify(context.Background(), raw); err == nil {
		t.Fatal("expected error for future nbf")
	}
}
