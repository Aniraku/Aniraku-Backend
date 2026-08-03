package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
)

type Verifier struct {
	jwks   *JWKS
	aud    string
	issuer string
	log    zerolog.Logger
}

func NewVerifier(jwks *JWKS, issuer, audience string, log zerolog.Logger) *Verifier {
	return &Verifier{
		jwks:   jwks,
		aud:    audience,
		issuer: issuer,
		log:    log,
	}
}

type Claims struct {
	Sub string `json:"sub"`
	jwt.RegisteredClaims
}

func (v *Verifier) Verify(ctx context.Context, rawToken string) (string, error) {
	token, err := jwt.ParseWithClaims(rawToken, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok {
			return nil, fmt.Errorf("missing kid in token header")
		}

		alg, ok := token.Header["alg"].(string)
		if !ok {
			return nil, fmt.Errorf("missing algorithm in token header")
		}
		if alg != "RS256" && alg != "ES256" {
			return nil, fmt.Errorf("unexpected signing algorithm: %s", alg)
		}

		return v.jwks.GetKey(ctx, kid)
	})
	if err != nil {
		return "", fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return "", fmt.Errorf("invalid token claims")
	}

	// Verify audience — exact match only, not substring
	if len(claims.Audience) > 0 {
		audMatch := false
		for _, a := range claims.Audience {
			if a == v.aud {
				audMatch = true
				break
			}
		}
		if !audMatch {
			return "", fmt.Errorf("invalid audience: expected %s, got %v", v.aud, []string(claims.Audience))
		}
	}

	// Verify issuer
	if v.issuer != "" && claims.Issuer != v.issuer {
		return "", fmt.Errorf("invalid issuer: expected %s, got %s", v.issuer, claims.Issuer)
	}

	// Verify expiry — reject tokens without expiry
	if claims.ExpiresAt == nil {
		return "", fmt.Errorf("token has no expiry claim")
	}
	if claims.ExpiresAt.Time.Before(time.Now()) {
		return "", fmt.Errorf("token expired")
	}

	return claims.Sub, nil
}
