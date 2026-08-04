package auth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type contextKey string

const UserIDKey contextKey = "user_id"

const tokenKey contextKey = "raw_token"

func Token(ctx context.Context) string {
	if t, ok := ctx.Value(tokenKey).(string); ok {
		return t
	}
	return ""
}

func Middleware(verifier *Verifier, log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, `{"error":"missing authorization header"}`, http.StatusUnauthorized)
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				http.Error(w, `{"error":"invalid authorization header format"}`, http.StatusUnauthorized)
				return
			}

			rawToken := parts[1]
			userID, err := verifier.Verify(r.Context(), rawToken)
			if err != nil {
				log.Warn().Err(err).Msg("token verification failed")
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), UserIDKey, userID)
			ctx = context.WithValue(ctx, tokenKey, rawToken)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func GetUserID(ctx context.Context) string {
	if uid, ok := ctx.Value(UserIDKey).(string); ok {
		return uid
	}
	return ""
}

// RequireAdmin gates a route behind a server-side admin check. It verifies
// the caller's Supabase JWT, then asks Supabase's is_admin() RPC (a SECURITY
// DEFINER function scoped to auth.uid()) whether that user is an admin. The
// client can never self-declare admin; the check runs with the caller's own
// JWT, so RLS and the function's auth.uid() scoping both apply.
func RequireAdmin(supabaseURL string, log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			userID := GetUserID(ctx)
			if userID == "" {
				// Authenticate first: run the standard middleware chain
				// (router applies authMiddleware before this) — if missing,
				// fail closed.
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}

			rawToken := Token(ctx)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost,
				strings.TrimRight(supabaseURL, "/")+"/rest/v1/rpc/is_admin", nil)
			if err != nil {
				log.Error().Err(err).Msg("admin check: build request failed")
				http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
				return
			}
			req.Header.Set("apikey", rawToken)
			req.Header.Set("Authorization", "Bearer "+rawToken)
			req.Header.Set("Content-Type", "application/json")

			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				log.Error().Err(err).Msg("admin check: supabase rpc failed")
				http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
				log.Warn().Str("user", userID).Int("status", resp.StatusCode).
					Str("body", strings.TrimSpace(string(body))).Msg("admin check denied")
				http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
				return
			}

			var isAdmin bool
			if err := json.NewDecoder(resp.Body).Decode(&isAdmin); err != nil || !isAdmin {
				log.Warn().Str("user", userID).Msg("admin check: not admin")
				http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
				return
			}

			log.Debug().Str("user", userID).Msg("admin check passed")
			next.ServeHTTP(w, r)
		})
	}
}
