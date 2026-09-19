package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

// allowLoopbackEgress opts this test binary into private-address egress so
// the SSRF-guarded admin client can reach the loopback httptest server. The
// env var is re-read on every dial (no caching in netguard), so tests can
// flip it at runtime; t.Cleanup guarantees other tests never inherit it.
func allowLoopbackEgress(t *testing.T) {
	t.Helper()
	t.Setenv("ANIRAKU_ALLOW_PRIVATE_EGRESS", "1")
}

// testContext carries a synthetic authenticated request: the middleware chain
// (auth.Middleware) normally injects UserIDKey and tokenKey before
// RequireAdmin runs; tests inject both directly.
func testContext(userID, rawToken string) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, UserIDKey, userID)
	ctx = context.WithValue(ctx, tokenKey, rawToken)
	return ctx
}

// TestRequireAdminSendsAnonKeyAndUserJWT pins the Supabase header contract:
// apikey must be the project anon key (the gateway rejects user JWTs with
// "Invalid API key"), Authorization must carry the caller's own JWT so
// is_admin()'s auth.uid() evaluates for the real user. This test locks in
// the fix for the bug where apikey was set to the user JWT, which made the
// admin RPC 401 at the gateway for every caller — including real admins.
func TestRequireAdminSendsAnonKeyAndUserJWT(t *testing.T) {
	allowLoopbackEgress(t)
	const (
		anonKey = "sb_test_anon_key"
		userJWT = "user.jwt.token"
		userID  = "00000000-0000-0000-0000-000000000001"
	)

	var gotAPIKey, gotAuth, gotPath, gotMethod string
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("apikey")
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		gotMethod = r.Method
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("true"))
	}))
	defer rpc.Close()

	log := zerolog.Nop()
	handler := RequireAdmin(rpc.URL, anonKey, log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("admin route reached"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/stats", nil)
	req = req.WithContext(testContext(userID, userJWT))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("admin route not reached: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if gotMethod != http.MethodPost || gotPath != "/rest/v1/rpc/is_admin" {
		t.Errorf("RPC called as %s %s, want POST /rest/v1/rpc/is_admin", gotMethod, gotPath)
	}
	if gotAPIKey != anonKey {
		t.Errorf("apikey header = %q, want project anon key %q", gotAPIKey, anonKey)
	}
	if gotAuth != "Bearer "+userJWT {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer "+userJWT)
	}
}

func TestRequireAdminDeniesNonAdmin(t *testing.T) {
	allowLoopbackEgress(t)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("false"))
	}))
	defer rpc.Close()

	log := zerolog.Nop()
	handler := RequireAdmin(rpc.URL, "anon", log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler must not run for non-admin")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/stats", nil)
	req = req.WithContext(testContext("user-id", "user.jwt.token"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

// TestRequireAdminDeniesWhenGatewayRejects covers an upstream failure (e.g.
// 401 at the Supabase gateway): fail closed with 403, never run the route.
func TestRequireAdminDeniesWhenGatewayRejects(t *testing.T) {
	allowLoopbackEgress(t)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"message":"Invalid API key"}`))
	}))
	defer rpc.Close()

	log := zerolog.Nop()
	handler := RequireAdmin(rpc.URL, "anon", log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler must not run when the RPC fails")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/stats", nil)
	req = req.WithContext(testContext("user-id", "user.jwt.token"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

// TestRequireAdminFailsClosedWithoutAnonKey: a missing anon key is a
// configuration error; the gate must 500 (fail closed) without calling
// Supabase at all.
func TestRequireAdminFailsClosedWithoutAnonKey(t *testing.T) {
	allowLoopbackEgress(t)
	called := false
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("true"))
	}))
	defer rpc.Close()

	log := zerolog.Nop()
	handler := RequireAdmin(rpc.URL, "", log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler must not run without an anon key")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/stats", nil)
	req = req.WithContext(testContext("user-id", "user.jwt.token"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if called {
		t.Error("Supabase RPC must not be called when anon key is missing")
	}
}

// TestRequireAdminRequiresAuthentication: no user in context → 401 before
// any RPC.
func TestRequireAdminRequiresAuthentication(t *testing.T) {
	allowLoopbackEgress(t)
	called := false
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("true"))
	}))
	defer rpc.Close()

	log := zerolog.Nop()
	handler := RequireAdmin(rpc.URL, "anon", log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler must not run for unauthenticated requests")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Error("Supabase RPC must not be called without authentication")
	}
}
