package api

// Route wiring for the bridge endpoints: an unknown path would answer
// the JSON catch-all 404, while a registered route reaches the handler
// and rejects the wrong token with 401 (token set).

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/config"
)

func TestMkissaBridgeRoutesRegistered(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "sekrit")
	r := NewRouter(&config.Config{}, zerolog.Nop())

	for _, path := range []string{
		"/api/v1/internal/mkissa/poll",
		"/api/v1/internal/mkissa/result",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer wrong-token")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("%s: want 401 from the registered handler, got %d (%s) — route missing?",
				path, w.Code, w.Body.String())
		}
	}
}
