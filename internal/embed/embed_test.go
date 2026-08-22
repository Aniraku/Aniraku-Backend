//go:build web

package embed

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesEmbeddedRootInterface(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	recorder := httptest.NewRecorder()

	Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected embedded root interface to return 200, got %d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "ANIRAKU") {
		t.Fatal("expected embedded root interface to contain Aniraku branding")
	}
}

func TestHandlerDoesNotFallbackOverAPIRoutes(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	recorder := httptest.NewRecorder()

	Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected API namespace to remain outside UI fallback, got %d", recorder.Code)
	}
}
