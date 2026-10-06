package v1

// Auth and payload contract for the relay-worker bridge endpoints:
//   - no ANIRAKU_BRIDGE_TOKEN configured  -> 404 (feature invisible)
//   - wrong/missing Bearer token         -> 401
//   - poll always answers jobs:[]         (never null)
//   - result validates id + line/error before touching the queue

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

const (
	bridgePollPath   = "/api/v1/internal/mkissa/poll"
	bridgeResultPath = "/api/v1/internal/mkissa/result"
)

func newBridgeTestHandler() *Handlers { return &Handlers{log: zerolog.Nop()} }

func bridgePost(h *Handlers, path, auth, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(http.MethodPost, path, nil)
	} else {
		req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	}
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	w := httptest.NewRecorder()
	if path == bridgePollPath {
		h.MkissaBridgePoll(w, req)
	} else {
		h.MkissaBridgeResult(w, req)
	}
	return w
}

func TestMkissaBridgeEndpointsHiddenWithoutToken(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "")
	h := newBridgeTestHandler()
	for _, path := range []string{bridgePollPath, bridgeResultPath} {
		w := bridgePost(h, path, "Bearer anything", `{}`)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s with no server token: want 404, got %d (%s)", path, w.Code, w.Body.String())
		}
	}
}

func TestMkissaBridgePollAuth(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "sekrit")
	h := newBridgeTestHandler()

	if w := bridgePost(h, bridgePollPath, "", `{}`); w.Code != http.StatusUnauthorized {
		t.Errorf("missing auth: want 401, got %d", w.Code)
	}
	if w := bridgePost(h, bridgePollPath, "Bearer nope", `{}`); w.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: want 401, got %d", w.Code)
	}
	w := bridgePost(h, bridgePollPath, "Bearer sekrit", `{}`)
	if w.Code != http.StatusOK {
		t.Fatalf("valid token: want 200, got %d (%s)", w.Code, w.Body.String())
	}
	var out struct {
		Jobs json.RawMessage `json:"jobs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("poll body is not JSON: %v (%s)", err, w.Body.String())
	}
	// Contract: always a JSON array, even with an empty queue.
	if len(out.Jobs) == 0 || out.Jobs[0] != '[' {
		t.Errorf("jobs must be a JSON array, got %s", string(out.Jobs))
	}
}

func TestMkissaBridgeResultValidation(t *testing.T) {
	t.Setenv("ANIRAKU_BRIDGE_TOKEN", "sekrit")
	h := newBridgeTestHandler()

	if w := bridgePost(h, bridgeResultPath, "", `{}`); w.Code != http.StatusUnauthorized {
		t.Errorf("missing auth: want 401, got %d", w.Code)
	}
	if w := bridgePost(h, bridgeResultPath, "Bearer sekrit", `not-json`); w.Code != http.StatusBadRequest {
		t.Errorf("bad json: want 400, got %d", w.Code)
	}
	if w := bridgePost(h, bridgeResultPath, "Bearer sekrit", `{"id":0,"line":"{}"}`); w.Code != http.StatusBadRequest {
		t.Errorf("id 0: want 400, got %d", w.Code)
	}
	if w := bridgePost(h, bridgeResultPath, "Bearer sekrit", `{"id":5}`); w.Code != http.StatusBadRequest {
		t.Errorf("no line/error: want 400, got %d", w.Code)
	}

	// Valid shape, nobody waiting: 200 with ok=false (a normal late
	// answer, not an error the worker should retry).
	w := bridgePost(h, bridgeResultPath, "Bearer sekrit", `{"id":987654,"line":"{\"id\":987654}"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("valid result: want 200, got %d (%s)", w.Code, w.Body.String())
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("result body is not JSON: %v", err)
	}
	if out.OK {
		t.Error("unknown id must answer ok=false")
	}
}
