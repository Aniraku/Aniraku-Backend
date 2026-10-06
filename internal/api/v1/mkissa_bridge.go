package v1

// Relay-worker bridge endpoints (pull model).
//
// The GitHub Actions worker (deploy/mkissa-relay-worker/worker.mjs) POSTs
// here about once a second: /poll hands it queued signed engine jobs, the
// worker executes them with its gate-clean runner egress and POSTs each
// raw engine line back to /result, which wakes the waiting Call. Both
// routes are Bearer-gated with the shared ANIRAKU_BRIDGE_TOKEN and answer
// 404 while the token is unset, so a deployment without the bridge has no
// visible surface at all.

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/Aniraku/Aniraku-Backend/internal/streaming"
)

// mkissaBridgeAuth enforces the shared-token gate. false means a response
// has already been written (404 disabled / 401 bad token).
func (h *Handlers) mkissaBridgeAuth(w http.ResponseWriter, r *http.Request) bool {
	token := streaming.MkissaBridgeToken()
	if token == "" {
		h.respondError(w, http.StatusNotFound, "not found")
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}

// MkissaBridgePoll hands queued signed jobs to the relay worker. Jobs
// stay queued until delivered (at-least-once), so an empty queue is the
// steady state between fan-outs.
func (h *Handlers) MkissaBridgePoll(w http.ResponseWriter, r *http.Request) {
	if !h.mkissaBridgeAuth(w, r) {
		return
	}
	jobs := streaming.MkissaBridgePoll(8)
	h.log.Debug().Int("jobs", len(jobs)).Msg("mkissa: bridge poll")
	if jobs == nil {
		// Contract: jobs is always a JSON array (never null).
		h.respondJSON(w, http.StatusOK, map[string]any{"jobs": []any{}})
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

// MkissaBridgeResult returns one worker's answer to the waiting Call.
// ok=false just means nobody is waiting anymore (late or duplicate
// answer) — a normal occurrence, not an error the worker should retry.
func (h *Handlers) MkissaBridgeResult(w http.ResponseWriter, r *http.Request) {
	if !h.mkissaBridgeAuth(w, r) {
		return
	}
	var body struct {
		ID    uint64 `json:"id"`
		Line  string `json:"line"`
		Error string `json:"error"`
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(&body); err != nil {
		h.respondError(w, http.StatusBadRequest, "bad json")
		return
	}
	if body.ID == 0 {
		h.respondError(w, http.StatusBadRequest, "id required")
		return
	}
	if body.Line == "" && body.Error == "" {
		h.respondError(w, http.StatusBadRequest, "line or error required")
		return
	}
	ok := streaming.MkissaBridgeDeliver(body.ID, body.Line, body.Error)
	h.respondJSON(w, http.StatusOK, map[string]any{"ok": ok})
}
