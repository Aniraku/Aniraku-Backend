package streaming

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// RelayClient routes blocked upstream API calls through the operator's
// relay worker (third_party/relay/worker.mjs): some endpoints
// Cloudflare-block datacenter egress while serving the relay fine, and the
// minted file URLs play from the backend egress with no IP binding
// (verified live). Only kilobytes of API JSON cross the relay — every
// video byte and every probe stays direct.
//
// Nil when ANIRAKU_RELAY_URL is unset: providers fall back to direct.
type RelayClient struct {
	base   string
	key    string
	client *http.Client
}

func NewRelayClient() *RelayClient {
	base := strings.TrimSpace(os.Getenv("ANIRAKU_RELAY_URL"))
	if base == "" {
		return nil
	}
	return &RelayClient{
		base:   strings.TrimRight(base, "/"),
		key:    strings.TrimSpace(os.Getenv("ANIRAKU_RELAY_KEY")),
		client: &http.Client{Timeout: 60 * time.Second, Transport: netguard.NewTransport()},
	}
}

func (r *RelayClient) postBytes(ctx context.Context, path string, payload any) ([]byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	actx, cancel := context.WithTimeout(ctx, 55*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodPost, r.base+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", browserUA)
	if r.key != "" {
		req.Header.Set("X-Relay-Key", r.key)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("relay %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("relay %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay %s: HTTP %d", path, resp.StatusCode)
	}
	return body, nil
}

func (r *RelayClient) post(ctx context.Context, path string, payload any, out any) error {
	body, err := r.postBytes(ctx, path, payload)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("relay %s json: %w", path, err)
	}
	return nil
}

// VidNestFetch resolves the VidNest API envelope through the relay (the
// direct API 403s datacenter egress). Returns the raw envelope for the
// normal decode path.
func (r *RelayClient) VidNestFetch(ctx context.Context, id, episode int, lang string) ([]byte, error) {
	return r.postBytes(ctx, "/vidnest", map[string]any{
		"id": id, "episode": episode, "lang": lang,
	})
}
