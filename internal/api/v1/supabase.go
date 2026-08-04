package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/Aniraku/Aniraku-Backend/internal/auth"
)

// supabaseRequest issues a PostgREST call against Supabase using the caller's
// own JWT. RLS then enforces ownership on every row, so a handler can never
// read or write another user's data even if its filter is wrong. This is the
// safe replacement for the old pattern of calling with the service key and
// hand-applying an ownership filter.
//
// Returned response body is the raw PostgREST body. The caller is
// responsible for checking status and decoding. Errors here mean the request
// was malformed or the transport failed.
func (h *Handlers) supabaseRequest(ctx context.Context, method, path string, body io.Reader, headers map[string]string) (*http.Response, error) {
	supabaseURL := h.cfg.Supabase.URL
	if supabaseURL == "" {
		return nil, fmt.Errorf("supabase URL not configured")
	}
	token := auth.Token(ctx)
	if token == "" {
		return nil, fmt.Errorf("no user token in request context")
	}
	// The gateway validates apikey against the project's own keys, so it must
	// be the anon key; a user JWT is not a project key and is rejected with
	// "Invalid API key" before RLS is ever consulted.
	apiKey := h.cfg.Supabase.AnonKey
	if apiKey == "" {
		return nil, fmt.Errorf("supabase anon key not configured")
	}

	req, err := http.NewRequestWithContext(ctx, method, supabaseURL+path, body)
	if err != nil {
		return nil, err
	}
	// These two headers do different jobs and cannot share a value. apikey
	// identifies the project to the gateway; the Authorization bearer is the
	// user's JWT, which PostgREST resolves to the 'authenticated' role so RLS
	// applies per-user. The anon key grants no data access on its own — it
	// only gets the request past the gateway.
	req.Header.Set("apikey", apiKey)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// supabaseErrorBody best-effort parses PostgREST's {"message":..., "code":...}
// error envelope so handlers can log and relay a useful message.
func supabaseErrorBody(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body = io.NopCloser(bytesReader(body))
	var envelope struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	}
	if json.Unmarshal(body, &envelope) == nil && envelope.Message != "" {
		return fmt.Sprintf("supabase %s (%s): %s", resp.Status, envelope.Code, envelope.Message)
	}
	return fmt.Sprintf("supabase returned %s", resp.Status)
}

func encodePath(value string) string {
	return url.PathEscape(value)
}

func bytesReader(b []byte) io.Reader {
	return bytes.NewReader(b)
}
