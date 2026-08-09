package v1

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Aniraku/Aniraku-Backend/internal/auth"
	"github.com/Aniraku/Aniraku-Backend/internal/config"
)

// ────────────────────────────────────────────────────────────────
// MAL / AniList watch-progress sync
//
// Flow:
//   1. Frontend (logged-in) asks GET /api/v1/sync/{provider}/authorize.
//      The backend builds the provider OAuth URL (PKCE, signed state) and
//      remembers the pending handshake in memory.
//   2. Provider redirects the browser to the registered redirect URI —
//      the frontend's /sync/callback route (never the backend).
//   3. The frontend POSTs {code, state} to /api/v1/sync/{provider}/callback
//      with the user's Supabase JWT. The backend exchanges the code for
//      tokens and stores them in the user's user_settings row.
//   4. While watching, the frontend calls POST /api/v1/sync/update to push
//      episode progress to whichever providers are connected.
// ────────────────────────────────────────────────────────────────

const (
	syncStateTTL       = 10 * time.Minute
	syncTokenSettingKey = "sync_tokens"
)

var syncLetterRunes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

type pendingOAuth struct {
	userID    string
	provider  string
	verifier  string
	expiresAt time.Time
}

// syncTokenSet is the value stored in user_settings under "sync_tokens".
type syncTokenSet map[string]syncProviderToken

type syncProviderToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"` // unix seconds; 0 = unknown
	Username     string `json:"username"`
}

func randomString(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		for i := range b {
			b[i] = syncLetterRunes[i%len(syncLetterRunes)]
		}
		return string(b)
	}
	for i := range b {
		b[i] = syncLetterRunes[int(b[i])%len(syncLetterRunes)]
	}
	return string(b)
}

// pkceChallenge computes the S256 PKCE challenge for a code verifier.
func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (h *Handlers) syncConfigured(provider string) bool {
	switch provider {
	case "mal":
		return h.cfg.Sync.MALConfigured()
	case "anilist":
		return h.cfg.Sync.AniListConfigured()
	}
	return false
}

// SyncStatus reports which providers the server can sync and which the
// current user has connected. Token secrets are never returned.
func (h *Handlers) SyncStatus(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	tokens, err := h.loadSyncTokens(r.Context(), userID)
	if err != nil {
		h.log.Warn().Err(err).Msg("sync status: token load failed")
		h.respondError(w, http.StatusBadGateway, "failed to read sync state")
		return
	}

	statusOf := func(provider string) map[string]any {
		connected := false
		username := ""
		expiresAt := int64(0)
		if t, ok := tokens[provider]; ok && t.AccessToken != "" {
			connected = true
			username = t.Username
			expiresAt = t.ExpiresAt
		}
		return map[string]any{
			"configured": h.syncConfigured(provider),
			"connected":  connected,
			"username":   username,
			"expires_at": expiresAt,
		}
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"mal":     statusOf("mal"),
		"anilist": statusOf("anilist"),
	})
}

// SyncAuthorize builds the provider authorization URL (PKCE + state).
func (h *Handlers) SyncAuthorize(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	provider := chi.URLParam(r, "provider")
	if !h.syncConfigured(provider) {
		h.respondError(w, http.StatusNotImplemented, fmt.Sprintf("%s sync is not configured on this server", provider))
		return
	}
	if h.cfg.Sync.RedirectURL == "" {
		h.respondError(w, http.StatusNotImplemented, "OAuth redirect URL is not configured on this server")
		return
	}

	verifier := randomString(43)
	state := randomString(43)
	h.syncPending.Store(state, &pendingOAuth{
		userID:    userID,
		provider:  provider,
		verifier:  verifier,
		expiresAt: time.Now().Add(syncStateTTL),
	})

	var authorizeURL string
	switch provider {
	case "mal":
		q := url.Values{}
		q.Set("response_type", "code")
		q.Set("client_id", h.cfg.Sync.MALClientID)
		// MAL only supports PLAIN code challenges.
		q.Set("code_challenge", verifier)
		q.Set("code_challenge_method", "plain")
		q.Set("state", state)
		q.Set("redirect_uri", h.cfg.Sync.RedirectURL)
		authorizeURL = "https://myanimelist.net/v1/oauth2/authorize?" + q.Encode()
	case "anilist":
		q := url.Values{}
		q.Set("response_type", "code")
		q.Set("client_id", h.cfg.Sync.AniListClientID)
		q.Set("code_challenge", pkceChallenge(verifier))
		q.Set("code_challenge_method", "S256")
		q.Set("state", state)
		q.Set("redirect_uri", h.cfg.Sync.RedirectURL)
		authorizeURL = "https://anilist.co/api/v2/oauth/authorize?" + q.Encode()
	default:
		h.respondError(w, http.StatusBadRequest, "unknown sync provider")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]any{"url": authorizeURL})
}

// SyncCallback exchanges the OAuth code for tokens and stores them.
func (h *Handlers) SyncCallback(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	provider := chi.URLParam(r, "provider")
	if !h.syncConfigured(provider) {
		h.respondError(w, http.StatusNotImplemented, fmt.Sprintf("%s sync is not configured on this server", provider))
		return
	}
	h.completeOAuthExchange(w, r, userID, provider)
}

// SyncCallbackGeneric is like SyncCallback but resolves the provider from
// the pending OAuth state instead of the URL path. MAL / AniList redirect
// back to the registered URI with only ?code= and ?state= — there is no
// provider in the URL — so the callback page cannot know which provider
// it came from. The pending-state map (keyed by state, bound to a user)
// already stores the provider, making the path parameter redundant.
func (h *Handlers) SyncCallbackGeneric(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	r.Body.Close()
	var input struct {
		Code  string `json:"code"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.Code == "" || input.State == "" {
		h.respondError(w, http.StatusBadRequest, "code and state are required")
		return
	}

	pendingRaw, ok := h.syncPending.Load(input.State)
	if !ok {
		h.respondError(w, http.StatusBadRequest, "invalid or expired OAuth state — start over")
		return
	}
	pending, ok := pendingRaw.(*pendingOAuth)
	if !ok || pending.userID != userID {
		h.respondError(w, http.StatusBadRequest, "invalid OAuth state")
		return
	}
	if !h.syncConfigured(pending.provider) {
		h.respondError(w, http.StatusNotImplemented, fmt.Sprintf("%s sync is not configured on this server", pending.provider))
		return
	}
	// Restore the body for the shared exchange path.
	r.Body = io.NopCloser(bytes.NewReader(body))
	h.completeOAuthExchange(w, r, userID, pending.provider)
}

// completeOAuthExchange validates the pending state and performs the code
// exchange for a user + provider, storing the tokens on success. The
// state's ownership and expiry checks run first so both entry points
// behave identically.
func (h *Handlers) completeOAuthExchange(w http.ResponseWriter, r *http.Request, userID, provider string) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	r.Body.Close()
	var input struct {
		Code  string `json:"code"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.Code == "" || input.State == "" {
		h.respondError(w, http.StatusBadRequest, "code and state are required")
		return
	}

	// Validate the state: must exist, match this user/provider, and be fresh.
	pendingRaw, ok := h.syncPending.Load(input.State)
	if !ok {
		h.respondError(w, http.StatusBadRequest, "invalid or expired OAuth state — start over")
		return
	}
	pending, ok := pendingRaw.(*pendingOAuth)
	if !ok || pending.userID != userID || pending.provider != provider {
		h.respondError(w, http.StatusBadRequest, "invalid OAuth state")
		return
	}
	if time.Now().After(pending.expiresAt) {
		h.syncPending.Delete(input.State)
		h.respondError(w, http.StatusBadRequest, "OAuth state expired — try again")
		return
	}
	defer h.syncPending.Delete(input.State)

	var access, refresh, username string
	var expiresIn int64
	var err error
	switch provider {
	case "mal":
		access, refresh, username, expiresIn, err = h.exchangeMALCode(r.Context(), input.Code, pending.verifier)
	case "anilist":
		access, refresh, username, expiresIn, err = h.exchangeAniListCode(r.Context(), input.Code, pending.verifier)
	}
	if err != nil {
		h.log.Warn().Err(err).Str("provider", provider).Msg("sync token exchange failed")
		h.respondError(w, http.StatusBadGateway, fmt.Sprintf("%s rejected the authorization code — %s", strings.ToUpper(provider), scrubSensitive(err.Error(), h.cfg)))
		return
	}

	expiresAt := int64(0)
	if expiresIn > 0 {
		expiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second).Unix()
	}

	if err := h.saveSyncToken(r.Context(), userID, provider, syncProviderToken{
		AccessToken:  access,
		RefreshToken: refresh,
		ExpiresAt:    expiresAt,
		Username:     username,
	}); err != nil {
		h.log.Warn().Err(err).Str("provider", provider).Msg("sync token save failed")
		h.respondError(w, http.StatusBadGateway, "could not save your sync connection")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]any{
		"connected": true,
		"provider":  provider,
		"username":  username,
	})
}

// SyncDisconnect removes a provider's tokens for the current user.
func (h *Handlers) SyncDisconnect(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	provider := chi.URLParam(r, "provider")
	if provider != "mal" && provider != "anilist" {
		h.respondError(w, http.StatusBadRequest, "unknown sync provider")
		return
	}

	tokens, err := h.loadSyncTokens(r.Context(), userID)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to read sync state")
		return
	}
	delete(tokens, provider)
	if err := h.storeSyncTokens(r.Context(), userID, tokens); err != nil {
		h.log.Warn().Err(err).Msg("sync disconnect: token save failed")
		h.respondError(w, http.StatusBadGateway, "could not disconnect")
		return
	}
	h.respondJSON(w, http.StatusOK, map[string]bool{"disconnected": true})
}

// SyncUpdate pushes watch progress to a connected provider. If the access
// token is stale it is refreshed first. Failures never break playback —
// the frontend treats any non-2xx as "skip".
func (h *Handlers) SyncUpdate(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	r.Body.Close()
	var input struct {
		Provider string `json:"provider"`
		AnimeID  int    `json:"animeId"` // AniList ID
		Episode  int    `json:"episode"`
		Progress int    `json:"progress"` // seconds of media played
	}
	if err := json.Unmarshal(body, &input); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	if input.Provider != "mal" && input.Provider != "anilist" {
		h.respondError(w, http.StatusBadRequest, "unknown sync provider")
		return
	}
	if input.AnimeID <= 0 || input.Episode <= 0 {
		h.respondError(w, http.StatusBadRequest, "animeId and episode are required")
		return
	}
	// Guard against marking episodes watched without playback: an empty or
	// not-yet-aired stream has no playable duration, so progress is 0.
	if input.Progress <= 0 {
		h.respondError(w, http.StatusBadRequest, "episode not watched")
		return
	}

	tokens, err := h.loadSyncTokens(r.Context(), userID)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to read sync state")
		return
	}
	token, ok := tokens[input.Provider]
	if !ok || token.AccessToken == "" {
		h.respondError(w, http.StatusNotFound, "provider not connected")
		return
	}

	// Refresh a stale token before pushing.
	if token.ExpiresAt > 0 && time.Now().Unix() >= token.ExpiresAt {
		var refreshed syncProviderToken
		var err error
		if input.Provider == "mal" {
			refreshed, err = h.refreshMALToken(r.Context(), token)
		} else {
			refreshed, err = h.refreshAniListToken(r.Context(), token)
		}
		if err != nil {
			h.log.Warn().Err(err).Str("provider", input.Provider).Msg("sync token refresh failed")
			h.respondError(w, http.StatusUnauthorized, "sync session expired — reconnect from Settings")
			return
		}
		token = refreshed
		if err := h.saveSyncToken(r.Context(), userID, input.Provider, token); err != nil {
			h.log.Warn().Err(err).Msg("sync refreshed token save failed")
		}
	}

	// AniList IDs map to MAL IDs via AniList itself (idMal).
	var malID int
	if input.Provider == "mal" {
		malID, err = h.anilistToMALID(r.Context(), input.AnimeID)
		if err != nil || malID <= 0 {
			h.log.Warn().Err(err).Int("animeId", input.AnimeID).Msg("sync: mal id lookup failed")
			h.respondError(w, http.StatusNotFound, "could not map anime to MAL")
			return
		}
	}

	if input.Provider == "mal" {
		err = h.updateMALProgress(r.Context(), token.AccessToken, malID, input.Episode)
	} else {
		err = h.updateAniListProgress(r.Context(), token.AccessToken, input.AnimeID, input.Episode)
	}
	if err != nil {
		h.log.Warn().Err(err).Str("provider", input.Provider).Int("animeId", input.AnimeID).Msg("sync progress update failed")
		h.respondError(w, http.StatusBadGateway, "progress update failed")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]any{"synced": true, "provider": input.Provider, "episode": input.Episode})
}

// ────────────────────────────────────────────────────────────────
// Token storage (user_settings, key "sync_tokens", per-user RLS)
// ────────────────────────────────────────────────────────────────

func (h *Handlers) loadSyncTokens(ctx context.Context, userID string) (syncTokenSet, error) {
	resp, err := h.supabaseRequest(ctx, "GET",
		"/rest/v1/user_settings?select=value&user_id=eq."+encodePath(userID)+"&key=eq."+url.QueryEscape(syncTokenSettingKey)+"&limit=1",
		nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return syncTokenSet{}, nil
	}
	var rows []struct {
		Value syncTokenSet `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 || rows[0].Value == nil {
		return syncTokenSet{}, nil
	}
	return rows[0].Value, nil
}

func (h *Handlers) storeSyncTokens(ctx context.Context, userID string, tokens syncTokenSet) error {
	raw, _ := json.Marshal([]map[string]any{{
		"user_id":    userID,
		"key":        syncTokenSettingKey,
		"value":      tokens,
		"updated_at": "now()",
	}})
	resp, err := h.supabaseRequest(ctx, "POST",
		"/rest/v1/user_settings?on_conflict=user_id,key",
		bytes.NewReader(raw),
		map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("supabase returned %s", resp.Status)
	}
	return nil
}

func (h *Handlers) saveSyncToken(ctx context.Context, userID, provider string, token syncProviderToken) error {
	tokens, err := h.loadSyncTokens(ctx, userID)
	if err != nil {
		return err
	}
	tokens[provider] = token
	return h.storeSyncTokens(ctx, userID, tokens)
}

// ────────────────────────────────────────────────────────────────
// MAL OAuth
// ────────────────────────────────────────────────────────────────

func (h *Handlers) exchangeMALCode(ctx context.Context, code, verifier string) (access, refresh, username string, expiresIn int64, err error) {
	form := url.Values{}
	form.Set("client_id", h.cfg.Sync.MALClientID)
	if h.cfg.Sync.MALClientSecret != "" {
		form.Set("client_secret", h.cfg.Sync.MALClientSecret)
	}
	form.Set("code", code)
	form.Set("code_verifier", verifier)
	form.Set("grant_type", "authorization_code")
	form.Set("redirect_uri", h.cfg.Sync.RedirectURL)

	tok, err := h.malTokenRequest(ctx, form)
	if err != nil {
		return "", "", "", 0, err
	}
	username, err = h.fetchMALUsername(ctx, tok.AccessToken)
	if err != nil {
		h.log.Warn().Err(err).Msg("mal username fetch failed (tokens still stored)")
	}
	return tok.AccessToken, tok.RefreshToken, username, tok.ExpiresIn, nil
}

func (h *Handlers) refreshMALToken(ctx context.Context, token syncProviderToken) (syncProviderToken, error) {
	form := url.Values{}
	form.Set("client_id", h.cfg.Sync.MALClientID)
	if h.cfg.Sync.MALClientSecret != "" {
		form.Set("client_secret", h.cfg.Sync.MALClientSecret)
	}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", token.RefreshToken)

	tok, err := h.malTokenRequest(ctx, form)
	if err != nil {
		return token, err
	}
	token.AccessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		token.RefreshToken = tok.RefreshToken
	}
	token.ExpiresAt = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second).Unix()
	return token, nil
}

type oauthTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

func (h *Handlers) malTokenRequest(ctx context.Context, form url.Values) (*oauthTokenResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", "https://myanimelist.net/v1/oauth2/token",
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mal token endpoint returned %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	var tok oauthTokenResponse
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, fmt.Errorf("mal token decode: %w", err)
	}
	if tok.AccessToken == "" {
		return nil, fmt.Errorf("mal token response missing access_token")
	}
	return &tok, nil
}

func (h *Handlers) fetchMALUsername(ctx context.Context, accessToken string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.myanimelist.net/v2/users/@me", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var me struct {
		Name string `json:"name"`
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("mal user endpoint returned %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil {
		return "", err
	}
	return me.Name, nil
}

func (h *Handlers) updateMALProgress(ctx context.Context, accessToken string, malID, episode int) error {
	form := url.Values{}
	form.Set("num_watched_episodes", fmt.Sprintf("%d", episode))
	form.Set("status", "watching")
	if total, ok := h.anilistEpisodeCount(ctx, malID); ok && episode >= total {
		form.Set("status", "completed")
	}

	req, err := http.NewRequestWithContext(ctx, "PUT",
		fmt.Sprintf("https://api.myanimelist.net/v2/anime/%d/my_list_status", malID),
		strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mal update returned %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	return nil
}

// ────────────────────────────────────────────────────────────────
// AniList OAuth
// ────────────────────────────────────────────────────────────────

func (h *Handlers) exchangeAniListCode(ctx context.Context, code, verifier string) (access, refresh, username string, expiresIn int64, err error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", h.cfg.Sync.AniListClientID)
	if h.cfg.Sync.AniListClientSecret != "" {
		form.Set("client_secret", h.cfg.Sync.AniListClientSecret)
	}
	form.Set("code", code)
	form.Set("code_verifier", verifier)
	form.Set("redirect_uri", h.cfg.Sync.RedirectURL)

	tok, err := h.anilistTokenRequest(ctx, form)
	if err != nil {
		return "", "", "", 0, err
	}
	username, err = h.fetchAniListUsername(ctx, tok.AccessToken)
	if err != nil {
		h.log.Warn().Err(err).Msg("anilist username fetch failed (tokens still stored)")
	}
	return tok.AccessToken, tok.RefreshToken, username, tok.ExpiresIn, nil
}

func (h *Handlers) refreshAniListToken(ctx context.Context, token syncProviderToken) (syncProviderToken, error) {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("client_id", h.cfg.Sync.AniListClientID)
	if h.cfg.Sync.AniListClientSecret != "" {
		form.Set("client_secret", h.cfg.Sync.AniListClientSecret)
	}
	form.Set("refresh_token", token.RefreshToken)

	tok, err := h.anilistTokenRequest(ctx, form)
	if err != nil {
		return token, err
	}
	token.AccessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		token.RefreshToken = tok.RefreshToken
	}
	token.ExpiresAt = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second).Unix()
	return token, nil
}

func (h *Handlers) anilistTokenRequest(ctx context.Context, form url.Values) (*oauthTokenResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", "https://anilist.co/api/v2/oauth/token",
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anilist token endpoint returned %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	var tok oauthTokenResponse
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, fmt.Errorf("anilist token decode: %w", err)
	}
	if tok.AccessToken == "" {
		return nil, fmt.Errorf("anilist token response missing access_token")
	}
	return &tok, nil
}

func (h *Handlers) fetchAniListUsername(ctx context.Context, accessToken string) (string, error) {
	query := `query { Viewer { name } }`
	payload, _ := json.Marshal(map[string]any{"query": query})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://graphql.anilist.co", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Data struct {
			Viewer struct {
				Name string `json:"name"`
			} `json:"Viewer"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Data.Viewer.Name, nil
}

func (h *Handlers) updateAniListProgress(ctx context.Context, accessToken string, anilistID, episode int) error {
	query := `mutation ($id: Int, $progress: Int, $status: MediaListStatus) {
		SaveMediaListEntry(mediaId: $id, progress: $progress, status: $status) { id }
	}`
	status := "CURRENT"
	if total, ok := h.anilistEpisodeCount(ctx, anilistID); ok && episode >= total {
		status = "COMPLETED"
	}
	payload, _ := json.Marshal(map[string]any{
		"query": query,
		"variables": map[string]any{
			"id": anilistID, "progress": episode, "status": status,
		},
	})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://graphql.anilist.co", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("anilist update returned %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	var out struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &out); err == nil && len(out.Errors) > 0 {
		return fmt.Errorf("anilist rejected update: %s", out.Errors[0].Message)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────
// Score sync (aggregated episode ratings → anime score)
// ────────────────────────────────────────────────────────────────

func (h *Handlers) SyncScore(w http.ResponseWriter, r *http.Request) {
	userID := auth.GetUserID(r.Context())
	if userID == "" {
		h.respondError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(r.Body, 16<<10))
	r.Body.Close()
	var input struct {
		Provider string `json:"provider"`
		AnimeID  int    `json:"animeId"` // AniList ID
		Score    int    `json:"score"`   // 1-10
	}
	if err := json.Unmarshal(body, &input); err != nil {
		h.respondError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	if input.Provider != "mal" && input.Provider != "anilist" {
		h.respondError(w, http.StatusBadRequest, "unknown sync provider")
		return
	}
	if input.AnimeID <= 0 || input.Score < 1 || input.Score > 10 {
		h.respondError(w, http.StatusBadRequest, "animeId and a 1-10 score are required")
		return
	}

	tokens, err := h.loadSyncTokens(r.Context(), userID)
	if err != nil {
		h.respondError(w, http.StatusBadGateway, "failed to read sync state")
		return
	}
	token, ok := tokens[input.Provider]
	if !ok || token.AccessToken == "" {
		h.respondError(w, http.StatusNotFound, "provider not connected")
		return
	}

	if token.ExpiresAt > 0 && time.Now().Unix() >= token.ExpiresAt {
		var refreshed syncProviderToken
		if input.Provider == "mal" {
			refreshed, err = h.refreshMALToken(r.Context(), token)
		} else {
			refreshed, err = h.refreshAniListToken(r.Context(), token)
		}
		if err != nil {
			h.log.Warn().Err(err).Str("provider", input.Provider).Msg("sync score token refresh failed")
			h.respondError(w, http.StatusUnauthorized, "sync session expired — reconnect from Settings")
			return
		}
		token = refreshed
		if err := h.saveSyncToken(r.Context(), userID, input.Provider, token); err != nil {
			h.log.Warn().Err(err).Msg("sync refreshed token save failed")
		}
	}

	var malID int
	if input.Provider == "mal" {
		malID, err = h.anilistToMALID(r.Context(), input.AnimeID)
		if err != nil || malID <= 0 {
			h.log.Warn().Err(err).Int("animeId", input.AnimeID).Msg("sync score: mal id lookup failed")
			h.respondError(w, http.StatusNotFound, "could not map anime to MAL")
			return
		}
	}

	if input.Provider == "mal" {
		err = h.updateMALScore(r.Context(), token.AccessToken, malID, input.Score)
	} else {
		err = h.updateAniListScore(r.Context(), token.AccessToken, input.AnimeID, input.Score)
	}
	if err != nil {
		h.log.Warn().Err(err).Str("provider", input.Provider).Int("animeId", input.AnimeID).Msg("sync score update failed")
		h.respondError(w, http.StatusBadGateway, "score update failed")
		return
	}

	h.respondJSON(w, http.StatusOK, map[string]any{"synced": true, "provider": input.Provider, "score": input.Score})
}

// updateMALScore sets the anime score (0-10) via the MAL API.
func (h *Handlers) updateMALScore(ctx context.Context, accessToken string, malID, score int) error {
	form := url.Values{}
	form.Set("score", fmt.Sprintf("%d", score))

	req, err := http.NewRequestWithContext(ctx, "PUT",
		fmt.Sprintf("https://api.myanimelist.net/v2/anime/%d/my_list_status", malID),
		strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mal score update returned %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	return nil
}

// updateAniListScore sets the anime score (1-10 → scoreRaw 10-100) via
// the AniList GraphQL API.
func (h *Handlers) updateAniListScore(ctx context.Context, accessToken string, anilistID, score int) error {
	query := `mutation ($mediaId: Int, $scoreRaw: Float) {
		SaveMediaListEntry(mediaId: $mediaId, scoreRaw: $scoreRaw) { id }
	}`
	payload, _ := json.Marshal(map[string]any{
		"query": query,
		"variables": map[string]any{
			"mediaId": anilistID, "scoreRaw": float64(score) * 10,
		},
	})
	req, err := http.NewRequestWithContext(ctx, "POST", "https://graphql.anilist.co", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := h.h2Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("anilist score update returned %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	var out struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &out); err == nil && len(out.Errors) > 0 {
		return fmt.Errorf("anilist rejected score update: %s", out.Errors[0].Message)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────
// AniList metadata helpers
// ────────────────────────────────────────────────────────────────

// anilistToMALID maps an AniList ID to its MAL ID (idMal), with a small
// positive cache to avoid hammering AniList per episode update.
func (h *Handlers) anilistToMALID(ctx context.Context, anilistID int) (int, error) {
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { id idMal } }`
	raw, err := h.anilistClient.do(ctx, query, map[string]any{"id": anilistID})
	if err != nil {
		return 0, err
	}
	var out struct {
		Data struct {
			Media struct {
				IDMal *int `json:"idMal"`
			} `json:"Media"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Data.Media.IDMal == nil {
		return 0, fmt.Errorf("anime has no MAL id")
	}
	return *out.Data.Media.IDMal, nil
}

// anilistEpisodeCount returns the total episode count, if known.
func (h *Handlers) anilistEpisodeCount(ctx context.Context, anilistID int) (int, bool) {
	query := `query ($id: Int) { Media(id: $id, type: ANIME) { episodes } }`
	raw, err := h.anilistClient.do(ctx, query, map[string]any{"id": anilistID})
	if err != nil {
		return 0, false
	}
	var out struct {
		Data struct {
			Media struct {
				Episodes *int `json:"episodes"`
			} `json:"Media"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Data.Media.Episodes == nil {
		return 0, false
	}
	return *out.Data.Media.Episodes, true
}

func truncate(b []byte, n int) string {
	s := string(b)
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// scrubSensitive strips configured credentials from an error string before
// it is surfaced to the client, in case a provider echoes them back.
func scrubSensitive(s string, cfg *config.Config) string {
	secrets := []string{
		cfg.Sync.MALClientID,
		cfg.Sync.MALClientSecret,
		cfg.Sync.AniListClientID,
		cfg.Sync.AniListClientSecret,
	}
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[redacted]")
		}
	}
	return s
}
