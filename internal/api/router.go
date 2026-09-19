package api

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/api/middleware"
	"github.com/Aniraku/Aniraku-Backend/internal/api/v1"
	"github.com/Aniraku/Aniraku-Backend/internal/auth"
	"github.com/Aniraku/Aniraku-Backend/internal/config"
	"github.com/Aniraku/Aniraku-Backend/internal/embed"
)

// envInt reads a numeric env override, falling back to def when unset or
// malformed. Used for rate-limit tuning knobs.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// http_pprof is the standard net/http/pprof mux, bound to the admin group
// only when the server config opts in.
var http_pprof = newPprofMux()

func NewRouter(cfg *config.Config, log zerolog.Logger) *chi.Mux {
	r := chi.NewRouter()

	r.Use(middleware.RealIP)
	r.Use(chimw.CleanPath)
	r.Use(middleware.RequestID)
	r.Use(middleware.Recover(log))
	r.Use(middleware.Logging(log))
	r.Use(middleware.SecurityHeaders)
	r.Use(middleware.CORS)

	// Compression applies only to the JSON API response bodies (attached per
	// group below). The media proxy/streaming paths are never routed through
	// it: HLS segments and keys are already-compressed or raw bytes, and the
	// proxy must stream them byte-exact (AES-128 keys, byte ranges) without
	// a second Content-Encoding layer.
	jsonCompress := func(next http.Handler) http.Handler {
		return chimw.Compress(5, "application/json", "text/plain")(next)
	}

	// ponytail: 30 req/s per IP with burst of 60 — enough for normal browsing,
	// prevents abuse that causes AniList 502s. Overridable via env for
	// deployments with different traffic profiles.
	rl := middleware.NewRateLimiter(
		envInt("ANIRAKU_RATE_LIMIT", 30),
		envInt("ANIRAKU_RATE_BURST", 60),
		time.Second)

	// The media proxy streams many small requests (playlists, segments, keys)
	// per playback, but far fewer than the general limit allows for abuse.
	// 10 req/s with burst 20 is comfortable for adaptive HLS while ~3x tighter
	// than the global limiter.
	proxyRL := middleware.NewRateLimiter(
		envInt("ANIRAKU_PROXY_RATE_LIMIT", 10),
		envInt("ANIRAKU_PROXY_RATE_BURST", 20),
		time.Second)

	// Supabase publishes JWKS under /auth/v1, not the project root.
	// The previous default (`/.well-known/jwks.json`) 404s, which made
	// every authenticated route fail with "invalid token".
	jwksURL := cfg.Supabase.JWKSURL
	if jwksURL == "" {
		jwksURL = strings.TrimRight(cfg.Supabase.URL, "/") + "/auth/v1/.well-known/jwks.json"
	}
	jwks := auth.NewJWKS(jwksURL, log)
	issuer := strings.TrimRight(cfg.Supabase.URL, "/") + "/auth/v1"
	verifier := auth.NewVerifier(jwks, issuer, cfg.Supabase.JWTAud, log)
	authMiddleware := auth.Middleware(verifier, log)

	h := v1.NewHandlers(cfg, log)

	// Public endpoints (rate limited)
	r.Group(func(r chi.Router) {
		r.Use(rl.Middleware)
		r.Use(jsonCompress)

		r.Get("/api/v1/health", h.Health)
		r.Get("/api/v1/version", h.Version)
		// Catalog reads are server-cached for 5 min; let shared/browser caches
		// do the same. Streaming and proxy paths stay uncacheable.
		r.With(middleware.CacheShort).Get("/api/v1/anime/{id}", h.GetAnime)
		r.With(middleware.CacheShort).Get("/api/v1/anime/{id}/episodes", h.GetEpisodes)
		r.With(middleware.CacheShort).Get("/api/v1/anime/{id}/similar", h.GetSimilar)
		r.With(middleware.CacheShort).Get("/api/v1/anime/{id}/relations", h.GetRelations)
		r.Post("/api/v1/stream", h.Stream)
		r.Get("/api/v1/servers", h.GetServers)
		r.With(proxyRL.Middleware).Get("/api/v1/proxy", h.Proxy)
		r.With(proxyRL.Middleware).Head("/api/v1/proxy", h.Proxy)
		r.With(proxyRL.Middleware).Get("/api/v1/download", h.Download)
		r.Get("/ani/v1/epsrc", h.LegacyEpsrc)
		r.Post("/api/v1/anilist", h.AniListProxy)
	})

	// Auth-required endpoints (rate limited)
	r.Group(func(r chi.Router) {
		r.Use(rl.Middleware)
		r.Use(jsonCompress)
		r.Use(authMiddleware)

		r.Post("/api/v1/anime/{id}/progress", h.SaveAnimeProgress)
		r.Post("/api/v1/anime/{animeId}/episode/{episode}/rating", h.SaveEpisodeRating)
		r.Get("/api/v1/anime/{animeId}/ratings", h.GetEpisodeRatings)
		r.Get("/api/v1/continue-watching", h.GetContinueWatching)
		r.Post("/api/v1/import/mal", h.ImportMAL)
		r.Post("/api/v1/import/anilist", h.ImportAniList)
		r.Get("/api/v1/import/{jobId}", h.ImportStatus)
		r.Post("/api/v1/export/mal", h.ExportMAL)
		r.Post("/api/v1/export/anilist", h.ExportAniList)
		r.Get("/api/v1/profile/{username}", h.GetProfile)
		r.Put("/api/v1/profile", h.UpdateProfile)
		r.Post("/api/v1/favorites", h.AddFavorite)
		r.Delete("/api/v1/favorites/{mediaId}", h.RemoveFavorite)
		r.Get("/api/v1/favorites", h.ListFavorites)
		r.Post("/api/v1/logs", h.ClientLog)
		r.Get("/api/v1/settings/{key}", h.GetSetting)
		r.Put("/api/v1/settings/{key}", h.UpdateSetting)
		r.Get("/api/v1/notifications", h.GetNotifications)
		r.Put("/api/v1/notifications/{id}/read", h.MarkNotificationRead)
		r.Put("/api/v1/notifications/read-all", h.MarkAllNotificationsRead)

		// MAL / AniList watch-progress sync
		r.Get("/api/v1/sync", h.SyncStatus)
		r.Get("/api/v1/sync/{provider}/authorize", h.SyncAuthorize)
		r.Post("/api/v1/sync/callback", h.SyncCallbackGeneric)
		r.Post("/api/v1/sync/{provider}/callback", h.SyncCallback)
		r.Delete("/api/v1/sync/{provider}", h.SyncDisconnect)
		r.Post("/api/v1/sync/update", h.SyncUpdate)
		r.Put("/api/v1/sync/score", h.SyncScore)
	})

	// Admin-only endpoints. RequireAdmin re-verifies the user's role against
	// Supabase server-side — the client can never gate itself into /admin.
	r.Group(func(r chi.Router) {
		r.Use(rl.Middleware)
		r.Use(jsonCompress)
		r.Use(authMiddleware)
		r.Use(auth.RequireAdmin(cfg.Supabase.URL, cfg.Supabase.AnonKey, log))

		r.Get("/api/v1/admin/stats", h.AdminStats)
		r.Get("/api/v1/metrics", h.Metrics)

		// pprof is opt-in (ANIRAKU_ENABLE_PPROF=true): live CPU/heap profiles
		// for production debugging, still behind auth + admin. When disabled
		// the routes 404 by simply not being registered.
		if cfg.Server.EnablePprof {
			r.Mount("/debug/pprof", http_pprof)
		}
	})

	uiFS := embed.FS()
	if uiFS != nil {
		r.Handle("/*", embed.Handler())
	}

	return r
}
