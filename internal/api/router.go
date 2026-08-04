package api

import (
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

func NewRouter(cfg *config.Config, log zerolog.Logger) *chi.Mux {
	r := chi.NewRouter()

	r.Use(chimw.RealIP)
	r.Use(chimw.CleanPath)
	r.Use(middleware.RequestID)
	r.Use(middleware.Recover(log))
	r.Use(middleware.Logging(log))
	r.Use(middleware.SecurityHeaders)
	r.Use(middleware.CORS)

	// ponytail: 30 req/s per IP with burst of 60 — enough for normal browsing,
	// prevents abuse that causes AniList 502s
	rl := middleware.NewRateLimiter(30, 60, time.Second)

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

	miruroProxyURL := cfg.Server.MiruroProxyURL
	h := v1.NewHandlers(cfg, log, miruroProxyURL)

	// Public endpoints (rate limited)
	r.Group(func(r chi.Router) {
		r.Use(rl.Middleware)

		r.Get("/api/v1/health", h.Health)
		r.Get("/api/v1/version", h.Version)
		r.Get("/api/v1/anime/{id}", h.GetAnime)
		r.Get("/api/v1/anime/{id}/episodes", h.GetEpisodes)
		r.Get("/api/v1/anime/{id}/similar", h.GetSimilar)
		r.Get("/api/v1/anime/{id}/relations", h.GetRelations)
		r.Get("/api/v1/manga/{id}", h.GetManga)
		r.Get("/api/v1/manga/{id}/chapters", h.GetChapters)
		r.Get("/api/v1/manga/{id}/chapters/{ch}/pages", h.GetChapterPages)
		r.Get("/api/v1/search", h.Search)
		r.Get("/api/v1/trending", h.GetTrending)
		r.Get("/api/v1/seasonal", h.GetSeasonal)
		r.Get("/api/v1/browse", h.Browse)
		r.Get("/api/v1/genres", h.GetGenres)
		r.Get("/api/v1/schedule", h.GetSchedule)
		r.Post("/api/v1/stream", h.Stream)
		r.Get("/api/v1/servers", h.GetServers)
		r.Get("/api/v1/proxy", h.Proxy)
		r.Get("/api/v1/miruro/episodes/{id}", h.GetMiruroEpisodes)
		r.Get("/api/v1/miruro/has-dub/{id}", h.HasDub)
		r.Get("/api/v1/miruro/probe/{id}", h.GetMiruroProbe)
		r.Post("/api/v1/anilist", h.AniListProxy)
	})

	// Auth-required endpoints (rate limited)
	r.Group(func(r chi.Router) {
		r.Use(rl.Middleware)
		r.Use(authMiddleware)

		r.Post("/api/v1/anime/{id}/progress", h.SaveAnimeProgress)
		r.Post("/api/v1/manga/{id}/progress", h.SaveMangaProgress)
		r.Get("/api/v1/continue-watching", h.GetContinueWatching)
		r.Post("/api/v1/import/mal", h.ImportMAL)
		r.Post("/api/v1/import/anilist", h.ImportAniList)
		r.Get("/api/v1/import/{jobId}", h.ImportStatus)
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
	})

	uiFS := embed.FS()
	if uiFS != nil {
		r.Handle("/*", embed.Handler())
	}

	return r
}
