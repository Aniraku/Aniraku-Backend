package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/api"
	"github.com/Aniraku/Aniraku-Backend/internal/api/v1"
	"github.com/Aniraku/Aniraku-Backend/internal/config"
)

var (
	Version   = "0.1.0"
	Commit    = "dev"
	BuildDate = "unknown"
)

func main() {
	output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	log := zerolog.New(output).With().Timestamp().Logger()

	configPath := ""
	args := os.Args[1:]
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--config" {
			configPath = args[i+1]
			break
		}
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	if cfg.Server.Debug {
		output := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
		log = zerolog.New(output).With().Timestamp().Logger()
	}

	v1.Version = Version
	v1.Commit = Commit
	v1.BuildDate = BuildDate

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	router := api.NewRouter(cfg, log)

	addr := cfg.Server.Addr()
	if addr == ":" {
		addr = "127.0.0.1:43211"
	}

	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info().
			Str("addr", addr).
			Str("version", Version).
			Str("commit", Commit).
			Msg("Aniraku server starting")

		// Start dynamic CDN allowlist cleanup (runs every hour)
		go func() {
			ticker := time.NewTicker(1 * time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					v1.CleanupDynamicCDNEntries()
					log.Debug().Int("dynamic_cdn_count", v1.GetDynamicCDNCount()).Msg("cleaned up dynamic CDN entries")
				case <-ctx.Done():
					return
				}
			}
		}()

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("server failed")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("server shutdown error")
	}

	log.Info().Msg("server stopped")
}
