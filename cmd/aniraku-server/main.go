package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

// buildLogger constructs the process logger from the logging config.
// JSON (machine-readable) is the default for production; the pretty
// ConsoleWriter is a developer affordance enabled by debug mode or an
// explicit `logging.format: console`. The returned logger is also stored
// globally so libraries that log before NewRouter (or during fatal config
// errors) share the same level and format.
func buildLogger(cfg *config.Config) zerolog.Logger {
	// JSON (default): write JSON lines directly to stderr. A zero-value
	// ConsoleWriter must never reach zerolog.New — its nil Out makes the
	// first log write panic with a nil-pointer dereference (bytes.Buffer
	// wrapping a nil writer).
	var log zerolog.Logger
	switch strings.ToLower(strings.TrimSpace(cfg.Logging.Format)) {
	case "console":
		log = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	default:
		log = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}

	var level zerolog.Level
	switch strings.ToLower(strings.TrimSpace(cfg.Logging.Level)) {
	case "trace":
		level = zerolog.TraceLevel
	case "debug":
		level = zerolog.DebugLevel
	case "warn", "warning":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	case "fatal":
		level = zerolog.FatalLevel
	default:
		// "info" and anything unrecognized: fail safe to info.
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)
	return log
}

func main() {
	// Provisional logger for startup/config errors, before config.Load can
	// populate logging.level/format. Always writes to stderr.
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()

	cfg, err := config.Load(configPath())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	log = buildLogger(cfg)

	if cfg.Server.Debug {
		// Debug mode implies the developer experience: pretty console output
		// at debug level, regardless of the configured format.
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
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
			Str("build_date", BuildDate).
			Str("log_level", zerolog.GlobalLevel().String()).
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

// configPath extracts the --config flag without pulling in a flag package:
// the server has exactly one flag and the value is the next argv entry.
func configPath() string {
	args := os.Args[1:]
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--config" {
			return args[i+1]
		}
	}
	return ""
}
