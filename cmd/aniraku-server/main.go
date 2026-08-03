package main

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
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
	for i, arg := range os.Args[1:] {
		if arg == "--config" && i+1 < len(os.Args)-1 {
			configPath = os.Args[i+2]
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

	// Start Miruro Cloudflare bypass proxy (Python + ViperTLS)
	var miruroProxyCmd *exec.Cmd
	if cfg.Server.MiruroProxyURL == "" {
		proxyScript := findMiruroProxyScript()
		python, err := exec.LookPath("python3")
		if err != nil {
			python, err = exec.LookPath("python")
			if err != nil {
				log.Warn().Msg("python not found, Miruro Cloudflare bypass disabled")
			}
		}
		if python != "" && proxyScript != "" {
			miruroProxyCmd = exec.Command(python, proxyScript)
			miruroProxyCmd.Stdout = os.Stderr
			miruroProxyCmd.Stderr = os.Stderr
			if err := miruroProxyCmd.Start(); err != nil {
				log.Warn().Err(err).Msg("failed to start Miruro proxy, Cloudflare bypass disabled")
				miruroProxyCmd = nil
			} else {
				log.Info().Int("pid", miruroProxyCmd.Process.Pid).Msg("Miruro Cloudflare bypass proxy started")
				time.Sleep(2 * time.Second)
			}
		}
	}

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

	if miruroProxyCmd != nil {
		log.Info().Int("pid", miruroProxyCmd.Process.Pid).Msg("stopping Miruro proxy")
		miruroProxyCmd.Process.Signal(os.Interrupt)
		miruroProxyCmd.Wait()
	}

	log.Info().Msg("server stopped")
}

func findMiruroProxyScript() string {
	candidates := []string{
		"proxy.py",
		"cmd/miruro-proxy/proxy.py",
		"../cmd/miruro-proxy/proxy.py",
	}
	execPath, err := os.Executable()
	if err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(execPath), "proxy.py"))
	}
	_, srcFile, _, ok := runtime.Caller(0)
	if ok {
		candidates = append(candidates, filepath.Join(filepath.Dir(srcFile), "../../cmd/miruro-proxy/proxy.py"))
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			abs, _ := filepath.Abs(c)
			return abs
		}
	}
	return ""
}
