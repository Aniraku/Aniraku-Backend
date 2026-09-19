package config

import (
	"os"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	// Isolate from any developer .env or ambient env.
	t.Setenv("ANIRAKU_SUPABASE_URL", "")
	t.Setenv("PORT", "")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Host != "127.0.0.1" {
		t.Errorf("host = %q, want 127.0.0.1", cfg.Server.Host)
	}
	if cfg.Server.Port != 43211 {
		t.Errorf("port = %d, want 43211", cfg.Server.Port)
	}
	if cfg.Supabase.JWTAud != "authenticated" {
		t.Errorf("jwt_aud = %q", cfg.Supabase.JWTAud)
	}
	if cfg.Logging.Level != "info" || cfg.Logging.Format != "json" {
		t.Errorf("logging = %q/%q, want info/json", cfg.Logging.Level, cfg.Logging.Format)
	}
	if cfg.TMDB.APIBase != "https://api.themoviedb.org/3" {
		t.Errorf("tmdb.api_base = %q", cfg.TMDB.APIBase)
	}
}

func TestLoadPortEnvBindsPublicly(t *testing.T) {
	t.Setenv("PORT", "8080")
	t.Setenv("ANIRAKU_SERVER_HOST", "")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("port = %d, want 8080 from PORT env", cfg.Server.Port)
	}
	// Cloud platforms inject PORT without a host override: the server must
	// listen publicly, not on loopback.
	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("host = %q, want 0.0.0.0 when PORT is set", cfg.Server.Host)
	}
}

func TestLoadServerHostOverride(t *testing.T) {
	t.Setenv("PORT", "9000")
	t.Setenv("ANIRAKU_SERVER_HOST", "10.1.2.3")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Host != "10.1.2.3" {
		t.Errorf("host = %q, want explicit ANIRAKU_SERVER_HOST to win over PORT", cfg.Server.Host)
	}
	if cfg.Server.Port != 9000 {
		t.Errorf("port = %d, want 9000", cfg.Server.Port)
	}
}

func TestLoadDebugFlag(t *testing.T) {
	t.Setenv("ANIRAKU_SERVER_DEBUG", "true")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Server.Debug {
		t.Error("debug = false, want true")
	}
}

func TestAddr(t *testing.T) {
	sc := ServerConfig{Host: "127.0.0.1", Port: 43211}
	if got := sc.Addr(); got != "127.0.0.1:43211" {
		t.Errorf("Addr = %q", got)
	}
}

// TestLoadDotEnvParsing covers the hand-rolled .env loader: KEY=VALUE lines,
// comments, and blank lines.
func TestLoadDotEnvParsing(t *testing.T) {
	f, err := os.CreateTemp("", "aniraku-env-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString("# comment\nANIRAKU_TEST_DOTENV=hello world\n\nBROKEN_LINE_OK\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	t.Cleanup(func() { os.Unsetenv("ANIRAKU_TEST_DOTENV") })
	loadEnv(f.Name())

	if got := os.Getenv("ANIRAKU_TEST_DOTENV"); got != "hello world" {
		t.Errorf("dotenv value = %q, want %q", got, "hello world")
	}
}
