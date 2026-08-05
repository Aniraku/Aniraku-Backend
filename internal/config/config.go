package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

func loadEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			os.Setenv(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
}

type Config struct {
	Server    ServerConfig   `mapstructure:"server"`
	Supabase  SupabaseConfig `mapstructure:"supabase"`
	Providers ProviderConfig `mapstructure:"providers"`
	Logging   LoggingConfig  `mapstructure:"logging"`
	Update    UpdateConfig   `mapstructure:"update"`
}

type ServerConfig struct {
	Host           string `mapstructure:"host"`
	Port           int    `mapstructure:"port"`
	UIDist         string `mapstructure:"ui_dist"`
	Debug          bool   `mapstructure:"debug"`
	MiruroProxyURL string `mapstructure:"miruro_proxy_url"`
}

type SupabaseConfig struct {
	URL        string `mapstructure:"url"`
	AnonKey    string `mapstructure:"anon_key"`
	ServiceKey string `mapstructure:"service_key"`
	JWTAud     string `mapstructure:"jwt_aud"`
	JWKSURL    string `mapstructure:"jwks_url"`
}

type ProviderConfig struct {
	Primary    string `mapstructure:"primary"`
	Fallback   string `mapstructure:"fallback"`
	ZenAPIBase string `mapstructure:"zen_api_base"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type UpdateConfig struct {
	Channel string `mapstructure:"channel"`
	URL     string `mapstructure:"url"`
}

func Load(configPath string) (*Config, error) {
	loadEnv(".env")

	v := viper.GetViper()

	// Defaults
	v.SetDefault("server.host", "127.0.0.1")
	v.SetDefault("server.port", 43211)
	v.SetDefault("server.ui_dist", "embedded")
	v.SetDefault("server.debug", false)
	v.SetDefault("supabase.jwt_aud", "authenticated")
	v.SetDefault("providers.primary", "miruro")
	v.SetDefault("providers.fallback", "miruro")

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("update.channel", "stable")
	v.SetDefault("update.url", "https://api.aniraku.app/update")

	// Env overrides
	v.SetEnvPrefix("ANIRAKU")
	v.AutomaticEnv()

	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	// Override from env vars
	if url := os.Getenv("ANIRAKU_SUPABASE_URL"); url != "" {
		v.Set("supabase.url", url)
	}
	if key := os.Getenv("ANIRAKU_SUPABASE_ANON_KEY"); key != "" {
		v.Set("supabase.anon_key", key)
	}
	if key := os.Getenv("ANIRAKU_SUPABASE_SERVICE_KEY"); key != "" {
		v.Set("supabase.service_key", key)
	}
	if jwks := os.Getenv("ANIRAKU_SUPABASE_JWKS_URL"); jwks != "" {
		v.Set("supabase.jwks_url", jwks)
	}
	// Render / cloud: bind all interfaces and honor PORT
	if port := os.Getenv("PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			v.Set("server.port", p)
		}
	}
	if host := os.Getenv("ANIRAKU_SERVER_HOST"); host != "" {
		v.Set("server.host", host)
	} else if os.Getenv("PORT") != "" {
		// Cloud platforms inject PORT — listen publicly
		v.Set("server.host", "0.0.0.0")
	}
	if debug := os.Getenv("ANIRAKU_SERVER_DEBUG"); debug == "true" || debug == "1" {
		v.Set("server.debug", true)
	}
	if mpu := os.Getenv("ANIRAKU_MIRURO_PROXY_URL"); mpu != "" {
		v.Set("server.miruro_proxy_url", mpu)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *ServerConfig) Addr() string {
	return c.Host + ":" + strconv.Itoa(c.Port)
}
