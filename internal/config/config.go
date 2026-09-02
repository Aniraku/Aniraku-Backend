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
	Sync      SyncConfig     `mapstructure:"sync"`
	TMDB      TMDBConfig     `mapstructure:"tmdb"`
	Scraping  ScrapingConfig `mapstructure:"scraping"`
}

type SyncConfig struct {
	// OAuth credentials for MAL / AniList watch-progress sync. Leave
	// empty to disable the feature — the UI shows "not configured".
	MALClientID       string `mapstructure:"mal_client_id"`
	MALClientSecret   string `mapstructure:"mal_client_secret"`
	AniListClientID   string `mapstructure:"anilist_client_id"`
	AniListClientSecret string `mapstructure:"anilist_client_secret"`
	// RedirectURL is the registered OAuth redirect URI — the frontend's
	// /sync/callback route (e.g. https://aniraku.app/sync/callback).
	RedirectURL string `mapstructure:"oauth_redirect_url"`
	// StateSecret signs OAuth state so callbacks can be validated
	// statelessly. Generate a random long string and keep it secret.
	StateSecret string `mapstructure:"oauth_state_secret"`
}

func (s *SyncConfig) MALConfigured() bool  { return s.MALClientID != "" }
func (s *SyncConfig) AniListConfigured() bool { return s.AniListClientID != "" }

type ServerConfig struct {
	Host               string `mapstructure:"host"`
	Port               int    `mapstructure:"port"`
	UIDist             string `mapstructure:"ui_dist"`
	Debug              bool   `mapstructure:"debug"`
	MiruroProxyURL     string `mapstructure:"miruro_proxy_url"`
	AnikotoMappingPath string `mapstructure:"anikoto_mapping_path"`
}

type TMDBConfig struct {
	ReadAccessToken string `mapstructure:"read_access_token"`
	APIBase         string `mapstructure:"api_base"`
	ImageBase       string `mapstructure:"image_base"`
	AnibridgeAPI    string `mapstructure:"anibridge_api"`
}

type ScrapingConfig struct {
	AnimeXBase   string `mapstructure:"animex_base"`
	FlixCloudBase string `mapstructure:"flixcloud_base"`
	AniZipBase   string `mapstructure:"anizip_base"`
}

type SupabaseConfig struct {
	URL        string `mapstructure:"url"`
	AnonKey    string `mapstructure:"anon_key"`
	ServiceKey string `mapstructure:"service_key"`
	JWTAud     string `mapstructure:"jwt_aud"`
	JWKSURL    string `mapstructure:"jwks_url"`
}

type ProviderConfig struct {
	Primary string `mapstructure:"primary"`
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

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("update.channel", "stable")
	v.SetDefault("update.url", "https://api.aniraku.app/update")
	v.SetDefault("tmdb.api_base", "https://api.themoviedb.org/3")
	v.SetDefault("tmdb.image_base", "https://image.tmdb.org/t/p/w780")
	v.SetDefault("tmdb.anibridge_api", "https://mappings.anibridge.eliasbenb.dev/api/v3/mappings")
	v.SetDefault("scraping.animex_base", "https://animex.one")
	v.SetDefault("scraping.flixcloud_base", "https://flixcloud.cc")
	v.SetDefault("scraping.anizip_base", "https://api.ani.zip")

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
	if akp := os.Getenv("ANIRAKU_ANIKOTO_MAPPING_PATH"); akp != "" {
		v.Set("server.anikoto_mapping_path", akp)
	}

	// OAuth sync credentials (optional — feature disabled when absent)
	if val := os.Getenv("ANIRAKU_MAL_CLIENT_ID"); val != "" {
		v.Set("sync.mal_client_id", val)
	}
	if val := os.Getenv("ANIRAKU_MAL_CLIENT_SECRET"); val != "" {
		v.Set("sync.mal_client_secret", val)
	}
	if val := os.Getenv("ANIRAKU_ANILIST_CLIENT_ID"); val != "" {
		v.Set("sync.anilist_client_id", val)
	}
	if val := os.Getenv("ANIRAKU_ANILIST_CLIENT_SECRET"); val != "" {
		v.Set("sync.anilist_client_secret", val)
	}
	if val := os.Getenv("ANIRAKU_OAUTH_REDIRECT_URL"); val != "" {
		v.Set("sync.oauth_redirect_url", val)
	}
	if val := os.Getenv("ANIRAKU_OAUTH_STATE_SECRET"); val != "" {
		v.Set("sync.oauth_state_secret", val)
	}
	if val := os.Getenv("TMDB_READ_ACCESS_TOKEN"); val != "" {
		v.Set("tmdb.read_access_token", val)
	}
	if val := os.Getenv("ANIRAKU_TMDB_READ_ACCESS_TOKEN"); val != "" {
		v.Set("tmdb.read_access_token", val)
	}
	if val := os.Getenv("ANIRAKU_TMDB_API_BASE"); val != "" {
		v.Set("tmdb.api_base", val)
	}
	if val := os.Getenv("ANIRAKU_TMDB_IMAGE_BASE"); val != "" {
		v.Set("tmdb.image_base", val)
	}
	if val := os.Getenv("ANIRAKU_ANIBRIDGE_API"); val != "" {
		v.Set("tmdb.anibridge_api", val)
	}
	if val := os.Getenv("ANIRAKU_ANIMEX_BASE"); val != "" {
		v.Set("scraping.animex_base", val)
	}
	if val := os.Getenv("ANIRAKU_FLIXCLOUD_BASE"); val != "" {
		v.Set("scraping.flixcloud_base", val)
	}
	if val := os.Getenv("ANIRAKU_ANIZIP_BASE"); val != "" {
		v.Set("scraping.anizip_base", val)
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
