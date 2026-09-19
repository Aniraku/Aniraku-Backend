package v1

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	"crypto/tls"

	utls "github.com/refraction-networking/utls"
	"github.com/rs/zerolog"
	"golang.org/x/net/http2"

	"github.com/Aniraku/Aniraku-Backend/internal/config"
	"github.com/Aniraku/Aniraku-Backend/internal/metadata/mal"
	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
	"github.com/Aniraku/Aniraku-Backend/internal/streaming"
)

var (
	Version   = "0.1.0"
	Commit    = "dev"
	BuildDate = "unknown"
)

type Handlers struct {
	cfg            *config.Config
	log            zerolog.Logger
	mal            *mal.Client
	stream         *streaming.Manager
	h2Client       *http.Client
	h1Client       *http.Client
	httpClient     *http.Client
	goTLSClient    *http.Client
	downloadClient *http.Client
	keyCache       sync.Map
	// Browse/trending cache with TTL
	browseCache    sync.Map
	browseCacheTTL time.Duration
	anilistClient  *anilistClient
	// In-flight OAuth handshakes for MAL/AniList sync: state -> pendingOAuth.
	syncPending sync.Map

	// --- Resilience layer ---
	anilistCircuit  *circuitBreaker
	anilistInflight sync.Map // request deduplication
	providerHealth  sync.Map // provider -> *providerHealth
}

func NewHandlers(cfg *config.Config, log zerolog.Logger) *Handlers {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 15 * time.Second}).DialContext(ctx, "udp", "8.8.8.8:53")
		},
	}
	baseDialer := &net.Dialer{
		Timeout:  15 * time.Second,
		Resolver: resolver,
		// SSRF guard: every outbound socket this server dials passes through
		// this check on the final resolved IP. This is the authoritative
		// boundary that defeats DNS rebinding and redirects, because any
		// connection to a private address must eventually dial it here.
		Control: netguard.Control,
	}

	h2Transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			conn, err := baseDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				conn.Close()
				return nil, err
			}
			tlsConn := utls.UClient(conn, &utls.Config{ServerName: host}, utls.HelloChrome_Auto)
			if err := tlsConn.Handshake(); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}
	h1Transport := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := baseDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				conn.Close()
				return nil, err
			}
			tlsConn := utls.UClient(conn, &utls.Config{ServerName: host}, utls.HelloChrome_Auto)
			if err := tlsConn.Handshake(); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
		MaxIdleConnsPerHost: 10,
	}

	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{DialContext: baseDialer.DialContext},
		// Never follow redirects server-side. hls.js re-requests the
		// redirected URL through the proxy itself; following here would let
		// an upstream bounce the server at any internal endpoint.
		CheckRedirect: netguard.NoRedirects,
	}
	goTLSClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext:         baseDialer.DialContext,
			MaxIdleConnsPerHost: 10,
		},
		CheckRedirect: netguard.NoRedirects,
	}
	h := &Handlers{
		cfg:         cfg,
		log:         log,
		mal:         mal.NewClient(log),
		stream:      streaming.NewManager(log),
		h2Client:    &http.Client{Timeout: 30 * time.Second, Transport: h2Transport, CheckRedirect: netguard.NoRedirects},
		h1Client:    &http.Client{Timeout: 30 * time.Second, Transport: h1Transport, CheckRedirect: netguard.NoRedirects},
		httpClient:  httpClient,
		goTLSClient: goTLSClient,
		// Full-file downloads: no client timeout (the request context bounds
		// the transfer), SSRF-guarded transport, redirects refused.
		downloadClient: netguard.NewHTTPClient(0),
		browseCacheTTL: 5 * time.Minute, // 5 min cache for browse/trending
	}
	h.anilistClient = newAnilistClient(h)
	h.anilistCircuit = newCircuitBreaker()

	// Load AnikotoTV AniList→slug mapping (bundled in the binary by default;
	// a configured ANIRAKU_ANIKOTO_MAPPING_PATH overrides it). Best-effort,
	// non-fatal.
	if err := streaming.LoadAnikotoMapping(cfg.Server.AnikotoMappingPath); err != nil {
		log.Warn().Err(err).Str("path", cfg.Server.AnikotoMappingPath).Msg("anikoto: mapping not loaded, will search dynamically")
	} else {
		log.Info().Msg("anikoto: mapping loaded")
	}

	// Anikoto-verified hosts (probeHLS-passed stream hosts, subtitle hosts,
	// download hosts) feed the media-proxy CDN allowlist as they surface, so
	// provider CDN rotation never 403s at the proxy gate.
	h.stream.SetHostLearner(func(host string) { LearnHostFromPlaylist(host) })

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			h.keyCache.Range(func(k, v any) bool {
				entry, ok := v.(keyCacheEntry)
				if ok && time.Since(entry.fetchedAt) > 10*time.Minute {
					h.keyCache.Delete(k)
				}
				return true
			})
			// Clean up browse cache
			h.browseCache.Range(func(k, v any) bool {
				entry, ok := v.(browseCacheEntry)
				if ok && time.Since(entry.fetchedAt) > h.browseCacheTTL {
					h.browseCache.Delete(k)
				}
				return true
			})
			// Evict the AniList GraphQL-proxy cache (was never evicted; it
			// grew with every unique query for the process lifetime).
			h.anilistClient.evictStale()
			h.anilistClient.evictOldest()
		}
	}()

	return h
}

func (h *Handlers) respondJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *Handlers) respondError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"uptime_seconds": int64(time.Since(metricsStartedAt).Seconds()),
		"version":        Version,
	})
}

func (h *Handlers) Version(w http.ResponseWriter, r *http.Request) {
	h.respondJSON(w, http.StatusOK, map[string]string{
		"version": Version,
		"commit":  Commit,
		"date":    BuildDate,
	})
}
