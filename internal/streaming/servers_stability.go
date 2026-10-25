package streaming

import (
	"sync"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

const (
	serverListStabilityTTL  = 90 * time.Second
	incompleteServerListTTL = 5 * time.Second
	providerLastGoodTTL     = 10 * time.Minute
	criticalLastGoodTTL     = 2 * time.Hour
	maxServerListSnapshots  = 256
	maxProviderSnapshots    = 2048
)

type serverRequestKey struct {
	animeID int
	episode int
	lang    string
	hentai  bool
}

type providerRequestKey struct {
	serverRequestKey
	provider string
}

type serverFlightKey struct {
	serverRequestKey
	refresh bool
}

type serverSnapshot struct {
	servers  []core.Server
	storedAt time.Time
	expires  time.Time
}

type serverFlight struct {
	done    chan struct{}
	servers []core.Server
}

// serverStabilityCache keeps only raw provider results. Request-specific
// proxy URLs are added later by the HTTP handler and never enter this cache.
type serverStabilityCache struct {
	mu        sync.Mutex
	lists     map[serverRequestKey]serverSnapshot
	providers map[providerRequestKey]serverSnapshot
	flights   map[serverFlightKey]*serverFlight
}

func newServerStabilityCache() *serverStabilityCache {
	return &serverStabilityCache{
		lists:     make(map[serverRequestKey]serverSnapshot),
		providers: make(map[providerRequestKey]serverSnapshot),
		flights:   make(map[serverFlightKey]*serverFlight),
	}
}

func (m *Manager) stability() *serverStabilityCache {
	m.stabilityMu.Lock()
	defer m.stabilityMu.Unlock()
	if m.serverStability == nil {
		m.serverStability = newServerStabilityCache()
	}
	return m.serverStability
}

func (c *serverStabilityCache) loadList(key serverRequestKey, now time.Time) ([]core.Server, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.lists[key]
	if !ok {
		return nil, false
	}
	if !entry.expires.IsZero() && !now.Before(entry.expires) {
		delete(c.lists, key)
		return nil, false
	}
	return cloneServers(entry.servers), true
}

func (c *serverStabilityCache) storeList(key serverRequestKey, servers []core.Server, now time.Time) {
	if len(servers) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.lists) >= maxServerListSnapshots {
		removeOldestServerSnapshot(c.lists)
	}
	ttl := serverListStabilityTTL
	if !key.hentai && (!hasProvider(servers, "kaa") || !hasProvider(servers, "tensho")) {
		// Retry incomplete cold rounds quickly. Once both critical providers
		// have returned successfully, their last-good entries keep later
		// transient misses from making the snapshot incomplete again.
		ttl = incompleteServerListTTL
	}
	c.lists[key] = serverSnapshot{servers: cloneServers(servers), storedAt: now, expires: now.Add(ttl)}
}

// stabilizeProvider records collector results. If a later round returns an
// empty or partial list, it retains previously verified server slots instead
// of letting a timeout, rate limit, or one failed probe remove them.
// allowFallback is false for refresh=1 requests.
func (c *serverStabilityCache) stabilizeProvider(key providerRequestKey, fresh []core.Server, allowFallback bool, now time.Time) ([]core.Server, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(fresh) > 0 {
		retained := false
		if previous, ok := c.providers[key]; ok && now.Sub(previous.storedAt) <= providerSnapshotTTL(key.provider) {
			fresh, retained = mergeProviderServers(fresh, previous.servers), true
		}
		if len(c.providers) >= maxProviderSnapshots {
			removeOldestProviderSnapshot(c.providers)
		}
		c.providers[key] = serverSnapshot{servers: cloneServers(fresh), storedAt: now}
		return fresh, retained
	}
	if !allowFallback {
		return fresh, false
	}
	entry, ok := c.providers[key]
	if !ok {
		return fresh, false
	}
	if now.Sub(entry.storedAt) > providerSnapshotTTL(key.provider) {
		delete(c.providers, key)
		return fresh, false
	}
	return cloneServers(entry.servers), true
}

func mergeProviderServers(fresh, previous []core.Server) []core.Server {
	if len(previous) == 0 {
		return fresh
	}
	seen := make(map[string]struct{}, len(fresh))
	merged := cloneServers(fresh)
	for _, server := range fresh {
		seen[server.Provider+"\x00"+server.Name] = struct{}{}
	}
	for _, server := range previous {
		key := server.Provider + "\x00" + server.Name
		if _, ok := seen[key]; ok {
			continue
		}
		merged = append(merged, cloneServers([]core.Server{server})[0])
	}
	return merged
}

func providerSnapshotTTL(provider string) time.Duration {
	if provider == "kaa" || provider == "tensho" {
		return criticalLastGoodTTL
	}
	return providerLastGoodTTL
}

func hasProvider(servers []core.Server, provider string) bool {
	for _, server := range servers {
		if server.Provider == provider {
			return true
		}
	}
	return false
}

func (c *serverStabilityCache) beginFlight(key serverFlightKey) (*serverFlight, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if call, ok := c.flights[key]; ok {
		return call, false
	}
	call := &serverFlight{done: make(chan struct{})}
	c.flights[key] = call
	return call, true
}

func (c *serverStabilityCache) finishFlight(key serverFlightKey, call *serverFlight, servers []core.Server) {
	c.mu.Lock()
	call.servers = cloneServers(servers)
	delete(c.flights, key)
	close(call.done)
	c.mu.Unlock()
}

func removeOldestServerSnapshot(entries map[serverRequestKey]serverSnapshot) {
	var oldestKey serverRequestKey
	var oldest time.Time
	set := false
	for key, entry := range entries {
		if !set || entry.storedAt.Before(oldest) {
			oldestKey = key
			oldest = entry.storedAt
			set = true
		}
	}
	if set {
		delete(entries, oldestKey)
	}
}

func removeOldestProviderSnapshot(entries map[providerRequestKey]serverSnapshot) {
	var oldestKey providerRequestKey
	var oldest time.Time
	set := false
	for key, entry := range entries {
		if !set || entry.storedAt.Before(oldest) {
			oldestKey = key
			oldest = entry.storedAt
			set = true
		}
	}
	if set {
		delete(entries, oldestKey)
	}
}

func cloneServers(servers []core.Server) []core.Server {
	if servers == nil {
		return nil
	}
	out := make([]core.Server, len(servers))
	for i, server := range servers {
		out[i] = server
		if server.Headers != nil {
			out[i].Headers = make(map[string]string, len(server.Headers))
			for key, value := range server.Headers {
				out[i].Headers[key] = value
			}
		}
		if server.Sources != nil {
			out[i].Sources = make([]core.Source, len(server.Sources))
			for j, source := range server.Sources {
				out[i].Sources[j] = source
				if source.Subtitles != nil {
					out[i].Sources[j].Subtitles = append([]core.Subtitle(nil), source.Subtitles...)
				}
			}
		}
		if server.Downloads != nil {
			out[i].Downloads = append([]core.DownloadLink(nil), server.Downloads...)
		}
	}
	return out
}
