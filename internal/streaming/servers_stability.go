package streaming

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

// stabilitySnapshotPath: the providers' last-good lists are snapshotted to
// disk every minute so a restart re-lists every provider instantly instead
// of re-resolving into rate-limit windows (supaplay hangs). TOKEN-BASED
// providers (anv, animex) are excluded on save and dropped on load: their
// source URLs carry short-lived session tokens that a snapshot cannot
// keep alive - only providers with durable URLs are persisted.
const stabilitySnapshotPath = "/var/tmp/server-stability-snapshot.json"

// tokenBasedProviders resolve into tokenized URLs whose validity is tied
// to the session that minted them; their results must never be persisted
// or served from a snapshot past the minting process's lifetime.
var tokenBasedProviders = map[string]bool{
	"anv":    true,
	"animex": true,
}

const (
	serverListStabilityTTL  = 90 * time.Second
	incompleteServerListTTL = 5 * time.Second
	// providerLastGoodTTL raised to match the critical tier (2026-11-05):
	// every provider's last-good list must survive provider-side outages
	// and rate-limit windows (animex 15m cooldown, novuscloud/nin/aniwaves
	// hangs) so server lists persist instead of flickering out.
	providerLastGoodTTL    = 2 * time.Hour
	criticalLastGoodTTL    = 2 * time.Hour
	maxServerListSnapshots = 256
	maxProviderSnapshots   = 2048
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
	mu            sync.Mutex
	lists         map[serverRequestKey]serverSnapshot
	providers     map[providerRequestKey]serverSnapshot
	flights       map[serverFlightKey]*serverFlight
	restoredCount int
}

type stabilitySnapshotJSON struct {
	SavedAt time.Time                         `json:"savedAt"`
	Entries map[string]stabilitySnapshotEntry `json:"providers"`
}

type stabilitySnapshotEntry struct {
	Servers  []core.Server `json:"servers"`
	StoredAt time.Time     `json:"storedAt"`
}

// loadProvidersSnapshot restores the providers' last-good lists written by
// a previous process. Expired entries (past their providerSnapshotTTL) are
// dropped, so a stale snapshot cannot resurrect dead sources.
func (c *serverStabilityCache) loadProvidersSnapshot() {
	raw, err := os.ReadFile(stabilitySnapshotPath)
	if err != nil {
		return
	}
	var snap stabilitySnapshotJSON
	if err := json.Unmarshal(raw, &snap); err != nil {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	restored := 0
	for k, e := range snap.Entries {
		parts := splitStabilityKey(k)
		if len(parts) != 5 {
			continue
		}
		if tokenBasedProviders[parts[4]] {
			continue // token-based: a snapshot cannot keep their URLs alive
		}
		if now.Sub(e.StoredAt) >= providerSnapshotTTL(parts[4]) {
			continue
		}
		key := providerRequestKey{
			serverRequestKey: serverRequestKey{
				animeID: atoiDefault(parts[0]),
				episode: atoiDefault(parts[1]),
				lang:    parts[2],
				hentai:  parts[3] == "1",
			},
			provider: parts[4],
		}
		c.providers[key] = serverSnapshot{servers: e.Servers, storedAt: e.StoredAt}
		restored++
	}
	if restored > 0 {
		// log is not reachable here; the caller reports the count.
		c.restoredCount = restored
	}
}

func splitStabilityKey(k string) []string {
	out := make([]string, 0, 5)
	start := 0
	for i := 0; i < len(k) && len(out) < 4; i++ {
		if k[i] == '/' {
			out = append(out, k[start:i])
			start = i + 1
		}
	}
	out = append(out, k[start:])
	return out
}

func atoiDefault(s string) int {
	n := 0
	neg := false
	if s != "" && s[0] == '-' {
		neg = true
		s = s[1:]
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0
		}
		n = n*10 + int(s[i]-'0')
	}
	if neg {
		return -n
	}
	return n
}

func stabilityKeyString(k providerRequestKey) string {
	h := 0
	if k.hentai {
		h = 1
	}
	return itoaDefault(k.animeID) + "/" + itoaDefault(k.episode) + "/" + k.lang + "/" + itoaDefault(h) + "/" + k.provider
}

func itoaDefault(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}

// providersSnapshotSaver persists the providers' last-good lists once a
// minute for the process lifetime.
func (c *serverStabilityCache) providersSnapshotSaver() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		c.saveProvidersSnapshot()
	}
}

func (c *serverStabilityCache) saveProvidersSnapshot() {
	c.mu.Lock()
	snap := stabilitySnapshotJSON{SavedAt: time.Now(), Entries: make(map[string]stabilitySnapshotEntry, len(c.providers))}
	for k, v := range c.providers {
		if tokenBasedProviders[k.provider] {
			continue // token-based: never persist
		}
		snap.Entries[stabilityKeyString(k)] = stabilitySnapshotEntry{Servers: v.servers, StoredAt: v.storedAt}
	}
	c.mu.Unlock()
	raw, err := json.Marshal(snap)
	if err != nil {
		return
	}
	tmp := stabilitySnapshotPath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, stabilitySnapshotPath)
}

func newServerStabilityCache() *serverStabilityCache {
	c := &serverStabilityCache{
		lists:     make(map[serverRequestKey]serverSnapshot),
		providers: make(map[providerRequestKey]serverSnapshot),
		flights:   make(map[serverFlightKey]*serverFlight),
	}
	c.loadProvidersSnapshot()
	go c.providersSnapshotSaver()
	return c
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

func (c *serverStabilityCache) storeList(key serverRequestKey, servers []core.Server, now time.Time) []core.Server {
	if len(servers) == 0 {
		return servers
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous, ok := c.lists[key]; ok {
		servers = mergeListSnapshot(servers, previous.servers, previous.storedAt, now)
	}
	if len(c.lists) >= maxServerListSnapshots {
		removeOldestServerSnapshot(c.lists)
	}
	ttl := serverListStabilityTTL
	if !key.hentai && (!hasProvider(servers, "kaa") || !hasProvider(servers, "zenbu")) {
		// Retry incomplete cold rounds quickly. Once both critical providers
		// have returned successfully, their last-good entries keep later
		// transient misses from making the snapshot incomplete again.
		ttl = incompleteServerListTTL
	}
	c.lists[key] = serverSnapshot{servers: cloneServers(servers), storedAt: now, expires: now.Add(ttl)}
	return cloneServers(servers)
}

// mergeListSnapshot keeps the last verified provider/server slot when a fresh
// round misses it. Current sources win for an existing slot; the prior list
// supplies only missing slots whose provider-specific last-good lifetime has
// not elapsed. The previous order remains authoritative, so one fresh round
// cannot reorder the visible server list.
func mergeListSnapshot(fresh, previous []core.Server, previousAt, now time.Time) []core.Server {
	if len(previous) == 0 {
		return fresh
	}
	current := make(map[string]core.Server, len(fresh))
	for _, server := range fresh {
		current[server.Provider+"\x00"+server.Name] = server
	}
	seen := make(map[string]struct{}, len(previous)+len(fresh))
	merged := make([]core.Server, 0, len(previous)+len(fresh))
	for _, server := range previous {
		key := server.Provider + "\x00" + server.Name
		if currentServer, ok := current[key]; ok {
			merged = append(merged, cloneServers([]core.Server{currentServer})[0])
			seen[key] = struct{}{}
			continue
		}
		if now.Sub(previousAt) <= providerSnapshotTTL(server.Provider) {
			merged = append(merged, cloneServers([]core.Server{server})[0])
			seen[key] = struct{}{}
		}
	}
	for _, server := range fresh {
		key := server.Provider + "\x00" + server.Name
		if _, ok := seen[key]; ok {
			continue
		}
		merged = append(merged, cloneServers([]core.Server{server})[0])
	}
	return merged
}

// stabilizeProvider records collector results. If a later round returns an
// empty or partial list, it retains previously verified server slots instead
// of letting a timeout, rate limit, or one failed probe remove them.
// A refresh bypasses the full-list snapshot, but last-good provider results
// remain eligible. Refresh must discover improvements without deleting a
// previously verified provider when the upstream is having a bad draw.
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
	if provider == "kaa" || provider == "zenbu" {
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
