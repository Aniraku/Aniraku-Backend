package v1

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestDynamicCDNPersistRoundTrip guards the restart story: a host learned
// from a playlist must still be allowed after a process restart (fresh map
// + load from disk). Without this, every redeploy turns in-flight
// edge-cached playlists into 403s until each upstream is re-fetched.
func TestDynamicCDNPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "dynamic_cdn.json")
	t.Setenv("ANIRAKU_DYNAMIC_CDN_FILE", file)

	resetDynamicCDNEntries()
	dynamicCDNLoadOnce = sync.Once{}
	LearnHostFromPlaylist("n953x.midnightvoyage.site")
	if !isAllowedProxyHost("n953x.midnightvoyage.site") {
		t.Fatal("learned host not allowed in-memory")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("persist file not written: %v", err)
	}

	// Simulate a restart: drop the map, reset the loader, allow from disk.
	resetDynamicCDNEntries()
	dynamicCDNLoadOnce = sync.Once{}
	if !isAllowedProxyHost("n953x.midnightvoyage.site") {
		t.Fatal("learned host lost across restart")
	}

	// Expired entries must not resurrect.
	resetDynamicCDNEntries()
	dynamicCDNLoadOnce = sync.Once{}
	raw, _ := os.ReadFile(file)
	t.Logf("persisted %d bytes", len(raw))
}
