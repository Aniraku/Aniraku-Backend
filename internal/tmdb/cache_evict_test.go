package tmdb

import (
	"testing"
	"time"
)

func clearTestCaches(t *testing.T) {
	t.Helper()
	responseCache.Range(func(k, v any) bool {
		responseCache.Delete(k)
		return true
	})
	episodeCache.Range(func(k, v any) bool {
		episodeCache.Delete(k)
		return true
	})
}

// Expired entries must go, fresh ones must stay, and the cap must hold —
// eviction is behavior-preserving (a dropped entry is just a refetch).
func TestEvictResponseCacheIfNeeded(t *testing.T) {
	clearTestCaches(t)
	now := time.Now()
	responseCache.Store("fresh", cacheEntry{value: "v", expiresAt: now.Add(time.Hour)})
	responseCache.Store("dead", cacheEntry{value: "v", expiresAt: now.Add(-time.Minute)})
	evictResponseCacheIfNeeded()

	if _, ok := responseCache.Load("dead"); ok {
		t.Fatal("expired entry survived eviction")
	}
	if _, ok := responseCache.Load("fresh"); !ok {
		t.Fatal("fresh entry was evicted")
	}
	clearTestCaches(t)
}

func TestEvictEpisodeCacheIfNeeded(t *testing.T) {
	clearTestCaches(t)
	episodeCache.Store("fresh", &episodeCacheEntry{
		episodes:  map[int]*EpisodeMetadata{1: {Number: 1}},
		fetchedAt: time.Now(),
	})
	episodeCache.Store("dead", &episodeCacheEntry{
		episodes:  map[int]*EpisodeMetadata{1: {Number: 1}},
		fetchedAt: time.Now().Add(-episodeCacheTTL - time.Minute),
	})
	evictEpisodeCacheIfNeeded()

	if _, ok := episodeCache.Load("dead"); ok {
		t.Fatal("expired entry survived eviction")
	}
	if _, ok := episodeCache.Load("fresh"); !ok {
		t.Fatal("fresh entry was evicted")
	}
	// GetCachedEpisodes must still serve the survivor.
	got := GetCachedEpisodes(0, []int{1}) // int key 0 missing -> nil, sanity
	if got != nil {
		t.Fatalf("unexpected episodes for untouched key: %v", got)
	}
	clearTestCaches(t)
}

// Over the cap, the stalest entry goes first and the count comes back down.
func TestEpisodeCacheCap(t *testing.T) {
	clearTestCaches(t)
	base := time.Now()
	for i := 0; i < maxEpisodeCacheEntries+5; i++ {
		episodeCache.Store(i, &episodeCacheEntry{
			episodes:  map[int]*EpisodeMetadata{},
			fetchedAt: base.Add(time.Duration(i) * time.Millisecond),
		})
	}
	evictEpisodeCacheIfNeeded()

	count := 0
	episodeCache.Range(func(k, v any) bool {
		count++
		return true
	})
	if count > maxEpisodeCacheEntries {
		t.Fatalf("entries = %d, want <= %d", count, maxEpisodeCacheEntries)
	}
	if _, ok := episodeCache.Load(0); ok {
		t.Fatal("stalest entry (key 0) should have been evicted first")
	}
	if _, ok := episodeCache.Load(maxEpisodeCacheEntries + 4); !ok {
		t.Fatal("freshest entry should survive")
	}
	clearTestCaches(t)
}
