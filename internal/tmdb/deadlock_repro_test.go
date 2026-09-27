package tmdb

import (
	"sync"
	"testing"
	"time"
)

// Regression test for the 2026-09-27 production incident: setCachedEpisodes
// acquired entry.mu.Lock() and then called evictEpisodeCacheIfNeeded(), whose
// Range callback re-acquired the SAME entry's RLock — Go's RWMutex is not
// reentrant, so the second write to any anime deadlocked the goroutine,
// poisoned the entry forever, and cascaded through every later cache write
// until no /episodes request could complete (context-blind, so the 45s
// request deadline could not save them).
//
// The fix: eviction runs with no entry lock held, evict uses non-blocking
// TryRLock, readers use a deadline-bounded RLock, writers use a
// deadline-bounded Lock. This test (and the hammer below) must complete.
func TestSetCachedEpisodesUpdateNoDeadlock(t *testing.T) {
	clearTestCaches(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		id := 777000111
		setCachedEpisodes(id, map[int]*EpisodeMetadata{1: {Number: 1}})
		// Second and third writes to the SAME existing entry — the trigger
		// that deadlocked production.
		setCachedEpisodes(id, map[int]*EpisodeMetadata{2: {Number: 2}})
		setCachedEpisodes(id, map[int]*EpisodeMetadata{3: {Number: 3}})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("setCachedEpisodes deadlocked on a write to an existing entry")
	}

	// Merge semantics preserved: all three writes visible.
	got := GetCachedEpisodes(777000111, []int{1, 2, 3})
	if got == nil || len(got) != 3 {
		t.Fatalf("merged episodes = %v, want all 3", got)
	}
	// Entry must be unlocked afterwards: immediate TryRLock succeeds.
	if val, ok := episodeCache.Load(777000111); ok {
		entry := val.(*episodeCacheEntry)
		if !entry.mu.TryRLock() {
			t.Fatal("entry lock still held after setCachedEpisodes returned")
		}
		entry.mu.RUnlock()
		episodeCache.Delete(777000111)
	} else {
		t.Fatal("entry missing after writes")
	}
	clearTestCaches(t)
}

// Hammer: concurrent readers, writers (new + update paths), and evictions —
// the exact production mix — must finish and leave the cache coherent.
// Run under -race in CI; a lock cycle hangs this test and the go test
// timeout turns it red.
func TestEpisodeCacheConcurrentHammer(t *testing.T) {
	clearTestCaches(t)
	const workers = 16
	const perWorker = 25
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				base := 770000000 + w*perWorker
				for i := 0; i < perWorker; i++ {
					id := base + i
					// New-entry write, update write, read, evict — every
					// path production takes, from every worker at once.
					setCachedEpisodes(id, map[int]*EpisodeMetadata{1: {Number: 1}})
					setCachedEpisodes(id, map[int]*EpisodeMetadata{2: {Number: 2}})
					_ = GetCachedEpisodes(id, []int{1, 2})
					evictEpisodeCacheIfNeeded()
				}
			}(w)
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent cache hammer deadlocked")
	}

	// Coherence: sampled entries complete, cap holds.
	for w := 0; w < workers; w++ {
		got := GetCachedEpisodes(770000000+w*perWorker, []int{1, 2})
		if got == nil || len(got) != 2 {
			t.Fatalf("worker %d entry incoherent: %v", w, got)
		}
	}
	count := 0
	episodeCache.Range(func(k, v any) bool {
		count++
		return true
	})
	if count > maxEpisodeCacheEntries {
		t.Fatalf("entries = %d, want <= %d", count, maxEpisodeCacheEntries)
	}
	clearTestCaches(t)
}
