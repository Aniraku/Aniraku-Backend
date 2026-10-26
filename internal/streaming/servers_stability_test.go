package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

func stabilityTestServer(provider, name string) core.Server {
	return core.Server{
		Name:     name,
		Provider: provider,
		Lang:     "sub",
		Headers:  map[string]string{"Referer": "https://example.invalid/"},
		Sources: []core.Source{{
			URL:     "https://media.example.invalid/master.m3u8",
			Type:    "hls",
			Quality: "auto",
			Subtitles: []core.Subtitle{{
				URL:   "https://media.example.invalid/en.vtt",
				Lang:  "en",
				Label: "English",
			}},
		}},
		Downloads: []core.DownloadLink{{URL: "https://media.example.invalid/file.mp4", Label: "1080p"}},
	}
}

func TestServerStabilitySnapshotReturnsDeepCopy(t *testing.T) {
	cache := newServerStabilityCache()
	key := serverRequestKey{animeID: 21, episode: 1, lang: "sub"}
	want := []core.Server{stabilityTestServer("kaa", "Nico")}
	now := time.Now()
	cache.storeList(key, want, now)

	got, ok := cache.loadList(key, now.Add(time.Second))
	if !ok {
		t.Fatal("expected a fresh server-list snapshot")
	}
	got[0].Name = "changed"
	got[0].Headers["Referer"] = "changed"
	got[0].Sources[0].URL = "changed"
	got[0].Sources[0].Subtitles[0].URL = "changed"
	got[0].Downloads[0].URL = "changed"

	again, ok := cache.loadList(key, now.Add(2*time.Second))
	if !ok {
		t.Fatal("expected the stored snapshot to remain fresh")
	}
	if again[0].Name != "Nico" || again[0].Headers["Referer"] == "changed" ||
		again[0].Sources[0].URL == "changed" || again[0].Sources[0].Subtitles[0].URL == "changed" ||
		again[0].Downloads[0].URL == "changed" {
		t.Fatalf("snapshot shared mutable state: %+v", again[0])
	}
}

func TestServerStabilityListKeepsMissingProviderInStableOrder(t *testing.T) {
	cache := newServerStabilityCache()
	key := serverRequestKey{animeID: 21, episode: 1, lang: "sub"}
	now := time.Now()
	previous := []core.Server{
		stabilityTestServer("animex", "Mochi"),
		stabilityTestServer("megavid", "Vidy"),
		stabilityTestServer("zenbu", "Ringo"),
	}
	cache.storeList(key, previous, now)
	got := cache.storeList(key, []core.Server{
		stabilityTestServer("animex", "Mochi"),
	}, now.Add(time.Second))
	if len(got) != 3 || got[0].Provider != "animex" || got[1].Provider != "megavid" || got[2].Provider != "zenbu" {
		t.Fatalf("fresh incomplete list dropped or reordered verified servers: %+v", got)
	}
}

func TestServerStabilityRetriesIncompleteCriticalSnapshotSoon(t *testing.T) {
	cache := newServerStabilityCache()
	key := serverRequestKey{animeID: 21, episode: 1, lang: "sub"}
	now := time.Now()
	cache.storeList(key, []core.Server{stabilityTestServer("kaa", "Nico")}, now)

	if _, ok := cache.loadList(key, now.Add(incompleteServerListTTL+time.Millisecond)); ok {
		t.Fatal("snapshot missing Zenbu stayed cached instead of allowing a quick retry")
	}

	cache.storeList(key, []core.Server{
		stabilityTestServer("kaa", "Nico"),
		stabilityTestServer("zenbu", "Yuzu"),
	}, now)
	if _, ok := cache.loadList(key, now.Add(incompleteServerListTTL+time.Millisecond)); !ok {
		t.Fatal("complete critical snapshot expired at the incomplete-list TTL")
	}
}

func TestServerStabilityRetainsProviderAfterTransientEmpty(t *testing.T) {
	cache := newServerStabilityCache()
	key := providerRequestKey{
		serverRequestKey: serverRequestKey{animeID: 21, episode: 1, lang: "sub"},
		provider:         "zenbu",
	}
	want := []core.Server{stabilityTestServer("zenbu", "Yuzu")}
	now := time.Now()

	got, reused := cache.stabilizeProvider(key, want, true, now)
	if reused || len(got) != 1 {
		t.Fatalf("fresh provider result was not used: reused=%v got=%+v", reused, got)
	}
	got, reused = cache.stabilizeProvider(key, nil, true, now.Add(time.Minute))
	if !reused || len(got) != 1 || got[0].Provider != "zenbu" {
		t.Fatalf("transient empty result dropped the provider: reused=%v got=%+v", reused, got)
	}
}

func TestServerStabilityRetainsProviderSlotsAfterPartialProbeMiss(t *testing.T) {
	cache := newServerStabilityCache()
	key := providerRequestKey{
		serverRequestKey: serverRequestKey{animeID: 21, episode: 1, lang: "sub"},
		provider:         "zenbu",
	}
	now := time.Now()
	cache.stabilizeProvider(key, []core.Server{
		stabilityTestServer("zenbu", "Ringo"),
		stabilityTestServer("zenbu", "Budo"),
	}, true, now)

	got, reused := cache.stabilizeProvider(key, []core.Server{
		stabilityTestServer("zenbu", "Ringo"),
	}, true, now.Add(time.Minute))
	if !reused || len(got) != 2 || got[0].Name != "Ringo" || got[1].Name != "Budo" {
		t.Fatalf("partial probe miss dropped a previously verified slot: reused=%v got=%+v", reused, got)
	}
}

func TestServerStabilityRefreshSkipsProviderFallback(t *testing.T) {
	cache := newServerStabilityCache()
	key := providerRequestKey{
		serverRequestKey: serverRequestKey{animeID: 21, episode: 1, lang: "sub"},
		provider:         "kaa",
	}
	now := time.Now()
	cache.stabilizeProvider(key, []core.Server{stabilityTestServer("kaa", "Nico")}, true, now)

	got, reused := cache.stabilizeProvider(key, nil, true, now.Add(time.Minute))
	if !reused || len(got) != 1 {
		t.Fatalf("refresh must retain the last verified provider: reused=%v got=%+v", reused, got)
	}
}

func TestServerStabilityCriticalProviderOutlivesNormalFallback(t *testing.T) {
	cache := newServerStabilityCache()
	now := time.Now()
	base := serverRequestKey{animeID: 21, episode: 1, lang: "sub"}
	normalKey := providerRequestKey{serverRequestKey: base, provider: "animex"}
	criticalKey := providerRequestKey{serverRequestKey: base, provider: "kaa"}
	cache.stabilizeProvider(normalKey, []core.Server{stabilityTestServer("animex", "Mochi")}, true, now)
	cache.stabilizeProvider(criticalKey, []core.Server{stabilityTestServer("kaa", "Nico")}, true, now)

	afterNormalTTL := now.Add(providerLastGoodTTL + time.Second)
	if got, reused := cache.stabilizeProvider(normalKey, nil, true, afterNormalTTL); reused || len(got) != 0 {
		t.Fatalf("normal provider survived past its fallback TTL: reused=%v got=%+v", reused, got)
	}
	if got, reused := cache.stabilizeProvider(criticalKey, nil, true, afterNormalTTL); !reused || len(got) != 1 {
		t.Fatalf("critical provider was dropped at the normal TTL: reused=%v got=%+v", reused, got)
	}
}

func TestServerStabilityCoalescesSameKeyFanOut(t *testing.T) {
	cache := newServerStabilityCache()
	key := serverFlightKey{serverRequestKey: serverRequestKey{animeID: 21, episode: 1, lang: "sub"}}
	leaderCall, leader := cache.beginFlight(key)
	if !leader {
		t.Fatal("first request must lead the fan-out")
	}
	followerCall, followerIsLeader := cache.beginFlight(key)
	if followerIsLeader || followerCall != leaderCall {
		t.Fatal("identical concurrent request did not join the existing fan-out")
	}

	want := []core.Server{stabilityTestServer("kaa", "Nico")}
	cache.finishFlight(key, leaderCall, want)
	select {
	case <-followerCall.done:
	case <-time.After(time.Second):
		t.Fatal("fan-out follower was not released")
	}
	if len(followerCall.servers) != 1 || followerCall.servers[0].Provider != "kaa" {
		t.Fatalf("fan-out follower received the wrong result: %+v", followerCall.servers)
	}

	nextCall, nextIsLeader := cache.beginFlight(key)
	if !nextIsLeader {
		t.Fatal("completed fan-out remained stuck in the in-flight map")
	}
	cache.finishFlight(key, nextCall, nil)
}

func TestServerStabilitySeparatesRefreshFanOut(t *testing.T) {
	cache := newServerStabilityCache()
	requestKey := serverRequestKey{animeID: 21, episode: 1, lang: "sub"}
	normalKey := serverFlightKey{serverRequestKey: requestKey}
	refreshKey := serverFlightKey{serverRequestKey: requestKey, refresh: true}
	normalCall, normalLeader := cache.beginFlight(normalKey)
	refreshCall, refreshLeader := cache.beginFlight(refreshKey)
	if !normalLeader || !refreshLeader || normalCall == refreshCall {
		t.Fatal("refresh request incorrectly joined the normal cached fan-out")
	}
	cache.finishFlight(normalKey, normalCall, nil)
	cache.finishFlight(refreshKey, refreshCall, nil)
}

func TestFindAllServersUsesFreshStabilitySnapshot(t *testing.T) {
	m := NewManager(zerolog.Nop())
	m.providers = nil
	key := serverRequestKey{animeID: 21, episode: 1, lang: "sub"}
	want := []core.Server{
		stabilityTestServer("kaa", "Nico"),
		stabilityTestServer("zenbu", "Yuzu"),
	}
	m.stability().storeList(key, want, time.Now())

	got := m.FindAllServers(context.Background(), 21, 1, "sub", []string{"Action"})
	if len(got) != len(want) {
		t.Fatalf("snapshot provider set changed: got=%+v want=%+v", got, want)
	}
	for i := range want {
		if got[i].Provider != want[i].Provider || got[i].Name != want[i].Name {
			t.Fatalf("snapshot order changed at %d: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}
