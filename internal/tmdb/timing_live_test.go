package tmdb

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestLiveEpisodesTiming is an env-guarded probe that breaks the cold
// episode-resolution path into timed phases so the latency bottleneck is
// measurable, not guessed:
//
//	ANIRAKU_LIVE_PROBE=1 go test ./internal/tmdb/ -run TestLiveEpisodesTiming -v
//
// Optional: ANIRAKU_LIVE_PROBE_ID=<anilist id> (default 21, One Piece).
// Requires ANIRAKU_TMDB_READ_ACCESS_TOKEN (or TMDB_READ_ACCESS_TOKEN).
func TestLiveEpisodesTiming(t *testing.T) {
	if os.Getenv("ANIRAKU_LIVE_PROBE") != "1" {
		t.Skip("live probe disabled (set ANIRAKU_LIVE_PROBE=1)")
	}
	token := os.Getenv("ANIRAKU_TMDB_READ_ACCESS_TOKEN")
	if token == "" {
		token = os.Getenv("TMDB_READ_ACCESS_TOKEN")
	}
	if token == "" {
		t.Skip("no TMDB token (set ANIRAKU_TMDB_READ_ACCESS_TOKEN)")
	}
	anilistID := 21 // One Piece: 1178 episodes, many TMDB seasons
	if v := os.Getenv("ANIRAKU_LIVE_PROBE_ID"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			anilistID = n
		}
	}
	episodeNumbers := make([]int, 0, 1200)
	for i := 1; i <= 1178; i++ {
		episodeNumbers = append(episodeNumbers, i)
	}
	if anilistID != 21 {
		episodeNumbers = []int{1}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t0 := time.Now()
	if _, err := getMapping(ctx, client, anilistID); err != nil {
		t.Logf("phase getMapping:        ERROR %v (%s)", err, time.Since(t0))
	} else {
		t.Logf("phase getMapping:        %s", time.Since(t0))
	}

	t1 := time.Now()
	data, err := FetchAniZipEpisodes(ctx, client, anilistID)
	if err != nil {
		t.Logf("phase FetchAniZip:       ERROR %v (%s)", err, time.Since(t1))
	} else {
		t.Logf("phase FetchAniZip:       %s (%d eps)", time.Since(t1), len(data))
	}

	t2 := time.Now()
	res, err := ResolveEpisodes(ctx, client, token, anilistID, episodeNumbers)
	if err != nil {
		t.Logf("phase ResolveEpisodes:   ERROR %v (%s)", err, time.Since(t2))
		return
	}
	t.Logf("phase ResolveEpisodes:   %s (%d eps, source=%s)", time.Since(t2), len(res.Episodes), res.Source)

	t3 := time.Now()
	_, _ = ResolveEpisodes(ctx, client, token, anilistID, episodeNumbers)
	t.Logf("phase ResolveEpisodes#2: %s (cache-warm)", time.Since(t3))
}
