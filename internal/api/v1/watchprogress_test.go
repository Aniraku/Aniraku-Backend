package v1

import (
	"reflect"
	"testing"
)

// fullWatchRow builds a finished episode row (progress == duration).
func fullWatchRow(animeID, ep int) watchHistoryRow {
	return watchHistoryRow{AnimeID: animeID, Episode: ep, Progress: 1440, Duration: 1440}
}

func TestFoldWatchRowsFinishedCount(t *testing.T) {
	rows := []watchHistoryRow{
		fullWatchRow(7, 1),
		fullWatchRow(7, 2),
		fullWatchRow(7, 3),
		{AnimeID: 7, Episode: 4, Progress: 300, Duration: 1440}, // opened, not finished
		fullWatchRow(9, 1),
		{AnimeID: 11, Episode: 1, Progress: 100, Duration: 1440}, // partial top only
		{AnimeID: 13, Episode: 1, Progress: 0, Duration: 0},      // unknown duration never counts as finished
	}
	acc := map[int]animeWatchProgress{}
	foldWatchRows(acc, rows)

	got := acc[7]
	// Episode must be the highest FINISHED episode (3), not the highest
	// opened one (4) — this is the number MAL/AniList store as watched.
	if got.Episode != 3 {
		t.Errorf("anime 7 Episode = %d, want 3 (finished count, not opened count)", got.Episode)
	}
	if got.MaxRow != 4 {
		t.Errorf("anime 7 MaxRow = %d, want 4 (import merge guard sees every row)", got.MaxRow)
	}
	if got.Completed {
		t.Errorf("anime 7 Completed = true, want false (newest row is partial)")
	}
	if got.Progress != 300 {
		t.Errorf("anime 7 Progress = %d, want 300 (seconds on the newest row)", got.Progress)
	}

	if acc[9].Episode != 1 || !acc[9].Completed || acc[9].MaxRow != 1 {
		t.Errorf("anime 9 = %+v, want Episode 1 / Completed true / MaxRow 1", acc[9])
	}
	if acc[11].Episode != 0 || acc[11].MaxRow != 1 || acc[11].Completed {
		t.Errorf("anime 11 = %+v, want Episode 0 (nothing finished) / MaxRow 1", acc[11])
	}
	if acc[13].Episode != 0 {
		t.Errorf("anime 13 Episode = %d, want 0 (duration 0 is not a finished episode)", acc[13].Episode)
	}
}

func TestFoldWatchRowsOrderIndependent(t *testing.T) {
	rows := []watchHistoryRow{
		fullWatchRow(5, 1),
		{AnimeID: 5, Episode: 3, Progress: 1439, Duration: 1440},
		fullWatchRow(5, 2),
		fullWatchRow(5, 12),
	}
	forward := map[int]animeWatchProgress{}
	foldWatchRows(forward, rows)

	backward := map[int]animeWatchProgress{}
	for i := len(rows) - 1; i >= 0; i-- {
		foldWatchRows(backward, []watchHistoryRow{rows[i]})
	}
	if !reflect.DeepEqual(forward, backward) {
		t.Errorf("fold depends on row order:\nforward=%+v\nbackward=%+v", forward, backward)
	}
	if forward[5].Episode != 12 || forward[5].MaxRow != 12 {
		t.Errorf("anime 5 = %+v, want Episode 12 / MaxRow 12", forward[5])
	}
}

func TestFoldWatchRowsAcrossPages(t *testing.T) {
	// The paged fetch folds page after page into one map; a title's rows
	// can span a page boundary. 95% of 1440 = 1368 exactly — that row
	// counts as finished (>=, not >), one second less does not, and the
	// unfinished rows at the top must not become the watched count.
	page1 := []watchHistoryRow{
		{AnimeID: 3, Episode: 5, Progress: 1367, Duration: 1440}, // 1 short
		{AnimeID: 3, Episode: 4, Progress: 1367, Duration: 1440}, // 1 short
	}
	page2 := []watchHistoryRow{
		{AnimeID: 3, Episode: 3, Progress: 1368, Duration: 1440}, // exactly 95%
		{AnimeID: 3, Episode: 2, Progress: 1367, Duration: 1440}, // 1 short
	}
	acc := map[int]animeWatchProgress{}
	foldWatchRows(acc, page1)
	foldWatchRows(acc, page2)
	got := acc[3]
	if got.MaxRow != 5 {
		t.Errorf("MaxRow = %d, want 5", got.MaxRow)
	}
	if got.Episode != 3 {
		t.Errorf("Episode = %d, want 3 (only ep 3 hit the 95%% threshold)", got.Episode)
	}
	if got.Completed {
		t.Errorf("Completed = true, want false (the row at MaxRow, ep 5, is unfinished)")
	}
	if got.Progress != 1367 {
		t.Errorf("Progress = %d, want 1367 (newest row's seconds)", got.Progress)
	}
}

func TestFoldWatchRowsRejectsNonPositive(t *testing.T) {
	acc := map[int]animeWatchProgress{}
	foldWatchRows(acc, []watchHistoryRow{
		{AnimeID: 0, Episode: 4, Progress: 1440, Duration: 1440},
		{AnimeID: 2, Episode: 0, Progress: 1440, Duration: 1440},
	})
	if len(acc) != 0 {
		t.Errorf("fold kept invalid rows: %+v", acc)
	}
}

func TestWatchHistoryPath(t *testing.T) {
	p := watchHistoryPath("user-1", []int{206814, 0, 111}, 2000)
	want := "/rest/v1/watch_history?select=anime_id,episode_number,progress,duration" +
		"&user_id=eq.user-1&anime_id=in.(206814,111)" +
		"&order=anime_id.asc,episode_number.desc&limit=1000&offset=2000"
	if p != want {
		t.Errorf("watchHistoryPath:\n got %s\nwant %s", p, want)
	}
}

func TestExportCompletedFinishedCount(t *testing.T) {
	// total known: every episode finished, even with a half-opened row
	// above the total — still completed.
	if !exportCompleted(animeWatchProgress{Episode: 12, MaxRow: 13, Completed: false}, 12, false) {
		t.Error("12 finished of 12 with a partial row above = want completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 11, MaxRow: 12, Completed: false}, 12, false) {
		t.Error("11 finished of 12 = want not completed")
	}
	// total unknown: only a series AniList says has ended, and only when
	// nothing is left half-watched at the top.
	if !exportCompleted(animeWatchProgress{Episode: 3, MaxRow: 3, Completed: true}, 0, true) {
		t.Error("unknown total, ended series, newest row full = want completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 3, MaxRow: 4, Completed: false}, 0, true) {
		t.Error("unknown total, newest row partial = want not completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 3, MaxRow: 3, Completed: true}, 0, false) {
		t.Error("airing show (no total, not finished) = want not completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 0, MaxRow: 0, Completed: false}, 0, false) {
		t.Error("no rows = want not completed")
	}
}

func TestFoldWatchRowsRealEpisode(t *testing.T) {
	rows := []watchHistoryRow{
		fullWatchRow(7, 1), // import-synthesized
		fullWatchRow(7, 2), // import-synthesized
		{AnimeID: 7, Episode: 3, Progress: 1422.06, Duration: 1467.1}, // real finish
		{AnimeID: 7, Episode: 4, Progress: 100, Duration: 1448.51},    // real, partial
		fullWatchRow(9, 5), // synthetic-only title
	}
	acc := map[int]animeWatchProgress{}
	foldWatchRows(acc, rows)

	if acc[7].Episode != 3 {
		t.Errorf("anime 7 Episode = %d, want 3", acc[7].Episode)
	}
	// Only the real player row counts as evidence — synthetic rows are
	// provider mirrors and may never raise a provider's count.
	if acc[7].RealEpisode != 3 {
		t.Errorf("anime 7 RealEpisode = %d, want 3 (synthetic 1-2 excluded)", acc[7].RealEpisode)
	}
	if acc[9].Episode != 5 || acc[9].RealEpisode != 0 {
		t.Errorf("anime 9 = Episode %d / RealEpisode %d, want 5 / 0 (all synthetic)", acc[9].Episode, acc[9].RealEpisode)
	}
}

func TestExportProgress(t *testing.T) {
	// PLANNING always pushes 0 — the plan_to_watch-with-full-progress fix.
	if num, write := exportProgress(animeWatchProgress{Episode: 12, MaxRow: 12}, listPlanning, 12); num != 0 || !write {
		t.Errorf("PLANNING with claimed rows = (%d, %v), want (0, true)", num, write)
	}
	// No local rows: provider progress untouched.
	if num, write := exportProgress(animeWatchProgress{}, "", 8); num != 8 || write {
		t.Errorf("no rows = (%d, %v), want (8, false)", num, write)
	}
	// Rows exist but nothing finished: also untouched (no evidence either way).
	if num, write := exportProgress(animeWatchProgress{MaxRow: 3}, "", 5); num != 5 || write {
		t.Errorf("opened-only rows = (%d, %v), want (5, false)", num, write)
	}
	// THE bug: synthetic import rows claim 12, provider says 6, no real
	// watches → a raise must be blocked so a correct provider stays correct.
	if num, write := exportProgress(animeWatchProgress{Episode: 12, MaxRow: 12, RealEpisode: 0}, "", 6); num != 6 || write {
		t.Errorf("synthetic-only claim over provider = (%d, %v), want (6, false)", num, write)
	}
	// Real player evidence above the provider raises — capped at the real
	// evidence, never at the synthetic claim.
	if num, write := exportProgress(animeWatchProgress{Episode: 12, MaxRow: 12, RealEpisode: 8}, "", 6); num != 8 || !write {
		t.Errorf("real evidence raise = (%d, %v), want (8, true)", num, write)
	}
	// Reconciled local below an inflated provider: local is curated truth,
	// the export heals the provider back down.
	if num, write := exportProgress(animeWatchProgress{Episode: 6, MaxRow: 6}, "", 12); num != 6 || !write {
		t.Errorf("reconciled local below provider = (%d, %v), want (6, true)", num, write)
	}
	// Equal: write the same value (exact-match skip handles the no-op).
	if num, write := exportProgress(animeWatchProgress{Episode: 6, MaxRow: 6}, "", 6); num != 6 || !write {
		t.Errorf("equal progress = (%d, %v), want (6, true)", num, write)
	}
}
