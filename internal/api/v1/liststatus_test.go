package v1

import "testing"

// Canonical status normalization: provider wire values → stored statuses.
func TestNormalizeAniListStatusPreservesRepeating(t *testing.T) {
	cases := map[string]string{
		"CURRENT":   listCurrent,
		"REPEATING": listRepeating,
		"COMPLETED": listCompleted,
		"PLANNING":  listPlanning,
		"PAUSED":    listPaused,
		"DROPPED":   listDropped,
		"bogus":     listCurrent,
		"":          listCurrent,
	}
	for in, want := range cases {
		if got := normalizeAniListStatus(in); got != want {
			t.Errorf("normalizeAniListStatus(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeMALStatus(t *testing.T) {
	cases := map[string]string{
		"watching":      listCurrent,
		"completed":     listCompleted,
		"on_hold":       listPaused,
		"dropped":       listDropped,
		"plan_to_watch": listPlanning,
		"bogus":         listCurrent,
	}
	for in, want := range cases {
		if got := normalizeMALStatus(in); got != want {
			t.Errorf("normalizeMALStatus(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStatusForMAL(t *testing.T) {
	cases := map[string]string{
		listCurrent:   "watching",
		listRepeating: "watching", // MAL has no rewatching flag
		listCompleted: "completed",
		listPaused:    "on_hold",
		listDropped:   "dropped",
		listPlanning:  "plan_to_watch",
	}
	for in, want := range cases {
		if got := statusForMAL(in); got != want {
			t.Errorf("statusForMAL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWantExportStatus(t *testing.T) {
	// Stored status always wins (import-saved or watch-advanced).
	stored := []string{listCurrent, listPlanning, listCompleted, listPaused, listDropped, listRepeating}
	watched := animeWatchProgress{MaxRow: 3, Episode: 3, Completed: true}
	for _, s := range stored {
		if got := wantExportStatus(s, watched, true); got != s {
			t.Errorf("wantExportStatus(%q, watched, done) = %q, want %q", s, got, s)
		}
		if got := wantExportStatus(s, watched, false); got != s {
			t.Errorf("wantExportStatus(%q, watched, !done) = %q, want %q", s, got, s)
		}
		if got := wantExportStatus(s, animeWatchProgress{}, false); got != s {
			t.Errorf("wantExportStatus(%q, unwatched, !done) = %q, want %q", s, got, s)
		}
	}
	// Watch-derived: finished → COMPLETED, mid-series → CURRENT,
	// favorited but never watched → PLANNING (never CURRENT, that
	// rewrote MAL plan_to_watch entries to watching).
	if got := wantExportStatus("", watched, true); got != listCompleted {
		t.Errorf("wantExportStatus(unknown, watched, done) = %q, want COMPLETED", got)
	}
	if got := wantExportStatus("", watched, false); got != listCurrent {
		t.Errorf("wantExportStatus(unknown, watched, !done) = %q, want CURRENT", got)
	}
	if got := wantExportStatus("", animeWatchProgress{}, false); got != listPlanning {
		t.Errorf("wantExportStatus(unknown, unwatched, !done) = %q, want PLANNING", got)
	}
	if got := wantExportStatus("bogus", watched, false); got != listCurrent {
		t.Errorf("wantExportStatus(bogus, watched, !done) = %q, want CURRENT", got)
	}
	if got := wantExportStatus("bogus", animeWatchProgress{}, false); got != listPlanning {
		t.Errorf("wantExportStatus(bogus, unwatched, !done) = %q, want PLANNING", got)
	}
}

func TestNoEvidencePlanningFix(t *testing.T) {
	// "watching with zero episodes" = old Aniraku export artifact → re-plan.
	if !noEvidencePlanningFix(listCurrent, 0, listPlanning) {
		t.Error("watching with 0 episodes should re-plan")
	}
	// Everything else with no local evidence stays untouched.
	if noEvidencePlanningFix(listCurrent, 3, listPlanning) {
		t.Error("watching with real episodes must stay")
	}
	if noEvidencePlanningFix(listPlanning, 0, listPlanning) {
		t.Error("already planning is an exact match, not a fix")
	}
	if noEvidencePlanningFix(listCompleted, 0, listPlanning) {
		t.Error("completed must stay")
	}
	if noEvidencePlanningFix(listDropped, 0, listPlanning) {
		t.Error("explicit statuses must stay")
	}
	if noEvidencePlanningFix(listCurrent, 0, listCurrent) {
		t.Error("only PLANNING targets re-plan (watched titles are skipped earlier)")
	}
}

func TestExportCompletedUnchanged(t *testing.T) {
	// Known total is authoritative: every episode finished.
	if !exportCompleted(animeWatchProgress{Episode: 12, Progress: 12, Completed: true}, 12, false) {
		t.Error("full series should be completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 3, Progress: 3, Completed: true}, 12, true) {
		t.Error("partial series must not be completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 0, Progress: 0, Completed: false}, 0, false) {
		t.Error("unwatched must not be completed")
	}
	// Unknown total: only a series AniList says has ENDED may complete,
	// and only with the newest row fully watched. Airing shows carry
	// episodes: null, the old rule ("newest row full → completed") was
	// mass-marking the seasonal watch list COMPLETED on export.
	if !exportCompleted(animeWatchProgress{Episode: 6, MaxRow: 6, Progress: 1440, Completed: true}, 0, true) {
		t.Error("ended series, all rows full, unknown total should be completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 6, MaxRow: 6, Progress: 1440, Completed: true}, 0, false) {
		t.Error("airing show (no total, not finished) must never be completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 6, MaxRow: 7, Completed: false}, 0, true) {
		t.Error("unknown total with a half-watched row on top must not be completed")
	}
}
