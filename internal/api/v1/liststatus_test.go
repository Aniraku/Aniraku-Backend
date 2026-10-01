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
	for _, s := range stored {
		if got := wantExportStatus(s, true); got != s {
			t.Errorf("wantExportStatus(%q, done) = %q, want %q", s, got, s)
		}
		if got := wantExportStatus(s, false); got != s {
			t.Errorf("wantExportStatus(%q, !done) = %q, want %q", s, got, s)
		}
	}
	// Legacy fallback: watch-derived targeting.
	if got := wantExportStatus("", true); got != listCompleted {
		t.Errorf("wantExportStatus(unknown, done) = %q, want COMPLETED", got)
	}
	if got := wantExportStatus("", false); got != listCurrent {
		t.Errorf("wantExportStatus(unknown, !done) = %q, want CURRENT", got)
	}
	if got := wantExportStatus("bogus", false); got != listCurrent {
		t.Errorf("wantExportStatus(bogus, !done) = %q, want CURRENT", got)
	}
}

func TestExportCompletedUnchanged(t *testing.T) {
	// Highest episode fully watched AND at/above the known total.
	if !exportCompleted(animeWatchProgress{Episode: 12, Progress: 12, Completed: true}, 12) {
		t.Error("full series should be completed")
	}
	// Unknown total preserves the legacy behavior.
	if !exportCompleted(animeWatchProgress{Episode: 3, Progress: 3, Completed: true}, 0) {
		t.Error("unknown total should preserve legacy completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 3, Progress: 3, Completed: true}, 12) {
		t.Error("partial series must not be completed")
	}
	if exportCompleted(animeWatchProgress{Episode: 0, Progress: 0, Completed: false}, 0) {
		t.Error("unwatched must not be completed")
	}
}
