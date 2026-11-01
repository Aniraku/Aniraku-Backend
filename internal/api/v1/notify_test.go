package v1

import (
	"testing"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

func TestEpisodeIsFresh(t *testing.T) {
	tests := []struct {
		name     string
		status   string
		episodes int
		next     int
		episode  int
		want     bool
	}{
		{"latest aired", "RELEASING", 12, 8, 7, true},
		{"source for upcoming episode", "RELEASING", 12, 8, 8, true},
		{"old episode", "RELEASING", 12, 8, 6, false},
		{"finished show", "FINISHED", 12, 0, 12, false},
		{"final episode without schedule", "RELEASING", 12, 0, 12, true},
		{"older final episode", "RELEASING", 12, 0, 11, false},
		{"invalid episode", "RELEASING", 12, 8, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := episodeIsFresh(tt.status, tt.episodes, tt.next, tt.episode); got != tt.want {
				t.Errorf("episodeIsFresh(%q, %d, %d, %d) = %v, want %v",
					tt.status, tt.episodes, tt.next, tt.episode, got, tt.want)
			}
		})
	}
}

func TestHasSources(t *testing.T) {
	servers := []core.Server{
		{Name: "empty"},
		{Name: "blank", Sources: []core.Source{{URL: "  "}}},
	}
	if hasSources(servers) {
		t.Fatal("hasSources returned true without a playable URL")
	}
	servers = append(servers, core.Server{Name: "playable", Sources: []core.Source{{URL: "https://cdn.example/ep1.m3u8"}}})
	if !hasSources(servers) {
		t.Fatal("hasSources returned false with a playable URL")
	}
}
