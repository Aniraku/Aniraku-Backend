package streaming

import (
	"reflect"
	"testing"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
)

func TestApplyQualityFilterReportsRealSourceQualities(t *testing.T) {
	manager := &Manager{}
	result := manager.applyQualityFilter(&SourceResult{Sources: []core.Source{
		{URL: "https://cdn.example/master.m3u8", Quality: "Auto"},
		{URL: "https://cdn.example/720.m3u8", Quality: "720p"},
		{URL: "https://cdn.example/720-alt.m3u8", Quality: "720P"},
		{URL: "https://cdn.example/480.m3u8", Quality: "480p"},
	}}, "720p")

	if !reflect.DeepEqual(result.Qualities, []string{"Auto", "720p", "480p"}) {
		t.Fatalf("qualities = %#v, want unique source labels", result.Qualities)
	}
	if len(result.Sources) != 2 {
		t.Fatalf("filtered sources = %#v, want two 720p sources", result.Sources)
	}
}
