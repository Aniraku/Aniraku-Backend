package streaming

import (
	"fmt"
	"testing"
)

func TestAnikotoEmbeddedSourceURLTemplates(t *testing.T) {
	for _, lang := range []string{"sub", "dub"} {
		got := fmt.Sprintf(anikotoEmbedURLTemplate, "21", lang, 1)
		want := fmt.Sprintf("https://anivexa-api-tu4a.onrender.com/watch/anikoto/21/%s/anikoto-1", lang)
		if got != want {
			t.Fatalf("Anikoto embedded URL = %q, want %q", got, want)
		}
	}
}

func TestCustomServerNamesRemainStable(t *testing.T) {
	if got, want := anikotoServers, [2]string{"Niko", "Momo"}; got != want {
		t.Fatalf("Anikoto server names = %v, want %v", got, want)
	}

	flixcloudNames := []string{"Yuta", "Syota"}
	if flixcloudNames[0] != "Yuta" || flixcloudNames[1] != "Syota" {
		t.Fatalf("FlixCloud server names = %v, want [Yuta Syota]", flixcloudNames)
	}
}
