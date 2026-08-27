package streaming

import (
	"testing"

	"github.com/rs/zerolog"
)

func TestNewMiruroProviderUsesAnirakuUpstreamByDefault(t *testing.T) {
	provider := NewMiruroProvider(zerolog.Nop(), "")
	if provider.apiBase != "https://miruro.aniraku.tech" {
		t.Fatalf("default Miruro upstream = %q, want %q", provider.apiBase, "https://miruro.aniraku.tech")
	}
}

func TestNewMiruroProviderPreservesConfiguredUpstream(t *testing.T) {
	const configured = "http://127.0.0.1:8099"
	provider := NewMiruroProvider(zerolog.Nop(), configured)
	if provider.apiBase != configured {
		t.Fatalf("configured Miruro upstream = %q, want %q", provider.apiBase, configured)
	}
}
