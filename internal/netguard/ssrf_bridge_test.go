package netguard

import (
	"strings"
	"testing"
)

// TestControlPrivateBridgeAllowlist walks the exact-address exception that
// lets the backend reach deliberately collocated services (the self-hosted
// AniList mirror) through the dialer guard: the listed ip:port passes, every
// other private address — even the same host on a neighboring port — stays
// blocked, which is what keeps redirects and rebinding attempts inert.
// Membership in the exception list and the Control verdict are separate
// axes: a public address dials because IsPublicIP says so, not because any
// list contains it.
func TestControlPrivateBridgeAllowlist(t *testing.T) {
	t.Setenv("ANIRAKU_PRIVATE_BRIDGE_HOSTS", "127.0.0.1:4401, [::1]:4401, localhost-nope")

	cases := []struct {
		address string
		member  bool   // present in the parsed exception list
		dialErr string // expected Control outcome: "" = allowed
	}{
		{"127.0.0.1:4401", true, ""},
		{"[::1]:4401", true, ""},
		// Same host, other port: still blocked.
		{"127.0.0.1:4399", false, "non-public"},
		{"[::1]:4399", false, "non-public"},
		// Other loopback bytes and private ranges: still blocked.
		{"127.0.0.2:4401", false, "non-public"},
		{"10.0.0.5:443", false, "non-public"},
		{"192.168.1.10:4401", false, "non-public"},
		// Public addresses are unaffected by the exception.
		{"1.1.1.1:443", false, ""},
		// Malformed entry "localhost-nope" is quietly ignored: nothing
		// with that name has an ip:port form, so no exception exists.
	}
	for _, tc := range cases {
		_ = allowedBridge(tc.address) // ensure the parse has happened before inspecting the table
		if _, ok := bridgeSet[tc.address]; ok != tc.member {
			t.Fatalf("allowlist membership for %q = %v, want %v (parsed: %v)", tc.address, ok, tc.member, bridgeSet)
		}
		err := Control("tcp", tc.address, nil)
		switch {
		case tc.dialErr == "" && err != nil:
			t.Fatalf("Control(%q) blocked: %v", tc.address, err)
		case tc.dialErr != "" && err == nil:
			t.Fatalf("Control(%q) unexpectedly allowed", tc.address)
		case tc.dialErr != "" && !strings.Contains(err.Error(), tc.dialErr):
			t.Fatalf("Control(%q) error %v lacks %q", tc.address, err, tc.dialErr)
		}
	}

	// Dropping the env must dissolve the whole exception, immediately
	// and without a restart.
	t.Setenv("ANIRAKU_PRIVATE_BRIDGE_HOSTS", "")
	allowedBridge("127.0.0.1:4401") // force the reload under the same mutex path
	if err := Control("tcp", "127.0.0.1:4401", nil); err == nil {
		t.Fatal("list empty yet the bridge address still dials")
	}
	if len(bridgeSet) != 0 {
		t.Fatalf("expected an empty parsed list, got %v", bridgeSet)
	}
}
