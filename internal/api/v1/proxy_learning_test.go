package v1

import (
	"strings"
	"testing"
	"time"
)

// A playlist served from a trusted host vouches for the media hosts it names,
// which is what lets a provider rotate CDN hostnames without a code change.
func TestPlaylistVouchesForRotatedCDNHost(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()

	const rotated = "vidtub-new-2026.rotated-cdn.example"
	if isAllowedProxyHost(rotated) {
		t.Fatalf("precondition: %q should not be allowed yet", rotated)
	}

	playlist := strings.Join([]string{
		"#EXTM3U",
		"#EXT-X-KEY:METHOD=AES-128,URI=\"https://" + rotated + "/enc.key\"",
		"#EXTINF:10.0,",
		"https://" + rotated + "/segment-1-.ts",
		"#EXT-X-ENDLIST",
	}, "\n")

	h := &Handlers{}
	// Base URL is on the static allowlist, so this playlist may vouch.
	h.rewriteHLSPlaylist(playlist, "https://hls.anidb.app/media/index.m3u8", "")

	if !isAllowedProxyHost(rotated) {
		t.Errorf("host %q named by a trusted playlist was not learned", rotated)
	}
}

// A playlist fetched from an untrusted host must not be able to vouch for
// anything, or the trust chain collapses into a general relay.
func TestUntrustedPlaylistCannotVouch(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()

	const target = "attacker-designated.example"
	playlist := "#EXTM3U\n#EXTINF:10.0,\nhttps://" + target + "/segment-1-.ts\n"

	h := &Handlers{}
	h.rewriteHLSPlaylist(playlist, "https://evil.example.com/index.m3u8", "")

	if isAllowedProxyHost(target) {
		t.Errorf("host %q was learned from an untrusted playlist", target)
	}
	if n := GetDynamicCDNCount(); n != 0 {
		t.Errorf("dynamic list = %d, want 0", n)
	}
}

// The ad beacon from the production logs must never be learned, even when it
// appears inside an otherwise-trusted playlist.
func TestAdBeaconNotLearnedFromTrustedPlaylist(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()

	const adHost = "p1.ipstatp.com"
	playlist := strings.Join([]string{
		"#EXTM3U",
		"#EXT-X-KEY:METHOD=AES-128,URI=\"https://" + adHost + "/obj/ad-site-i18n/beacon\"",
		"#EXTINF:10.0,",
		"https://" + adHost + "/obj/ad-site-i18n/202603305d0d5c515be3279c4b3db830",
		"#EXT-X-ENDLIST",
	}, "\n")

	h := &Handlers{}
	h.rewriteHLSPlaylist(playlist, "https://hls.anidb.app/media/index.m3u8", "")

	if isAllowedProxyHost(adHost) {
		t.Errorf("ad host %q was learned; it has no media extension", adHost)
	}
}

// Learned hosts are exact-match only. Suffix-matching them would let a single
// rotated hostname widen the boundary to every subdomain beneath it.
func TestLearnedHostDoesNotMatchSubdomains(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()

	LearnHostFromPlaylist("cdn7.rotated.example")
	if !isAllowedProxyHost("cdn7.rotated.example") {
		t.Fatal("learned host should match exactly")
	}
	if isAllowedProxyHost("evil.cdn7.rotated.example") {
		t.Error("learned host must not match as a suffix")
	}
}

// Private and loopback targets are never worth an allowlist entry; the dialer
// guard would reject them at connect time regardless.
func TestLearningRejectsNonPublicAddresses(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()

	for _, host := range []string{"127.0.0.1", "10.0.0.5", "169.254.169.254", "192.168.1.1"} {
		LearnHostFromPlaylist(host)
		if isAllowedProxyHost(host) {
			t.Errorf("non-public address %q was learned", host)
		}
	}
}

// Expiry is enforced on read, so a lapsed entry stops being allowed
// immediately rather than lingering until the cleanup sweep runs.
func TestExpiredEntryRejectedBeforeCleanupRuns(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()

	const host = "stale.rotated.example"
	dynamicCDNMu.Lock()
	dynamicCDNSuffixes[host] = time.Now().Add(-dynamicEntryTTL - time.Minute)
	dynamicCDNMu.Unlock()

	if isAllowedProxyHost(host) {
		t.Error("expired entry still allowed without a cleanup sweep")
	}
}
