package v1

import (
	"strings"
	"testing"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/core"
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
	h.rewriteHLSPlaylist(playlist, "https://hls.anidb.app/media/index.m3u8", "", "https://backend.test", "")

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
	h.rewriteHLSPlaylist(playlist, "https://evil.example.com/index.m3u8", "", "https://backend.test", "")

	if isAllowedProxyHost(target) {
		t.Errorf("host %q was learned from an untrusted playlist", target)
	}
	if n := GetDynamicCDNCount(); n != 0 {
		t.Errorf("dynamic list = %d, want 0", n)
	}
}

// The segment CDN from the production logs (extension-less URLs on a
// Bytedance host) must be learnable, those are real video segments, not
// ads, despite the "ad-site-i18n" path segment.
func TestExtensionLessSegmentHostLearnedFromTrustedPlaylist(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()

	const segmentHost = "p1.ipstatp.com"
	if isAllowedProxyHost(segmentHost) {
		t.Fatalf("precondition: %q should not be allowed yet", segmentHost)
	}

	playlist := strings.Join([]string{
		"#EXTM3U",
		"#EXTINF:10.0,",
		"https://" + segmentHost + "/obj/ad-site-i18n/202604075d0d5bea89a6f3b6459c98bc",
		"#EXT-X-ENDLIST",
	}, "\n")

	h := &Handlers{}
	h.rewriteHLSPlaylist(playlist, "https://hls.anidb.app/media/index.m3u8", "", "https://backend.test", "")

	if !isAllowedProxyHost(segmentHost) {
		t.Errorf("extension-less segment host %q was not learned", segmentHost)
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

// The automated-learning contract: every URL the backend seals into an
// opaque k-link is a provider-resolved playback target (stream or subtitle),
// so minting alone must vouch the host for the proxy gate. This is what
// makes provider CDN rotation — including subtitle-host rotation, the
// subtites4you incident — self-healing without an allowlist edit.
func TestMintProxyLinkLearnsHost(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()
	if !proxyLinkInit(strings.Repeat("ab", 32)) {
		t.Fatal("proxy link key init failed")
	}
	defer func() { proxyLinkKeyBytes = nil }() // disable minting for other tests

	const rotated = "subs-2026.rotated-subcdn.example"
	if isAllowedProxyHost(rotated) {
		t.Fatalf("precondition: %q should not be allowed yet", rotated)
	}

	link, err := mintProxyLink("https://"+rotated+"/subtitles/eng.ass", map[string]string{"Referer": "https://flixcloud.cc/"})
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if !strings.Contains(link, "/api/v1/proxy?k=") {
		t.Fatalf("minted link shape: %q", link)
	}
	if !isAllowedProxyHost(rotated) {
		t.Errorf("minting a k-link did not vouch host %q", rotated)
	}
}

// Resolving a self-minted token vouches its host too — the safety net for
// links minted by an earlier process whose learned file was lost.
func TestResolveProxyLinkLearnsHost(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()
	if !proxyLinkInit(strings.Repeat("cd", 32)) {
		t.Fatal("proxy link key init failed")
	}
	defer func() { proxyLinkKeyBytes = nil }()

	const rotated = "cdn-2026.rotated-cdn.example"
	// Simulate the lost learned file: forget anything about the host.
	resetDynamicCDNEntries()

	link, err := mintProxyLink("https://"+rotated+"/video.m3u8", nil)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	token := link[len("/api/v1/proxy?k="):]
	if _, _, err := resolveProxyLink(token); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !isAllowedProxyHost(rotated) {
		t.Errorf("resolving a self-minted token did not vouch host %q", rotated)
	}
}

// Legacy mode (seal disabled): the /servers handout still vouches every
// host it exposes — sources and subtitles — so raw-URL clients keep working
// through rotated CDNs.
func TestLegacyHandoutLearnsSubtitleHosts(t *testing.T) {
	t.Cleanup(resetDynamicCDNEntries)
	resetDynamicCDNEntries()
	proxyLinkKeyBytes = nil // legacy mode

	const subHost = "legacy-subs.rotated-subcdn.example"
	servers := []core.Server{{
		Name:     "Yokan",
		Provider: "flixcloud",
		Sources: []core.Source{{
			URL: "https://legacy-media.rotated-cdn.example/master.m3u8",
			Subtitles: []core.Subtitle{{
				URL:   "https://" + subHost + "/eng.ass",
				Lang:  "en",
				Label: "English (styled)",
			}},
		}},
	}}

	h := &Handlers{}
	h.mintSourcesForClient(servers)

	if !isAllowedProxyHost(subHost) {
		t.Errorf("legacy handout did not vouch subtitle host %q", subHost)
	}
	if !isAllowedProxyHost("legacy-media.rotated-cdn.example") {
		t.Errorf("legacy handout did not vouch media host")
	}
}
