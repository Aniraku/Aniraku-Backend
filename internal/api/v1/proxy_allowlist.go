package v1

import (
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Aniraku/Aniraku-Backend/internal/netguard"
)

// defaultCDNSuffixes are media-CDN host suffixes the media proxy may fetch.
// Provider CDNs rotate hostnames (observed: vidtub.akirax.buzz,
// vidtub.shiora.site, hls.anidb.app, www.animegg.org, video.wixstatic.com),
// so matching is suffix-based and the list is extendable at runtime via
// ANIRAKU_PROXY_CDN_ALLOWLIST (comma-separated hostnames/suffixes).
var defaultCDNSuffixes = []string{
	// Miruro-family HLS CDNs (observed live + referenced in code)
	"akirax.buzz", "shiora.site", "anidb.app", "uwucdn.com", "owocdn.com",
	"kwik.cx", "ninstream.com", "ninstream.co",
	// FlixCloud embed CDN
	"flixcloud.cc", "flixcloud.com",
	// Direct mp4 mirrors
	"animegg.org",
	// Wix-hosted video (repackager.wixmp.com)
	"wixmp.com", "wixstatic.com",
	// Miruro's own domains
	"miruro.tv", "miruro.cc", "miruro.be",
	// Common video CDN platforms
	"cloudfront.net", "akamaized.net", "storage.googleapis.com", "googleapis.com",
	"bunnycdn.com", "fastly.net", "fastlylb.net",
	// Gogo-family CDN hosts
	"gogocdn.net", "streamani.net",
	// Additional observed CDN hosts
	"vidtub.akirax.buzz", "vidtub.shiora.site", "hls.anidb.app",
	"vidcloud.net", "vidstreaming.io", "streamtape.net",
	"rapidvideo.com", "mp4upload.com", "vidhide.net",
	// New CDN hosts (observed 2026-08-04)
	"norami.top",
	"fast4speed.rsvp",
	// Additional anime CDN hosts from DeepSeek audit
	"ans-bio-video.com",
	"ans-bio-video.net",
	"ans-bio-video.org",
	"bio-video.net",
	"bio-video.org",
	"bio-video.com",
	"ans-bio-video.s3.amazonaws.com",
	"ans-bio-video.s3.us-east-1.amazonaws.com",
	"streamtape.to",
	"streamtape.cc",
	"megaplay.live",
	"megaplay.site",
	"megaplay.top",
	"megaplay.xyz",
	"megaplay.pro",
	"megaplay.club",
	"megaplay.cc",
	"uwucdn.net",
	"owocdn.net",
	"kotocdn.net",
	"kotocdn.site",
	"kotocdn.top",
	"vivibebe.net",
	"vivibebe.site",
	"vivibebe.top",
	"vivibebe.com",
	"nekostream.net",
	"nekostream.site",
	"nekostream.top",
	"nekostream.com",
	"watching.onl",
	"watching.site",
	"krussdomi.net",
	"krussdomi.site",
	"krussdomi.com",
	"mewstream.net",
	"mewstream.site",
	"mewstream.com",
	"fast4speed.net",
	"fast4speed.site",
	"fast4speed.top",
	"fast4speed.com",
	// Anikoto provider embed CDNs
	"anikototv.to", "megaplay.buzz",
	// Observed Miruro provider CDN hosts (2026-08-04)
	"mikora.top",
	"mikora.site",
	"mikora.buzz",
	"vidtub.mikora.top",
	// Observed live 2026-08-05: vault-10.uwucdn.top (HLS), CF workers front
	// for rotated CDNs (morning-credit-3bcc.vibevibe.workers.dev), and
	// a1.mp4upload.com:183 direct mp4 (non-standard port handled in Proxy).
	"uwucdn.top",
	"uwucdn.site",
	"workers.dev",
	"pages.dev",
	// kwik.cx media servers (referenced by IP in the referer logic)
	"185.237.106.79", "203.188.166.228",
}

// nuisanceProxyHostSuffixes is deliberately narrow. The media proxy must not
// become a relay for known popup, tracking, and advertising endpoints, while
// valid provider pages and video CDNs remain unaffected.
var nuisanceProxyHostSuffixes = []string{
	"doubleclick.net", "googlesyndication.com", "googleadservices.com",
	"adnxs.com", "adskeeper.co.uk", "adsterra.com", "exoclick.com",
	"popads.net", "popcash.net", "propellerads.com", "trafficjunky.net",
	"hilltopads.net", "onclicka.com", "clickadu.com", "monetag.com",
	"ad-maven.com", "juicyads.com", "push.house", "richpush.com", "tsyndicate.com",
}

func isNuisanceProxyHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.Trim(host, "[]")
	if host == "" {
		return false
	}
	for _, suffix := range nuisanceProxyHostSuffixes {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return false
}

var (
	proxyCDNList     []string
	proxyCDNListOnce sync.Once
	proxyCDNMu       sync.RWMutex
	// Dynamic allowlist: hosts that have been successfully proxied
	dynamicCDNSuffixes = make(map[string]time.Time)
	dynamicCDNMu       sync.RWMutex
	// Max dynamic entries to prevent unbounded growth
	maxDynamicEntries = 500
	// TTL for dynamic entries (24 hours)
	dynamicEntryTTL = 24 * time.Hour
)

func proxyCDNSuffixes() []string {
	proxyCDNListOnce.Do(func() {
		seen := map[string]bool{}
		add := func(v string) {
			v = strings.ToLower(strings.TrimSpace(v))
			if v == "" || seen[v] {
				return
			}
			seen[v] = true
			proxyCDNList = append(proxyCDNList, v)
		}
		for _, s := range defaultCDNSuffixes {
			add(s)
		}
		for _, s := range strings.Split(os.Getenv("ANIRAKU_PROXY_CDN_ALLOWLIST"), ",") {
			add(s)
		}
	})
	return proxyCDNList
}

// resetProxyCDNList re-reads the allowlist (test hook).
func resetProxyCDNList() {
	proxyCDNListOnce = sync.Once{}
	proxyCDNList = nil
}

// resetDynamicCDNEntries clears all learned hosts (test hook).
func resetDynamicCDNEntries() {
	dynamicCDNMu.Lock()
	defer dynamicCDNMu.Unlock()
	dynamicCDNSuffixes = make(map[string]time.Time)
}

// isAllowedProxyHost reports whether host is a CDN the media proxy may fetch.
// Hostnames match on exact host or any subdomain of a listed suffix; IP
// literals match exactly. Hostnames fail closed when the allowlist is empty.
func isAllowedProxyHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.Trim(host, "[]")
	if host == "" {
		return false
	}
	suffixes := proxyCDNSuffixes()
	if ip := net.ParseIP(host); ip != nil {
		for _, s := range suffixes {
			if host == s {
				return true
			}
		}
		// Also check dynamic entries for IPs
		return dynamicEntryLive(host)
	}
	for _, s := range suffixes {
		if host == s || strings.HasSuffix(host, "."+s) {
			return true
		}
	}
	// Check dynamic allowlist. Unlike the static list, learned hosts match
	// exactly and never as a suffix: a learned host is vouched for only by
	// the playlist that named it, which says nothing about its subdomains.
	// Suffix-matching a learned host would let one rotated CDN name widen
	// the boundary to every subdomain beneath it.
	return dynamicEntryLive(host)
}

// dynamicEntryLive reports whether host has an unexpired dynamic entry.
// Expiry is enforced on read rather than relying on CleanupDynamicCDNEntries,
// so a host stops being allowed the moment its TTL lapses instead of
// lingering until the next sweep.
func dynamicEntryLive(host string) bool {
	dynamicCDNMu.RLock()
	addedAt, ok := dynamicCDNSuffixes[host]
	dynamicCDNMu.RUnlock()
	return ok && time.Since(addedAt) <= dynamicEntryTTL
}

// LearnHostFromPlaylist adds host to the dynamic allowlist because it was
// referenced by a playlist we fetched from an already-trusted host.
func LearnHostFromPlaylist(host string) {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.Trim(host, "[]")
	if host == "" {
		return
	}
	if isNuisanceProxyHost(host) {
		return
	}
	// Never learn a host that is already covered, and never learn a
	// non-public address — the dialer guard would reject it at connect
	// time anyway, so storing it only wastes an entry.
	if isAllowedProxyHost(host) {
		return
	}
	if ip := net.ParseIP(host); ip != nil && !netguard.IsPublicIP(ip) {
		return
	}
	dynamicCDNMu.Lock()
	defer dynamicCDNMu.Unlock()
	// Enforce max entries with LRU-style eviction
	if len(dynamicCDNSuffixes) >= maxDynamicEntries {
		// Remove oldest entry
		var oldestHost string
		var oldestTime time.Time
		first := true
		for h, t := range dynamicCDNSuffixes {
			if first || t.Before(oldestTime) {
				oldestHost = h
				oldestTime = t
				first = false
			}
		}
		if oldestHost != "" {
			delete(dynamicCDNSuffixes, oldestHost)
		}
	}
	dynamicCDNSuffixes[host] = time.Now()
}

// CleanupDynamicCDNEntries removes expired entries from the dynamic allowlist.
func CleanupDynamicCDNEntries() {
	dynamicCDNMu.Lock()
	defer dynamicCDNMu.Unlock()
	now := time.Now()
	for host, addedAt := range dynamicCDNSuffixes {
		if now.Sub(addedAt) > dynamicEntryTTL {
			delete(dynamicCDNSuffixes, host)
		}
	}
}

// GetDynamicCDNCount returns the number of dynamically learned CDN hosts.
func GetDynamicCDNCount() int {
	dynamicCDNMu.RLock()
	defer dynamicCDNMu.RUnlock()
	return len(dynamicCDNSuffixes)
}
