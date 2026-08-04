package v1

import (
	"net"
	"os"
	"strings"
	"sync"
)

// defaultCDNSuffixes are media-CDN host suffixes the media proxy may fetch.
// Provider CDNs rotate hostnames (observed: vidtub.akirax.buzz,
// vidtub.shiora.site, hls.anidb.app, www.animegg.org, video.wixstatic.com),
// so matching is suffix-based and the list is extendable at runtime via
// ANIRAKU_PROXY_CDN_ALLOWLIST (comma-separated hostnames/suffixes).
var defaultCDNSuffixes = []string{
	// Miruro-family HLS CDNs (observed live + referenced in code)
	"akirax.buzz", "shiora.site", "anidb.app", "uwucdn.com", "owocdn.com",
	"kwik.cx", "senshi.live", "ninstream.com", "ninstream.co",
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
	// kwik.cx media servers (referenced by IP in the referer logic)
	"185.237.106.79", "203.188.166.228",
}

var (
	proxyCDNList     []string
	proxyCDNListOnce sync.Once
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
		return false
	}
	for _, s := range suffixes {
		if host == s || strings.HasSuffix(host, "."+s) {
			return true
		}
	}
	return false
}
