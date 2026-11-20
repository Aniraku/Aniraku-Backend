# Operating notes

Upstream-behavior log for the streaming package. Dates are check dates,
not contract. New findings belong in commit messages or here, never
inside source comments.

## Provider checks

- 2026-11-20 lineup (operator): FlixCloud is the MAIN server — fan-out
  entry first, merge first, /stream fallback first (also first for
  hentai), and serverVerdictRank tier 4 so the verdict sort cannot bury
  it. Verified live: provider order flixcloud > anikoto > animex > zoko
  > ...  with Yokan/Monaka at positions 0-1. The AnimeDL/animeheaven
  external download source is REMOVED (fetch + attach + file gone;
  animedl_downloads.go deleted); zoko's own download link remains.
- 2026-11-21 mkissa REMOVED (operator decision): provider files deleted
  (mkissa.go/mkissa_test.go/mkissa_turnstile.go/captcha probe), fan-out
  entry, /stream aliases and collect hook all gone; allowlist additions
  for it (ok.ru, mycdn.me, allanime.day) reverted. History note: the
  boot-token crypto was byte-verified and the episode query resolved
  live from a residential IP, but the VPS datacenter IP sat in a long
  NEED_CAPTCHA penalty window, so it never worked reliably where it
  mattered. Full RE notes preserved in git history (30400b9..8aef332).
- 2026-11-17 flixcloud sub/dub: reanime hands the SAME dataLink for sub
  and dub entries (one dual-audio master carrying jpn "Native" + eng
  "English" renditions) — the languages differ by the al=sub/dub strip,
  which flixcloud URLs never received (proxySources gated the strip on
  isKaaHLSSource, and the k-link mint dropped al during the nested
  unwrap). Fix: wantsAudioStrip covers flixcloud hosts, withAudioLang
  appends al at mint time, mintProxyLink preserves it across the unwrap,
  and the k-route promotes the sealed al to the request query so
  stripAudioRenditions sees it. Verified live: SUB master serves jpn
  only, DUB master serves eng only, variant playlists strip too.
  Subtitle chain re-verified through the exact browser flow (Range head
  fetch -> [Script Info] detect -> Artplayer ass type).
- 2026-11-17 flixcloud (final piece): the direct-browser flow
  (VITE_PLAYBACK_HOST_B64 -> api.aniraku.tech) served the RAW wrapped
  bodies for all three masters — logUnwrappedFlixcloud caught the decode
  failing while the bytes decoded fine via curl minutes later: Cloudflare
  edge nodes hold MIXED key-generations of the encrypted playlists, and a
  fetch can land on a copy wrapped by a key the candidate set does not
  hold. The proxy now busts the CDN cache (fcb= param) and refetches once
  on decode failure, refreshes the key set from a live embed page
  (FlixcloudRefreshKeys, throttled 20s, provider remembers its last embed
  URL), retries the decode, and drops undecodable VOD-cache entries so
  they never serve again. Verified live: 24/24 rounds across all three
  servers, master/variant/key/segment/subtitle green through
  api.aniraku.tech with browser Origin. The zone's "API Accessing" WAF
  rule was ALSO blocking the function's backend subrequests (empty UA);
  serveKViaBackend now sends Aniraku-PagesFunction/1.0 and the rule needs
  Origin/UA exemptions for the site's own playback (user-side dashboard
  change, expression provided).
- 2026-11-16 flixcloud (app path): the box-side chain was green but the
  APP still failed because the Pages function's serveK fetched sealed
  masters from Cloudflare's own IPs — the JWT client_ip check 403'd the
  edge, and where the edge answered from cache the body was the undecoded
  base64(XOR) wrapper. serveK now routes flixcloud-sealed tokens and
  backend-host child URIs through the backend (internal header attached);
  the shield then wraps everything as usual. Verified with the REAL
  function code driven against the REAL backend (Node harness, no CF in
  the loop): shielded /servers with zero upstream-host leaks, master ->
  variant -> key(16B) -> segment(1.3MB) -> styled .ass subtitle, all
  same-origin tokens. ALSO found: the Pages function's token env name
  (BACKEND_INTERNAL_TOKEN) and the gate's (ANIRAKU_INTERNAL_TOKEN) held
  different values in production — the gate now accepts either, so the
  token check is actually live instead of riding the UA exemption.
  /servers consistency re-verified after all changes: 3 identical runs,
  17 servers / 10 providers each, cold titles included.
- 2026-11-16 flixcloud (again, same day): the master URLs embed an
  IP-bound JWT whose window flixcloud mints per token — observed iat
  7.5h stale inside a "fresh" resolve, expiring mid-flight, which took
  Yokan/Monaka down while Mikan (a fresher cached generation) kept
  playing. Resolve cache now expires at min(45min, exp-15min) read
  straight from the JWT payload, refresh=true bypasses it entirely
  (token-bound playback must never ride a stale token), and the
  stability snapshot for flixcloud is 30min with exact expired-JWT
  eviction on both the merge and fallback paths. Verified: fresh
  resolve mints a 6h window (iat=now), all three servers green
  end-to-end (master, both audio renditions, key.bin, segments to
  valid TS).
## Provider checks

- 2026-11-16 flixcloud: embed playback replaced with the reverse-engineered
  direct chain (flixcloud.go + flixcloud_wasmrun.go). Verified live on the
  box: all three reanime embeds (v=1/2/3 → fetch9/fetch8/fetch11 masters)
  resolve to direct HLS, master + child playlists unwrap (b64+XOR) and
  rewrite, key.bin serves 16 raw bytes, vault-N.rundowncdn.top segments
  carry the sealed Referer and AES-128-decrypt to valid TS, and the
  island's styled English .ass subs ride the proxy as source subtitles.
  Tokens behind /api/m3u8/ are single-use (410 invalid_or_used_token on
  reuse) and the two v1/v3 masters carry an IP-bound JWT (client_ip claim,
  ~6h expiry) — valid only because every fetch egresses from this box.
  The wasm mix module randomizes both structure and constants per page
  render, so it is executed (wazero) rather than reimplemented; the
  playlist XOR key is baked into the module's data section and refreshed
  from every resolve. POST /api/v1/stream now mints k-links like /servers
  (it previously leaked url=+headers= on the cached path).

- 2026-11-05 animex: VPS egress cycles the fingerprint 429
  (fingerprint source limit) once per cooldown window since the Chrome-uTLS
  client (commit 930a4bc): window expires → 3-4 racing sub-provider pokes →
  fresh 429 → window reopens, hour after hour. Fix: /stream resolves
  sub-providers lazily (first success in plyr order — one sources-API call
  per request instead of the full 4-6 fan-out) and consecutive throttle
  cycles double the window 30s→1m (cap 2m); a healthy 200 resets the
  ladder. /servers keeps the full parallel fan-out.
- 2026-11-05 anipm: ani.pm verified working from the VPS egress but was
  not part of the automatic /stream chain (only reachable by explicit
  provider=request). It now rides last in the /stream candidates, after
  Anixo; Daifuku stays the site's own second embed slot.
- 2026-10-26 zenbu: zen-bu.cc index verified end-to-end from the VPS
  egress; megadown JSON, Worker-edge masters, child playlists and segment
  bytes all 200. shorekeeper 2 sub + 2 dub, encore 4 + 4, roccia and
  carlotta 2 sub each, no shared URLs between pools. `suisui` and
  `mornye` (koyeb host) resolve a master but serve no segment.
- 2026-10-26 animex: plyr lists `nero` as the new default sub-provider;
  mapped to Hana. Every listed provider returns exactly one source per
  episode, so the hidden 2-3 sources were not there; unknown ids now
  publish as Nuri instead of collapsing into Hana. nero hands out
  `s-91.megavid.se` masters, allowlisted.
- 2026-09-30 kaa: kaa.lt search/listings/watch 200 from this egress, no
  datacenter block; krussdomi masters 200 direct and via the media proxy;
  segment bytes need `Origin: https://krussdomi.com` (Referer alone 403s).
- 2026-10-01 kaa: same show, two listing slugs (ja-JP ep1 vs en-US ep1),
  dub lane can carry zero embeds while sub resolves.
- 2026-10-02 animegg, aniwaves: flows verified end-to-end from the
  production egress.
- 2026-10-03 megavid: MegaPlay shape `({mal|ani}/{id}/{ep}/{lang}/source)`verified.
- 2026-10-07 tensho: zangetsu flow verified end-to-end; animedl downloader
  adopted over the zoko/kiwi fetcher; manager gates dropped (animeheaven,
  zenime, ogflix among them).
- 2026-10-18 anixo, anv: verified end-to-end from the VPS egress; anixo's
  dub flag proved lit-for-nothing (hasDub false on every probed show), so
  anixo ships sub-only pending a live dub verification.

## Latency findings (2026-10-20)

- kaa.lt listing page fetch varies wildly per draw: 0.9s / 6.2s / 11.2s
  for the identical page. The 6s listing cap turns a guaranteed budget
  burn into a fast fail.
- krussdomi and cdn.4animo: fresh connections answer in under a second
  (python probes), while the Go client on reused keep-alives hangs to the
  full budget. kaa now dials fresh per request.
- One hung fan-out slot used to erase the other slots' completed results
  (context deadline discarded everything). Collectors now ship survivors.
- Dead tensho hops measured long on reused paths; the per-call cap is 6s.

## Manager history

- A hand-counted WaitGroup Add() once deadlocked every /servers call
  after a provider removal; the fan-out is now a data table whose count
  derives from its own length.
- A cache once froze localhost health-probe URLs into public responses for a
  full TTL. The current stability cache stores raw upstream results only;
  request-specific proxy URLs are added after every load.
- 2026-10-25 server-list flapping came from treating every transient empty
  collector as the provider's current truth. Identical cold requests now
  coalesce, complete lists stay fixed for 90 seconds, incomplete lists retry
  after 5 seconds, and each provider can reuse its last good result, including
  previously verified slots lost from a partial probe. kaa and tensho keep that
  fallback for 2 hours; other providers keep it for 10 minutes.
- 2026-10-25 playback validation found provider URLs that passed a collector
  probe but returned a redirect at the media proxy, which intentionally refuses
  redirects. Tensho now resolves the final HLS URL before publication;
  local validation passed every returned source for the available lanes.

## API layer history

- 2026-10-14 proxy egress shaping was a temporary order (was to expire
  2026-10-17); before touching it, re-measure the origin's egress first.
- 2026-10-16/17 proxy memory guard + fill limiter landed after the OOM
  wedges (kernel killed aniraku-server 3x on Oct 15-16).
- 2026-10-17 the frontends' vercel routes went stale; cors.go sources
  were migrated to the main production origin.
- 2026-09-27 tmdb deadlock: episode enrichment blocked the catalog
  Range callback through setCachedEpisodes for a cache-wide cascade.

## kaa island

- 2026-10-21 the cat-player Astro island's props carry full subtitle
  metadata per track (format, language code, native name, file URL);
  subtitle tracks are decoded from there instead of scraped from
  bare URLs.

## tensho tracks

- 2026-10-21 hd-2 getSources tracks carry no language field on any entry;
  kind is the shape's only metadata ("captions" for text, "thumbnails"
  for the sprite preview). The sprite rides along under the label
  "Subtitles" and reached Hoshi's subtitle list as a langless track;
  tenshoSubs now drops kind "thumbnails". Caption labels are decorated
  ("Spanish (Latin American)", "French (European)") and resolve through
  label aliasing.
- 2026-10-21 episode embeds key on ani/mal per-episode values that
  already embed the episode ("185874/9"): /embed/ani/185874/9/sub, not
  /embed/ani/<ep-less>/sub, which 404s.
