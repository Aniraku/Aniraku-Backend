# Streaming providers — contributor guide

All provider code lives in `internal/streaming/`, orchestrated by
`manager.go`, played through `internal/api/v1/proxy.go`. This is the
document to read before touching any of it. It states the rules as they
are enforced in code, not as aspirations.

## Provider catalog

Resolves are AniList-keyed: `FindEpisodeSource(ctx, anilistID, episode,
lang)` with `lang` ∈ `sub`/`dub`. Order below is both the `/stream`
fallback order and the `/servers` merge order.

| Family (`provider`) | Display servers (`name`) | What it serves |
|:--|:--|:--|
| `anikoto` | Niko (AniList-keyed), Momo (MAL-keyed) | MegaPlay direct HLS, verified segments |
| `animex` | Mochi/yuki, Chibi/neko, Kira/zuna, Sora, Koharu/uwu, Lumi/beep, Anzu/loli, Hana (fallback) | plyr API, XOR-decoded direct URLs |
| `zoko` | Zoko | ZokoAnime direct HLS (AniList → MAL fallback), probe-verified |
| `flixcloud` | Yuta, Syota, Mike (by access-ID order) | Reanime **embeds** for the embedded player |
| `nin` | NiN | Supaplay hls-proxy relay, playlist-verified; subtitles borrowed from siblings |
| `kaa` | Nico, robin, D'Luff, zoro, sanji, nami, usopp, chopper, franky, brook, jimbei, then `kaa-N` (positional) | kaa.lt krussdomi **master** playlists (dual-audio) |
| `animegg` | Sunny, Yolky, Eggy (positional per mirror tab) | animegg.org direct mp4, highest quality per tab |
| `aniwaves` | Nami (Vidplay), Coral (MyCloud), Pearl (DatSaV/savedly), Wavy/Bubbles/Shelly (fallback) | echovideo multi-rendition HLS masters + top savedly mp4 |
| `vidnest` | Nest | VidNest MegaPlay HLS + subs + skips (custom-b64 API) |
| `lee` | Lee | ani.pm direct HLS (series → bootstrap → settlar session → embed session) |
| `megavid` | Vidy | megavid.buzz JSON API (AnimeX-codec files + `/vid/` gateway), language-verified |
| `mkissa` | Chuu (clock/wixmp), Mua, Kissy, Smooch, Peck, Xoxo (ok.ru) — by source kind | mkissa.to signed GraphQL → **direct m3u8/mp4 only**, probe-verified; JS engine daemon owns the crypto |

Removed providers return a `removed` error naming them explicitly
(`miruro`, `zenime`, `tryembed`, …) — never silently fall
through.

## Request flows

### `GET /api/v1/servers` — fan-out list
`FindAllServers` runs every provider collector **concurrently** (45 s hard
cap for the whole fan-out; per-collector timings logged as `collectorMs`),
then:
1. `mergeZokoDownloads` — Anikoto download links ride onto Zoko servers.
2. `mergeNiNSubtitles` — NiN borrows verified tracks (Niko → animex → Zoko).
3. `verifyMegaVidLang` — Vidy language verification (below).
4. Merge in fixed provider order; `attachKiwiDownloads`; stable-sort by
   playback verdict (`proxy > direct > embed > dead` — ordering hint only,
   never a filter).
5. Every source URL is wrapped into `/api/v1/proxy?...` per request
   (**copy-on-wrap**: provider resolve caches are never mutated).

Hentai titles only reach Zoko (MAL-keyed) + FlixCloud; every other
collector is skipped before any upstream call.

### `POST /api/v1/stream` — explicit / fallback resolve
With `provider` set (family **or** cute name — `Sunny`, `Nami`, `Nest`,
`Lee`, `animegg` sub-ids like `yuki` all route), only that provider
resolves. Empty `provider` walks the fallback chain serially, first hit
wins. `quality` filters provider-returned labels (`auto` = all).

### `/api/v1/proxy` — media proxy
uTLS transport chain, SSRF dialer guard, static + dynamic CDN allowlist,
**no redirects ever** (upstream 3xx → 502, so providers must ship final
URLs), HLS rewrite (child URIs absolutized per RFC 3986, `al=sub/dub`
audio-strip, per-request nonce), VOD playlist body cache (45 s, raw bytes
only — never wrapped URLs).

## Operator rules (enforced, not optional)

- **Copy-on-wrap / clone-on-cache.** Provider resolve caches store *and*
  return deep copies (`cloneSourceResult`). No caller may mutate a shared
  result. (History: in-place wrapping once froze localhost URLs into every
  response.)
- **Strict per-lang.** Sub reads sub listings, dub reads dub listings, no
  cross fallback — except Lee, whose dub-only/sub-only gaps are genuine,
  and explicit operator-approved exceptions documented in code. Caches are
  keyed **with** lang.
- **Dub subtitles: Sora-only.** Only Sora (animex) dub carries the Nico
  (kaa) subtitle files, enforced centrally in
  `withDubSubtitles`. Every other dub keeps its defaults.
- **`al=` audio strip: kaa HLS only.** Attached solely to
  krussdomi/kaa masters (the only dual-audio in the catalog).
- **Cute names are routing aliases.** `GetSourcesForProviderWithSlug`
  normalizes them (case-insensitive) before the switch; keep the alias
  lists complete when adding servers.
- **AnimeGG dub gate.** Dub lists only at ≥720p best quality; low-quality
  dubs never surface. Each tab ships its single best mp4 (mp4 has no
  seamless switching; the player starts on the highest).
- **AnimeGG prequel-shadow rejection.** A numbered sequel query answered
  by an unnumbered slug covering exactly the prequel span is the
  prequel's listing — rejected instead of serving wrong-season content.
- **AnimeGG watch-page guard.** The watch page must name the requested
  episode number; mismatches drop instead of listing wrong content.
- **MegaVid verification** (`verifyMegaVidLang`), first decisive signal
  wins per source: (0) corroboration gate — Vidy lists only alongside
  another provider (fan-out only); (1) m3u `TYPE=AUDIO LANGUAGE`
  declarations; (2) TS PMT ISO-639 segment descriptors (ground truth);
  (3) file identity vs Anikoto (drop only with a same-lang reference).
  Unverifiable lists; every drop logs its layer reason.
- **Mkissa show matching — anti-mismatch** (`resolveShow`): an exact
  `aniListId` match wins outright. The title-scored fallback is accepted
  only when the edge records **no** `aniListId` *and* scores ≥ 80 (exact
  name/English or a full-substring hit — never token overlap). An edge
  that records a *different* `aniListId` is rejected outright: a
  sub-only title searched in dub mode returns unrelated shows (Big X
  dub → "Boonie Bears: The Big Top Secret", score 35, must not win).
  A score-only match logs a Warn naming both sides; no match means no
  mkissa server — never a wrong one. Episode existence is then checked
  against that show's own `availableEpisodesDetail` for the requested
  audio track (strict per-lang: missing dub list = no dub server).
- **Mkissa engine daemon** (`third_party/mkissa-engine`, bun): the
  signed-call crypto (lane key + AES-GCM `tobeparsed`) lives in JS and
  cannot be re-derived in Go, so a long-lived `mkissa_daemon.mjs` keeps
  the lane key and discovery warm across requests. It talks **POST
  only** — GET from datacenter egress answers `NEED_CAPTCHA`, POST
  does not (no captcha, no token, no relay). Go serializes runs with a
  1.5 s gap, retries the rate message, and trips a breaker after two
  consecutive throttle-class failures (20 min cool-down) instead of
  stampeding a shared per-IP bucket.
- **Mkissa is direct-only.** Only sources with a direct `extractedUrl`
  (m3u8/mp4) are listed; pure embeds are skipped. ok.ru and the
  allanime clock are the live extractors; mp4upload/streamsb/streamlare
  return null and cost a fetch. Every kept URL is probe-verified
  master → media → segment before it lists.
- **Hentai gate.** Anikoto/AnimeX/NiN/kaa/AnimeGG/AniWaves/VidNest/Lee/
  MegaVid/mkissa never receive hentai titles (mkissa also forces
  `allowAdult:false` in its own search).
- **No server-list snapshot cache.** Tokenized URLs expire without a
  reliable invalidation signal; every request computes a fresh,
  honestly-probed list. Speed comes from provider resolve caches:
  Anikoto 5 min fresh (+stale), kaa slug 24 h + resolve 10 min
  (lang-keyed), AnimeGG/AniWaves show 24 h + resolve 10 min, mkissa
  show 24 h + resolve 30 min (lang-keyed),
  VidNest/Lee/MegaVid fresh per resolve (short-lived tokens).
- **Upstream AniList endpoint is `https://graphql.aniraku.tech`** (zero
  rate limit) — never `graphql.anilist.co`.

## Probes (honesty before listing)

A source lists only if reachable **from this egress** with player-shaped
requests: `probeSegmentsLenient` (master → media → segment bytes),
`probePlaylistsLenient` (playlist depth, warms the VOD cache),
`probeMediaFileLenient` (direct files + magic bytes),
`probeSegmentsStrict` (where edge alternatives exist). Verdicts are
lenient by default — definitive blocks only. Shared helpers live in
`segment_probe.go`; TS language parsing (`tsAudioLangs`) and m3u audio
parsing (`m3uAudioLangs`) back the verification layers.

## The relay (blocked-egress API calls only)

Some JSON APIs Cloudflare-block datacenter egress
(`new.vidnest.fun`); their minted file URLs play fine from it (verified,
no IP binding). `ANIRAKU_RELAY_URL` (+ optional `ANIRAKU_RELAY_KEY`)
routes **only those API calls** through `third_party/relay/worker.mjs`
(key-gated, path-restricted — never an open proxy). Video bytes, probes
and playback stay direct. Unset = direct with silent skip on 403.
Contract: `POST /vidnest {id, episode, lang}` → upstream JSON verbatim.
See `third_party/relay/README.md` for deploy.

## Adding a provider — checklist

1. New file `internal/streaming/<family>.go`: `Provider` interface
   (`Name/Search/FindEpisodes/FindEpisodeSource`), own `http.Client`
   with `netguard` transport, `SetHostLearner` + `learnURLHost` every
   verified host (feeds the proxy allowlist).
2. Resolve AniList-keyed; normalize `lang` to `sub`/`dub` on entry.
3. Probe every shipped URL from this egress; skip embeds unless the
   provider *is* the embed path (flixcloud); resolve redirects
   provider-side (the proxy refuses 3xx).
4. Cache with TTL + `cloneSourceResult` on store **and** load; key with
   lang; document why the TTL is safe for the token lifetimes.
5. `SourceResult{Headers}` carries playback headers (Referer/Origin);
   `Verification: "proxy"` only when probed; `ServerNames` set.
6. Wire `manager.go` in all places: `providers` list, `SetHostLearner`,
   alias normalization, hentai gate, explicit case, fallback chain,
   fan-out collector (append to the slice — never hand-count the
   WaitGroup), merge order, getter + `try*` + `collect*`.
7. `proxy_allowlist.go`: static suffixes for any new CDN host.
8. Tests: pure parsing/scoring units + `httptest` fixture end-to-end
   (override base URLs + plain client — netguard blocks fixture IPs) +
   failure/gate cases. Extend `live_probe_test.go` (env-guarded,
   never CI-blocking).
9. Update this doc's catalog table + `docs/openapi.yaml` if routes change.

## Verification & ops runbook

- Gates (CI): `gofmt -l .` clean, `go build ./...`, `go vet ./...`,
  `go test ./... -race -count=1`, `govulncheck ./...`, `go mod tidy`
  clean. Full `-race` suite is mandatory before push.
- Live probe (needs prod-egress semantics):
  `go test -c ./internal/streaming/ -o /tmp/streaming.test`,
  copy to the server, run with
  `ANIRAKU_LIVE_PROBE=1 ANIRAKU_LIVE_PROBE_ID=.. ANIRAKU_LIVE_PROBE_EP=.. ANIRAKU_LIVE_PROBE_LANG=..`.
  The probe prints per-provider OK/FAIL — the evidence required before
  claiming a provider works.
- Deploy: `git pull --ff-only && docker compose build aniraku-api &&
  docker compose up -d aniraku-api`, then health `200` + a live
  `/servers` check. Always rebase before push (another actor commits).
- Disk hard caps (incident 2026-10-02: 9.4 GB build cache filled the
  disk → 504s): daily cron `docker builder prune -af --keep-storage
  2GB` + `docker image prune -f`; container logs capped in compose
  (`max-size/max-file`); journald capped at 200M. The app writes zero
  bytes to disk (VOD cache is RAM-only).
- Debugging: `collectorMs` + `fan-out complete` lines show the slow
  collector; a missing `fan-out complete` with resolving providers means
  the fan-out is wedged; per-layer drop reasons log at Warn.
