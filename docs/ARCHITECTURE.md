# Aniraku Backend: Architecture

A single static Go 1.26 binary (Chi v5, Zerolog) serving catalog metadata,
multi-provider playback resolution, a hardened media proxy, and Supabase-backed
account features. No Node/Python sidecars in the request path.

```
Client (web/Android)
   │  Bearer JWT (Supabase)
   ▼
Chi router ── RealIP → CleanPath → RequestID → Recover → Logging → SecurityHeaders → CORS → [rate limits] → [gzip on JSON] → [JWT] → [RequireAdmin]
   │
   ├── catalog.go       AniList GraphQL ⇄ AniZip ⇄ TMDB ⇄ Anisearch
   ├── proxy.go         uTLS media proxy · HLS rewrite · downloads
   ├── anilist_client.go token bucket · circuit breaker · stale cache · dedup
   ├── account.go       progress · favorites · settings · notifications · sync I/O
   └── streaming/       Anikoto → AnimeX → Zoko → FlixCloud → NiN → kaa → AnimeGG → AniWaves → MegaVid → Zenbu → Mkissa (+ AnV, Anixo)
```

## Code layout (`internal/api/v1`)

| File | Responsibility |
|:--|:--|
| `handlers.go` | Package core: `Handlers` struct, construction & background janitors, JSON responders, health/version |
| `catalog.go` | `GetAnime`, `GetEpisodes`, similar/relations, browse/search/trending/seasonal/genres/schedule, AniList browse helpers |
| `proxy.go` | `Stream`, `GetServers`, `Proxy`, `Download`, HLS playlist rewriting, proxy header policy, allowlist glue |
| `anilist_client.go` | AniList resilience layer: token bucket, circuit breaker, request cache + eviction, dedup, `AniListProxy` |
| `account.go` | Progress, watch history, favorites, profile, settings, notifications, client logs, ratings |
| `supabase.go` | PostgREST calls under the user's own JWT (RLS enforced server-side) |
| `ssrf.go` / `proxy_allowlist.go` | Fast-fail host checks; CDN allowlist + learned-host cache |
| `metrics.go` | Admin-only metrics snapshot |

## Request flow

1. **Edge middleware**: `RealIP` derives the client IP from the socket peer or
   the rightmost untrusted `X-Forwarded-For` entry (`ANIRAKU_TRUSTED_PROXY_CIDRS`).
   With no trusted CIDRs, XFF is ignored entirely (spoofing cannot mint fresh
   rate-limit buckets).
2. **Rate limiting**: global bucket (default 30 req/s, burst 60) plus a tighter
   proxy bucket (10 req/s, burst 20). Both tunable via `ANIRAKU_RATE_*` env.
3. **Compression**: gzip on JSON API groups only. The media proxy is never
   compressed: HLS keys/segments must arrive byte-exact.
4. **Auth**: Supabase JWKS verification (RS256/ES256, audience + issuer +
   expiry checks). JWKS refresh is single-flight; on refresh failure the
   previous key set keeps working for a bounded grace window.
5. **Admin gate**: `RequireAdmin` calls Supabase's `is_admin()` RPC with
   `apikey: <anon key>` + `Authorization: Bearer <user JWT>`; RLS and
   `auth.uid()` scoping still apply.

## The proxy trust chain

The media proxy is the most sensitive surface. Defense in depth, in order:

1. **Fast-fail checks**: scheme must be http(s); blocked ports; nuisance-host
   filter; hostname validate; CDN suffix allowlist.
2. **Dialer SSRF boundary** (`internal/netguard`): every outbound socket is
   validated at the `Control` hook *after DNS resolution*: loopback, private,
   CGNAT, link-local, metadata, and multicast addresses are refused. This is
   the authoritative guard (defeats rebinding/redirects/alt-encodings).
3. **No redirects**: clients return `ErrUseLastResponse`; upstream 3xx is
   never forwarded to the browser.
4. **Playlist vouching**: only a playlist fetched from an allowlisted host may
   nominate new media hosts (`LearnHostFromPlaylist`), feeding a dynamic allowlist
   with hourly cleanup, so provider CDN rotation doesn't 403 at the gate.
5. **Header hygiene**: client-supplied `headers` JSON is filtered (`host`,
   `x-*`, hop-by-hop headers dropped); sensible Referer/UA defaults per CDN class.

## Caching layers

| Cache | TTL | Notes |
|:--|:--|:--|
| AniList GraphQL proxy cache | 5 min fresh / 1 h stale-bound | Janitor evicts stale entries; hard cap 5,000 entries (oldest-first). Stale serves as fallback during upstream failures. |
| Browse/trending cache | 5 min | Same janitor. |
| AES key cache | 5 min | Per playback session; `sn`/`iv`/`t` params stripped from the cache key. |
| TMDB episode cache | package-level | Only non-empty results are cached (empty results poison hentai titles). |
| Anikoto resolve | 5 min fresh + stale window | Served without re-contacting megaplay; stale backs failures. |
| kaa slug / resolve | 24 h / 10 min (lang-keyed) | Strict per-lang keys; deep-copied on store and load. |
| AnimeGG / AniWaves show + resolve | 24 h / 10 min (lang-keyed) | Same copy discipline. |
| MegaVid episode | none (fresh per resolve) | Short-lived signed/session tokens. |
| Zenbu index episode | none (fresh per resolve) | The index hands short-lived edge URLs; each resolve re-reads it and probes every slot. |
| Mkissa show slug | 24 h | AniList → mkissa show id mapping. |
| Mkissa resolve | 30 min | Secured GraphQL resolve + AES-GCM decrypted sources, deep-copied. |
| AnimeDL download links | 60 min / 60 s miss | Parsed group→track→quality response; a miss (their flapping upstream) resolves empty for 60 s: no in-request retries. |
| VOD playlist bodies | 45 s | Raw upstream bytes only (never wrapped URLs); static playlists only; 256 entries × 256 KiB. |
| Hentai verdict | 10 min | Per title; caller-passed genres skip the lookup. |
| Server-list snapshot | 90 s complete / 5 s incomplete | Raw provider URLs only; complete means kaa and zenbu are present. `refresh=1` bypasses it. |
| Per-provider last good | 10 min / 2 h kaa+zenbu | Previously verified slots are retained across empty or partial collector results; keyed by title, episode, language, content gate, and provider. |
| Dynamic CDN allowlist | 24 h TTL | Learned hosts (playlist-vouched) expire on read past TTL; capped at 500 entries. |

## Streaming fan-out

`GetServers` fans out to all enabled collectors concurrently (15 s total,
12 s per collector, timings in `collectorMs`), merges in fixed provider order,
applies the Zoko-download / NiN-subtitle / MegaVid-language merges, attaches
AnimeDL downloads (all sources but FlixCloud), and stable-sorts by playback
verdict (`proxy > direct >
embed > dead`, ordering hint only, never a filter). The collector set is
a data table so the WaitGroup count cannot drift. Hentai titles reach only
Zoko (MAL-keyed) + FlixCloud before any other upstream call. Same-key cold
requests share one fan-out. A short raw-result snapshot keeps sequential
requests stable, while per-provider last-good results prevent one timeout or
probe miss from removing a working provider. `Stream`
walks the fallback chain serially with per-request quality filtering;
explicit `provider` accepts families and cute-name aliases; `?refresh=1`
bypasses the server-list stability layer. Full rule list:
[`docs/PROVIDERS.md`](./PROVIDERS.md).

## Observability

- Structured JSON logs (or console in dev) with `request_id` on every request;
  level/format from `logging.level`/`logging.format`.
- `GET /api/v1/metrics` (admin only): request counters, AniList cache/circuit
  state, dynamic CDN count, goroutines/heap.
- Optional pprof at `/debug/pprof` (admin only) behind `ANIRAKU_ENABLE_PPROF=true`.

## Deployment

- Multi-stage Docker (`golang:1.26-alpine` → `debian:bookworm-slim`, non-root), health
  check at `/api/v1/health` (see `render.yaml`).
- Behind Render without `ANIRAKU_TRUSTED_PROXY_CIDRS`, all visitors share one
  rate-limit bucket keyed on the proxy IP: safe but coarse (see README).
- API contract: [`docs/openapi.yaml`](./openapi.yaml).
