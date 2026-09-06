<div align="center">

# Aniraku Backend

Go service layer for the Aniraku web and Android clients.

<a href="https://github.com/Aniraku/Aniraku">Client</a>
&nbsp; · &nbsp;
<a href="https://github.com/Aniraku/Aniraku-App">Android client</a>
&nbsp; · &nbsp;
<a href="CONTRIBUTING.md">Contribute</a>
&nbsp; · &nbsp;
<a href="LICENSE">License</a>

</div>

---

## Support Aniraku

Aniraku is open source. Voluntary support helps fund **hosting, releases, and open-source development** and never changes access to API or app features.

## Sponsor☕💘

<a href="https://patreon.com/ShoIslam"><img src="https://user-images.githubusercontent.com/61944859/180249027-678b01b8-c336-451e-b147-6d84a5b9d0e7.png" width="250"/></a>
| Optional crypto support | Value |
|:--|:--|
| Asset | USDT |
| Network | **BNB Smart Chain (BEP20) only** |
| Address | `0x0dc085fc880f2f67b4e200f125bc0de352da904e` |

> **Send USDT on BNB Smart Chain (BEP20) only.** Do not use Ethereum, Polygon, Arbitrum, or another network. Verify both the asset and network before sending because crypto transfers cannot be reversed.

<img src="./docs/assets/usdt-bep20-support-qr.png" width="180" alt="USDT on BNB Smart Chain BEP20 support QR code" />

Read the full [Support Guide](./SUPPORT.md).

## What this service does

Aniraku-Backend keeps the client-facing API separate from provider-specific work. It handles API routing, authentication, episode metadata, playback coordination, account data, sync, and the network checks needed around upstream requests.

The service is written entirely in **Go** — one static binary, no Node.js, no Python, no sidecars. Streaming resolution is fully in-process:

- **Anikoto (primary)** — AniList ID → show resolve → episode data-ids → server list → embed decrypt → verified `m3u8` + subtitles + intro/outro.
- **FlixCloud (fallback)** — embed URLs for the client's embedded player.

Episode titles/thumbnails are resolved via **AniZip + TMDB** (AniBridge verified mappings + Fribb fallback, bidirectional).

## Request flow

```text
Aniraku web / Android client
              │
              ▼
        Chi HTTP router
              │
    ┌─────────┼─────────┐
    ▼         ▼         ▼
  auth    episodes   streaming
 Supabase AniZip↔TMDB Anikoto (direct)
 JWT/JWKS  unlimited   + FlixCloud (embed)
   │        │          │
   ▼        ▼          ▼
     normalized API response
```

## API areas

The current router includes playback, episodes, account, sync, and admin.

| Area | Endpoints | Notes |
|:--|:--|:--|
| **Episodes** | `GET /api/v1/anime/{id}/episodes` | AniZip + TMDB bidirectional fallback (AniBridge + Fribb, `coverFallback` if missing) |
| **Playback** | `POST /api/v1/stream`, `GET /api/v1/servers`, `GET /api/v1/proxy`, `GET /api/v1/download`, `GET /ani/v1/epsrc` | Anikoto decrypt → `hls` `master.m3u8` + `subtitles[]`/`intro/outro`; `proxy` is `uTLS` + `netguard` SSRF guard + CDN allowlist + HLS rewrite; `download` streams provider file links through the same allowlisted gate |
| **Catalog** | `search`, `trending`, `seasonal`, `browse`, `genres`, `schedule` | served through the backend's rate-limited AniList client (token bucket + circuit breaker) |
| **Account** | `profiles`, `favorites`, `settings`, `notifications`, `logs`, `progress`, `ratings`, `continue-watching` | Supabase RLS via `supabaseRequest` (`apikey=anon` + `Bearer user JWT`) |
| **Import/export** | `POST /api/v1/import/mal`/`anilist`, `export`, `sync` (`mal`/`anilist` OAuth), `SyncScore`/`SyncUpdate` | `resolveMalIDsToAniList` via `idMal_in` |
| **Administration** | `GET /api/v1/admin/stats` | `auth.RequireAdmin` `is_admin()` RPC |

All versioned routes live under `/api/v1`. The legacy `/ani/v1/epsrc` route is kept for compatibility.

## Stack

`Go 1.25` · `Chi v5` · `Zerolog` · `Supabase JWT/JWKS` · `Docker` · `Render`

| Responsibility | Location |
|:--|:--|
| Server entrypoint | `cmd/aniraku-server/` |
| HTTP routing | `internal/api/` |
| API handlers | `internal/api/v1/` |
| Authentication | `internal/auth/` |
| Configuration | `internal/config/` (`TMDB`, `Scraping` bases) |
| Core models and errors | `internal/core/` |
| Embedded UI support | `internal/embed/` |
| Network safety | `internal/netguard/` (SSRF `Control` + `NoRedirects` + guarded `http.Client` factory) |
| Streaming providers | `internal/streaming/` (`anikoto.go`, `flixcloud.go`, `manager.go`) |
| TMDB resolver | `internal/tmdb/` (`resolver.go` AniBridge+Fribb, `merge.go`) |

## Configuration

The default configuration is in [`config.yaml`](config.yaml). Secrets are read from environment variables (see `.env.example`) rather than being committed. **Never commit real keys** — and note that deleting a file from git does not remove it from history; secrets must be rotated after any leak.

**Key areas:**
- `server.host/port/debug`, `ui_dist`, `anikoto_mapping_path`;
- `supabase.url/anon_key/service_key/jwt_aud` — `jwks_url` is derived automatically (`{url}/auth/v1/.well-known/jwks.json`), override with `ANIRAKU_SUPABASE_JWKS_URL` only if needed;
- `tmdb.read_access_token/api_base/image_base/anibridge_api` — `TMDB_READ_ACCESS_TOKEN` (v4) for episode fallback;
- `scraping.animex_base/flixcloud_base/anizip_base` — override via `ANIRAKU_*` env vars;
- `logging.level/format`, `update.channel/url`.

The default local address is `127.0.0.1:43211` with bounded `Read/Write/Idle` timeouts and `SIGINT`/`SIGTERM` shutdown.

See `.env.example` for a template (placeholders, no real keys).

## Embedded API interface

Production container builds embed the Aniraku API interface at the service root. API routes under `/api/` remain served by the Go router.

## Run locally

```bash
go mod download

# configure
cp .env.example .env
# edit .env: set ANIRAKU_SUPABASE_* and TMDB_READ_ACCESS_TOKEN

go run ./cmd/aniraku-server/ --config config.yaml
```

To build the container:

```bash
docker build -t aniraku-backend .
docker run --rm -p 43211:43211 --env-file .env aniraku-backend
```

`render.yaml` and `Dockerfile` (multi-stage `golang:1.25-alpine` → `alpine:3.20`, non-root) describe deployment.

## Security and upstream use

Do not commit Supabase keys, service credentials, JWT material, or other local secrets. Keep the authentication middleware, admin checks, SSRF protections (`internal/netguard/ssrf.go:16`), request timeouts, cancellation, provider fallback, and error handling intact.

Upstream services have their own terms, limits, and content policies. Use the service only for media and requests you are authorized to access, and avoid unnecessary request volume.

For security concerns, follow [PRIVACY_POLICY.md](PRIVACY_POLICY.md) and the repository's contribution process rather than posting sensitive details publicly.

<div align="center"><sub>Go · Chi · Supabase · AniZip↔TMDB · Anikoto (direct) + FlixCloud · Docker</sub></div>
