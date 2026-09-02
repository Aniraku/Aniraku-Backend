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

The service is written in **Go**. Episode titles/thumbnails are resolved via **AniZip + TMDB** (AniBridge verified mappings + Fribb fallback, bidirectional). Streaming is **Anikoto + FlixCloud** via a small **Node.js** `decrypt.mjs` (WASM + PBKDF2 + AES-256-CBC) that extracts direct `m3u8` + subtitles from `flixcloud.cc` embeds. No Python is required.

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
 Supabase AniZip↔TMDB FlixCloud (Node decrypt)
 JWT/JWKS  unlimited   + Anikoto
   │        │          │
   ▼        ▼          ▼
     normalized API response
```

Frontend calls **AniList GraphQL directly** for `GetAnime`/`Similar`/`Relations`/`Browse`/`Search`/`Trending`/`Schedule`/`Genres`; backend only serves `GET /api/v1/anime/{id}/episodes` (AniZip + TMDB) to bypass blocked episode metadata.

## API areas

The current router includes playback, episodes, account, sync, and admin.

| Area | Endpoints | Notes |
|:--|:--|:--|
| **Episodes** | `GET /api/v1/anime/{id}/episodes` | AniZip unlimited + TMDB bidirectional fallback (AniBridge + Fribb, verified `proxy` ranked first, `coverFallback` if missing) |
| **Playback** | `POST /api/v1/stream`, `GET /api/v1/servers`, `GET /api/v1/proxy`, `GET /ani/v1/epsrc` | `FlixCloud` decrypt → `hls` `master.m3u8` + `subtitles[]`/`intro/outro`, `Anikoto` embed; `proxy` is `uTLS` + `netguard` SSRF + CDN allowlist + HLS rewrite |
| **Catalog (frontend-direct)** | `search`, `trending`, `seasonal`, `browse`, `genres`, `schedule`, `manga` | **Removed from backend** — frontend uses AniList directly (`internal/metadata/mal` and `kitsu` unused, kept only for `ImportMAL` ID mapping) |
| **Account** | `profiles`, `favorites`, `settings`, `notifications`, `logs`, `progress`, `ratings`, `continue-watching` | Supabase RLS via `supabaseRequest` (`apikey=anon` + `Bearer user JWT`) |
| **Import/export** | `POST /api/v1/import/mal`/`anilist`, `export`, `sync` (`mal`/`anilist` OAuth), `SyncScore`/`SyncUpdate` | `resolveMalIDsToAniList` via `idMal_in` |
| **Administration** | `GET /api/v1/admin/stats` | `auth.RequireAdmin` `is_admin()` RPC |

All versioned routes live under `/api/v1`. The legacy `/ani/v1/epsrc` route is kept for compatibility.

## Stack

`Go 1.25` · `Chi v5` · `Zerolog` · `Node 22` (FlixCloud decrypt) · `Supabase JWT/JWKS` · `Docker` · `Render`

| Responsibility | Location |
|:--|:--|
| Server entrypoint | `cmd/aniraku-server/` |
| HTTP routing | `internal/api/` |
| API handlers | `internal/api/v1/` |
| Authentication | `internal/auth/` |
| Configuration | `internal/config/` (`TMDB`, `Scraping` bases) |
| Core models and errors | `internal/core/` |
| Embedded UI support | `internal/embed/` |
| Network safety | `internal/netguard/` (`SSRF` `Control` + `NoRedirects`) |
| Streaming providers | `internal/streaming/` (`flixcloud.go` + `decrypt.mjs`, `anikoto.go`, `manager.go`) |
| TMDB resolver | `internal/tmdb/` (`resolver.go` AniBridge+Fribb, `merge.go`) |

No Python runtime is required. The former `cmd/miruro-proxy/proxy.py` and `vipertls` are disabled (Miruro/Mimi removed).

## Configuration

The default configuration is in [`config.yaml`](config.yaml). Secrets are read from environment variables (see `.env.example`) rather than being committed.

**Key areas:**
- `server.host/port/debug`, `ui_dist`, `miruro_proxy_url` (deprecated), `anikoto_mapping_path`;
- `supabase.url/anon_key/service_key/jwt_aud/jwks_url`;
- `tmdb.read_access_token/api_base/image_base/anibridge_api` — `TMDB_READ_ACCESS_TOKEN` (v4) for episode fallback;
- `scraping.animex_base/flixcloud_base/anizip_base` — override via `ANIRAKU_ANIMEX_BASE` etc or `ANIMEX_BASE`/`FLIXCLOUD_BASE`;
- `providers.primary` (now `flixcloud`/`anikoto`);
- `logging.level/format`, `update.channel/url`.

`TMDB`, `AniZip`, `AnimeX`, `FlixCloud` bases are configurable via `ANIRAKU_*` env vars. The default local address is `127.0.0.1:43211` with bounded `Read/Write/Idle` timeouts and `SIGINT`/`SIGTERM` shutdown.

See `.env.example` for a DMCA-safe template (placeholders, no real keys).

## Embedded API interface

Production container builds embed the Aniraku API interface at the service root. API routes under `/api/` remain served by the Go router.

## Run locally

```bash
# Go + Node required (Node 20+ for FlixCloud decrypt)
go mod download
node --version  # 20+

# configure
cp .env.example .env
# edit .env: set ANIRAKU_SUPABASE_* and TMDB_READ_ACCESS_TOKEN

go run ./cmd/aniraku-server/ --config config.yaml
```

FlixCloud decrypt needs `node` in `PATH` (`internal/streaming/decrypt.mjs` is invoked as `node decrypt.mjs -` with embed HTML on stdin, as in `walterwhite-69/ReAnime.to-API`). Tokens are one-time-use (`410` on reuse), `m3u8` JWTs are short-lived (`~6h`).

To build the container:

```bash
docker build -t aniraku-backend .
docker run --rm -p 43211:43211 --env-file .env aniraku-backend
```

`render.yaml` and `Dockerfile` (multi-stage `golang:alpine` → `node:22-alpine`) describe deployment.

## Security and upstream use

Do not commit Supabase keys, service credentials, JWT material, or other local secrets. Keep the authentication middleware, admin checks, SSRF protections (`internal/netguard/ssrf.go:16`), request timeouts, cancellation, provider fallback, and error handling intact.

Upstream services have their own terms, limits, and content policies. Use the service only for media and requests you are authorized to access, and avoid unnecessary request volume.

For security concerns, follow [PRIVACY_POLICY.md](PRIVACY_POLICY.md) and the repository's contribution process rather than posting sensitive details publicly.

<div align="center"><sub>Go · Chi · Supabase · AniZip↔TMDB · FlixCloud (Node) + Anikoto · Docker</sub></div>
