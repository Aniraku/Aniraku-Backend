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

Aniraku-Backend keeps the client-facing API separate from provider-specific work. It handles API routing, authentication, metadata, playback coordination, account data, sync, and the network checks needed around upstream requests.

The service is written in Go. A small Python proxy is used for the Miruro path when the local configuration does not point to an external proxy.

## Request flow

```text
Aniraku web / Android client
              │
              ▼
        Chi HTTP router
              │
    ┌─────────┼─────────┐
    ▼         ▼         ▼
  auth     metadata   streaming
 Supabase  AniList   Miruro / Senshi
 JWT/JWKS  catalog   provider fallback
              │
              ▼
     normalized API response
```

## API areas

The current router includes public catalog and playback routes, authenticated account routes, provider synchronization, and admin-only statistics.

| Area | Examples |
|:--|:--|
| Catalog | Anime, manga, episodes, chapters, search, trending, seasonal, browse, genres, and schedule. |
| Playback | Stream requests, available servers, Miruro episode lookup, dub checks, and provider probes. |
| Account | Profiles, favorites, settings, notifications, logs, progress, ratings, and Continue Watching. |
| Import/export | AniList and MyAnimeList authorization, callbacks, import jobs, export, disconnect, and score sync. |
| Administration | Protected statistics endpoint for users with the admin role. |

All versioned routes live under `/api/v1`. The legacy `/ani/v1/epsrc` route is kept separately for compatibility.

## Stack

`Go 1.24` · `Chi` · `Zerolog` · `Supabase JWT/JWKS` · `Docker` · `Render` · `Python proxy`

| Responsibility | Location |
|:--|:--|
| Server entrypoint | `cmd/aniraku-server/` |
| HTTP routing | `internal/api/` |
| API handlers | `internal/api/v1/` |
| Authentication | `internal/auth/` |
| Configuration | `internal/config/` |
| Core models and errors | `internal/core/` |
| Embedded UI support | `internal/embed/` |
| Network safety | `internal/netguard/` |
| Streaming providers | `internal/streaming/` |
| Miruro helper proxy | `cmd/miruro-proxy/` |

## Configuration

The default configuration is in [`config.yaml`](config.yaml). Secrets are read from environment variables rather than being committed to the repository.

The main configuration areas are:

- server host, port, debug mode, and optional embedded UI;
- Supabase URL, anonymous key, service key, JWT audience, and JWKS URL;
- primary and fallback streaming providers;
- structured log level and format; and
- update channel and update URL.

The default local address is `127.0.0.1:43211`. The server also uses bounded read, write, and idle timeouts and shuts down on `SIGINT` or `SIGTERM`.

## Embedded API interface

Production container builds embed the Aniraku API interface at the service root. It provides a read-only overview of public catalog routes, a health signal, a trending sample, and upcoming airing records through the existing AniList proxy. API routes under `/api/` remain served by the Go router; the interface never exposes private account, admin, proxy, or streaming actions.

## Run locally

```bash
go mod download
go run ./cmd/aniraku-server/ --config config.yaml
```

The auxiliary proxy has its own Python dependency path:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

To build the container:

```bash
docker build -t aniraku-backend .
docker run --rm -p 43211:43211 aniraku-backend
```

The repository also includes [`render.yaml`](render.yaml) and a [`Dockerfile`](Dockerfile) for deployment configuration.

## Security and upstream use

Do not commit Supabase keys, service credentials, JWT material, cookies, or other local secrets. Keep the authentication middleware, admin checks, SSRF protections, request timeouts, cancellation, provider fallback, and error handling intact when changing the service.

Upstream services have their own terms, limits, and content policies. Use the service only for media and requests you are authorized to access, and avoid unnecessary request volume.

For security concerns, follow [PRIVACY_POLICY.md](PRIVACY_POLICY.md) and the repository's contribution process rather than posting sensitive details publicly.

<div align="center"><sub>Go · Chi · Supabase · streaming providers · Docker</sub></div>
