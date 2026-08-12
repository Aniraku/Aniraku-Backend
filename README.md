<p align="center"><img src="./assets/aura-banner.svg" alt="The engine behind the calm" width="100%" /></p>
<p align="center"><a href="https://github.com/Aniraku/Aniraku">Client</a>&nbsp;&nbsp;·&nbsp;&nbsp;<a href="CONTRIBUTING.md">Contribute</a>&nbsp;&nbsp;·&nbsp;&nbsp;<a href="LICENSE">License</a></p>

> The service layer behind Aniraku’s calm surface.

## The job

Aniraku-Backend is the Go service that gives the client a dependable boundary for API access, identity, metadata, network safety, and streaming-provider coordination. It keeps provider-specific behavior away from the interface so the product can stay focused.

## The flow

```text
Client request
      │
      ▼
Chi router + Go service
      │
      ├── Supabase JWT / JWKS verification
      ├── AniList metadata
      ├── Miruro provider
      ├── Senshi HLS provider
      └── network guard + normalized errors
```

## Stack signal

`Go 1.24` · `Chi` · `Zerolog` · `Supabase JWT/JWKS` · `Docker` · `Render` · `Python proxy`

| Boundary | Where to look |
|:--|:--|
| API routing | `internal/api/` |
| Authentication | `internal/auth/` |
| Configuration | `internal/config/` |
| Core models | `internal/core/` |
| Network safety | `internal/netguard/` |
| Streaming providers | `internal/streaming/` |
| Service entrypoint | `cmd/aniraku-server/` |

## Run it locally

```bash
go mod download
go run ./cmd/aniraku-server/ --config config.yaml
```

The default local address is `127.0.0.1:43211`; confirm the current value in `config.yaml`. The auxiliary Python proxy has its own isolated dependency path:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Guardrails

Do not commit Supabase keys, service credentials, JWT material, cookies, or local secrets. Preserve authentication checks, SSRF protections, timeouts, cancellation, fallback behavior, and responsible upstream usage.

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

<p align="center"><sub>Explicit boundaries make dependable systems.</sub></p>
