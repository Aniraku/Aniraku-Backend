# Aniraku Backend

The Aniraku Backend is the Go service layer for the Aniraku anime application. It exposes the application API, verifies Supabase JWTs, retrieves metadata, coordinates streaming providers, and applies network-safety checks around upstream requests. The frontend is maintained separately in [Aniraku/Aniraku](https://github.com/Aniraku/Aniraku).

## Verified Technology Stack

| Layer | Technology | Repository evidence |
|---|---|---|
| Runtime | Go 1.24 | `go.mod` |
| HTTP service | Chi router and standard Go HTTP packages | `go.mod`, `internal/api/` |
| Logging | Zerolog | `go.mod`, service configuration |
| Authentication | Supabase JWT/JWKS verification | `internal/auth/`, `config.yaml` |
| Application modules | API, auth, config, core, embedding, network guard, streaming | `internal/` |
| Streaming integrations | Miruro and Senshi provider implementations | `internal/streaming/` |
| Auxiliary proxy | Python script with declared Python requirements | `cmd/miruro-proxy/proxy.py`, `requirements.txt` |
| Deployment | Docker and Render configuration | `Dockerfile`, `render.yaml` |

## Local Development

Install Go 1.24 or newer, download dependencies, and run the server with a local configuration file:

```bash
go mod download
go run ./cmd/aniraku-server/ --config config.yaml
```

The default configuration targets `127.0.0.1:43211`; confirm the current values in `config.yaml` before running. The repository also contains a Python-based auxiliary proxy. Install its requirements only when working on that component:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Never commit Supabase keys, service credentials, JWT material, or local configuration containing secrets.

## API Surface

The service includes health, anime details, episode listing, catalog browsing, search, trending, Miruro episode availability, and streaming-source routes. The authoritative route definitions are in `internal/api/router.go`; confirm the implementation before relying on a route in a client or integration.

## Architecture

```text
Aniraku React client
        |
        v
Go API service ── authentication and network guard
        |
        +── AniList metadata
        +── Miruro provider
        +── Senshi HLS provider
```

## Contributing and Responsible Use

Read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a change. Preserve the existing provider boundaries, authentication checks, SSRF protections, and upstream rate-respectful behavior. Use the project only where permitted by applicable laws, service terms, and content rights.

## License

See [LICENSE](LICENSE) for the project’s license terms.
