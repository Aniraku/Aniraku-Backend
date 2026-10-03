# Contributing to Aniraku Backend

Contributions should improve reliability, security, maintainability, or compatibility while preserving the service’s clear separation between API routing, authentication, configuration, network safety, and streaming providers.

## Development Workflow

Use Go 1.25 (see `go.mod`; the repo pins its toolchain). Run `go mod download`, keep local configuration outside commits, and use focused changes that are easy to review.

## Validation

Mandatory before every push (mirrors CI):

```bash
gofmt -l .
go build ./...
go vet ./...
go test ./... -race -count=1
```

`gofmt` must print nothing; `go mod tidy` must leave `go.mod`/`go.sum`
untouched. For provider or proxy changes, exercise representative
success, fallback, timeout, malformed-response, and unavailable-upstream
cases — preferably as `httptest` fixtures (override base URLs + plain
client; `netguard` blocks fixture IPs) plus the env-guarded live probe:

```bash
go test -c ./internal/streaming/ -o /tmp/streaming.test
ANIRAKU_LIVE_PROBE=1 ANIRAKU_LIVE_PROBE_ID=<anilist> ANIRAKU_LIVE_PROBE_EP=<n> ANIRAKU_LIVE_PROBE_LANG=<sub|dub> /tmp/streaming.test -test.run TestLiveProviderProbe -v
```

Root-cause evidence (prod-egress probe output, log lines) comes before
implementing a fix. Do not weaken the network guard or authentication
middleware to make a test pass.

## Adding a Provider

Follow [`docs/PROVIDERS.md`](docs/PROVIDERS.md) — catalog, operator
rules, and the 9-step wiring checklist. Keep provider-specific behavior
inside `internal/streaming/`, preserve cancellation, timeout, error
normalization, and fallback behavior, and add the allowlist entries for
any new CDN host.

## Pull Requests

Describe the behavior changed, the affected module, validation performed
(including live-probe results for provider work), and any upstream
assumptions. Include API examples when route behavior changes. Never
commit credentials, cookies, service keys, generated binaries, or private
user data. Rebase before push — another actor commits to this repo.
