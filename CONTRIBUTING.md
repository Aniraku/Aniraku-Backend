# Contributing to Aniraku Backend

Contributions should improve reliability, security, maintainability, or compatibility while preserving the service’s clear separation between API routing, authentication, configuration, network safety, and streaming providers.

## Development Workflow

Use Go 1.24 or newer. Run `go mod download`, keep local configuration outside commits, and use focused changes that are easy to review. When working on the auxiliary Python proxy, use an isolated virtual environment and the repository’s `requirements.txt`.

## Validation

Run the relevant Go tests and build checks before opening a pull request:

```bash
go test ./...
go build ./cmd/aniraku-server/
```

For provider or proxy changes, exercise representative success, fallback, timeout, malformed-response, and unavailable-upstream cases. Do not weaken the network guard or authentication middleware to make a test pass.

## Pull Requests

Describe the behavior changed, the affected module, validation performed, and any upstream assumptions. Include API examples when route behavior changes. Never commit credentials, cookies, service keys, generated binaries, or private user data.

## Adding a Provider

Keep provider-specific behavior inside `internal/streaming/`, follow the existing provider manager abstractions, and preserve cancellation, timeout, error normalization, and fallback behavior. Add tests for parsing and failure cases before requesting review.

## Reporting Security Issues

Do not disclose sensitive vulnerabilities in a public issue. Follow the repository’s security guidance and provide the smallest reproducible description needed for maintainers to investigate safely.
