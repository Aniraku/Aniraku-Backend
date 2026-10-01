# Build stage: pinned Go builder. go.mod's toolchain directive (go1.25.3)
# upgrades the 1.25-alpine base automatically — pinned for CVE fixes
# (GO-2025-39xx/40xx series, all stdlib, fixed in 1.25.2/1.25.3).
FROM golang:1.25-alpine AS gobuild
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Release workflow passes version metadata; defaults keep local builds honest.
ARG VERSION=0.1.0
ARG COMMIT=dev
ARG BUILDDATE=unknown
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w -X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildDate=${BUILDDATE}" \
    -o /aniraku-server ./cmd/aniraku-server

# Runtime stage: debian-slim for glibc. Bun (mkissa engine one-shot per
# cache-miss) and the engine's TLS binding link glibc — alpine/musl
# cannot load them. Minimal, non-root; only CA certs + wget (healthcheck)
# join the static Go binary, bun, and the vendored engine.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/* && useradd -m -u 65532 appuser
# Bun pinned to major 1 (official slim image): copied, not installed, so
# the build needs no extra network fetch beyond base pulls.
COPY --from=oven/bun:1-slim /usr/local/bin/bun /usr/local/bin/bun
WORKDIR /app
COPY --from=gobuild /aniraku-server /app/aniraku-server
COPY --from=gobuild /src/third_party/mkissa-engine /app/third_party/mkissa-engine
COPY start.sh /start.sh
# Engine JS deps (wreq TLS binding) install at build time so the repo
# stays free of vendored node_modules; bun needs HOME-writable cache
# only during this root-owned step.
RUN chmod +x /start.sh && /usr/local/bin/bun install --cwd /app/third_party/mkissa-engine --production --silent && chown -R appuser /app && /usr/local/bin/bun --version
USER appuser
EXPOSE 43211
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD wget -qO- http://127.0.0.1:43211/api/v1/health >/dev/null || exit 1
ENTRYPOINT ["/start.sh"]
