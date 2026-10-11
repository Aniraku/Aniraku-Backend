# Build stage: pinned Go builder matching go.mod's toolchain (go1.26.x).
# The toolchain directive can auto-upgrade a base image, but the official
# alpine images ship GOTOOLCHAIN=local, which refuses — so the base must
# satisfy go.mod on its own. Pinned for CVE fixes (GO-2026-66xx series).
FROM golang:1.26-alpine AS gobuild
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

# Runtime stage: debian-slim for glibc: minimal, non-root; CA certs +
# wget (healthcheck) join the static Go binary.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* && useradd -m -u 65532 appuser
WORKDIR /app

COPY --from=gobuild /aniraku-server /app/aniraku-server
COPY start.sh /start.sh
RUN chmod +x /start.sh && chown -R appuser /app
USER appuser
EXPOSE 43211
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD wget -qO- http://127.0.0.1:43211/api/v1/health >/dev/null || exit 1
ENTRYPOINT ["/start.sh"]
