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
RUN CGO_ENABLED=0 go build -tags web \
    -ldflags="-s -w -X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildDate=${BUILDDATE}" \
    -o /aniraku-server ./cmd/aniraku-server

# Runtime stage: minimal, non-root, no unused runtimes. The static Go binary
# embeds the UI; only CA certificates are needed for outbound TLS.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget && adduser -D -u 65532 -g "" appuser
WORKDIR /app
COPY --from=gobuild /aniraku-server /app/aniraku-server
COPY start.sh /start.sh
RUN chmod +x /start.sh && chown appuser /app
USER appuser
EXPOSE 43211
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD wget -qO- http://127.0.0.1:43211/api/v1/health >/dev/null || exit 1
ENTRYPOINT ["/start.sh"]
