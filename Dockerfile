# Build stage: pinned Go builder.
FROM golang:1.25-alpine AS gobuild
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -tags web -ldflags="-s -w" -o /aniraku-server ./cmd/aniraku-server

# Runtime stage: minimal, non-root, no unused runtimes. The static Go binary
# embeds the UI; only CA certificates are needed for outbound TLS.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 65532 -g "" appuser
WORKDIR /app
COPY --from=gobuild /aniraku-server /app/aniraku-server
COPY start.sh /start.sh
RUN chmod +x /start.sh && chown appuser /app
USER appuser
EXPOSE 43211
ENTRYPOINT ["/start.sh"]
