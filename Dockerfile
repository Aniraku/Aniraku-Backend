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

# Runtime stage: debian-slim for glibc. Bun runs the vendored mkissa
# engine as a long-lived daemon (signed-call crypto lives in JS; the
# wreq TLS binding links glibc, so alpine/musl cannot load it). python3
# exists only to exec the animepahe solver on demand (spawn -> clearance
# -> exit; zero resident cost). Minimal, non-root; CA certs + wget
# (healthcheck) join the static Go binary, bun and the vendored engines.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates wget python3 python3-venv \
        libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 libcairo2 \
        libdbus-1-3 libdrm2 libgbm1 libglib2.0-0 libgtk-3-0 libnspr4 \
        libnss3 libpango-1.0-0 libx11-6 libxcb1 libxcomposite1 libxdamage1 \
        libxext6 libxfixes3 libxkbcommon0 libxrandr2 \
    && rm -rf /var/lib/apt/lists/* && useradd -m -u 65532 appuser
# Bun pinned to major 1 (official slim image): copied, not installed, so
# the build needs no extra network fetch beyond base pulls.
COPY --from=oven/bun:1-slim /usr/local/bin/bun /usr/local/bin/bun
WORKDIR /app

# --- animepahe solver (Cloudflare clearance, spawn-on-demand) -------------
# Layer order is deliberate: everything below is keyed ONLY on the static
# solver files, so code pushes never re-download camoufox (browser ≈1.3 GB
# with fingerprint fonts trimmed). Go auto-detects this exact layout;
# ANIRAKU_PAHE_SOLVER / ANIRAKU_PAHE_PYTHON override it (0 = disabled).
COPY third_party/pahe-solver/requirements.txt /app/pahe-solver/requirements.txt
RUN python3 -m venv /app/pahe-solver/venv \
    && /app/pahe-solver/venv/bin/pip install --no-cache-dir -q -r /app/pahe-solver/requirements.txt
COPY third_party/pahe-solver/solve_once.py third_party/pahe-solver/trim_fonts.py /app/pahe-solver/
# Bake the browser as appuser (no runtime downloads), then trim the
# mac/win font groups camoufox's own groups.json marks unreadable on
# Linux: M (810 MB) + W (354 MB) + MW (20 MB). Solve re-verified after
# the trim; groups never regrow at runtime.
RUN HOME=/home/appuser /app/pahe-solver/venv/bin/python -m camoufox fetch \
    && HOME=/home/appuser python3 /app/pahe-solver/trim_fonts.py \
    && chown -R appuser:appuser /home/appuser/.cache/camoufox
# -------------------------------------------------------------------------

COPY --from=gobuild /aniraku-server /app/aniraku-server
COPY --from=gobuild /src/third_party/mkissa-engine /app/third_party/mkissa-engine
COPY --from=gobuild /src/third_party/animepahe-engine /app/third_party/animepahe-engine
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
