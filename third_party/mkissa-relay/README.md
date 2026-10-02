# mkissa API relay (only needed while the server egress IP is throttled)

Why: `api.mkissa.net` answers searches from the server but rates signed
episode calls to ~zero for datacenter IPs. Routing just those API calls
(kilobytes; video stays direct) through a Cloudflare Worker restores a
clean egress reputation. Free tier: 100k req/day; backend uses hundreds.

## Deploy (2 minutes, no card)

1. https://dash.cloudflare.com → Workers & Pages → Create → copy the
   contents of `worker.mjs` → Deploy.
2. Note the worker URL, e.g. `https://mkissa-relay.xyz.workers.dev`.
3. On the server, add to `~/compose.yaml` under `aniraku-api.environment`:
   `MKISSA_API: "https://mkissa-relay.xyz.workers.dev"`
4. `docker compose up -d aniraku-api` (container env passes through to
   both the Go provider and the bun daemon automatically).
5. Verify: mkissa servers appear for anime 21 ep 1; backend logs stop
   showing `Too many requests` for mkissa.

To revert: remove the env line and redeploy (defaults go direct).
