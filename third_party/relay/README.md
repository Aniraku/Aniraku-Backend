# Upstream API relay (only for Cloudflare-blocked API calls)

Why: `new.vidnest.fun` (VidNest API) serves Cloudflare 403 to the backend's datacenter egress while working from
clean egress — and the minted file URLs play from the backend with no IP
binding (verified live). So ONLY those JSON API calls route here; every
video byte, every probe, and all playback stay direct from the backend.

## Deploy (2 minutes, free)

1. https://dash.cloudflare.com → Workers & Pages → Create → paste the
   contents of `worker.mjs` → Deploy.
2. Worker Settings → Variables → add `RELAY_KEY` = a long random string.
3. On the server, add under `aniraku-api.environment` in `~/compose.yaml`:
   `ANIRAKU_RELAY_URL: "https://<your-worker>.workers.dev"`
   `ANIRAKU_RELAY_KEY: "<same value>"`
4. `docker compose up -d aniraku-api`.
5. Verify: the Nest server appears for anime 21 ep 1; backend logs
   show `vidnest resolved`.

To revert: remove the two env lines and redeploy (defaults go direct;
blocked endpoints skip silently as before).

## Contract

- `POST /vidnest {id, episode, lang}` → upstream API JSON verbatim.
Free tier allows 100k requests/day; backend volume is in the hundreds.
If Cloudflare egress itself gets blocked by these WAFs, the same env
pair points at any cheap VPS running an equivalent forwarder instead.
