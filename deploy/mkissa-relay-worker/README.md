# mkissa relay worker (pull model)

Keeps a near-continuous worker alive on GitHub Actions that executes
mkissa's signed engine calls from the runner's gate-clean US egress. The
backend cannot push to a runner (no inbound traffic), so the worker
**pulls**: poll the bridge queue → run the vendored engine → post the raw
response line back. Mechanics live in
`internal/streaming/mkissa_bridge.go`; the design doc is
`docs/PROVIDERS.md` § *The mkissa relay bridge*.

```
backend (queue, waits ≤15s)  ←POST /poll—  worker.mjs (runner)
        ↑result wakes Call     —POST /result→  mkissa_daemon.mjs (engine)
```

## Files

- `worker.mjs` — the loop (bun). Spawns the engine once, ~1 s polls,
  45 s per-job cap, exits cleanly at 5 h 45 m for the next window.
- `../.github/workflows/mkissa-relay.yml` — `*/5 min` schedule +
  `concurrency: mkissa-relay` (1 running + 1 queued, never cancelled
  mid-window) + `timeout-minutes: 355`.

## Secrets (repo Actions secrets — never in the tree)

| Secret | Value |
|---|---|
| `BRIDGE_URL` | `https://<api-host>/api/v1/internal/mkissa` |
| `BRIDGE_TOKEN` | same as the server's `ANIRAKU_BRIDGE_TOKEN` |

Server side: set `ANIRAKU_BRIDGE_TOKEN` in the API container env. Unset
= endpoints 404 = bridge disabled (worker waits quietly).

```sh
gh secret set BRIDGE_URL   --body 'https://api.example.com/api/v1/internal/mkissa'
gh secret set BRIDGE_TOKEN --body '<openssl rand -hex 32>'
```

## Operating

- Start a window now: `gh workflow run mkissa-relay`
- Follow it: `gh run watch` / Actions → *mkissa-relay* → *Bridge worker*
  (idle heartbeat every ~30 s, job lines when fan-outs need signed calls)
- Chain gaps are normal: GitHub occasionally delays scheduled starts;
  during a hole the backend silently falls back to the local daemon.
- Many *cancelled* runs in the history are expected — scheduled runs
  replacing the queued spare, never the running worker.

## Exit codes

| Code | Meaning | Action |
|---|---|---|
| 0 | window complete (5 h 45 m) | none — chain re-arms |
| 2 | `BRIDGE_URL`/`BRIDGE_TOKEN` unset | fix the secrets |
| 3 | server said 401 — token mismatch | re-set `BRIDGE_TOKEN` to the server's value |
| (404 = disabled) | server has no `ANIRAKU_BRIDGE_TOKEN` | set it server-side; worker retries by itself |

## Local test (no real mkissa traffic)

Point the worker at a mock bridge + stub engine:

```sh
MKISSA_WORKER_ENGINE=/path/to/stub-engine.mjs   # test hook: replaces the real engine
BRIDGE_URL=http://127.0.0.1:PORT/api/v1/internal/mkissa
BRIDGE_TOKEN=anything
MAX_RUNTIME_MS=12000 bun worker.mjs
```

The stub must speak the engine line protocol:
stdin `{"id","showId","audio","episodes":[...]}` → stdout one JSON line
carrying the same `id`.
