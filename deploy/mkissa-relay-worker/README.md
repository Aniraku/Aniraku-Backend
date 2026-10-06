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
  45 s per-job cap, exits cleanly at 5 h 45 m — but not before **arming
  its own successor**: at T−2 min it POSTs `workflow_dispatch` with the
  job's `GITHUB_TOKEN`, and the queued window boots the moment this one
  ends. GitHub's scheduler is *not* part of the chain: this repo's `*/5`
  cron measurably delivers only every few hours (keep-awake history),
  so it stays as a harmless backup only.
- `../.github/workflows/mkissa-relay.yml` — `concurrency: mkissa-relay`
  (1 running + 1 queued, never cancelled mid-window) +
  `timeout-minutes: 355`, plus an optional `window_ms` dispatch input
  for chain tests.

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
- Prove the chain end to end (3-minute window whose self-arm must
  produce the successor): `gh workflow run mkissa-relay -f window_ms=180000`
- Follow it: `gh run watch` / Actions → *mkissa-relay* → *Bridge worker*
  (idle heartbeat every ~30 s, `armed the next window` near the end of
  every window, job lines when fan-outs need signed calls)
- Chain gaps are normal: the backend silently falls back to the local
  daemon whenever no worker answers in time.
- Many *cancelled* runs in the history are expected — newer dispatches
  replacing the queued spare, never the running worker.

## Exit codes

| Code | Meaning | Action |
|---|---|---|
| 0 | window complete, successor armed | none — chain continues |
| 2 | `BRIDGE_URL`/`BRIDGE_TOKEN` unset | fix the secrets |
| 3 | server said 401 — token mismatch | re-set `BRIDGE_TOKEN` to the server's value |
| 4 | window ended without arming a successor | chain broke: `gh workflow run mkissa-relay` and check the arm logs |
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
