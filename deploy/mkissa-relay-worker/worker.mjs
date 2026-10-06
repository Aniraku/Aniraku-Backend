#!/usr/bin/env bun
// mkissa relay worker — persistent pull-worker for the Aniraku backend.
//
// The backend cannot push signed mkissa calls out through this runner
// (runners accept no inbound traffic), so this worker PULLS: it polls the
// backend's bridge queue about once a second, executes each job with the
// vendored engine (mkissa_daemon.mjs, spawned once and kept alive) using
// THIS runner's gate-clean US egress, and posts the raw engine response
// line back so the waiting API call can continue.
//
// Secrets (repo Actions secrets, never in the repo):
//   BRIDGE_URL    e.g. https://api.aniraku.tech/api/v1/internal/mkissa
//   BRIDGE_TOKEN  must equal the server's ANIRAKU_BRIDGE_TOKEN
//
// Lifecycle / exit codes (the mkissa-relay workflow re-arms every 5 min
// with a 1-running + 1-queued concurrency chain, so the next run picks up
// seconds after this one ends):
//   0  window complete (default 5h45, under GitHub's 6h job cap)
//   2  missing configuration (BRIDGE_URL/BRIDGE_TOKEN unset)
//   3  server rejected the token (401) — a config mismatch, fail loudly
//      instead of polling forever; 404 is different: the bridge is simply
//      disabled server-side, so we wait and retry quietly.

import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const BASE = (process.env.BRIDGE_URL || "").trim().replace(/\/+$/, "");
const TOKEN = (process.env.BRIDGE_TOKEN || "").trim();
const POLL_MS = 1000;
const HEARTBEAT_EVERY = 30; // ~30s of idle polls between log lines
const RESULT_TIMEOUT_MS = 45_000; // engine self-answers within 35s
const POLL_LIMIT = 4;
// Worker window: default 5h45 (under GitHub's 6h job cap), overridable
// for chain tests. Guarded against NaN so a bad input can't zero it.
const rawMax = Number(process.env.MAX_RUNTIME_MS);
const MAX_RUNTIME_MS = Number.isFinite(rawMax) && rawMax > 0 ? rawMax : 5.75 * 60 * 60 * 1000;
// Self-re-arm: the repo's GitHub-schedule delivery is unreliable (an
// */5 cron measurably fires only every few hours here), so the worker
// dispatches its OWN successor ~2min before its window ends using the
// job's GITHUB_TOKEN (workflow_dispatch is exempt from GitHub's
// GITHUB_TOKEN anti-recursion rule). The successor inherits no inputs
// => default full-length window => the chain sustains itself with no
// dependence on GitHub's scheduler at all.
const ARM_LEAD_MS = 2 * 60 * 1000;
const ARM_RETRY_MS = 30_000;
const WORKFLOW_FILE = "mkissa-relay.yml";

if (!BASE || !TOKEN) {
  console.error("[worker] BRIDGE_URL and BRIDGE_TOKEN are required");
  process.exit(2);
}

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
// MKISSA_WORKER_ENGINE is a test hook: it points the worker at a stub
// engine so the full loop can be exercised without a real signed call.
const ENGINE = process.env.MKISSA_WORKER_ENGINE
  ? path.resolve(process.env.MKISSA_WORKER_ENGINE)
  : path.join(repoRoot, "third_party/mkissa-engine/mkissa_daemon.mjs");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- engine

let daemon = null;
const waiters = new Map(); // engine job id -> settle(line, reason)

function startDaemon() {
  const p = spawn("bun", [ENGINE], { cwd: path.dirname(ENGINE), stdio: ["pipe", "pipe", "inherit"] });
  let buf = "";
  p.stdout.setEncoding("utf8");
  p.stdout.on("data", (chunk) => {
    buf += chunk;
    let i;
    while ((i = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, i).trim();
      buf = buf.slice(i + 1);
      if (!line) continue;
      let id;
      try {
        id = JSON.parse(line).id;
      } catch {
        console.error("[worker] unparsable engine line:", line.slice(0, 200));
        continue;
      }
      const w = waiters.get(id);
      if (w) w(line, null);
      else console.error("[worker] engine line matched no waiter, id =", id);
    }
  });
  const died = (code, sig) => {
    console.error(`[worker] engine exited (code=${code} sig=${sig})`);
    for (const w of waiters.values()) w(null, "engine exited");
    waiters.clear();
    if (daemon === p) daemon = null;
  };
  p.on("error", (err) => {
    console.error("[worker] engine spawn error:", err.message);
    died("spawn", null);
  });
  p.on("exit", died);
  return p;
}

function ensureDaemon() {
  if (!daemon) daemon = startDaemon();
  return daemon;
}

// runJob pipes one job through the engine and resolves with the raw
// response line (or the failure reason). The job id rides the engine
// protocol unchanged, so lines match waiters exactly like Go's readLoop.
function runJob(job) {
  const p = ensureDaemon();
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      // The engine bounds itself at 35s; silence past 45s means it hung.
      // Kill it so the next job starts from a fresh discovery.
      console.error(`[worker] engine timeout on job ${job.id}, respawning engine`);
      try {
        p.kill("SIGKILL");
      } catch {}
      finish(null, "engine timeout");
    }, RESULT_TIMEOUT_MS);
    const finish = (line, reason) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      waiters.delete(job.id);
      resolve({ line, reason });
    };
    waiters.set(job.id, finish);
    const payload = JSON.stringify({ id: job.id, showId: job.showId, audio: job.audio, episodes: [job.ep] }) + "\n";
    p.stdin.write(payload, (err) => {
      if (err) finish(null, `engine stdin: ${err.message}`);
    });
  });
}

// ---------------------------------------------------------------- bridge

async function post(pathname, body) {
  const res = await fetch(BASE + pathname, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}` },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
  if (res.status === 401 || res.status === 403) {
    console.error(`[worker] auth rejected (HTTP ${res.status}) — BRIDGE_TOKEN does not match the server`);
    process.exit(3);
  }
  if (res.status === 404) return { disabled: true };
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

// postResult retries transient failures; if it ultimately fails, the job
// simply stays queued for the next worker run (at-least-once), and the
// waiting API call has already fallen back — a lost result is never
// worth crashing the loop for.
async function postResult(id, line, errMsg) {
  const body = line != null ? { id, line } : { id, error: errMsg };
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const data = await post("/result", body);
      if (data && data.ok === false) console.log(`[worker] result for id=${id} arrived late (nobody waiting)`);
      return;
    } catch (e) {
      if (attempt === 3) console.error(`[worker] result post for id=${id} failed: ${e?.message || e}`);
      else await sleep(attempt * 1000);
    }
  }
}

// armNextWindow asks GitHub for one more worker run (workflow_dispatch,
// no inputs => default full window). Returns false on transient failure
// so the loop can retry; a run that ends without arming exits 4 (a red
// run is the alarm that the chain is broken).
async function armNextWindow() {
  const token = process.env.GITHUB_TOKEN;
  const repo = process.env.GITHUB_REPOSITORY;
  if (!token || !repo) return null; // local run: nothing to arm
  try {
    const res = await fetch(`https://api.github.com/repos/${repo}/actions/workflows/${WORKFLOW_FILE}/dispatches`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        accept: "application/vnd.github+json",
        "content-type": "application/json",
        "x-github-api-version": "2022-11-28",
      },
      body: JSON.stringify({ ref: "main" }),
      signal: AbortSignal.timeout(15_000),
    });
    if (res.status === 204) {
      console.log("[worker] armed the next window (workflow_dispatch accepted)");
      return true;
    }
    console.error(`[worker] arm dispatch failed: HTTP ${res.status} ${(await res.text().catch(() => "")).slice(0, 200)}`);
    return false;
  } catch (e) {
    console.error(`[worker] arm dispatch error: ${e?.message || e}`);
    return false;
  }
}

// ---------------------------------------------------------------- loop

const startedAt = Date.now();
const deadline = startedAt + MAX_RUNTIME_MS;
let polls = 0;
let jobsDone = 0;
let lastDisabledLog = 0;
let armed = false;
let nextArmAt = 0;
let armAvailable = true; // false once we know no token exists (local runs)

console.log(`[worker] start bridge=${BASE} window=${Math.round(MAX_RUNTIME_MS / 60000)}m`);

while (Date.now() < deadline) {
  // Self-re-arm near the end of the window (also immediate for tiny
  // test windows whose whole span is under the lead time).
  if (!armed && armAvailable && Date.now() >= deadline - ARM_LEAD_MS && Date.now() >= nextArmAt) {
    const res = await armNextWindow();
    if (res === null) armAvailable = false;
    else if (res) armed = true;
    else nextArmAt = Date.now() + ARM_RETRY_MS;
  }
  let data;
  try {
    data = await post("/poll", { limit: POLL_LIMIT });
  } catch (e) {
    console.error(`[worker] poll failed: ${e?.message || e}`);
    await sleep(3000);
    continue;
  }
  if (data.disabled) {
    if (Date.now() - lastDisabledLog > 60_000) {
      console.warn("[worker] bridge disabled on server (404) — ANIRAKU_BRIDGE_TOKEN not set? retrying");
      lastDisabledLog = Date.now();
    }
    await sleep(10_000);
    continue;
  }
  const jobs = Array.isArray(data.jobs) ? data.jobs : [];
  if (jobs.length === 0) {
    polls++;
    if (polls % HEARTBEAT_EVERY === 0) {
      console.log(`[worker] idle ${Math.round((Date.now() - startedAt) / 1000)}s polls=${polls} jobsDone=${jobsDone}`);
    }
    await sleep(POLL_MS);
    continue;
  }
  for (const job of jobs) {
    if (Date.now() >= deadline) break;
    console.log(`[worker] job id=${job.id} show=${job.showId} audio=${job.audio} ep=${job.ep}`);
    const t0 = Date.now();
    const { line, reason } = await runJob(job);
    if (line) {
      await postResult(job.id, line, null);
      console.log(`[worker] done id=${job.id} in ${Date.now() - t0}ms (${line.length}B)`);
    } else {
      await postResult(job.id, null, reason || "worker: engine gave no answer");
      console.error(`[worker] failed id=${job.id}: ${reason}`);
    }
    jobsDone++;
  }
  // Loop straight back into polling — jobs only exist when the backend
  // is actively waiting for them.
}

console.log(`[worker] window complete (${Math.round((Date.now() - startedAt) / 60000)}m, jobsDone=${jobsDone}) — exiting for re-arm`);
try {
  if (daemon) daemon.kill();
} catch {}
// Inside Actions a window that failed to arm its successor means the
// chain dies with it — exit red so the break is visible, not silent.
// GITHUB_ACTIONS is the runner's own marker: local runs (no token to
// arm with) still exit 0.
if (process.env.GITHUB_ACTIONS && !armed) {
  console.error("[worker] FAILED to arm the next window — chain broken, dispatch manually: gh workflow run mkissa-relay");
  process.exit(4);
}
process.exit(0);
