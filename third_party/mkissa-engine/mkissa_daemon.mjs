#!/usr/bin/env bun
// mkissa_daemon.mjs — persistent MKissa source runner.
//
// Same engine as run_sources.mjs (lane key, signing, extractors), but speaks
// line-delimited JSON on stdin/stdout and STAYS ALIVE: the lane key and the
// discovered crypto config persist across requests, so each episode costs
// one signed call instead of a full discovery crawl + bootstrap + calls.
// The backend (Go) serializes requests; one in flight at a time.
//
// Request line:  {"id":1,"showId":"...","audio":"sub"|"dub","episodes":["1"]}
// Response line: {"id":1,"showId":"...","audio":"...","results":[{"episode":"1","sources":[...]|"error":...,"code":...}]}
// Fatal errors:  {"id":0,"error":"..."} (backend restarts the daemon).

import crypto from "node:crypto";
import readline from "node:readline";
import { getLaneKey, makeAaReq, decryptTobeparsed, episodeQuery, extractSource, relayKeyHeaders } from "./providers/mkissa_dbg.js";

const API_URL = (process.env.MKISSA_API || "https://api.mkissa.net") + "/api";
const LANE = "k7";
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36";

const QUERY = episodeQuery();
const QUERY_HASH = crypto.createHash("sha256").update(QUERY).digest("hex");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Watchdogs: a plain fetch() that never settles (a silently dropped SYN
// from a hot rate bucket) used to stall the whole line protocol until the
// backend's 40s call timeout killed the child with no diagnostic at all
// (observed 2026-10-04: every prod call timed out, the same script
// answered in 2s by hand). Bound every wait and answer with an error
// instead of silence.
const FETCH_TIMEOUT_MS = 15000;
const REQUEST_TIMEOUT_MS = 35000;

function withTimeout(promise, ms, label) {
  let timer;
  const guard = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  // A late settle must not become an unhandled rejection after the race.
  promise.catch(() => {});
  return Promise.race([promise, guard]).finally(() => clearTimeout(timer));
}

let lane = null;
async function ensureLane(force = false) {
  if (!lane || force) lane = await getLaneKey(LANE, force);
  return lane;
}

async function signedPost(variables) {
  const l = await ensureLane();
  const extensions = {
    persistedQuery: { version: 1, sha256Hash: QUERY_HASH },
    k: LANE,
    aaReq: makeAaReq(l.key, l.epoch, l.buildId, QUERY_HASH, LANE),
  };
  const res = await fetch(API_URL, {
    method: "POST",
    headers: {
      "User-Agent": UA,
      "Content-Type": "application/json",
      Referer: "https://mkissa.to/",
      Origin: "https://mkissa.to",
      "x-build-id": String(l.buildId),
      ...relayKeyHeaders(),
      Accept: "*/*",
      "Accept-Language": "en-US,en;q=0.9",
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "cross-site",
      Priority: "u=1, i",
    },
    body: JSON.stringify({ query: QUERY, variables, extensions }),
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  const text = await res.text();
  return { status: res.status, text };
}

async function fetchEpisode(showId, audio, ep) {
  const variables = { showId, translationType: audio, episodeString: String(ep) };
  const { status, text } = await signedPost(variables);
  if (status !== 200) throw Object.assign(new Error(`HTTP ${status}`), { code: "HTTP_" + status, raw: text.slice(0, 300) });
  const json = JSON.parse(text);
  const messages = (json.errors || []).map((e) => e.message).filter(Boolean);
  if (messages.length) throw Object.assign(new Error(messages.join(" · ")), { code: messages.includes("NEED_CAPTCHA") ? "NEED_CAPTCHA" : messages[0], raw: text.slice(0, 300) });
  if (json.data?.tobeparsed) {
    const parsed = decryptTobeparsed(json.data.tobeparsed, (await ensureLane()).key);
    return parsed.episode ?? parsed;
  }
  throw Object.assign(new Error("unexpected payload"), { code: "BAD_PAYLOAD", raw: text.slice(0, 300) });
}

async function handleEpisode(showId, audio, ep) {
  let data = null;
  let lastErr = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      data = await fetchEpisode(showId, audio, ep);
      break;
    } catch (err) {
      lastErr = err;
      const rate = /try again in (\d+) seconds?/.exec(err.raw || err.message || "");
      if (rate) await sleep(Number(rate[1]) * 1000 + 250);
      else if (err.code === "NEED_CAPTCHA" || err.code?.startsWith("HTTP_")) {
        await sleep(1200 + attempt * 800);
        try { await ensureLane(true); } catch {}
      } else break;
    }
  }
  if (!data) return { episode: String(ep), error: lastErr?.message || "failed", code: lastErr?.code || null };
  const rawSources = Array.isArray(data.sourceUrls) ? data.sourceUrls : [];
  const sources = await Promise.all(rawSources.map((s) => extractSource(s).catch(() => null)));
  sources.sort((a, b) => (b?.priority ?? 0) - (a?.priority ?? 0));
  return { episode: String(ep), sources: sources.filter(Boolean) };
}

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", async (line) => {
  let req;
  try {
    req = JSON.parse(line);
  } catch {
    return;
  }
  try {
    const audio = req.audio === "dub" ? "dub" : "sub";
    const episodes = (req.episodes || ["1"]).map(String);
    const results = await withTimeout((async () => {
      const out = [];
      for (const ep of episodes) {
        out.push(await handleEpisode(req.showId, audio, ep));
        await sleep(350);
      }
      return out;
    })(), REQUEST_TIMEOUT_MS, "engine request");
    process.stdout.write(JSON.stringify({ id: req.id ?? 0, showId: req.showId, audio, results }) + "\n");
  } catch (err) {
    process.stdout.write(JSON.stringify({ id: req?.id ?? 0, error: String(err?.message || err) }) + "\n");
  }
});
