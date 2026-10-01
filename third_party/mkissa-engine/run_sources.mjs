#!/usr/bin/env bun
// MKissa signed episode-source runner.
// Reads JSON from stdin: { showId, audio: "sub"|"dub", episodes: ["1","2",...] }
// Prints JSON: { showId, audio, results: [ { episode, sources: [...] } ] }
//
// Bypass notes (verified):
//  - episode queries require an aaReq signature (AES-256-GCM over lane key from
//    /client-crypto/v1/bootstrap); WITHOUT it the API answers NEED_CAPTCHA.
//  - Plain fetch works — no TLS impersonation needed. The heavy 600-chunk JS
//    discovery used by Anivexa's provider is what triggers the captcha; we skip
//    it entirely and sign directly with a fresh lane key.

import crypto from "node:crypto";
import { getLaneKey, makeAaReq, decryptTobeparsed, episodeQuery, extractSource } from "./providers/mkissa_dbg.js";

const API_URL = "https://api.mkissa.net/api";
const LANE = "k7";
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36";

function readStdin() {
  return new Promise((resolve, reject) => {
    let data = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (c) => (data += c));
    process.stdin.on("end", () => resolve(data));
    process.stdin.on("error", reject);
  });
}

const QUERY = episodeQuery();
const QUERY_HASH = crypto.createHash("sha256").update(QUERY).digest("hex");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function signedPost(lane, variables) {
  const extensions = {
    persistedQuery: { version: 1, sha256Hash: QUERY_HASH },
    k: LANE,
    aaReq: makeAaReq(lane.key, lane.epoch, lane.buildId, QUERY_HASH, LANE),
  };
  const res = await fetch(API_URL, {
    method: "POST",
    headers: {
      "User-Agent": UA,
      "Content-Type": "application/json",
      Referer: "https://mkissa.to/",
      Origin: "https://mkissa.to",
      "x-build-id": String(lane.buildId),
      Accept: "*/*",
      "Accept-Language": "en-US,en;q=0.9",
      "sec-fetch-dest": "empty",
      "sec-fetch-mode": "cors",
      "sec-fetch-site": "cross-site",
      Priority: "u=1, i",
    },
    body: JSON.stringify({ query: QUERY, variables, extensions }),
  });
  const text = await res.text();
  return { status: res.status, text };
}

async function fetchEpisode(lane, showId, audio, ep) {
  const variables = { showId, translationType: audio, episodeString: String(ep) };
  const { status, text } = await signedPost(lane, variables);
  if (status !== 200) throw Object.assign(new Error(`HTTP ${status}`), { code: "HTTP_" + status, raw: text.slice(0, 300) });
  const json = JSON.parse(text);
  const messages = (json.errors || []).map((e) => e.message).filter(Boolean);
  if (messages.length) throw Object.assign(new Error(messages.join(" · ")), { code: messages.includes("NEED_CAPTCHA") ? "NEED_CAPTCHA" : messages[0], raw: text.slice(0, 300) });
  if (json.data?.tobeparsed) {
    const parsed = decryptTobeparsed(json.data.tobeparsed, lane.key);
    return parsed.episode ?? parsed;
  }
  throw Object.assign(new Error("unexpected payload"), { code: "BAD_PAYLOAD", raw: text.slice(0, 300) });
}

async function main() {
  const input = JSON.parse((await readStdin()).trim() || "{}");
  const showId = input.showId;
  const audio = input.audio === "dub" ? "dub" : "sub";
  const episodes = (input.episodes || ["1"]).map(String);
  if (!showId) throw new Error("missing showId");

  let lane = await getLaneKey(LANE);
  const results = [];

  for (const ep of episodes) {
    let data = null;
    let lastErr = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        data = await fetchEpisode(lane, showId, audio, ep);
        break;
      } catch (err) {
        lastErr = err;
        // refresh lane key on captcha/signature errors, back off on rate limits
        const rate = /try again in (\d+) seconds?/.exec(err.raw || err.message || "");
        if (rate) await sleep(Number(rate[1]) * 1000 + 250);
        else if (err.code === "NEED_CAPTCHA" || err.code?.startsWith("HTTP_")) {
          await sleep(1200 + attempt * 800);
          try { lane = await getLaneKey(LANE, true); } catch {}
        } else break;
      }
    }
    if (!data) {
      results.push({ episode: ep, error: lastErr?.message || "failed", code: lastErr?.code || null });
      continue;
    }
    const rawSources = Array.isArray(data.sourceUrls) ? data.sourceUrls : [];
    const sources = await Promise.all(rawSources.map((s) => extractSource(s).catch(() => null)));
    sources.sort((a, b) => (b?.priority ?? 0) - (a?.priority ?? 0));
    results.push({ episode: ep, sources: sources.filter(Boolean) });
    await sleep(350);
  }

  process.stdout.write(JSON.stringify({ showId, audio, results }));
}

main().catch((err) => {
  process.stdout.write(JSON.stringify({ error: String(err.message || err), code: err.code || null }));
  process.exitCode = 1;
});
