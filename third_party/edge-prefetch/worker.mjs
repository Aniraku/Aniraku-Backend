// Aniraku edge prefetch worker (Cloudflare Workers, free tier).
//
// WHY: video watching is sequential. When the edge serves segment N, the
// player will ask for N+1 within seconds. Fetching N+1 into cache in the
// background (waitUntil, off the critical path) converts the next MISS
// (~1.4s origin round trip) into a HIT (~0.3s). First-byte latency
// mid-playback effectively vanishes for linear viewing.
//
// SCOPE: only GETs under /api/v1/proxy* that resolve to media segments.
// Playlists (.m3u8), keys, API JSON and errors pass through untouched with
// the origin's own cache semantics. One segment ahead, never more: seeks
// waste at most one fill.
//
// NOTE on URLs: the edge URL is opaque (always /api/v1/proxy?url=<upstream
// stream>&headers=...). Every decision keys off the UPSTREAM url inside
// the param; prefetches rebuild the edge URL with the incremented upstream.
//
// ORIGIN: plain HTTP to the VM (env ORIGIN_HOST, e.g. the EC2 public DNS
// name). TLS still terminates at Cloudflare toward viewers. Host and
// X-Forwarded-Host are set to the public API host so the backend mints
// stable proxy URLs (identical bytes = identical URLs = shared edge cache).
//
// COST: each prefetch is one cache fill that linear playback would have
// caused seconds later anyway. Abandoned views waste at most one segment.
// Free tier: ~2x request volume still fits 100k req/day at current scale.
//
// DEPLOY: dashboard Workers & Pages -> Create -> paste this file ->
// Settings -> Variables: ORIGIN_HOST=<vm public dns>, API_HOST=
// api.aniraku.tech -> Deploy. Attach to a TEST route first
// (test.aniraku.tech/*), verify, then attach
// api.aniraku.tech/api/v1/proxy*.

const SEGMENT_TTL = 31536000; // immutable bytes, same as origin header
const PLAYLIST_TTL = 300; // matches origin max-age for rewritten playlists

// The upstream URL carried in the edge request's url= param.
function upstreamOf(reqUrl) {
  try {
    return new URL(reqUrl).searchParams.get("url") || "";
  } catch {
    return "";
  }
}

function isPlaylistUpstream(upstream) {
  try {
    return new URL(upstream).pathname.toLowerCase().endsWith(".m3u8");
  } catch {
    return false;
  }
}

// Derive the "next" upstream by incrementing the segment counter in its
// filename, preserving zero-padding:
//   .../seg-147-f1-v1-a1.css -> .../seg-148-f1-v1-a1.css
//   .../64af31/001.jpg        -> .../64af31/002.jpg
// Counter rules (learned from real misses): the FIRST non-quality number
// run is the counter (nexabloom embeds it first: seg-{N}-f1-v1-a1); runs
// glued inside alphanumerics (the 3/8 in master.m3u8) or past the final
// dot are never counters; quality tokens (1080, 720, ...) are skipped.
// Anything else returns null: no guess made.
const QUALITY_RUNS = new Set(["2160", "1080", "720", "480", "360", "240"]);
function nextUpstreamURL(upstream) {
  let u;
  try {
    u = new URL(upstream);
  } catch {
    return null;
  }
  const parts = u.pathname.split("/");
  const last = parts[parts.length - 1];
  const dot = last.lastIndexOf(".");
  const base = dot > 0 ? last.slice(0, dot) : last;
  const ext = dot > 0 ? last.slice(dot) : "";
  const runRe = /\d+/g;
  let m;
  while ((m = runRe.exec(base)) !== null) {
    const num = m[0];
    if (QUALITY_RUNS.has(num)) continue;
    const after = base[m.index + num.length];
    if (after !== undefined && after !== "-" && after !== "_" && after !== ".") continue;
    const next = String(parseInt(num, 10) + 1).padStart(num.length, "0");
    parts[parts.length - 1] = base.slice(0, m.index) + next + base.slice(m.index + num.length) + ext;
    u.pathname = parts.join("/");
    return u.toString();
  }
  return null;
}

// Rebuild the edge proxy URL with a new upstream, keeping headers/al params.
function withUpstream(reqUrl, upstream) {
  const u = new URL(reqUrl);
  u.searchParams.set("url", upstream);
  return u.toString();
}

function originRequest(edgeUrl, req, env) {
  const incoming = new URL(edgeUrl);
  const origin = new URL(incoming.pathname + incoming.search, `http://${env.ORIGIN_HOST}`);
  const headers = new Headers(req.headers);
  headers.set("Host", env.API_HOST);
  headers.set("X-Forwarded-Host", env.API_HOST);
  headers.set("X-Forwarded-Proto", "https");
  return new Request(origin.toString(), {
    method: req.method,
    headers,
    redirect: "manual",
  });
}

function cacheableFetch(req, ttl) {
  return fetch(req, {
    cf: {
      cacheEverything: true,
      cacheTtlByStatus: { "200-299": ttl, "404": 1, "500-599": 0 },
    },
  });
}

export default {
  async fetch(req, env, ctx) {
    const upstream = upstreamOf(req.url);
    const playlist = isPlaylistUpstream(upstream);

    let res;
    try {
      res = await cacheableFetch(
        originRequest(req.url, req, env),
        playlist ? PLAYLIST_TTL : SEGMENT_TTL,
      );
    } catch (e) {
      return new Response("origin unreachable", { status: 502 });
    }
    if (res.status !== 200) return res;

    // Prefetch the next segment: media bytes only, full GETs only, one
    // ahead. waitUntil keeps it off the critical path; any failure dies
    // silently and the main response is already decided.
    const ctype = (res.headers.get("Content-Type") || "").toLowerCase();
    const isMedia =
      !playlist &&
      req.method === "GET" &&
      !req.headers.has("Range") &&
      (ctype.startsWith("video/") ||
        ctype.startsWith("audio/") ||
        ctype === "application/octet-stream" ||
        ctype === "text/vtt");
    if (isMedia) {
      const nextUp = nextUpstreamURL(upstream);
      if (nextUp) {
        ctx.waitUntil(
          (async () => {
            try {
              const prefetchReq = new Request(withUpstream(req.url, nextUp), {
                method: "GET",
              });
              const r = await cacheableFetch(
                originRequest(withUpstream(req.url, nextUp), prefetchReq, env),
                SEGMENT_TTL,
              );
              // Drain so the edge actually stores the body.
              await r.arrayBuffer();
            } catch {
              /* prefetch is best effort by design */
            }
          })(),
        );
      }
    }
    return res;
  },
};
