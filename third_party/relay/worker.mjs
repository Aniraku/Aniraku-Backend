// Aniraku upstream relay (Cloudflare Worker, free tier).
//
// WHY: some upstream API endpoints Cloudflare-block the backend's
// datacenter egress (vidnest API, tryembed stream_data) while serving
// residential/CF egress fine — and the minted file URLs play from the
// backend egress with no IP binding (verified live). So ONLY the blocked
// API calls route through here (kilobytes of JSON); every video byte and
// every probe stays direct from the backend.
//
// Deploy: dash.cloudflare.com → Workers & Pages → Create → paste this
// file → Deploy → Settings → Variables → add RELAY_KEY=<long random>.
// Then set on the backend host:
//   ANIRAKU_RELAY_URL=https://<worker>.workers.dev
//   ANIRAKU_RELAY_KEY=<same value>
// and redeploy the backend. Unset = direct (today's behavior).
//
// Abuse guard: every route requires ?key= (or X-Relay-Key) matching
// RELAY_KEY, and only the paths below are ever fetched — this can never
// become an open proxy. Free tier: 100k req/day; backend volume is in the
// hundreds.

const VIDNEST_API = "https://new.vidnest.fun";
const TRYEMBED_BASE = "https://tryembed.us.cc";
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

function authorized(req, env) {
  const url = new URL(req.url);
  const key = url.searchParams.get("key") || req.headers.get("X-Relay-Key");
  return !!env.RELAY_KEY && key === env.RELAY_KEY;
}

function denied() {
  return new Response("forbidden", { status: 403 });
}

// Minimal cookie jar: upstream sessions are cookie-bound (tryembed_auth),
// and Workers don't persist cookies between fetches.
function jarStore(jar, res) {
  const setCookies =
    res.headers.getSetCookie?.() ||
    (res.headers.get("set-cookie") ? [res.headers.get("set-cookie")] : []);
  for (const line of setCookies) {
    const m = /^\s*([^=;\s]+)=([^;]*)/.exec(line);
    if (m) jar.set(m[1], m[2]);
  }
}

function jarHeader(jar) {
  return [...jar.entries()].map(([k, v]) => `${k}=${v}`).join("; ");
}

async function vidnestResolve(env, body) {
  const { id, episode, lang } = body;
  if (!id || !episode || !lang) {
    return new Response("bad request", { status: 400 });
  }
  const target =
    `${VIDNEST_API}/hianime/anime/${encodeURIComponent(id)}/${episode}/` +
    `${encodeURIComponent(lang)}/hd-2`;
  const res = await fetch(target, {
    headers: {
      "User-Agent": UA,
      Accept: "application/json",
      Referer: "https://vidnest.fun/",
    },
  });
  const text = await res.text();
  return new Response(text, {
    status: res.status,
    headers: { "Content-Type": "application/json" },
  });
}

const TRYEMBED_SERVERS = ["astra", "beta", "sora", "zen"];

async function tryembedResolve(env, body) {
  const { id, episode, lang } = body;
  if (!id || !episode || !lang) {
    return new Response("bad request", { status: 400 });
  }
  const pageURL = `${TRYEMBED_BASE}/embed/anime/${id}/${episode}/${lang}`;
  const jar = new Map();
  const fetchHeaders = {
    "User-Agent": UA,
    Accept: "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
  };
  const pageRes = await fetch(pageURL, { headers: fetchHeaders });
  if (!pageRes.ok) {
    return Response.json({ error: `page HTTP ${pageRes.status}` }, { status: 502 });
  }
  jarStore(jar, pageRes);
  const page = await pageRes.text();
  const ticket = /window\.BOOTSTRAP_TICKET="([^"]+)"/.exec(page)?.[1];
  if (!ticket) {
    return Response.json({ error: "no ticket" }, { status: 502 });
  }
  const bootRes = await fetch(`${TRYEMBED_BASE}/api/bootstrap`, {
    method: "POST",
    headers: {
      ...fetchHeaders,
      Origin: TRYEMBED_BASE,
      Referer: pageURL,
      Cookie: jarHeader(jar),
      "X-TryEmbed-Bootstrap": ticket,
    },
  });
  jarStore(jar, bootRes);
  if (!bootRes.ok) {
    return Response.json({ error: `bootstrap HTTP ${bootRes.status}` }, { status: 502 });
  }
  const boot = await bootRes.json();
  if (!boot.embedNonce) {
    return Response.json({ error: "no nonce" }, { status: 502 });
  }
  const mirrors = [];
  let intro = null;
  let outro = null;
  for (const server of TRYEMBED_SERVERS) {
    try {
      const q = new URLSearchParams({
        id: String(id),
        episode: String(episode),
        audio: lang,
        player: "jw",
        server,
        nonce: boot.embedNonce,
      });
      const sdRes = await fetch(`${TRYEMBED_BASE}/api/stream_data?${q}`, {
        headers: {
          ...fetchHeaders,
          Referer: pageURL,
          Cookie: jarHeader(jar),
          "X-Embed-Nonce": boot.embedNonce,
        },
      });
      if (!sdRes.ok) continue;
      const sd = await sdRes.json();
      if (!intro && sd.intro) intro = sd.intro;
      if (!outro && sd.outro) outro = sd.outro;
      const block = (sd.providers || []).find((p) => p.id === server);
      if (!block) continue;
      const cands = (block.qualities || []).filter((x) => x && x.token);
      if (!cands.length) continue;
      const pick =
        cands.find((x) => String(x.name || "").toLowerCase() === "auto") ||
        [...cands].sort((a, b) => (b.height || 0) - (a.height || 0))[0];
      const ext = String(block.type || "").toLowerCase() === "mp4" ? "mp4" : "m3u8";
      const fileRes = await fetch(`${TRYEMBED_BASE}/s/${pick.token}.${ext}`, {
        headers: { ...fetchHeaders, Referer: pageURL, Cookie: jarHeader(jar) },
      });
      if (!fileRes.ok && fileRes.status !== 206) continue;
      const head = await fileRes.clone().arrayBuffer().then((b) => b.byteLength).catch(() => 0);
      if (!head) continue;
      mirrors.push({
        server,
        type: ext === "mp4" ? "mp4" : "hls",
        url: fileRes.url,
        captions: block.captions || [],
      });
    } catch {
      continue;
    }
  }
  return Response.json({ mirrors, intro, outro });
}

export default {
  async fetch(req, env) {
    if (!authorized(req, env)) return denied();
    if (req.method !== "POST") {
      return new Response("method not allowed", { status: 405 });
    }
    const url = new URL(req.url);
    let body = null;
    try {
      body = await req.json();
    } catch {
      return new Response("bad request", { status: 400 });
    }
    if (url.pathname === "/vidnest") return vidnestResolve(env, body);
    if (url.pathname === "/tryembed") return tryembedResolve(env, body);
    return new Response("not found", { status: 404 });
  },
};
