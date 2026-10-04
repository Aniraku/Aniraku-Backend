// Aniraku upstream relay (Cloudflare Worker, free tier).
//
// WHY: some upstream API endpoints Cloudflare-block the backend's
// datacenter egress (the VidNest API) while serving
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

export default {
  async fetch(req, env) {
    if (!authorized(req, env)) return denied();
    const wurl = new URL(req.url);
    // mkissa signed-API relay (option A test): preserves method, query,
    // headers and body; only the mkissa API surface is reachable.
    if (wurl.pathname === "/mkissa" || wurl.pathname.startsWith("/mkissa/")) {
      const stripped = wurl.pathname.replace(/^\/mkissa/, "") || "/";
      if (
        stripped !== "/api" &&
        !stripped.startsWith("/api?") &&
        !stripped.startsWith("/client-crypto/")
      ) {
        return new Response("not found", { status: 404 });
      }
      const target = "https://api.mkissa.net" + stripped + wurl.search;
      const headers = new Headers(req.headers);
      headers.delete("host");
      headers.delete("cf-connecting-ip");
      headers.delete("cf-ipcountry");
      const init = { method: req.method, headers };
      if (req.method !== "GET" && req.method !== "HEAD") {
        init.body = req.body;
      }
      const res = await fetch(target, init);
      return new Response(res.body, { status: res.status, headers: res.headers });
    }
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
    return new Response("not found", { status: 404 });
  },
};
