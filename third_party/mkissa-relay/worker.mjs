// mkissa-relay worker (Cloudflare, free tier): forwards mkissa API calls
// with Cloudflare egress instead of the throttled datacenter IP.
//
// Deploy: dash.cloudflare.com → Workers & Pages → Create → paste this
// file → Deploy. Then set MKISSA_API=https://<name>.<sub>.workers.dev
// in the backend container env (compose.yaml environment) and redeploy.
// Only kilobytes of API calls flow here; video segments stay direct.
//
// Free tier allows 100k requests/day; backend volume is in the hundreds.

const UPSTREAM = "https://api.mkissa.net";

// Only the API surface the backend uses. Anything else is rejected so
// this can never become an open proxy.
function allowed(path) {
  return path === "/api" || path.startsWith("/api?") || path.startsWith("/client-crypto/");
}

export default {
  async fetch(req) {
    const url = new URL(req.url);
    if (!allowed(url.pathname)) {
      return new Response("not found", { status: 404 });
    }
    const target = UPSTREAM + url.pathname + url.search;
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
  },
};
