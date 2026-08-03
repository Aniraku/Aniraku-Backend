import asyncio
import os
import sys

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import Response
import vipertls

PORT = int(os.getenv("MIRURO_PROXY_PORT", "8099"))
MIRURO_API_BASE = os.getenv("MIRURO_API_BASE", "https://miruro-api-v3.onrender.com")
WARMUP_URL = os.getenv("MIRURO_WARMUP_URL", "https://www.miruro.tv/")

app = FastAPI()
client = None
_warmup_lock = asyncio.Lock()

@app.on_event("startup")
async def startup():
    global client
    client = vipertls.AsyncClient(impersonate="chrome_145", timeout=90)
    sys.stderr.write(f"[proxy] warming up against {WARMUP_URL}...\n")
    try:
        r = await client.get(WARMUP_URL)
        sys.stderr.write(f"[proxy] warmup done: {r.status_code} solved_by={r.solved_by}\n")
    except Exception as e:
        sys.stderr.write(f"[proxy] warmup failed: {e}\n")

@app.get("/health")
async def health():
    ok = client is not None
    return {"status": "ok" if ok else "starting"}

@app.api_route("/proxy")
async def generic_proxy(url: str, request: Request):
    r = await _do_get(url)
    return Response(content=r.content, status_code=r.status_code, media_type=r.headers.get("content-type"))

@app.api_route("/{path:path}", methods=["GET"])
async def miruro_proxy(path: str, request: Request):
    url = f"{MIRURO_API_BASE}/{path}"
    qs = str(request.query_params)
    if qs:
        url += "?" + qs
    r = await _do_get(url)
    return Response(content=r.content, status_code=r.status_code, media_type=r.headers.get("content-type"))

async def _do_get(url: str):
    global client
    if client is None:
        return Response(content="proxy not ready", status_code=503)
    r = await client.get(url)
    if r.status_code == 403:
        async with _warmup_lock:
            sys.stderr.write("[proxy] 403, re-solving CF challenge...\n")
            await client.get(WARMUP_URL)
            r = await client.get(url)
    return r

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
