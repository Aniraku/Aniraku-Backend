# mkissa-proxy-fwd — local HTTP forwarder for the engine's egress

The api container's engine child runs with
`MKISSA_PROXY=http://172.18.0.1:18080` (see `docs/PROVIDERS.md`): bun's
`NODE_USE_ENV_PROXY` only understands `http://` proxies, but the vetted
free upstream is socks4. This host-side forwarder bridges the two:

```
bun plain-fetch (bootstrap/discovery) ──┐
                                       ├─> http://172.18.0.1:18080 (pproxy)
wreq signed gate calls (MKISSA_PROXY) ─┘            │
                                                    v
                                     socks4://183.106.215.208:1080
```

The API process itself never sees proxy env (see the spawn block in
`internal/streaming/mkissa_daemon.go`): playback relays, probes and
every other provider stay direct.

## Install

```sh
sudo apt-get install -y python3-pip
sudo pip3 install --break-system-packages pproxy
```

**Python >= 3.14 patch** (pproxy 2.7.9 calls the removed
`asyncio.get_event_loop()` at startup and crashes). Re-apply after every
pip upgrade of pproxy:

```sh
sudo python3 - <<'PY'
import pathlib
f = pathlib.Path("/usr/local/lib/python3.14/dist-packages/pproxy/server.py")
s = f.read_text()
s = s.replace("loop = asyncio.get_event_loop()",
              "loop = asyncio.new_event_loop()\n    asyncio.set_event_loop(loop)")
f.write_text(s)
PY
```

Then install and enable the unit:

```sh
sudo cp mkissa-proxy-fwd.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now mkissa-proxy-fwd.service
```

## Verify

The forwarder must answer with the upstream's exit IP, from the network
the api container uses:

```sh
docker exec aniraku-api bun -e \
  'process.env.NODE_USE_ENV_PROXY=1;
   process.env.HTTP_PROXY="http://172.18.0.1:18080";
   process.env.HTTPS_PROXY="http://172.18.0.1:18080";
   fetch("http://api.ipify.org?format=json").then(r=>r.text()).then(console.log)'
# -> {"ip":"59.24.42.101"}   (the socks4 upstream's egress)
```

The real gate check is one engine call through the proxy — success
means signed sources, `NEED_CAPTCHA` / `Too many requests` means this
egress is no longer accepted:

```sh
echo '{"id":1,"showId":"ReooPAxPMsHM4KPMY","audio":"sub","episodes":["1"]}' | \
docker exec -i \
  -e MKISSA_PROXY=http://172.18.0.1:18080 -e NODE_USE_ENV_PROXY=1 \
  -e HTTP_PROXY=http://172.18.0.1:18080 -e HTTPS_PROXY=http://172.18.0.1:18080 \
  aniraku-api bun /app/third_party/mkissa-engine/mkissa_daemon.mjs
```

## When the upstream dies

Free proxies flap. Symptoms: the gate test above starts failing, or logs
show the *local* path throttled while the bridge keeps serving (that is
the per-egress breaker doing its job — only local rests). Swap the
upstream in the unit and restart:

```sh
sudo sed -i 's|-r socks4://.*|-r socks4://NEW_IP:PORT|' /etc/systemd/system/mkissa-proxy-fwd.service
sudo systemctl restart mkissa-proxy-fwd.service
```

Candidate upstreams come from a free proxy list filtered by
`hosting=false` + residential ASN, connectivity-tested, then gate-tested
end-to-end (the engine call above) **three times in a row** — a proxy
that passes once and flakes is worthless; the wired one passed 4/4.
