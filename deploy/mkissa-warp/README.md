# mkissa WARP (mkissa-only egress)

mkissa's episode gate blocks the VPS's datacenter IP. This stack gives ONLY
the mkissa engine a free Cloudflare WARP egress, isolated so that a dead
tunnel can never affect any other provider again (the 2026-10-06 incident:
mkissa's Cloudflare anycast IPs are shared with other providers, and routing
them into a dead tunnel blackholed aniwaves etc.).

## Design

```
api container (bun engine)
  |  extra_hosts: api.mkissa.net -> 10.77.0.2   (compose)
  v
veth pair vmk0(10.77.0.1, host) <-> vmk1(10.77.0.2, ns mkissawg)
  v
mkissawg namespace
  |- mkissa-relay.py  (python3 stdlib TLS splice, port 443, never terminates TLS)
  |- wgcf interface   (WARP tunnel, AllowedIPs = api.mkissa.net IPs only)
  '- routes: mkissa IPs -> wgcf (healthy) | vmk1 direct fallback (unhealthy)
```

- Host routes: untouched by construction. Other providers always route direct.
- Watchdog (every 10 min): proves the tunnel with a real `warp=on` trace
  inside the namespace, flips routes, and rotates the WARP registration only
  when CF ignores us — rate-limited (>=45 min gap, <=6/day) because rapid
  re-registration is what made Cloudflare start ignoring us the first time.
- Fallback: tunnel down => mkissa runs direct (breaker + resolve cache absorb
  rate limits) — the pre-WARP behavior.

## Files

| file | install path |
|---|---|
| `mkissa-relay.py` | `/usr/local/bin/mkissa-relay.py` |
| `mkissa-warp-setup.sh` | `/usr/local/sbin/mkissa-warp-setup.sh` |
| `mkissa-warp-watch.sh` | `/usr/local/sbin/mkissa-warp-watch.sh` |
| `mkissa-warp-wglib.sh` | `/usr/local/sbin/mkissa-warp-wglib.sh` |
| `mkissa-warp-ns.service` | `/etc/systemd/system/` |
| `mkissa-relay.service` | `/etc/systemd/system/` |
| `mkissa-warp-watch.service` | `/etc/systemd/system/` |
| `mkissa-warp-watch.timer` | `/etc/systemd/system/` |

Compose (server `~/compose.yaml`) needs, under `aniraku-api`:

```yaml
    extra_hosts:
      - "api.mkissa.net:10.77.0.2"
```

Logs: `journalctl -u mkissa-relay -u mkissa-warp-watch -n 50`.
