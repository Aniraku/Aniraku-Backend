#!/usr/bin/env bash
# mkissa WARP watchdog.
#
# Runs on the host but performs EVERY state change inside the mkissawg
# namespace:
#   1. refresh api.mkissa.net IPs (Cloudflare rotates DNS answers)
#   2. prove the tunnel with a real HTTP round trip to api.mkissa.net
#      from inside the namespace (its only AllowedIPs traffic)
#   3. flip mkissa routes: tunnel when healthy, direct fallback otherwise
#   4. rate-limited WARP re-registration when Cloudflare stops answering
#
# Host routes are never touched: other providers cannot be affected.
set -uo pipefail

NS=mkissawg
NDEV=vmk1
CONF=/etc/wireguard/wgcf.conf
WARP_DIR=/root/warp
MK_HOST=api.mkissa.net
STATE_DIR=/var/lib/mkissa-warp
MIN_REG_GAP=2700    # seconds between WARP registrations (CF blocks rapid re-regs)
REG_MAX_PER_DAY=6

LIB=/usr/local/sbin/mkissa-warp-wglib.sh
# shellcheck source=/dev/null
[ -f "$LIB" ] && . "$LIB"

log() { echo "$(date -u '+%F %T') watch: $*"; }
ns()  { ip netns exec "$NS" "$@"; }

mkdir -p "$STATE_DIR"

resolve_ips() { getent ahostsv4 "$MK_HOST" | awk '{print $1}' | sort -u | tr '\n' ' '; }

apply_routes() { # $1=ips $2=dev
  # Direct routes MUST go via the host gateway: a bare "dev vmk1 scope link"
  # makes the ns ARP for the remote IP itself, nobody answers, and SYNs hang.
  # wgcf is point-to-point (NOARP) so it takes no gateway.
  local ip via=""
  [ "$2" = wgcf ] || via="via 10.77.0.1 "
  for ip in $1; do
    # shellcheck disable=SC2086
    ns ip route replace "${ip}/32" $via dev "$2" 2>/dev/null || true
  done
}

sync_allowedips() {
  local ips="$1" allowed="" ip
  [ -n "${ips// /}" ] || return 1
  for ip in $ips; do allowed="$allowed${ip}/32, "; done
  allowed=${allowed%, }
  sed -i "s|^AllowedIPs.*|AllowedIPs = $allowed|" "$CONF"
  local strip_out
  strip_out=$(wg-quick strip wgcf 2>/dev/null) || return 1
  printf '%s\n' "$strip_out" | sed '/^Endpoint/d' | ns wg setconf wgcf /dev/stdin
}

ensure_wg_up() {
  ns ip link show wgcf >/dev/null 2>&1 && ns wg show wgcf >/dev/null 2>&1 && return 0
  wg_create
}

bounce_wg() {
  wg_destroy
  sleep 1
  wg_create
  sleep 1
}

tunnel_healthy() { # $1 = mkissa IPs
  # Full data-plane proof for the ONLY traffic this tunnel carries: a real
  # HTTP round trip to api.mkissa.net with its routes pinned to wgcf.
  # The old test curled a Cloudflare trace IP that is NOT in AllowedIPs;
  # WireGuard drops such packets itself, so the test could never pass and
  # every run reported "unhealthy" no matter what Cloudflare did (proven
  # 2026-10-06: same session, mkissa IPs routed into the tunnel -> HTTP
  # 403 + application response, transfer counters moving).
  local ip out
  [ -n "${1// /}" ] || return 1
  for ip in $1; do
    # shellcheck disable=SC2086
    ns ip route replace "${ip}/32" dev wgcf 2>/dev/null || return 1
  done
  out=$(ns curl -s -m 8 -o /dev/null -w '%{http_code}' "https://$MK_HOST/" 2>&1) || true
  case "$out" in
    [1-5][0-9][0-9]) return 0 ;;  # any HTTP status proves data crossed the tunnel
    *) return 1 ;;
  esac
}

reg_allowed() {
  local now last dayfile count
  now=$(date +%s)
  last=$(cat "$STATE_DIR/last-reg" 2>/dev/null || echo 0)
  [ $((now - last)) -ge "$MIN_REG_GAP" ] || return 1
  dayfile="$STATE_DIR/reg-count-$(date +%Y%m%d)"
  count=$(cat "$dayfile" 2>/dev/null || echo 0)
  [ "$count" -lt "$REG_MAX_PER_DAY" ] || return 1
  return 0
}

rotate_warp() { # $1 = current mkissa ips
  local ips="$1" i allowed="" ip
  reg_allowed || { log "rotate skipped (pace limit)"; return 1; }
  log "rotating WARP registration (fresh account/exit)"
  wg_destroy
  cd "$WARP_DIR" || return 1
  rm -f wgcf-account.toml
  for i in 1 2 3; do wgcf register --accept-tos >/dev/null 2>&1 && break; sleep 3; done
  [ -f wgcf-account.toml ] || { log "rotate: register failed"; return 1; }
  wgcf generate >/dev/null 2>&1 || { log "rotate: generate failed"; return 1; }
  for ip in $ips; do allowed="$allowed${ip}/32, "; done
  allowed=${allowed%, }
  cp wgcf-profile.conf "$CONF"
  sed -i '/^DNS/d' "$CONF"
  sed -i "s|^AllowedIPs.*|AllowedIPs = $allowed|" "$CONF"
  grep -q '^PersistentKeepalive' "$CONF" || sed -i '/^\[Peer\]/a PersistentKeepalive = 25' "$CONF"
  grep -q MASQUERADE "$CONF" || {
    sed -i '/^\[Interface\]/a PreDown = iptables -t nat -D POSTROUTING -o wgcf -j MASQUERADE || true' "$CONF"
    sed -i '/^\[Interface\]/a PostUp = iptables -t nat -A POSTROUTING -o wgcf -j MASQUERADE' "$CONF"
  }
  chmod 600 "$CONF"
  wg_create || { log "rotate: interface create failed"; return 1; }
  date +%s > "$STATE_DIR/last-reg"
  echo $(( $(cat "$STATE_DIR/reg-count-$(date +%Y%m%d)" 2>/dev/null || echo 0) + 1 )) \
    > "$STATE_DIR/reg-count-$(date +%Y%m%d)"
}

main() {
  local ips mode hs

  ips=$(resolve_ips)
  if [ -z "${ips// /}" ]; then log "DNS for $MK_HOST failed; keeping current state"; return 1; fi
  log "mkissa IPs: $ips"

  ensure_wg_up || { log "wg create failed; routing direct"; apply_routes "$ips" "$NDEV"; return 1; }
  sync_allowedips "$ips" || log "AllowedIPs sync failed (continuing)"
  refresh_endpoint

  if tunnel_healthy "$ips"; then
    mode=tunnel
  else
    log "tunnel unhealthy on first test; bouncing"
    bounce_wg
    if tunnel_healthy "$ips"; then
      mode=tunnel
    else
      log "still unhealthy; attempting paced rotation"
      rotate_warp "$ips"
      if tunnel_healthy "$ips"; then
        mode=tunnel
      else
        mode=direct
      fi
    fi
  fi

  apply_routes "$ips" "$([ "$mode" = tunnel ] && echo wgcf || echo "$NDEV")"

  hs=$(ns wg show wgcf latest-handshakes 2>/dev/null | awk '{print $2}')
  log "mode=$mode handshake=${hs:-none}"
  [ "$mode" = tunnel ] || log "mkissa runs direct (breakers/cache absorb rate limits)"
}

main "$@"
