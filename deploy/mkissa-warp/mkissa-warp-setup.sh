#!/usr/bin/env bash
# Build the mkissa-only WARP scaffolding: netns + veth + host forwarding rules.
#
# Safety by construction: this script NEVER creates a route for mkissa IPs on
# the HOST. All tunnel interfaces and mkissa routes live inside the mkissawg
# namespace, so other providers (which share Cloudflare anycast IPs with
# api.mkissa.net) can never be blackholed again.
set -euo pipefail

NS=mkissawg
HDEV=vmk0
NDEV=vmk1
HADDR=10.77.0.1/30
NADDR=10.77.0.2/30
SUBNET=10.77.0.0/30
EGRESS=ens5
MK_HOST=api.mkissa.net
CONF=/etc/wireguard/wgcf.conf

LIB=/usr/local/sbin/mkissa-warp-wglib.sh
# shellcheck source=/dev/null
[ -f "$LIB" ] && . "$LIB"
ns() { ip netns exec "$NS" "$@"; }

log() { echo "$(date -u '+%F %T') setup: $*"; }

command -v ip >/dev/null || { log "CRITICAL: iproute2 missing"; exit 1; }
command -v wg-quick >/dev/null || { log "CRITICAL: wireguard-tools missing"; exit 1; }

# --- namespace -------------------------------------------------------------
ip netns add "$NS" 2>/dev/null || true

# --- veth pair (create once, keep peer inside the ns) ----------------------
if ! ip link show "$HDEV" >/dev/null 2>&1; then
  ip link add "$HDEV" type veth peer name "$NDEV"
fi
ip link set "$NDEV" netns "$NS" 2>/dev/null || true
ip addr replace "$HADDR" dev "$HDEV"
ip link set "$HDEV" up
ip netns exec "$NS" ip addr replace "$NADDR" dev "$NDEV"
ip netns exec "$NS" ip link set "$NDEV" up
ip netns exec "$NS" ip route replace default via 10.77.0.1

# --- DNS inside the namespace (ip netns exec bind-mounts /etc/netns/NS/*) --
mkdir -p "/etc/netns/$NS"
printf 'nameserver 8.8.8.8\nnameserver 1.1.1.1\n' > "/etc/netns/$NS/resolv.conf"

# --- host forwarding/nat for the veth subnet only --------------------------
[ "$(cat /proc/sys/net/ipv4/ip_forward)" = 1 ] || sysctl -w net.ipv4.ip_forward=1 >/dev/null
iptables -N DOCKER-USER 2>/dev/null || true
iptables -C DOCKER-USER -s "$SUBNET" -j ACCEPT 2>/dev/null || iptables -I DOCKER-USER 1 -s "$SUBNET" -j ACCEPT
iptables -C DOCKER-USER -d "$SUBNET" -j ACCEPT 2>/dev/null || iptables -I DOCKER-USER 1 -d "$SUBNET" -j ACCEPT
iptables -t nat -C POSTROUTING -s "$SUBNET" -o "$EGRESS" -j MASQUERADE 2>/dev/null || \
  iptables -t nat -A POSTROUTING -s "$SUBNET" -o "$EGRESS" -j MASQUERADE

# --- wireguard inside the namespace (raw create: wg-quick's route adds would
#     collide with the direct-fallback routes below) ------------------------
if ! ip netns exec "$NS" ip link show wgcf >/dev/null 2>&1; then
  if type wg_create >/dev/null 2>&1 && wg_create; then
    log "wgcf created inside namespace"
  else
    log "wg not up yet (will be managed by watchdog)"
  fi
fi

# --- SAFE INITIAL STATE: mkissa IPs route DIRECT until the watchdog proves
#     the tunnel actually passes traffic -----------------------------------
ips=$(getent ahostsv4 "$MK_HOST" | awk '{print $1}' | sort -u | tr '\n' ' ')
for ip in $ips; do
  # via host gateway (bare scope-link route would ARP for the remote IP and hang)
  ip netns exec "$NS" ip route replace "${ip}/32" via 10.77.0.1 dev "$NDEV" 2>/dev/null || true
done
log "namespace ready; mkissa routes = direct fallback (watchdog decides tunnel)"
