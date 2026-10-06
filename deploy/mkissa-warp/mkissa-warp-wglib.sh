#!/usr/bin/env bash
# Shared mkissa WARP helpers (sourced by setup + watch).
# Manages the wgcf interface with raw ip/wg commands instead of wg-quick,
# because wg-quick's `ip route add` aborts when our direct-fallback route for
# the same /32 already exists ("File exists"). ip route replace never conflicts.
#
# Callers must set: NS (namespace), CONF (wireguard conf path).
# wg_create never adds mkissa routes itself — only apply_routes (ip route
# replace) does that, so there is no window where mkissa is blackholed.

if ! type ns >/dev/null 2>&1; then
  ns() { ip netns exec "$NS" "$@"; }
fi

conf_field() { awk -F' = ' -v k="$1" '$1 == k { print $2; exit }' "$CONF"; }

refresh_endpoint() {
  local ep pk
  ep=$(getent ahostsv4 engage.cloudflareclient.com 2>/dev/null | awk '{print $1}' | head -1)
  [ -n "${ep:-}" ] || return 0
  pk=$(awk '/^\[Peer\]/{p=1} p && /^PublicKey/{print $3; exit}' "$CONF")
  [ -n "$pk" ] && ns wg set wgcf peer "$pk" endpoint "${ep}:2408" 2>/dev/null || true
}

wg_create() {
  local strip_out addr v4 mtu
  strip_out=$(wg-quick strip wgcf 2>/dev/null) || return 1
  ns ip link add wgcf type wireguard 2>/dev/null || true
  # Endpoint is set separately (refresh_endpoint) with a resolved IP so a DNS
  # failure inside the namespace can never abort interface creation.
  if ! printf '%s\n' "$strip_out" | sed '/^Endpoint/d' | ns wg setconf wgcf /dev/stdin; then
    ns ip link del wgcf 2>/dev/null || true
    return 1
  fi
  addr=$(conf_field Address | awk '{print $1}' | sed 's/,//')
  v4=${addr%%/*}
  [ -n "$v4" ] && ns ip address replace "${v4}/32" dev wgcf 2>/dev/null || true
  mtu=$(conf_field MTU)
  ns ip link set mtu "${mtu:-1280}" up dev wgcf
  ns iptables -t nat -C POSTROUTING -o wgcf -j MASQUERADE 2>/dev/null || \
    ns iptables -t nat -A POSTROUTING -o wgcf -j MASQUERADE
  refresh_endpoint
}

wg_destroy() {
  ns ip link del wgcf 2>/dev/null || true
}
