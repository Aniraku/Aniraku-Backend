#!/usr/bin/env python3
"""Local TLS splice for the mkissa API.

Runs ONLY inside the mkissawg network namespace. The mkissa engine (in the
api container) is pointed here via extra_hosts (api.mkissa.net -> 10.77.0.2),
so TLS stays end-to-end between the engine and mkissa: this process never
terminates TLS, it opens a TCP connection to api.mkissa.net from inside the
namespace (where the WARP tunnel or the direct-fallback route applies) and
pumps bytes both ways.

Nothing but mkissa traffic can reach this listener: 10.77.0.2 exists only
inside the namespace and the veth pair.
"""

import asyncio
import socket
import sys
import time

BIND_IP = "10.77.0.2"
BIND_PORT = 443
TARGET_HOST = "api.mkissa.net"
TARGET_PORT = 443
CONNECT_TIMEOUT = 6.0
IDLE_TIMEOUT = 120.0


def log(*parts):
    print(
        "%s relay: %s" % (time.strftime("%Y-%m-%dT%H:%M:%S"), " ".join(str(p) for p in parts)),
        flush=True,
    )


async def pipe(reader, writer):
    try:
        while True:
            try:
                data = await asyncio.wait_for(reader.read(65536), IDLE_TIMEOUT)
            except asyncio.TimeoutError:
                break
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            if not writer.is_closing():
                writer.close()
        except Exception:
            pass


async def resolve_v4():
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(
        TARGET_HOST,
        TARGET_PORT,
        family=socket.AF_INET,
        type=socket.SOCK_STREAM,
        proto=socket.IPPROTO_TCP,
    )
    seen = set()
    addrs = []
    for _fam, _type, _proto, _canon, addr in infos:
        if addr[0] not in seen:
            seen.add(addr[0])
            addrs.append(addr[0])
    return addrs


async def handle(client_reader, client_writer):
    up_writer = None
    try:
        addrs = await resolve_v4()
        if not addrs:
            raise OSError("no A record for " + TARGET_HOST)
        up_reader = None
        last_err = None
        for ip in addrs:
            try:
                up_reader, up_writer = await asyncio.wait_for(
                    asyncio.open_connection(ip, TARGET_PORT), CONNECT_TIMEOUT
                )
                break
            except (OSError, asyncio.TimeoutError) as exc:
                last_err = exc
        if up_reader is None:
            raise OSError("connect failed for %s: %s" % (TARGET_HOST, last_err))
        await asyncio.gather(
            pipe(client_reader, up_writer),
            pipe(up_reader, client_writer),
            return_exceptions=True,
        )
    except Exception as exc:
        log("splice error:", type(exc).__name__, str(exc)[:160])
    finally:
        for writer in (client_writer, up_writer):
            try:
                if writer is not None and not writer.is_closing():
                    writer.close()
            except Exception:
                pass


async def main():
    server = await asyncio.start_server(
        handle, BIND_IP, BIND_PORT, reuse_address=True, backlog=128
    )
    sock = server.sockets[0]
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    log("listening on %s:%d -> %s:%d" % (BIND_IP, BIND_PORT, TARGET_HOST, TARGET_PORT))
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
