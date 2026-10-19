#!/usr/bin/env python3
"""Warm-set memory + latency probe for the local mirror (read-only usage
of the running service; no engine changes). Loads fresh-id batches range
195000+ (new-anime territory, untouched so far) and reports per-batch
latency plus the systemd cgroup MemoryPeak delta, so the SHARD_CACHE_SIZE
tuning number comes from data instead of a guess."""
import json
import time
import urllib.request

URL = "http://127.0.0.1:4401/"


def peak():
    # read the cgroup peak via systemctl (shoislam can read its own unit)
    import subprocess
    v = subprocess.run(
        ["systemctl", "--user", "show", "anilist-mirror.service", "-p", "MemoryPeak", "--value"],
        capture_output=True, text=True,
    ).stdout.strip()
    return int(v)


def query(ids):
    q = ("{ Page(perPage:50){ media(id_in:[" + ",".join(map(str, ids))
         + "], type:ANIME){ id idMal } } }")
    req = urllib.request.Request(
        URL, data=json.dumps({"query": q}).encode(),
        headers={"Content-Type": "application/json"},
    )
    t0 = time.time()
    body = urllib.request.urlopen(req, timeout=120).read()
    return time.time() - t0, body


def main():
    before = peak()
    print(f"MemoryPeak before: {before / 1048576:.0f} MB")
    times = []
    for i in range(6):
        ids = [195000 + i * 2500 + j for j in range(50)]
        dt, body = query(ids)
        times.append(dt)
        hits = body.count(b"idMal") // 2 + body.count(b'"idMal"')
        print(f"batch {i} (ids {ids[0]}-{ids[-1]}): {dt:.3f}s")
    after = peak()
    grew = (after - before) / 1048576
    print(f"MemoryPeak after: {after / 1048576:.0f} MB (grew {grew:.0f} MB for 6x50 fresh ids)")
    print(f"slowest fresh batch: {max(times):.3f}s | avg {sum(times) / len(times):.3f}s")


if __name__ == "__main__":
    main()
