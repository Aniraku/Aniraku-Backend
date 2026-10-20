#!/usr/bin/env python3
"""Hourly freshness for the local AniList shard base.

Checks anilist-offline-db's latest GitHub release. On a new tag:
  1. fetch shard_manifest.json + docs from jsDelivr@main
  2. the manifest MUST carry the same releaseTag (coherent revision set)
  3. download every shard_*.json.gz release asset into a staging dir
  4. verify EVERY sha256 against the manifest
  5. atomically swap the ~/mirror-data symlink to the verified release
Any failure anywhere leaves the currently-served release untouched.

First run uses an adopt path: if the existing ~/mirror-data directory
already holds the newest release (verified in place), it is renamed into
position and only the symlink is created - no re-download.

The mirror engine and its API mechanism are untouched: it keeps reading
${DATA_BASE_URL}/shards/... ; nginx follows the symlink per request.
"""
import concurrent.futures
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request

BASE = os.path.expanduser("~/mirror-sync")
LIVE = os.path.expanduser("~/mirror-data")
JS = "https://cdn.jsdelivr.net/gh/Shoislam0311/anilist-offline-db@main/docs/api"
RELEASES = "https://api.github.com/repos/Shoislam0311/anilist-offline-db/releases/latest"
UA = {"User-Agent": "aniraku-mirror-sync/1.0 (henix user timer)"}
KEEP = 2  # keep the newest release dir + one previous for instant rollback


def get(url, timeout=120):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def latest_release():
    r = json.loads(get(RELEASES))
    return r["tag_name"], {a["name"]: a["browser_download_url"] for a in r["assets"]}


def synced_tag():
    try:
        return json.load(open(os.path.join(BASE, ".synced"))).get("tag")
    except Exception:
        return None


def manifest_tag(path):
    try:
        return json.load(open(os.path.join(path, "shard_manifest.json"))).get("releaseTag")
    except Exception:
        return None


def verify_shards(root, manifest):
    bad = []
    for s in manifest["shards"]:
        p = os.path.join(root, "shards", s["file"])
        h = hashlib.sha256()
        try:
            with open(p, "rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(chunk)
        except OSError:
            bad.append(s["file"])
            continue
        if h.hexdigest() != s["sha256"]:
            bad.append(s["file"])
    return bad


def promote(rel_dir):
    tmp = os.path.join(BASE, "current.tmp")
    if os.path.lexists(tmp):
        os.remove(tmp)
    os.symlink(rel_dir, tmp)
    os.replace(tmp, LIVE)


def prune():
    rels = sorted(
        (os.path.join(BASE, d) for d in os.listdir(BASE) if d.startswith("rel-")),
        key=os.path.getmtime, reverse=True,
    )
    live_target = os.path.realpath(LIVE)
    for old in rels[KEEP:]:
        if os.path.realpath(old) != live_target:
            shutil.rmtree(old, ignore_errors=True)
            print(f"pruned old release dir {os.path.basename(old)}")


def sync_new(tag, assets):
    stage = os.path.join(BASE, f"stage-{tag}")
    if os.path.lexists(stage):
        shutil.rmtree(stage)
    os.makedirs(os.path.join(stage, "shards"))

    manifest_bytes = get(f"{JS}/shard_manifest.json")
    manifest = json.loads(manifest_bytes)
    if manifest.get("releaseTag") != tag:
        print(f"docs on main still say {manifest.get('releaseTag')} != release {tag}; retry next tick")
        shutil.rmtree(stage, ignore_errors=True)
        return False
    open(os.path.join(stage, "shard_manifest.json"), "wb").write(manifest_bytes)
    for name in ("metadata.json", "schedule_index.json", "search_index.json"):
        open(os.path.join(stage, name), "wb").write(get(f"{JS}/{name}", timeout=180))

    shard_assets = {k: v for k, v in assets.items()
                    if k.startswith("shard_") and k.endswith(".json.gz")}
    if len(shard_assets) != len(manifest["shards"]):
        print(f"release has {len(shard_assets)} shard assets, manifest lists "
              f"{len(manifest['shards'])}; prior set untouched")
        shutil.rmtree(stage, ignore_errors=True)
        return False

    def grab(item):
        name, url = item
        dst = os.path.join(stage, "shards", name)
        tmp = dst + ".part"
        for _ in range(3):
            try:
                r = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=180)
                with r, open(tmp, "wb") as f:
                    shutil.copyfileobj(r, f)
                os.replace(tmp, dst)
                return True
            except Exception:
                time.sleep(3)
        print(f"failed: {name}")
        return False

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(12) as ex:
        ok = list(ex.map(grab, shard_assets.items()))
    if not all(ok):
        print(f"download incomplete ({ok.count(False)} shards failed); prior set untouched")
        shutil.rmtree(stage, ignore_errors=True)
        return False

    bad = verify_shards(stage, manifest)
    if bad:
        print(f"sha256 mismatch on {len(bad)} shards (first: {bad[0]}); prior set untouched")
        shutil.rmtree(stage, ignore_errors=True)
        return False

    rel_dir = os.path.join(BASE, f"rel-{tag}")
    if os.path.lexists(rel_dir):
        shutil.rmtree(rel_dir)
    os.rename(stage, rel_dir)
    promote(rel_dir)
    print(f"downloaded+verified {len(shard_assets)} shards in {time.time() - t0:.0f}s; promoted {tag}")
    return True


def adopt(tag, assets):
    """First run: ~/mirror-data is a plain dir -> verify in place, then symlink."""
    mtag = manifest_tag(LIVE)
    if mtag != tag:
        print(f"existing mirror-data holds {mtag}, release is {tag}; downloading instead")
        return sync_new(tag, assets)
    print("existing corpus matches latest release; verifying before adopting")
    bad = verify_shards(LIVE, json.load(open(os.path.join(LIVE, "shard_manifest.json"))))
    if bad:
        print(f"local corpus corrupt on {len(bad)} shards (first: {bad[0]}); downloading instead")
        return sync_new(tag, assets)
    rel_dir = os.path.join(BASE, f"rel-{tag}")
    if os.path.lexists(rel_dir):
        shutil.rmtree(rel_dir)
    os.rename(LIVE, rel_dir)          # LIVE disappears for microseconds only
    promote(rel_dir)                  # symlink back into place at once
    print(f"adopted existing verified corpus as rel-{tag} (no download)")
    return True


def spawn_warm(full):
    """Run the warmer as its own transient user unit. A plain Popen child
    lives inside THIS service's cgroup and is killed by systemd the moment
    the sync service exits (that silently killed every spawn since the hook
    shipped). systemd-run escapes the cgroup and keeps it alive+logged."""
    warm = os.path.expanduser("~/bin/mirror-warm.py")
    if not os.path.exists(warm):
        return
    unit = f"mirror-warm-{int(time.time())}"
    mode = ["--full"] if full else []
    try:
        subprocess.run(
            ["systemd-run", "--user", f"--unit={unit}", "--collect",
             sys.executable, warm, *mode],
            check=True, timeout=30, capture_output=True,
        )
        print(f"warm sweep spawned as {unit} ({'full' if full else 'top'})")
    except Exception as ex:
        print(f"warm spawn failed: {ex}")


def restart_and_warm():
    """Long cache TTLs (self-host tuning) mean stale pages would outlive the
    data swap; restart the engine right after promotion (only on release-swap
    days), then re-fill the hot surface so first users hit warm caches."""
    try:
        subprocess.run(["systemctl", "--user", "restart", "anilist-mirror.service"], check=True, timeout=90)
    except Exception as ex:
        print(f"engine restart failed: {ex}")
        return
    print("engine restarted after release swap")
    for _ in range(30):
        try:
            req = urllib.request.Request(
                "http://127.0.0.1:4401/",
                data=b'{"query":"{ Page(perPage:1) { media(id:21) { id } } }"}',
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5).read()
            break
        except Exception:
            time.sleep(1)
    spawn_warm(full=True)


def main():
    os.makedirs(BASE, exist_ok=True)
    lock = open(os.path.join(BASE, ".lock"), "a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("another sync is running; exiting")
        return
    tag, assets = latest_release()
    if tag == synced_tag() and manifest_tag(LIVE) == tag:
        print(f"up-to-date: {tag}")
        # keep the hourly-refreshed hot surface warm (3-10h TTLs would decay)
        spawn_warm(full=False)
        return
    if os.path.islink(LIVE):
        done = sync_new(tag, assets)
    else:
        done = adopt(tag, assets)
    if not done:
        return
    prune()
    json.dump({"tag": tag, "at": time.time()}, open(os.path.join(BASE, ".synced"), "w"))
    print(f"synced -> {tag}")
    restart_and_warm()


if __name__ == "__main__":
    main()
