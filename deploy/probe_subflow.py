#!/usr/bin/env python3
"""Replay the Aniraku-App subtitle flow exactly: wrap each subtitle URL the
/servers response returns through /api/v1/proxy with the app's UA + provider
headers blob, and report status/size so failing hosts/providers are visible."""
import json
import random
import string
import urllib.parse
import urllib.request

UA = "okhttp/4.12.0"
GROUPS = json.load(open("/tmp/servers185874.json"))

# when a group has NO headers, the app sends no headers param (anirakuProxyUrl)
def rn():
    ms = 1792520000000 + random.randint(0, 10_000_000)
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    return f"{ms}-{suffix}"


def fetch(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "*/*",
    })
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status, len(r.read())
    except urllib.error.HTTPError as e:
        return e.code, len(e.read())
    except Exception as ex:
        return 0, str(ex)[:60]


def main():
    for idx, g in enumerate(GROUPS):
        name = f"{g.get('name')} ({g.get('provider')})"
        src = (g.get("sources") or [{}])[0]
        subs = src.get("subtitles") or []
        if not subs:
            print(f"[{idx}] {name}: no subtitles, skip")
            continue
        headers = g.get("headers")
        sub = subs[0]
        params = {"url": sub["url"], "rn": rn()}
        if headers:
            params["headers"] = json.dumps(headers, separators=(",", ":"))
        proxied = ("https://api.aniraku.tech/api/v1/proxy?"
                   + urllib.parse.urlencode(params))
        code, size = fetch(proxied)
        print(f"[{idx}] {name}: sub[0] {sub.get('label')} -> {code} {size}B  host={urllib.parse.urlparse(sub['url']).netloc}")


if __name__ == "__main__":
    main()
