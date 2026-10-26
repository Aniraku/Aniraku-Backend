#!/usr/bin/env python3
"""Probe the mirror the way the app would: response bodies, not just codes."""
import json
import time
import urllib.request

URL = "http://127.0.0.1:4401/"

QUERIES = [
    ("media-by-id", "{ Media(id:11061,type:ANIME){ id idMal genres title{romaji} } }"),
    ("search-naruto", '{ Page(perPage:5){ media(search:"naruto",type:ANIME){ id title{romaji} } } }'),
    ("batch3", "{ Page(perPage:50){ media(id_in:[11061,21,113415],type:ANIME){ id idMal } } }"),
    ("popular-page1", "{ Page(perPage:5){ media(type:ANIME,sort:POPULARITY_DESC){ id title{romaji} } } }"),
    ("trending", "{ Page(perPage:5){ media(type:ANIME,sort:TRENDING_DESC){ id title{romaji} } } }"),
    ("idmal", "{ Media(idMal:11061,type:ANIME){ id idMal } }"),
]


def post(query):
    req = urllib.request.Request(
        URL, data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json"},
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, time.time() - t0, r.read()
    except urllib.error.HTTPError as e:
        return e.code, time.time() - t0, e.read()


def main():
    for name, q in QUERIES:
        code, dt, body = post(q)
        try:
            data = json.loads(body)
            errs = data.get("errors")
            payload = data.get("data")
            if isinstance(payload, dict) and "Page" in payload:
                n = len(payload["Page"].get("media") or [])
                first = (payload["Page"]["media"] or [{}])[0]
                summary = f"results={n} first={json.dumps(first, ensure_ascii=False)[:80]}"
            elif payload:
                summary = json.dumps(payload, ensure_ascii=False)[:110]
            else:
                summary = "data=null"
        except Exception:
            errs, summary = None, body[:110].decode("utf-8", "replace") if isinstance(body, bytes) else str(body)[:110]
        flag = " ERR" if errs else " ok"
        print(f"{name:16s} {code} {dt*1000:8.1f}ms{flag} {summary}")
        if errs:
            print(f"    errors: {json.dumps(errs)[:160]}")


if __name__ == "__main__":
    main()
