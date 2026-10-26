#!/usr/bin/env python3
"""Probe with the FULL AniList field set the app realistically sends."""
import json
import time
import urllib.request

URL = "http://127.0.0.1:4401/"

FULL_MEDIA = """{ Media(id:11061,type:ANIME){
  id idMal
  title{ romaji english native userPreferred }
  coverImage{ extraLarge large medium color }
  bannerImage
  format status episodes duration season seasonYear
  source countryOfOrigin isLicensed isAdult
  description averageScore meanScore popularity favourites
  genres hashtag siteUrl
  studios(isMain:true){ nodes{ id name } }
  trailer{ id site thumbnail }
  tags{ name rank }
  nextAiringEpisode{ airingAt timeUntilAiring episode }
  rankings{ rank type context }
  relations{ edges{ relationType node{ id title{romaji} } } }
  characters(sort:ROLE){ edges{ role node{ id name{full} } voiceActors(sort:RELEVANCE){ id name{full} } } }
  externalLinks{ id url site }
  streamingEpisodes{ title thumbnail url site }
} }"""

FULL_DETAILS2 = """{ Page(perPage:10){
  media(sort:POPULARITY_DESC,type:ANIME){
    id title{romaji} coverImage{extraLarge color} bannerImage
    format episodes averageScore genres
    nextAiringEpisode{ episode airingAt }
    studios(isMain:true){ nodes{ name } }
  }
} }"""

FINDBY_MAL = """{ Media(idMal:11061,type:ANIME){
  id title{romaji} coverImage{extraLarge} episodes format status
  externalLinks{ url site } nextAiringEpisode{ episode }
} }"""

QUERIES = [
    ("full-media-11061", FULL_MEDIA),
    ("full-popular-p1", FULL_DETAILS2),
    ("full-idmal", FINDBY_MAL),
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
            d = json.loads(body)
            errs = d.get("errors")
            data = d.get("data")
        except Exception:
            errs, data, d = None, None, {}
        flag = "OK" if code == 200 and not errs else "FAIL"
        print(f"== {name}: {code} {dt*1000:.0f}ms {flag}")
        if errs:
            for e in errs[:4]:
                print("   err:", json.dumps(e, ensure_ascii=False)[:220])
        if data:
            s = json.dumps(data, ensure_ascii=False)
            print("   data len:", len(s), " head:", s[:120])
        if code != 200:
            print("   body:", body[:200])


if __name__ == "__main__":
    main()
