#!/usr/bin/env python3
"""Canonical warm-set for the local AniList mirror engine (127.0.0.1:4401).

The engine caches page/card results per (clause, args, order, page, perPage,
projection, children) with TTLs tuned for self-hosting (3-10h). Builds are
lazy: the FIRST hit for a never-seen page/sort/projection combination pays the
full shard-merge cost (seconds for deep pages), so unseen catalog cells feel
slow until something touches them. This script walks the exact query shapes
the Aniraku-App sends - byte-identical query text and variables, copied from
Aniraku-App/lib/anilist.ts so cache keys line up (the engine hashes the query
projection into the cache key) - and therefore turns every routine app request
into a warm cache hit.

Modes:
  --top   hot surface only (default): home + rails, pages 1-2 of the main
          sorts, current schedule windows, details/recommendations for the
          top-N trending ids.
  --full  everything above plus the extra catalog sorts (pages to 4),
          the random-pool base pages, and more details. Run after an engine
          restart or a release swap.
"""
import argparse
import json
import sys
import time
import urllib.request

URL = "http://127.0.0.1:4401/"
UA = {"Content-Type": "application/json", "User-Agent": "aniraku-mirror-warm/1.0"}

# ---- app field texts (verbatim from Aniraku-App/lib/anilist.ts) -------------
FIELDS = ("\n  id type title { romaji english native } coverImage { large extraLarge color } bannerImage\n"
          "  description(asHtml: false) genres format status episodes duration averageScore popularity\n"
          "  season seasonYear isAdult idMal nextAiringEpisode { episode airingAt }\n"
          "  trailer { id site thumbnail }\n")
# build detailFields exactly: `${fields}\n  relations {\n    edges {\n      relationType\n      node { ${fields} }\n    }\n  }\n`
DETAIL_FIELDS = (FIELDS + "\n  relations {\n    edges {\n      relationType\n"
                 "      node { " + FIELDS + " }\n    }\n  }\n")
SLIM = ("\n  id type title { romaji english native } coverImage { large extraLarge } format status episodes\n"
        "  nextAiringEpisode { episode airingAt }\n")
POOL_FIELDS = ("\n  id type title { romaji english native } coverImage { large extraLarge color } format status episodes genres season seasonYear isAdult\n")
SCHEDULE_FIELDS = ("\n  id type title { romaji english native } coverImage { large extraLarge } format status\n")

PAGE_QUERY = ("query MediaPage($page: Int!, $perPage: Int!, $sort: [MediaSort], $search: String, "
              "$status: MediaStatus, $season: MediaSeason, $seasonYear: Int, $genre: String, "
              "$format: MediaFormat, $isAdult: Boolean) {\n"
              "  Page(page: $page, perPage: $perPage) { pageInfo { currentPage hasNextPage total } "
              "media(type: ANIME, isAdult: $isAdult, sort: $sort, search: $search, status: $status, "
              "season: $season, seasonYear: $seasonYear, genre: $genre, format: $format) { "
              + FIELDS + " } }\n}")
HOME_QUERY = ("query Home($isAdult: Boolean) {\n"
              "  trending: Page(page: 1, perPage: 12) { media(type: ANIME, isAdult: $isAdult, sort: [TRENDING_DESC, POPULARITY_DESC]) { " + FIELDS + " } }\n"
              "  popular: Page(page: 1, perPage: 12) { media(type: ANIME, isAdult: $isAdult, sort: [POPULARITY_DESC]) { " + FIELDS + " } }\n"
              "  upcoming: Page(page: 1, perPage: 12) { media(type: ANIME, isAdult: $isAdult, status: NOT_YET_RELEASED, sort: [POPULARITY_DESC]) { " + FIELDS + " } }\n}")
HOME_RAILS_QUERY = ("query HomeRails($isAdult: Boolean) {\n"
                    "  ongoing: Page(page: 1, perPage: 12) { media(type: ANIME, isAdult: $isAdult, status: RELEASING, sort: [POPULARITY_DESC]) { " + SLIM + " } }\n"
                    "  topMovies: Page(page: 1, perPage: 12) { media(type: ANIME, isAdult: $isAdult, format: MOVIE, sort: [SCORE_DESC]) { " + SLIM + " } }\n"
                    "  justFinished: Page(page: 1, perPage: 12) { media(type: ANIME, isAdult: $isAdult, status: FINISHED, sort: [END_DATE_DESC]) { " + SLIM + " } }\n}")
POOL_QUERY = ("query AnimePool($perPage: Int!, $pageA: Int!, $pageB: Int!, $pageC: Int!, "
              "$sort: [MediaSort], $genre: String, $isAdult: Boolean) {\n"
              "  a: Page(page: $pageA, perPage: $perPage) { media(type: ANIME, isAdult: $isAdult, sort: $sort, genre: $genre) { " + POOL_FIELDS + " } }\n"
              "  b: Page(page: $pageB, perPage: $perPage) { media(type: ANIME, isAdult: $isAdult, sort: $sort, genre: $genre) { " + POOL_FIELDS + " } }\n"
              "  c: Page(page: $pageC, perPage: $perPage) { media(type: ANIME, isAdult: $isAdult, sort: $sort, genre: $genre) { " + POOL_FIELDS + " } }\n}")
AIRING_BATCH_QUERY = ("query AiringScheduleBatch($perPage: Int!, $startAt: Int, $endAt: Int) {\n"
                      "  first: Page(page: 1, perPage: $perPage) { pageInfo { currentPage hasNextPage total } "
                      "airingSchedules(notYetAired: true, airingAt_greater: $startAt, airingAt_lesser: $endAt, sort: [TIME]) "
                      "{ airingAt episode media { " + SCHEDULE_FIELDS + " } } }\n"
                      "  second: Page(page: 2, perPage: $perPage) { pageInfo { currentPage hasNextPage total } "
                      "airingSchedules(notYetAired: true, airingAt_greater: $startAt, airingAt_lesser: $endAt, sort: [TIME]) "
                      "{ airingAt episode media { " + SCHEDULE_FIELDS + " } } }\n}")
DETAIL_QUERY = "query Anime($id: Int!) { Media(id: $id, type: ANIME) { " + DETAIL_FIELDS + " } }"
RECOMMEND_QUERY = ("query Recommendations($mediaId: Int!) {\n"
                   "  Media(id: $mediaId) {\n"
                   "    recommendations(sort: RATING_DESC, perPage: 12) {\n"
                   "      edges {\n"
                   "        node {\n"
                   "          rating\n"
                   "          mediaRecommendation { " + FIELDS + " }\n"
                   "        }\n"
                   "      }\n"
                   "    }\n"
                   "  }\n"
                   "}")


def post(query, variables, timeout=240):
    body = json.dumps({"query": query, "variables": variables}).encode()
    req = urllib.request.Request(URL, data=body, headers=UA)
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    dt = time.time() - t0
    try:
        ok = "errors" not in json.loads(raw)
    except Exception:
        ok = False
    return dt, len(raw), ok


def run(cells, deadline, log):
    """cells: iterable of (name, query, variables). Skipped when past deadline."""
    total = 0.0
    for name, query, variables in cells:
        left = deadline - time.time()
        if left <= 0:
            log(f"budget exhausted; skipping {name}")
            return total
        try:
            dt, size, ok = post(query, variables)
            if not ok:
                log(f"{name}: ERROR body {size}B")
            else:
                log(f"{name}: {dt:.2f}s {size}B")
            total += dt
        except Exception as ex:
            log(f"{name}: FAILED {str(ex)[:70]}")
    return total


def window_days():
    """Midnight-anchored 7-day windows like the app's currentWeekWindow():
    one per plausible device-TZ anchor (UTC and UTC+6 cover the main user base)."""
    now = int(time.time())
    utc_mid = now - (now % 86400)
    bd_mid = utc_mid - 6 * 3600  # UTC+6 devices: their local midnight is 6h earlier
    return [("schedule UTC-day", {"perPage": 50, "startAt": utc_mid, "endAt": utc_mid + 7 * 86400}),
            ("schedule BD-day", {"perPage": 50, "startAt": bd_mid, "endAt": bd_mid + 7 * 86400})]


def top_ids():
    """Trending page 1 (perPage 20, exact app text) -> id list for detail warm."""
    try:
        _, raw, _ = post(PAGE_QUERY, {"page": 1, "perPage": 20, "sort": ["TRENDING_DESC"], "isAdult": False})
        ids = [m["id"] for m in json.loads(raw).get("data", {}).get("Page", {}).get("media", [])]
        return ids or [1, 5, 21, 25, 30, 16498, 16936, 196, 999, 11061]
    except Exception:
        return [1, 5, 21, 25, 30, 16498, 16936, 196, 999, 11061]


def cells_for(mode, log):
    sorts_top = ["TRENDING_DESC", "POPULARITY_DESC", "SCORE_DESC"]
    sorts_full = sorts_top + ["START_DATE_DESC", "EPISODES_DESC", "FAVOURITES_DESC"]
    cells = []
    # home + rails (two isAdult variants: the app sends the key stripped or false)
    for v in ({}, {"isAdult": False}):
        cells.append((f"home{v}", HOME_QUERY, dict(v)))
    for v in ({}, {"isAdult": False}):
        cells.append((f"rails{v}", HOME_RAILS_QUERY, dict(v)))
    # trending detail page lands in queries through pageQuery too
    cells.append(("page trending p1", PAGE_QUERY, {"page": 1, "perPage": 20, "sort": ["TRENDING_DESC"], "isAdult": False}))
    sorts = sorts_top if mode == "top" else sorts_full
    pages = range(1, 3) if mode == "top" else range(1, 5)
    for s in sorts:
        for p in pages:
            cells.append((f"page {s} p{p}x20", PAGE_QUERY,
                          {"page": p, "perPage": 20, "sort": [s], "isAdult": False}))
    if mode == "full":
        # catalog tiles use perPage 12
        for s in sorts:
            cells.append((f"page {s} p1x12", PAGE_QUERY,
                          {"page": 1, "perPage": 12, "sort": [s], "isAdult": False}))
        # random pool base pages (app default [1,2,3] perPage 50; then a few deeper)
        pool_pages = [(1, 2, 3), (4, 5, 6)] + ([(7, 8, 9), (10, 11, 12)] if mode == "full" else [])
        for s in ("ID_DESC", "POPULARITY_DESC"):
            for a, b, c in pool_pages:
                cells.append((f"pool {s} {a}/{b}/{c}", POOL_QUERY,
                              {"perPage": 50, "pageA": a, "pageB": b, "pageC": c,
                               "sort": [s], "isAdult": False}))
    for name, w in window_days():
        cells.append((name, AIRING_BATCH_QUERY, w))
    ids = top_ids()
    for i in ids[:24 if mode == "full" else 12]:
        cells.append((f"detail {i}", DETAIL_QUERY, {"id": i}))
    for i in ids[:12 if mode == "full" else 6]:
        cells.append((f"rec {i}", RECOMMEND_QUERY, {"mediaId": i}))
    return cells


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="complete warm set")
    ap.add_argument("--budget", type=int, default=0, help="seconds of wall time (0 = default)")
    args = ap.parse_args()
    mode = "full" if args.full else "top"
    deadline = time.time() + (args.budget or (900 if mode == "full" else 480))

    def log(msg):
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

    log(f"mirror-warm mode={mode}")
    log(f"engine says ping: {post('{ Page(perPage:1){ media(id:21){ id } } }', {}, timeout=20)[0]:.2f}s")
    t = run(cells_for(mode, log), deadline, log)
    log(f"done in {t:.0f}s query-time")


if __name__ == "__main__":
    main()
