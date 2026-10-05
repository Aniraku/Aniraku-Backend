#!/usr/bin/env python3
"""solve_once — one Cloudflare clearance for animepahe, then exit.

Invoked by the Go provider (internal/streaming/animepahe.go) on demand:

    python solve_once.py        ->  stdout: "PAHE_SOLVE_RESULT {json}"
                                    stderr:  progress/errors
                                    exit 0  -> {"cookies": {...}, "ua": "..."}
                                    exit 1  -> challenge not cleared (retryable)
                                    exit 2  -> usage error

The clearance is ALSO written atomically to $PAHE_CLEARANCE_FILE first, so a
caller that gave up mid-solve (fan-out deadline) never wastes the work: the
next attempt reads the finished file instead of spawning the browser again.

Design constraints (operator: 2 vCPU / 2 GB box, super-lightweight mandate):
  * No resident anything. camoufox (real Firefox that passes the managed
    challenge from a datacenter IP) spawns, extracts cf_clearance + the exact
    UA, and the process exits. Verified 2026-10-05: every full-chain run
    passed; peak RSS ~453 MB transient.
  * The UA and cookie jar travel together: the clearance is bound to
    UA + TLS fingerprint + IP — a plain-TLS fetch with this cookie alone
    measures 403, the Firefox-impersonated client with this exact UA gets 200.
  * Attempts + budget are env-tunable; defaults cut a hung interstitial at
    45 s per attempt (2 attempts) so Go's 100 s cap is never hit blind.
"""

import json
import os
import re
import sys
import time

MARKER = "PAHE_SOLVE_RESULT "

SITE = os.environ.get("PAHE_SOLVER_SITE", "https://animepahe.pw/")
BUDGET_S = float(os.environ.get("PAHE_SOLVER_BUDGET_S", "45"))
ATTEMPTS = max(1, int(os.environ.get("PAHE_SOLVER_ATTEMPTS", "2")))
CLEARANCE_FILE = os.environ.get(
    "PAHE_CLEARANCE_FILE", "/tmp/aniraku_pahe_clearance.json"
)

CHALLENGE_RE = re.compile(r"just a moment|attention|checking|enable javascript", re.I)


def log(msg: str) -> None:
    print(f"[pahe-solve] {msg}", file=sys.stderr, flush=True)


def solve_once(deadline: float) -> dict:
    """One camoufox spawn: returns {cookies, ua} or raises."""
    from camoufox.sync_api import Camoufox

    with Camoufox(headless=True, humanize=True) as browser:
        page = browser.new_page()
        page.goto(SITE, wait_until="domcontentloaded", timeout=60000)
        title = ""
        while time.time() < deadline:
            try:
                title = page.title() or ""
            except Exception:
                title = ""
            if title and not title.lower().startswith("loading") and not CHALLENGE_RE.search(title):
                break
            page.wait_for_timeout(1500)
        cookies = {c["name"]: c["value"] for c in page.context.cookies()}
        ua = page.evaluate("() => navigator.userAgent")
        if "cf_clearance" not in cookies or not ua:
            raise RuntimeError(
                f"no clearance (title={title!r} cookies={sorted(cookies)})"
            )
        return {"cookies": cookies, "ua": ua}


def persist(result: dict) -> None:
    """Atomic write, owner-only: the file carries a live session token."""
    if not CLEARANCE_FILE:
        return
    tmp = CLEARANCE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(result, fh)
        os.chmod(tmp, 0o600)
        os.replace(tmp, CLEARANCE_FILE)
    except OSError as exc:
        log(f"clearance file not written: {exc}")


def main() -> int:
    last = "no attempt"
    for attempt in range(1, ATTEMPTS + 1):
        try:
            result = solve_once(time.time() + BUDGET_S)
        except Exception as exc:  # noqa: BLE001 — every failure mode reports the same way
            last = f"attempt {attempt}: {type(exc).__name__}: {exc}"
            log(last)
            continue
        persist(result)
        # Marker line first so a Go side reading line-by-line never waits on
        # stdout buffering; the payload is one line of JSON.
        sys.stdout.write(MARKER + json.dumps(result) + "\n")
        sys.stdout.flush()
        return 0
    print(last, file=sys.stderr, flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
