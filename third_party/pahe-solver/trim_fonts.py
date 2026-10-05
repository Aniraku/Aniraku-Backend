#!/usr/bin/env python3
"""trim_fonts — drop camoufox's platform font groups a Linux browser never reads.

camoufox ships fingerprint fonts in groups; its own cache/groups.json maps
each OS to the groups it reads:

    "readBy": {"lin": ["L", "LM", "LMW", "LW"],
               "mac": ["LM", "LMW", "M", "MW"],
               "win": ["LMW", "LW", "MW", "W"]}

The solver runs headless on Linux, so M (810 MB), MW (20 MB) and W (354 MB)
are dead weight — 1.18 GB of the image. Measured 2026-10-05 after trimming:
clearance still issued (challenge title cleared, cf_clearance cookie present,
API recheck 200) and the groups do not regrow at runtime.

Run with HOME pointing at the browser owner's home so the cache path
resolves (see Dockerfile).
"""

import pathlib
import shutil

TRIM = ("M", "MW", "W")


def main() -> int:
    root = pathlib.Path.home() / ".cache" / "camoufox" / "browsers"
    if not root.is_dir():
        print(f"no camoufox cache at {root}")
        return 0
    trimmed = 0
    freed = 0
    for fonts in root.glob("**/fonts"):
        for group in TRIM:
            target = fonts / group
            if not target.exists():
                continue
            freed += sum(f.stat().st_size for f in target.rglob("*") if f.is_file())
            shutil.rmtree(target, ignore_errors=True)
            trimmed += 1
    print(f"trimmed {trimmed} font groups ({freed // (1 << 20)} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
