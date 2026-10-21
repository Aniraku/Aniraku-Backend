# Operating notes

Upstream-behavior log for the streaming package. Dates are check dates,
not contract. New findings belong in commit messages or here, never
inside source comments.

## Provider checks

- 2026-09-30 kaa: kaa.lt search/listings/watch 200 from this egress, no
  datacenter block; krussdomi masters 200 direct and via the media proxy;
  segment bytes need `Origin: https://krussdomi.com` (Referer alone 403s).
- 2026-10-01 kaa: same show, two listing slugs (ja-JP ep1 vs en-US ep1),
  dub lane can carry zero embeds while sub resolves.
- 2026-10-02 animegg, aniwaves, lee: flows verified end-to-end from the
  production egress.
- 2026-10-03 megavid: MegaPlay shape `({mal|ani}/{id}/{ep}/{lang}/source)`verified.
- 2026-10-07 tensho: zangetsu flow verified end-to-end; animedl downloader
  adopted over the zoko/kiwi fetcher; manager gates dropped (animeheaven,
  zenime, ogflix among them).
- 2026-10-18 anixo, anv: verified end-to-end from the VPS egress; anixo's
  dub flag proved lit-for-nothing (hasDub false on every probed show), so
  anixo ships sub-only pending a live dub verification.

## Latency findings (2026-10-20)

- kaa.lt listing page fetch varies wildly per draw: 0.9s / 6.2s / 11.2s
  for the identical page. The 6s listing cap turns a guaranteed budget
  burn into a fast fail.
- krussdomi and cdn.4animo: fresh connections answer in under a second
  (python probes), while the Go client on reused keep-alives hangs to the
  full budget. kaa now dials fresh per request.
- One hung fan-out slot used to erase the other slots' completed results
  (context deadline discarded everything). Collectors now ship survivors.
- Dead tensho hops measured long on reused paths; the per-call cap is 6s.

## Manager history

- A hand-counted WaitGroup Add() once deadlocked every /servers call
  after a provider removal; the fan-out is now a data table whose count
  derives from its own length.
- The final server list is never snapshotted: tokenized URLs expire, and
  a cache once froze localhost health-probe URLs into public responses
  for a full TTL.

## API layer history

- 2026-10-14 proxy egress shaping was a temporary order (was to expire
  2026-10-17); before touching it, re-measure the origin's egress first.
- 2026-10-16/17 proxy memory guard + fill limiter landed after the OOM
  wedges (kernel killed aniraku-server 3x on Oct 15-16).
- 2026-10-17 the frontends' vercel routes went stale; cors.go sources
  were migrated to the main production origin.
- 2026-09-27 tmdb deadlock: episode enrichment blocked the catalog
  Range callback through setCachedEpisodes for a cache-wide cascade.

## kaa island

- 2026-10-21 the cat-player Astro island's props carry full subtitle
  metadata per track (format, language code, native name, file URL);
  subtitle tracks are decoded from there instead of scraped from
  bare URLs.

## tensho tracks

- 2026-10-21 hd-2 getSources tracks carry no language field on any entry;
  kind is the shape's only metadata ("captions" for text, "thumbnails"
  for the sprite preview). The sprite rides along under the label
  "Subtitles" and reached Hoshi's subtitle list as a langless track;
  tenshoSubs now drops kind "thumbnails". Caption labels are decorated
  ("Spanish (Latin American)", "French (European)") and resolve through
  label aliasing.
- 2026-10-21 episode embeds key on ani/mal per-episode values that
  already embed the episode ("185874/9"): /embed/ani/185874/9/sub, not
  /embed/ani/<ep-less>/sub, which 404s.
