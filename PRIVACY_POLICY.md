# Privacy Policy

**Last updated: August 2026**

Aniraku ("we", "our") operates the backend API service for the Aniraku anime streaming platform at [aniraku.vercel.app](https://aniraku.vercel.app). This Privacy Policy describes how the service handles your information.

## What the backend does

The backend is a thin proxy and API layer. It:

- Proxies video-stream requests from the Aniraku app to upstream anime hosts (Miruro), so the hosts don't see your IP address — our server connects on your behalf.
- Fetches anime metadata from AniList and search fallbacks from Jikan (MyAnimeList).
- Reads and writes your user data (profile, watch history, bookmarks, favorites, comments, likes, settings) in Supabase — strictly scoped to your account via your signed JWT. Row-level security prevents any request from reading or modifying another account's data. The backend holds no service-level credentials for user data; every write is performed as you.

## Information We Collect

### Account and usage data
- We do not collect account information ourselves. Account data (email, username, profile fields, watch history, bookmarks, comments, likes, settings) is stored in Supabase (hosted on AWS, encrypted at rest) and accessed only through your authenticated session. Your password is never transmitted to or stored by us.
- Watch history and bookmarks are also mirrored to your browser's localStorage by the app; that copy stays on your device.

### Request metadata
- Standard web-server logs (IP address, user agent, timestamp, route) are kept transiently for rate limiting, error diagnosis and abuse prevention. These are not used to track you, are not linked to browsing profiles, and are not shared with third parties.

### Streaming data
- We do not log which anime you watch. Video-stream requests are proxied on demand and the upstream hosts receive our server's IP, not yours.

## What we don't do

- No tracking cookies, no fingerprinting, no analytics.
- No selling, renting or sharing of personal data with advertisers.
- No advertising on our own pages (upstream video hosts may display their own ads inside their players).

## Third-Party Services

- **Supabase** — authentication and database (AWS).
- **AniList** — anime metadata; search queries are forwarded to them.
- **Jikan / MyAnimeList** — search fallback.
- **Miruro** — video streaming sources, accessed through our proxy.
- **Vercel** — frontend hosting.
- **Render** — hosting for this backend service.

## Data Retention

- Account data: retained until you delete your account (Settings → Danger Zone → Delete Account in the app), which permanently removes your profile, watch history, bookmarks, comments, likes and settings.
- Watch history: retained until you clear it (Profile → Clear History), which removes both the server copy and the device copy.
- Request logs: kept only as long as needed for operational purposes, then deleted.

## Your Rights

- Access and correct your data in the app (Profile → Edit Profile).
- Delete your watch history (Profile → Clear History).
- Delete your account and all associated data (Settings → Danger Zone → Delete Account).
- Contact us (below) with any other privacy request; we respond within 30 days.

## Children

The service is not intended for children under 13. Mature (NSFW) titles are hidden by default and only surfaced when an adult account explicitly enables them. If we learn we have collected data from someone under 13, we will delete it promptly.

## Changes

We may update this policy; the revision date above reflects the latest version. Continued use of the service constitutes acceptance.

## Contact

Open an issue on [GitHub](https://github.com/Aniraku/Aniraku/issues) for privacy concerns.
