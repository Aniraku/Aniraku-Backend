package core

import (
	"os"
	"strings"
)

// AnilistGraphQLEndpoint returns the AniList bridge every provider resolves
// against. Default is the public mirror (graphql.aniraku.tech); a VPS that
// self-hosts the mirror next to the backend overrides it via
// ANIRAKU_GRAPHQL_URL=http://127.0.0.1:4401, the .env route turns every
// internal AniList call (hentai gate, MAL-id lookup, provider title/search
// bridges, the API client) into a same-host hop: no external latency, no
// Vercel usage, and the public mirror keeps serving everyone else.
// Plain http is accepted deliberately: the override is only meaningful for
// a same-host localhost hop, where TLS would be ceremony.
func AnilistGraphQLEndpoint() string {
	if u := strings.TrimSpace(os.Getenv("ANIRAKU_GRAPHQL_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "https://graphql.aniraku.tech"
}
