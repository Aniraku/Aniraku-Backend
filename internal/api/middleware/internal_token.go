package middleware

import (
	"net/http"
	"os"
	"strings"
	"sync"
)

// InternalToken gates the API on a shared secret injected SERVER-SIDE by the
// Cloudflare Pages frontend (BACKEND_INTERNAL_TOKEN -> X-Internal-Token).
//
// Threat model: scrapers that mirror the Aniraku frontend can replay browser
// traffic, but the token never reaches any browser — it is added by the
// Pages function while proxying to this backend, so a scraper clone can only
// see the frontend's rendered output.
//
// The mechanism is fully env-driven — no trusted UA lists, origin names, or
// header semantics are hardcoded here. Everything lives in the deployment
// environment of the machine running the backend, not in this repository.
//
// Env (all read once at startup; rolling back = unset and restart):
//   - ANIRAKU_INTERNAL_TOKEN      the shared secret
//   - ANIRAKU_INTERNAL_ENFORCE    "1"/"true" to activate (default off)
//   - ANIRAKU_TRUSTED_UA_PREFIXES comma-separated UA prefixes exempt from
//     the gate (the native app's own client)
//   - ANIRAKU_TRUSTED_ORIGINS     comma-separated Origin/Referer substrings
//     exempt from the gate (browser traffic from the site's own pages)
//   - Health endpoints stay open for uptime monitors.
type internalTokenGate struct {
	once       sync.Once
	token      string
	tokens     map[string]struct{}
	enforce    bool
	uaPrefixes []string
	origins    []string
}

var internalGate internalTokenGate

func (g *internalTokenGate) init() {
	g.token = strings.TrimSpace(os.Getenv("ANIRAKU_INTERNAL_TOKEN"))
	// The Cloudflare Pages function presents the secret as
	// x-aniraku-internal sourced from ITS OWN env var name
	// (BACKEND_INTERNAL_TOKEN) — accept both spellings so the deployment
	// pairing works no matter which side named the variable.
	g.tokens = map[string]struct{}{}
	if g.token != "" {
		g.tokens[g.token] = struct{}{}
	}
	if alt := strings.TrimSpace(os.Getenv("BACKEND_INTERNAL_TOKEN")); alt != "" {
		g.tokens[alt] = struct{}{}
	}
	e := strings.ToLower(strings.TrimSpace(os.Getenv("ANIRAKU_INTERNAL_ENFORCE")))
	g.enforce = len(g.tokens) > 0 && (e == "1" || e == "true")
	g.uaPrefixes = splitList(os.Getenv("ANIRAKU_TRUSTED_UA_PREFIXES"))
	g.origins = splitList(os.Getenv("ANIRAKU_TRUSTED_ORIGINS"))
}

func splitList(v string) []string {
	out := []string{}
	for _, part := range strings.Split(v, ",") {
		if p := strings.ToLower(strings.TrimSpace(part)); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// trustedUA reports whether the UA starts with any trusted prefix (the
// native app's own client, exempt per operator decision).
func (g *internalTokenGate) trustedUA(ua string) bool {
	ua = strings.ToLower(strings.TrimSpace(ua))
	for _, prefix := range g.uaPrefixes {
		if prefix != "" && strings.HasPrefix(ua, prefix) {
			return true
		}
	}
	return false
}

// trustedOrigin reports whether the Origin/Referer belongs to a trusted
// site page (browser traffic arriving from the Aniraku frontend).
func (g *internalTokenGate) trustedOrigin(r *http.Request) bool {
	for _, h := range []string{"Origin", "Referer"} {
		v := strings.ToLower(r.Header.Get(h))
		for _, o := range g.origins {
			if o != "" && strings.Contains(v, o) {
				return true
			}
		}
	}
	return false
}

// InternalToken is the per-request gate: when enforcement is enabled,
// clients must carry the internal token or match a trusted exemption.
// Health endpoints stay open for uptime monitors.
func InternalToken(next http.Handler) http.Handler {
	internalGate.once.Do(internalGate.init)
	g := &internalGate
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !g.enforce {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/health") || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		if g.trustedUA(r.UserAgent()) {
			next.ServeHTTP(w, r)
			return
		}
		// The Pages function sends the secret as `x-aniraku-internal`; the
		// original contract was `X-Internal-Token`. Accept both so either
		// side can be rolled without breaking the site's proxied traffic.
		token := r.Header.Get("X-Internal-Token")
		if token == "" {
			token = r.Header.Get("X-Aniraku-Internal")
		}
		if token != "" {
			if _, ok := g.tokens[token]; ok {
				next.ServeHTTP(w, r)
				return
			}
		}
		if g.trustedOrigin(r) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Retry-After", "1")
		http.Error(w, "unauthorized client", http.StatusForbidden)
	})
}
