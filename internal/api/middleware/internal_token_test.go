package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func gateFor(t *testing.T, token, enforce, uas, origins string) http.Handler {
	t.Helper()
	t.Setenv("ANIRAKU_INTERNAL_TOKEN", token)
	t.Setenv("ANIRAKU_INTERNAL_ENFORCE", enforce)
	t.Setenv("ANIRAKU_TRUSTED_UA_PREFIXES", uas)
	t.Setenv("ANIRAKU_TRUSTED_ORIGINS", origins)
	internalGate = internalTokenGate{}
	internalGate.init()
	return InternalToken(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func newReq(ua string) *http.Request {
	req := httptest.NewRequest("GET", "/api/v1/servers?animeId=21&episode=1&lang=sub", nil)
	req.Header.Set("User-Agent", ua)
	return req
}

func TestInternalTokenGate(t *testing.T) {
	cases := []struct {
		name    string
		token   string
		enforce string
		ua      string
		origin  string
		itok    string
		want    int
	}{
		{"off by default passes everything", "secret1", "", "some-scraper/1.0", "", "", 200},
		{"enforced: browser UA without token blocked", "secret1", "1", "Mozilla/5.0 Chrome/124", "", "", 403},
		{"enforced: trusted app UA allowed", "secret1", "1", "okhttp/4.9 AnirakuApp", "", "", 200},
		{"enforced: trusted origin allowed", "secret1", "1", "Mozilla/5.0 Chrome/124", "https://www.aniraku.tech", "", 200},
		{"enforced: valid token allowed", "secret1", "1", "Mozilla/5.0 Chrome/124", "", "secret1", 200},
		{"enforced: wrong token blocked", "secret1", "1", "Mozilla/5.0 Chrome/124", "", "", 403},
		{"health always open", "secret1", "1", "BetterUptime Monitor", "", "", 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := gateFor(t, tc.token, tc.enforce, "aniraku.anime.app,okhttp,dalvik", "aniraku.tech")
			req := newReq(tc.ua)
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
				req.Header.Set("Referer", tc.origin)
			}
			if tc.itok != "" {
				req.Header.Set("X-Internal-Token", tc.itok)
			}
			if tc.name == "health always open" {
				req = httptest.NewRequest("GET", "/api/v1/health", nil)
				req.Header.Set("User-Agent", tc.ua)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("%s: got %d, want %d", tc.name, rec.Code, tc.want)
			}
		})
	}
}
