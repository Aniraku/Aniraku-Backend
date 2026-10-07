package streaming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// heaveFixture serves the animeheaven.me three-GET flow (fastsearch →
// show page → gate.php) plus the mp4 endpoint the mirror probe hits.
// Handlers build absolute URLs from srv after it is assigned.
type heaveFixture struct {
	srv         *httptest.Server
	gateHits    atomic.Int64
	placeholder atomic.Bool
}

const (
	heaveShowID   = "nc7bk"
	heaveEp1Key   = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
	heaveEp2Key   = "b1b2c3d4e5f60718293a4b5c6d7e8f91"
	heaveTitleEN  = "Naruto Shippuden"
	heaveTitleROM = "NARUTO: Shippuuden"
)

func newHeaveFixture(t *testing.T) *heaveFixture {
	t.Helper()
	f := &heaveFixture{}
	mux := http.NewServeMux()

	// Search lists a WRONG title first: exact-normalized matching must
	// pick "Naruto Shippuden", not the first containment hit.
	mux.HandleFunc("/fastsearch.php", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("s"); got == "" {
			t.Errorf("fastsearch: missing query")
		}
		fmt.Fprintf(w, `<a class='ac' href='/anime.php?zz99'><div class='fastitem bc1 ac'><div class='fastimg'><img alt='Naruto'></div><div class='fastname'>Naruto</div></div></a>`+
			`<a class='ac' href='/anime.php?%s'><div class='fastitem bc1 ac'><div class='fastimg'><img alt='%s'></div><div class='fastname'>%s</div></div></a>`,
			heaveShowID, heaveTitleEN, heaveTitleEN)
	})

	mux.HandleFunc("/anime.php", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.RawQuery; got != heaveShowID {
			t.Errorf("show page: query %q, want %q", got, heaveShowID)
		}
		fmt.Fprintf(w, `<title>%s</title><div class='boldtext'>
<a class='c' id ="%s" onclick='gatea( "%s")' onmouseover='gateh( "%s")' href= 'gate.php'>
<div class='trackep0 watch bc2'><div class='trackep watchb bc'><div class='watch1 bc c'>Episode</div><div  class= ' watch2 bc '  >1</div><div class='watch1 bc c'>1187 d ago</div></div></div></a>
<a class='c' id ="%s" onclick='gatea( "%s")' onmouseover='gateh( "%s")' href= 'gate.php'>
<div class='trackep0 watch bc2'><div class='trackep watchb bc'><div class='watch1 bc c'>Episode</div><div  class= ' watch2 bc '  >2</div></div></div></a>`,
			heaveTitleEN, heaveEp1Key, heaveEp1Key, heaveEp1Key, heaveEp2Key, heaveEp2Key, heaveEp2Key)
	})

	mux.HandleFunc("/gate.php", func(w http.ResponseWriter, r *http.Request) {
		f.gateHits.Add(1)
		cookie := r.Header.Get("Cookie")
		if cookie != "key="+heaveEp1Key && cookie != "key="+heaveEp2Key {
			t.Errorf("gate: cookie %q, want key=<episode md5>", cookie)
		}
		if f.placeholder.Load() {
			fmt.Fprint(w, `<html><body>watch episode on animeheaven.me</body></html>`)
			return
		}
		fmt.Fprintf(w, `<video><source src='%s/cdn/one.mp4' type='video/mp4'><source src='%s/cdn/two.mp4' type='video/mp4'></video>`,
			f.srv.URL, f.srv.URL)
	})

	// Probeable mp4 bytes (ftyp magic judged by segmentBytesPlayable).
	mp4 := append([]byte{0x00, 0x00, 0x00, 0x18}, []byte("ftypisom")...)
	mp4 = append(mp4, make([]byte, 4096)...)
	mux.HandleFunc("/cdn/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.Write(mp4)
	})

	// AniList title fixture (shared with tensho: same query shape).
	mux.HandleFunc("/anilist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":{"Media":{"title":{"english":%q,"romaji":%q,"native":"NARUTO -ナルト- 疾風伝"},"synonyms":["Naruto Shippuden"]}}}`,
			heaveTitleEN, heaveTitleROM)
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func newHeaveTestProvider(f *heaveFixture) *HeaveProvider {
	p := NewHeaveProvider(zerolog.Nop(), f.srv.URL, f.srv.URL+"/anilist")
	// httptest servers are loopback: the netguard transport's SSRF guard
	// would block them, so tests use a plain client (production always
	// uses the guarded one built by NewHeaveProvider).
	p.client = &http.Client{Timeout: 30 * time.Second}
	return p
}

func heaveTestCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestHeaveSearch(t *testing.T) {
	f := newHeaveFixture(t)
	p := newHeaveTestProvider(f)

	results, err := p.Search(heaveTestCtx(t), heaveTitleEN)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	if results[0].ID != "zz99" || results[1].ID != heaveShowID {
		t.Fatalf("ids = %q,%q want zz99,%s", results[0].ID, results[1].ID, heaveShowID)
	}
	if results[1].Title != heaveTitleEN {
		t.Fatalf("title = %q, want %q", results[1].Title, heaveTitleEN)
	}
}

// Full resolve: AniList titles → exact show match → episode key → gate
// mirrors → probe → mp4 sources. Asserts the cached second call does not
// re-hit the gate.
func TestHeaveFindEpisodeSource(t *testing.T) {
	f := newHeaveFixture(t)
	p := newHeaveTestProvider(f)
	ctx := heaveTestCtx(t)

	sr, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr == nil || len(sr.Sources) != 2 {
		t.Fatalf("sources = %+v, want 2 mirrors", sr)
	}
	for _, s := range sr.Sources {
		if s.Type != "mp4" || s.Verification != "direct" || s.Quality != "auto" {
			t.Fatalf("source = %+v, want mp4/direct/auto", s)
		}
		if !strings.HasPrefix(s.URL, f.srv.URL+"/cdn/") {
			t.Fatalf("source URL = %q, want fixture cdn", s.URL)
		}
	}
	if sr.ServerName != heaveServerName {
		t.Fatalf("ServerName = %q, want %q", sr.ServerName, heaveServerName)
	}
	hits := f.gateHits.Load()

	// Cached: same resolve again must not re-hit gate.php.
	if _, err := p.FindEpisodeSource(ctx, "21", 1, "sub"); err != nil {
		t.Fatalf("second FindEpisodeSource: %v", err)
	}
	if f.gateHits.Load() != hits {
		t.Fatalf("gate hits %d after cached call, want %d", f.gateHits.Load(), hits)
	}

	// Cached results must be deep copies: mutating one cannot poison the
	// cache (copy-on-wrap rule).
	sr.Sources[0].URL = "http://mutated.invalid/x"
	sr2, err := p.FindEpisodeSource(ctx, "21", 1, "sub")
	if err != nil {
		t.Fatalf("third FindEpisodeSource: %v", err)
	}
	if strings.Contains(sr2.Sources[0].URL, "mutated") {
		t.Fatalf("cache poisoned by caller mutation: %q", sr2.Sources[0].URL)
	}
}

func TestHeaveMissingEpisode(t *testing.T) {
	f := newHeaveFixture(t)
	p := newHeaveTestProvider(f)

	sr, err := p.FindEpisodeSource(heaveTestCtx(t), "21", 99, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr != nil {
		t.Fatalf("source = %+v, want nil for unknown episode", sr)
	}
}

// The gate may answer with an interstitial player (Anivault's rule) —
// never expose it as a stream.
func TestHeavePlaceholderGate(t *testing.T) {
	f := newHeaveFixture(t)
	f.placeholder.Store(true)
	p := newHeaveTestProvider(f)

	sr, err := p.FindEpisodeSource(heaveTestCtx(t), "21", 1, "dub")
	if err != nil {
		t.Fatalf("FindEpisodeSource: %v", err)
	}
	if sr != nil {
		t.Fatalf("source = %+v, want nil for placeholder gate", sr)
	}
}

func TestHeaveFindEpisodes(t *testing.T) {
	f := newHeaveFixture(t)
	p := newHeaveTestProvider(f)

	eps, err := p.FindEpisodes(heaveTestCtx(t), heaveShowID)
	if err != nil {
		t.Fatalf("FindEpisodes: %v", err)
	}
	if len(eps) != 2 || eps[0].Number != 1 || eps[1].Number != 2 {
		t.Fatalf("episodes = %+v, want [1 2]", eps)
	}
}

// An AniList title the site does not carry is a hard error for explicit
// requests (the fan-out logs it and moves on).
func TestHeaveNoShowMatch(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	empty := NewHeaveProvider(zerolog.Nop(), srv.URL, srv.URL)
	empty.client = &http.Client{Timeout: 10 * time.Second}
	if _, err := empty.FindEpisodeSource(heaveTestCtx(t), "21", 1, "sub"); err == nil {
		t.Fatal("expected no-show-match error, got nil")
	}
}
