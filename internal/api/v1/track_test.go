package v1

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

func trackRequest(id string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/anime/"+id+"/track", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", id)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func resetTrackState() {
	trackCache = sync.Map{}
	trackFallback.mu.Lock()
	trackFallback.data, trackFallback.at, trackFallback.nextTry = nil, time.Time{}, time.Time{}
	trackFallback.mu.Unlock()
}

func TestGetAnimeTrackReportsLastReleasedEpisode(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/anime/ani/21" {
			t.Errorf("unexpected upstream path %q", r.URL.Path)
		}
		if r.Header.Get("User-Agent") == "" {
			t.Error("missing User-Agent")
		}
		io.WriteString(w, `{"subCount":100,"dubCount":4,"episodes":[
			{"number":1,"sub":true,"dub":true},
			{"number":2,"sub":true,"dub":false},
			{"number":98,"sub":true,"dub":false},
			{"number":97,"sub":true,"dub":true}
		]}`)
	}))
	defer upstream.Close()

	resetTrackState()
	oldBase, oldClient := trackAPIBase, trackClient
	trackAPIBase, trackClient = upstream.URL, upstream.Client()
	defer func() { trackAPIBase, trackClient = oldBase, oldClient }()

	h := &Handlers{log: zerolog.Nop()}
	rec := httptest.NewRecorder()
	h.GetAnimeTrack(rec, trackRequest("21"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got trackCounts
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	// dubCount is 4 but the last dub episode released is 97.
	if got.Sub != 98 || got.Dub != 97 {
		t.Fatalf("got %+v, want {Sub:98 Dub:97}", got)
	}
}

func TestTrackFallsBackWhenAniPMFails(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "blocked", http.StatusInternalServerError)
	}))
	defer upstream.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"42":{"mal_id":1,"format":"TV","sub":12,"dub":5}}`)
	}))
	defer fallback.Close()

	resetTrackState()
	oldBase, oldFallback, oldClient := trackAPIBase, trackFallbackURL, trackClient
	trackAPIBase, trackFallbackURL, trackClient = upstream.URL, fallback.URL, upstream.Client()
	defer func() { trackAPIBase, trackFallbackURL, trackClient = oldBase, oldFallback, oldClient }()

	h := &Handlers{log: zerolog.Nop()}
	rec := httptest.NewRecorder()
	h.GetAnimeTrack(rec, trackRequest("42"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got trackCounts
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Sub != 12 || got.Dub != 5 {
		t.Fatalf("got %+v, want {Sub:12 Dub:5}", got)
	}
}

func TestGetAnimeTrackRejectsBadID(t *testing.T) {
	h := &Handlers{log: zerolog.Nop()}
	for _, id := range []string{"", "abc", "0", "-1"} {
		rec := httptest.NewRecorder()
		h.GetAnimeTrack(rec, trackRequest(id))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("id %q: status = %d, want 400", id, rec.Code)
		}
	}
}
