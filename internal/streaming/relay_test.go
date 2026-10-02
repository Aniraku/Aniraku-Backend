package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newRelayTestClient(srv *httptest.Server) *RelayClient {
	return &RelayClient{base: srv.URL, key: "test-key", client: srv.Client()}
}

func TestRelayRequiresKey(t *testing.T) {
	var gotKey string
	mux := http.NewServeMux()
	mux.HandleFunc("/vidnest", func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("X-Relay-Key")
		fmt.Fprint(w, `{"data":"","encrypted":false}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	r := newRelayTestClient(srv)
	if _, err := r.VidNestFetch(context.Background(), 1, 1, "sub"); err != nil {
		t.Fatalf("VidNestFetch: %v", err)
	}
	if gotKey != "test-key" {
		t.Fatalf("relay key header = %q, want test-key", gotKey)
	}
}

func TestRelayTryEmbedResolveShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/tryembed", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ID      int    `json:"id"`
			Episode int    `json:"episode"`
			Lang    string `json:"lang"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.ID != 7 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"mirrors":[{"server":"astra","type":"hls","url":"https://cdn.example/m.m3u8","captions":[{"label":"English","lang":"en","url":"https://cdn.example/e.vtt"}]}],"intro":{"start":31,"end":111},"outro":null}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	r := newRelayTestClient(srv)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	mirrors, intro, outro, err := r.TryEmbedResolve(ctx, 7, 1, "sub")
	if err != nil {
		t.Fatalf("TryEmbedResolve: %v", err)
	}
	if len(mirrors) != 1 || mirrors[0].Server != "astra" || mirrors[0].URL != "https://cdn.example/m.m3u8" {
		t.Fatalf("mirrors = %+v", mirrors)
	}
	if intro == nil || intro.Start != 31 || outro != nil {
		t.Fatalf("skips = %+v / %+v", intro, outro)
	}
}

func TestVidNestViaRelay(t *testing.T) {
	var contentBase string
	mux := http.NewServeMux()
	mux.HandleFunc("/vidnest", func(w http.ResponseWriter, r *http.Request) {
		payload, _ := json.Marshal(map[string]any{
			"sources": []any{map[string]any{"file": contentBase + "/r/master.m3u8", "type": "hls"}},
			"tracks":  []any{},
			"status":  200, "success": true,
		})
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"data":%q,"encrypted":true}`, vidnestEncode(payload))
	})
	mux.HandleFunc("/r/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\n/r/media.m3u8\n")
	})
	mux.HandleFunc("/r/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:10,\n/r/seg.ts\n")
	})
	mux.HandleFunc("/r/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	contentBase = srv.URL

	p := NewVidNestProvider(zerolog.Nop(), srv.URL)
	p.client = srv.Client()
	p.relay = newRelayTestClient(srv)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource via relay: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want 1 source, got %+v", sr)
	}
	if !strings.HasSuffix(sr.Sources[0].URL, "/r/master.m3u8") {
		t.Errorf("URL = %q", sr.Sources[0].URL)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Nest" {
		t.Errorf("ServerNames = %v", sr.ServerNames)
	}
}

func TestTryEmbedViaRelay(t *testing.T) {
	var contentBase string
	mux := http.NewServeMux()
	mux.HandleFunc("/tryembed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"mirrors":[{"server":"beta","type":"hls","url":"%s/r/master.m3u8","captions":[]}],"intro":null,"outro":null}`, contentBase)
	})
	mux.HandleFunc("/r/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000\n/r/media.m3u8\n")
	})
	mux.HandleFunc("/r/media.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXTINF:10,\n/r/seg.ts\n")
	})
	mux.HandleFunc("/r/seg.ts", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte{0x47, 0x40, 0x00, 0x10})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	contentBase = srv.URL

	p := NewTryEmbedProvider(zerolog.Nop(), srv.URL)
	p.transport = srv.Client().Transport
	p.relay = newRelayTestClient(srv)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sr, err := p.FindEpisodeSource(ctx, "7", 1, "sub")
	if err != nil {
		t.Fatalf("FindEpisodeSource via relay: %v", err)
	}
	if sr == nil || len(sr.Sources) != 1 {
		t.Fatalf("want 1 source, got %+v", sr)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Beta" {
		t.Errorf("ServerNames = %v, want [Beta]", sr.ServerNames)
	}
}
