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

func TestRelayTryEmbedStreamDataShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/tryembed-stream", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ID      int    `json:"id"`
			Episode int    `json:"episode"`
			Lang    string `json:"lang"`
			Server  string `json:"server"`
			Nonce   string `json:"nonce"`
			Cookies string `json:"cookies"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.ID != 7 || in.Server != "beta" || in.Nonce == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if in.Cookies == "" {
			http.Error(w, "missing cookies", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"providers":[{"id":"beta","type":"hls","status":"ready","qualities":[{"name":"720p","height":720,"token":"T"}]}],"intro":null,"outro":null}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	r := newRelayTestClient(srv)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	body, err := r.TryEmbedStreamData(ctx, "a=b", "n", 7, 1, "sub", "beta")
	if err != nil {
		t.Fatalf("TryEmbedStreamData: %v", err)
	}
	var sd tryembedStreamData
	if err := json.Unmarshal(body, &sd); err != nil || len(sd.Providers) != 1 || sd.Providers[0].ID != "beta" {
		t.Fatalf("stream_data = %s, err %v", body, err)
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
	// Direct legs (page+bootstrap+/s/) run against the fixture origin;
	// only stream_data is relayed.
	mux.HandleFunc("/embed/anime/7/1/sub", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><script>window.BOOTSTRAP_TICKET="fixture-ticket";</script></html>`)
	})
	mux.HandleFunc("/api/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"embedNonce":"fixture-nonce"}`)
	})
	mux.HandleFunc("/tryembed-stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"providers":[{"server":"x","id":"beta","type":"hls","status":"ready","qualities":[{"name":"720p","height":720,"token":"TOK-R"}],"captions":[]}],"intro":null,"outro":null}`)
	})
	mux.HandleFunc("/s/TOK-R.m3u8", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, contentBase+"/r/master.m3u8?exp=9&sig=x", http.StatusFound)
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
	if !strings.HasSuffix(sr.Sources[0].URL, "/r/master.m3u8?exp=9&sig=x") {
		t.Errorf("URL = %q, want followed signed target", sr.Sources[0].URL)
	}
	if len(sr.ServerNames) != 1 || sr.ServerNames[0] != "Beta" {
		t.Errorf("ServerNames = %v, want [Beta]", sr.ServerNames)
	}
}
