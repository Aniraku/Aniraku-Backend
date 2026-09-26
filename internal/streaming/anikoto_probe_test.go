package streaming

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestSegmentBytesPlayable(t *testing.T) {
	ts := append([]byte{0x47}, []byte("mpeg-ts-payload")...)
	fmp4 := append([]byte{0, 0, 0, 24}, []byte("ftypmp42rest")...)
	moof := append([]byte{0, 0, 0, 8}, []byte("moofpayload")...)
	id3 := append([]byte("ID3\x04\x00"), []byte("timed-metadata")...)
	nested := []byte("#EXTM3U\n#EXT-X-VERSION:3\n")
	html := []byte("<!DOCTYPE html>\n<html class=\"no-js\" lang=\"en\">\n<head>")
	png := append([]byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"), []byte("1x1-decoy")...)
	cases := []struct {
		name string
		head []byte
		want bool
	}{
		{name: "mpeg-ts", head: ts, want: true},
		{name: "fmp4 init", head: fmp4, want: true},
		{name: "fmp4 moof", head: moof, want: true},
		{name: "id3 timed metadata", head: id3, want: true},
		{name: "nested playlist", head: nested, want: true},
		{name: "cloudflare html", head: html, want: false},
		{name: "png decoy", head: png, want: false},
		{name: "empty", head: nil, want: false},
		{name: "text garbage", head: []byte("Internal Server Error"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := segmentBytesPlayable(tc.head); got != tc.want {
				t.Fatalf("segmentBytesPlayable(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestFirstPlaylistURL(t *testing.T) {
	body := "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n\n  hd/index.m3u8  \n"
	if got := firstPlaylistURL(body, "https://cdn.example/a/master.m3u8"); got != "https://cdn.example/a/hd/index.m3u8" {
		t.Fatalf("relative = %q", got)
	}
	if got := firstPlaylistURL("#EXTM3U\nhttps://other.example/x.m3u8\n", "https://cdn.example/a/master.m3u8"); got != "https://other.example/x.m3u8" {
		t.Fatalf("absolute = %q", got)
	}
	if got := firstPlaylistURL("#EXTM3U\n/root/index.m3u8\n", "https://cdn.example/a/master.m3u8"); got != "https://cdn.example/root/index.m3u8" {
		t.Fatalf("path-absolute = %q", got)
	}
	if got := firstPlaylistURL("#EXTM3U\n# comment only\n", "https://cdn.example/a/master.m3u8"); got != "" {
		t.Fatalf("empty = %q, want empty", got)
	}
}

// probeMux serves master -> media -> segment; segBody controls the verdict.
func probeMux(segStatus int, segBody []byte) *httptest.Server {
	var mux http.ServeMux
	mux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nmedia.m3u8\n"))
	})
	mux.HandleFunc("/media.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:1\nseg.ts\n"))
	})
	mux.HandleFunc("/seg.ts", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(segStatus)
		_, _ = w.Write(segBody)
	})
	return httptest.NewServer(&mux)
}

func TestProbeHLSegments(t *testing.T) {
	tsSeg := append([]byte{0x47}, []byte(strings.Repeat("v", 2000))...)
	htmlSeg := []byte("<!DOCTYPE html><html><head><title>403</title></head></html>")
	pngSeg := append([]byte("\x89PNG\r\n\x1a\n"), []byte(strings.Repeat("p", 200))...)

	t.Run("playable chain", func(t *testing.T) {
		server := probeMux(200, tsSeg)
		defer server.Close()
		p := NewAnikotoProvider(zerolog.Nop())
		p.client = server.Client()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if !p.probeHLSegments(ctx, server.URL+"/master.m3u8", server.URL) {
			t.Fatal("healthy master->media->segment chain must pass")
		}
	})
	t.Run("blocked segment", func(t *testing.T) {
		server := probeMux(403, htmlSeg)
		defer server.Close()
		p := NewAnikotoProvider(zerolog.Nop())
		p.client = server.Client()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if p.probeHLSegments(ctx, server.URL+"/master.m3u8", server.URL) {
			t.Fatal("403 HTML segment must fail the probe")
		}
	})
	t.Run("decoy segment", func(t *testing.T) {
		server := probeMux(200, pngSeg)
		defer server.Close()
		p := NewAnikotoProvider(zerolog.Nop())
		p.client = server.Client()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if p.probeHLSegments(ctx, server.URL+"/master.m3u8", server.URL) {
			t.Fatal("PNG decoy segment must fail the probe")
		}
	})
	t.Run("dead master", func(t *testing.T) {
		p := NewAnikotoProvider(zerolog.Nop())
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if p.probeHLSegments(ctx, "http://127.0.0.1:1/master.m3u8", "http://x") {
			t.Fatal("unreachable master must fail the probe")
		}
	})
}
