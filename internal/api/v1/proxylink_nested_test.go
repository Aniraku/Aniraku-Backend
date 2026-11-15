package v1

import "testing"

func TestProxyLinkUnwrapsNestedSelfProxy(t *testing.T) {
	if !proxyLinkInit("6a1d2c3b4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f9") {
		t.Fatal("init failed")
	}
	nested := "http://127.0.0.1:43211/api/v1/proxy?url=https%3A%2F%2Ffetch.nexabloom.top%2Fanime%2Fx%2Fmaster.m3u8&headers=%7B%22Referer%22%3A%22https%3A%2F%2Fmegaplay.buzz%2F%22%7D"
	tok, err := mintProxyLink(nested, nil)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	const prefix = "/api/v1/proxy?k="
	url, headers, err := resolveProxyLink(tok[len(prefix):])
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if url != "https://fetch.nexabloom.top/anime/x/master.m3u8" {
		t.Fatalf("unwrapped url = %s", url)
	}
	if headers["Referer"] != "https://megaplay.buzz/" {
		t.Fatalf("sealed headers = %v", headers)
	}
}
