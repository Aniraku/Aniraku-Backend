package v1

import (
	"net/http"
	"testing"
)

func TestProxyRedirectBlocked(t *testing.T) {
	for _, status := range []int{http.StatusMultipleChoices, http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		if !proxyRedirectBlocked(status) {
			t.Fatalf("proxyRedirectBlocked(%d) = false, want true", status)
		}
	}
	for _, status := range []int{http.StatusOK, http.StatusPartialContent, http.StatusBadRequest, http.StatusBadGateway} {
		if proxyRedirectBlocked(status) {
			t.Fatalf("proxyRedirectBlocked(%d) = true, want false", status)
		}
	}
}
