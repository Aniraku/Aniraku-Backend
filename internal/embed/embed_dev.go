//go:build !web

package embed

import "net/http"

func FS() http.FileSystem {
	return nil
}

func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`<!DOCTYPE html><html><body><h1>Aniraku Dev Mode</h1><p>Run <code>pnpm --filter web dev</code> to start the Next.js dev server.</p></body></html>`))
	})
}
