package api

import (
	"net/http"
	"net/http/pprof"

	"github.com/go-chi/chi/v5"
)

// newPprofMux exposes the standard pprof handlers under /debug/pprof.
// Mounted only when cfg.Server.EnablePprof is set, and always behind the
// auth + RequireAdmin middleware chain, so profiles are never public.
func newPprofMux() http.Handler {
	m := chi.NewRouter()
	m.Get("/", pprof.Index)
	m.Get("/cmdline", pprof.Cmdline)
	m.Get("/profile", pprof.Profile)
	m.Get("/symbol", pprof.Symbol)
	m.Post("/symbol", pprof.Symbol)
	m.Get("/trace", pprof.Trace)
	m.Get("/allocs", pprof.Handler("allocs").ServeHTTP)
	m.Get("/block", pprof.Handler("block").ServeHTTP)
	m.Get("/goroutine", pprof.Handler("goroutine").ServeHTTP)
	m.Get("/heap", pprof.Handler("heap").ServeHTTP)
	m.Get("/mutex", pprof.Handler("mutex").ServeHTTP)
	m.Get("/threadcreate", pprof.Handler("threadcreate").ServeHTTP)
	return m
}
