package service

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"
)

// DebugHandler serves net/http/pprof. It is mounted only on the separate
// -debug-listen address (bind it to localhost), never on the public port:
// profiles expose internals and a CPU profile costs CPU.
func DebugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// StartDebug runs the pprof server in g when addr is non-empty.
func StartDebug(ctx context.Context, g *Group, log *slog.Logger, addr string) {
	if addr == "" {
		return
	}
	g.Go("debug", func() error { return Serve(ctx, log.With("server", "debug"), addr, DebugHandler(), TLS{}, time.Second) })
}
