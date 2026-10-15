// Package service holds process plumbing shared by every binary in cmd/:
// signal handling, graceful HTTP shutdown, cluster-token setup and the
// fault-injection gate. It contains no storage logic.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/michael-emmanuel/objstore/internal/clusterauth"
)

// SignalContext is cancelled on SIGINT or SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// ClusterToken reads the shared cluster token from the environment. An
// empty token is allowed only when allowInsecure is set (local development).
func ClusterToken(log *slog.Logger, allowInsecure bool) (string, error) {
	tok := os.Getenv(clusterauth.EnvVar)
	switch {
	case tok == "" && allowInsecure:
		log.Warn("cluster authentication DISABLED: " + clusterauth.EnvVar + " is empty and -insecure-no-cluster-auth is set")
		return "", nil
	case tok == "":
		return "", fmt.Errorf("%s must be set (or pass -insecure-no-cluster-auth for local experiments)", clusterauth.EnvVar)
	case len(tok) < clusterauth.MinTokenLen:
		return "", fmt.Errorf("%s must be at least %d characters", clusterauth.EnvVar, clusterauth.MinTokenLen)
	}
	return tok, nil
}

// TLS holds optional certificate paths.
type TLS struct{ Cert, Key string }

// Serve runs an HTTP server until ctx is cancelled, then shuts it down,
// giving in-flight requests up to grace to finish. Long transfers that are
// still running after grace are cut off; the commit protocol guarantees they
// leave no visible partial state.
func Serve(ctx context.Context, log *slog.Logger, addr string, h http.Handler, tls TLS, grace time.Duration) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		// No ReadTimeout/WriteTimeout: they would cap the duration of any
		// object transfer. Slow or idle clients are handled per transfer by
		// progress deadlines in the gateway instead.
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 64 << 10,
	}
	errc := make(chan error, 1)
	go func() {
		var err error
		if tls.Cert != "" {
			err = srv.ListenAndServeTLS(tls.Cert, tls.Key)
		} else {
			err = srv.ListenAndServe()
		}
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errc <- err
	}()
	log.Info("listening", "addr", addr, "tls", tls.Cert != "")
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down", "grace", grace)
	sctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Warn("graceful shutdown incomplete; closing remaining connections", "error", err)
		srv.Close()
	}
	return <-errc
}

// GateFaults hides fault-injection routes (any path under /admin/faults or
// /admin/corrupt) unless enabled. Fault injection must be an explicit,
// per-process opt-in: a stray request must never be able to corrupt data.
func GateFaults(enabled bool, next http.Handler) http.Handler {
	if enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/admin/faults") || strings.HasPrefix(r.URL.Path, "/admin/corrupt") {
			http.Error(w, "fault injection disabled (start with -enable-fault-injection)", http.StatusNotFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Group runs goroutines that share one lifecycle: Wait returns after all of
// them have returned. Every background loop in a binary is started through
// a Group, so shutdown is never left to goroutines nobody waits for.
type Group struct {
	wg   sync.WaitGroup
	mu   sync.Mutex
	errs []error
}

func (g *Group) Go(name string, fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(); err != nil {
			g.mu.Lock()
			g.errs = append(g.errs, fmt.Errorf("%s: %w", name, err))
			g.mu.Unlock()
		}
	}()
}

func (g *Group) Wait() error {
	g.wg.Wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	return errors.Join(g.errs...)
}

// SplitList splits a comma-separated flag value, dropping empty entries.
func SplitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Fatal logs and exits; used only from main.
func Fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "error", err)
	os.Exit(1)
}
