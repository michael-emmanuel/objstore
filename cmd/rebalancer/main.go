// Command rebalancer moves segment replicas toward their placement-algorithm
// owners after membership changes (node added, node DRAINING), using
// copy-before-delete: the new replica is written and committed to metadata
// before the old one is released, so a segment is temporarily
// over-replicated but never under-replicated by a move.
//
// It runs at lower priority than repair (smaller bandwidth budget) because
// misplaced data is a balance problem, while under-replicated data is a
// durability problem.
package main

import (
	"flag"
	"net/http"
	"time"

	"github.com/michael-emmanuel/objstore/internal/clusterauth"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/rebalancer"
	"github.com/michael-emmanuel/objstore/internal/service"
)

func main() {
	var (
		listen   = flag.String("listen", ":9200", "metrics/health listen address")
		metaURLs = flag.String("metadata", "http://localhost:7000", "comma-separated metadata shard URLs, shard 0 first")
		coordURL = flag.String("coordinator", "http://localhost:7000", "coordinator URL")
		algo     = flag.String("placement", string(placement.AlgoRing), "placement algorithm: ring|rendezvous")

		every     = flag.Duration("interval", 30*time.Second, "pause between rebalance passes")
		maxMoves  = flag.Int("max-moves-per-pass", 1000, "cap on replica moves per pass")
		bandwidth = flag.Float64("bandwidth-bytes-per-sec", 20<<20, "rebalance bandwidth budget (keep below repair's)")
		maxFull   = flag.Float64("target-max-fullness", 0.90, "never move data onto a node above this used fraction")

		insecure = flag.Bool("insecure-no-cluster-auth", false, "allow running without CLUSTER_TOKEN")
		logLevel = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()
	log := observability.NewLogger("rebalancer", *logLevel)
	token, err := service.ClusterToken(log, *insecure)
	if err != nil {
		service.Fatal(log, "configuration", err)
	}
	ctx, stop := service.SignalContext()
	defer stop()

	cc, err := service.Dial(ctx, service.ClusterOptions{
		MetadataURLs: service.SplitList(*metaURLs), CoordinatorURL: *coordURL,
		Algorithm: placement.Algorithm(*algo), Token: token, Log: log,
	})
	if err != nil {
		service.Fatal(log, "connect to cluster", err)
	}
	reg := observability.NewRegistry()
	rb := rebalancer.New(rebalancer.Config{MaxMovesPerPass: *maxMoves, BandwidthBytesPerSec: *bandwidth, TargetMaxFullness: *maxFull},
		cc.Meta, cc.Watcher, cc.Nodes, log, reg)

	var g service.Group
	g.Go("membership", func() error { cc.Watcher.Run(ctx, time.Second); return nil })
	g.Go("rebalance", func() error { rb.Run(ctx, *every); return nil })
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", reg.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	g.Go("http", func() error {
		return service.Serve(ctx, log, *listen, clusterauth.Middleware(token, mux), service.TLS{}, 5*time.Second)
	})
	if err := g.Wait(); err != nil {
		service.Fatal(log, "exit", err)
	}
}
