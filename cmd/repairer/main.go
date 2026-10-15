// Command repairer restores the replication factor of under-replicated
// segments (DOWN nodes, quarantined corrupt copies, writes that succeeded
// with fewer than RF acks) and runs garbage collection.
//
// Both loops are idempotent and safe to restart at any point: every action
// is "copy, verify, then commit to metadata", and metadata updates are
// conditional. Running two repairers concurrently is safe but wasteful;
// production would shard the scan by metadata shard and elect one owner.
package main

import (
	"flag"
	"net/http"
	"time"

	"github.com/michael-emmanuel/objstore/internal/clusterauth"
	"github.com/michael-emmanuel/objstore/internal/garbagecollector"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/repair"
	"github.com/michael-emmanuel/objstore/internal/service"
)

func main() {
	var (
		listen   = flag.String("listen", ":9100", "metrics/health listen address")
		metaURLs = flag.String("metadata", "http://localhost:7000", "comma-separated metadata shard URLs, shard 0 first")
		coordURL = flag.String("coordinator", "http://localhost:7000", "coordinator URL")
		algo     = flag.String("placement", string(placement.AlgoRing), "placement algorithm: ring|rendezvous")

		repairOn    = flag.Bool("repair", true, "run the repair loop")
		every       = flag.Duration("interval", 10*time.Second, "pause between repair passes")
		delay       = flag.Duration("repair-delay", 30*time.Second, "wait after a node goes DOWN before re-replicating its data (absorbs reboots; avoids repair storms)")
		concurrency = flag.Int("concurrency", 4, "parallel segment copies")
		bandwidth   = flag.Float64("bandwidth-bytes-per-sec", 50<<20, "total repair bandwidth budget (0 = unlimited)")

		gcOn      = flag.Bool("gc", true, "run the garbage collector")
		gcEvery   = flag.Duration("gc-interval", time.Minute, "pause between GC passes")
		gcGrace   = flag.Duration("gc-grace", time.Hour, "minimum age before anything is physically reclaimed")
		uploadTTL = flag.Duration("upload-ttl", 24*time.Hour, "abort multipart uploads idle for longer than this")
		tombTTL   = flag.Duration("tombstone-ttl", 24*time.Hour, "purge delete markers in unversioned buckets after this")
		sweep     = flag.Bool("orphan-sweep", true, "scan storage nodes for blobs no metadata references")

		insecure = flag.Bool("insecure-no-cluster-auth", false, "allow running without CLUSTER_TOKEN")
		logLevel = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()
	log := observability.NewLogger("repairer", *logLevel)
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

	var g service.Group
	g.Go("membership", func() error { cc.Watcher.Run(ctx, time.Second); return nil })
	if *repairOn {
		rp := repair.New(repair.Config{RepairDelay: *delay, Concurrency: *concurrency, BandwidthBytesPerSec: *bandwidth},
			cc.Meta, cc.Watcher, cc.Nodes, log, reg)
		g.Go("repair", func() error { rp.Run(ctx, *every); return nil })
	}
	if *gcOn {
		gc := garbagecollector.New(garbagecollector.Config{Grace: *gcGrace, UploadTTL: *uploadTTL, TombstoneTTL: *tombTTL, Sweep: *sweep},
			cc.Meta, cc.Watcher, cc.Nodes, log, reg)
		g.Go("gc", func() error { gc.Run(ctx, *gcEvery); return nil })
	}

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
