// Command metadata runs one metadata shard and, optionally, the cluster
// coordinator (membership registry and failure detector).
//
// Metadata is partitioned by bucket across N shards; every shard is a
// separate process with its own write-ahead log. A shard is NOT replicated:
// there is no consensus protocol in this project. Losing a shard's disk loses
// the metadata for the buckets it owns (the blobs survive but become
// unreachable orphans). See docs/metadata-scaling.md and docs/tradeoffs.md
// for what a production deployment would use instead (a Raft group per shard).
package main

import (
	"context"
	"flag"
	"net/http"
	"time"

	"github.com/michael-emmanuel/objstore/internal/clusterauth"
	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/service"
)

func main() {
	var (
		listen       = flag.String("listen", ":7000", "listen address")
		shardIndex   = flag.Int("shard-index", 0, "index of this shard (0..N-1); must match its position in the gateway's -metadata list")
		dataDir      = flag.String("data-dir", "./data/metadata", "directory for the WAL and snapshots")
		fsync        = flag.Bool("fsync", true, "fsync the WAL on every commit (disable only for experiments)")
		minPart      = flag.Int64("min-part-size", 5<<20, "minimum multipart part size (all parts except the last)")
		compactEvery = flag.Int("compact-every", 10000, "snapshot and truncate the WAL after this many records")

		coordinator  = flag.Bool("coordinator", false, "also run the membership registry / failure detector")
		suspectAfter = flag.Duration("suspect-after", 3*time.Second, "heartbeat silence before a node is SUSPECT")
		downAfter    = flag.Duration("down-after", 10*time.Second, "heartbeat silence before a node is DOWN")
		statePath    = flag.String("membership-state", "", "file persisting operator node states (default <data-dir>/membership.json)")

		faults    = flag.Bool("enable-fault-injection", false, "expose /admin/faults (never in production)")
		insecure  = flag.Bool("insecure-no-cluster-auth", false, "allow running without CLUSTER_TOKEN")
		debugAddr = flag.String("debug-listen", "", "optional pprof address, e.g. localhost:6060 (never expose publicly)")
		logLevel  = flag.String("log-level", "info", "debug|info|warn|error")
		tlsCert   = flag.String("tls-cert", "", "TLS certificate file")
		tlsKey    = flag.String("tls-key", "", "TLS key file")
	)
	flag.Parse()
	log := observability.NewLogger("metadata", *logLevel).With("shard", *shardIndex)

	token, err := service.ClusterToken(log, *insecure)
	if err != nil {
		service.Fatal(log, "configuration", err)
	}
	ctx, stop := service.SignalContext()
	defer stop()

	shard, err := metadata.OpenShard(metadata.ShardOptions{
		Index: *shardIndex, Dir: *dataDir, Sync: *fsync, MinPartSize: *minPart, CompactEvery: *compactEvery, Log: log,
	})
	if err != nil {
		service.Fatal(log, "open shard", err)
	}
	defer shard.Close()

	mux := http.NewServeMux()
	mux.Handle("/", metadata.NewServer(shard))
	mux.Handle("GET /metrics", shardMetrics(ctx, shard).Handler())

	var g service.Group
	service.StartDebug(ctx, &g, log, *debugAddr)
	if *coordinator {
		sp := *statePath
		if sp == "" {
			sp = *dataDir + "/membership.json"
		}
		reg, err := membership.NewRegistry(membership.Options{
			SuspectAfter: *suspectAfter, DownAfter: *downAfter, StatePath: sp, Log: log,
		})
		if err != nil {
			service.Fatal(log, "membership registry", err)
		}
		reg.Register(mux)
		g.Go("failure-detector", func() error { reg.Run(ctx, *suspectAfter/4); return nil })
		log.Info("coordinator enabled", "suspect_after", *suspectAfter, "down_after", *downAfter)
	}

	// Continue the caller's trace (traceparent) so metadata RPCs appear in the
	// same trace as the gateway request that issued them.
	traced := observability.Middleware("metadata", log, &observability.Tracer{Log: log}, mux)
	h := clusterauth.Middleware(token, service.GateFaults(*faults, traced))
	g.Go("http", func() error {
		return service.Serve(ctx, log, *listen, h, service.TLS{Cert: *tlsCert, Key: *tlsKey}, 10*time.Second)
	})
	if err := g.Wait(); err != nil {
		service.Fatal(log, "exit", err)
	}
}

// shardMetrics exposes shard utilization: how full and how busy this
// partition of the metadata plane is. Uneven values across shards are the
// signal that bucket-hash partitioning has produced a hot or oversized shard.
func shardMetrics(ctx context.Context, shard *metadata.Shard) *observability.Registry {
	reg := observability.NewRegistry()
	stat := func(f func(metadata.Stats) int) func() float64 {
		return func() float64 {
			st, err := shard.Stats(ctx)
			if err != nil || len(st) == 0 {
				return 0
			}
			return float64(f(st[0]))
		}
	}
	reg.GaugeFunc("metadata_buckets", "Buckets owned by this shard.", stat(func(s metadata.Stats) int { return s.Buckets }))
	reg.GaugeFunc("metadata_keys", "Keys (objects) owned by this shard.", stat(func(s metadata.Stats) int { return s.Keys }))
	reg.GaugeFunc("metadata_versions", "Object versions retained, including noncurrent and delete markers.", stat(func(s metadata.Stats) int { return s.Versions }))
	reg.GaugeFunc("metadata_segments", "Segment references (each points at RF physical replicas).", stat(func(s metadata.Stats) int { return s.Segments }))
	reg.GaugeFunc("metadata_multipart_uploads", "In-progress multipart uploads.", stat(func(s metadata.Stats) int { return s.Uploads }))
	reg.GaugeFunc("metadata_pending_blobs", "Blobs registered by in-flight writes (GC-protected).", stat(func(s metadata.Stats) int { return s.Pending }))
	reg.GaugeFunc("metadata_garbage_queue", "Dereferenced blobs awaiting physical deletion.", stat(func(s metadata.Stats) int { return s.GarbageQueue }))
	reg.GaugeFunc("metadata_wal_records", "WAL records since the last snapshot (recovery time grows with this).", stat(func(s metadata.Stats) int { return s.WALRecords }))
	reg.CounterFunc("metadata_lsn", "Log sequence number: total committed mutations (rate = write throughput).",
		func() float64 {
			st, err := shard.Stats(ctx)
			if err != nil || len(st) == 0 {
				return 0
			}
			return float64(st[0].LSN)
		})
	return reg
}
