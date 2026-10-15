// Command gateway serves the public object API. It is stateless apart from
// caches: run as many as needed behind a load balancer.
//
// Per request it authenticates the tenant, applies rate limits and quotas,
// asks the metadata plane where things are, and moves bytes between the
// client and storage nodes without buffering whole objects.
package main

import (
	"flag"
	"time"

	"github.com/michael-emmanuel/objstore/internal/api"
	"github.com/michael-emmanuel/objstore/internal/config"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/quota"
	"github.com/michael-emmanuel/objstore/internal/service"
)

func main() {
	var (
		listen      = flag.String("listen", ":8080", "listen address")
		metaURLs    = flag.String("metadata", "http://localhost:7000", "comma-separated metadata shard URLs, shard 0 first")
		coordURL    = flag.String("coordinator", "http://localhost:7000", "coordinator URL")
		tenantsFile = flag.String("tenants", "./tenants.json", "tenant definitions (token hashes or token env-var names; never plaintext tokens)")
		algo        = flag.String("placement", string(placement.AlgoRing), "placement algorithm: ring|rendezvous (must match repairer and rebalancer)")
		zone        = flag.String("zone", "", "this gateway's zone, preferred for reads")
		memberEvery = flag.Duration("membership-refresh", time.Second, "membership poll interval")

		w       = flag.Int("write-quorum", 0, "W: replica acks required for a PUT to succeed (0 = majority of RF)")
		r       = flag.Int("read-quorum", 0, "R: replicas that must agree for ?consistency=quorum reads (0 = majority)")
		segment = flag.Int64("segment-size", 64<<20, "simple PUTs are split into segments (independently placed blobs) of this size")
		chunk   = flag.Int("chunk-size", 256<<10, "fan-out chunk size")
		queue   = flag.Int("queue-depth", 8, "per-replica chunk queue: memory per upload is about replicas*queue-depth*chunk-size")
		stall   = flag.Duration("replica-stall-timeout", 5*time.Second, "drop a replica that accepts no data for this long")
		slowPen = flag.Duration("slow-node-penalty", 30*time.Second, "after a replica stalls, avoid placing new data on that node for this long (<0 disables)")
		strag   = flag.Duration("straggler-timeout", 2*time.Second, "after W acks, wait this long for remaining replicas")

		metaTTL     = flag.Duration("metadata-cache-ttl", 0, "metadata cache TTL (0 disables; >0 trades cross-gateway freshness for load)")
		objCache    = flag.Int64("object-cache-bytes", 256<<20, "hot-object cache size (0 disables)")
		objCacheMax = flag.Int64("object-cache-max-object", 1<<20, "largest object eligible for the hot-object cache")

		maxUploads  = flag.Int("max-inflight-uploads", 256, "global upload concurrency; beyond it requests get 503 (load shedding)")
		idle        = flag.Duration("client-idle-timeout", 60*time.Second, "abort transfers whose client makes no progress")
		metaTimeout = flag.Duration("metadata-timeout", 5*time.Second, "per-call metadata deadline")
		minPart     = flag.Int64("min-part-size", 5<<20, "minimum multipart part size (except the last)")
		maxPart     = flag.Int64("max-part-size", 5<<30, "maximum multipart part size")

		insecure  = flag.Bool("insecure-no-cluster-auth", false, "allow running without CLUSTER_TOKEN")
		debugAddr = flag.String("debug-listen", "", "optional pprof address, e.g. localhost:6060 (never expose publicly)")
		logLevel  = flag.String("log-level", "info", "debug|info|warn|error")
		tlsCert   = flag.String("tls-cert", "", "TLS certificate file")
		tlsKey    = flag.String("tls-key", "", "TLS key file")
	)
	flag.Parse()
	log := observability.NewLogger("gateway", *logLevel)

	token, err := service.ClusterToken(log, *insecure)
	if err != nil {
		service.Fatal(log, "configuration", err)
	}
	tenants, err := config.LoadTenants(*tenantsFile)
	if err != nil {
		service.Fatal(log, "load tenants", err)
	}
	log.Info("tenants loaded", "count", len(tenants))

	ctx, stop := service.SignalContext()
	defer stop()

	cc, err := service.Dial(ctx, service.ClusterOptions{
		MetadataURLs: service.SplitList(*metaURLs), CoordinatorURL: *coordURL,
		Algorithm: placement.Algorithm(*algo), Token: token, Log: log,
	})
	if err != nil {
		service.Fatal(log, "connect to cluster", err)
	}

	gw := api.New(api.Config{
		Zone: *zone, WriteQuorum: *w, ReadQuorum: *r,
		SegmentSize: *segment, ChunkSize: *chunk, QueueDepth: *queue,
		StallTimeout: *stall, StragglerTimeout: *strag, SlowNodePenalty: *slowPen,
		MetaCacheTTL: *metaTTL, ObjectCacheBytes: *objCache, ObjectCacheMaxObject: *objCacheMax,
		MaxInflightUploads: *maxUploads, ClientIdleTimeout: *idle, MetadataTimeout: *metaTimeout,
		MinPartSize: *minPart, MaxPartSize: *maxPart,
	}, api.Deps{
		Meta: cc.Meta, Members: cc.Watcher, Coord: cc.Coord, Nodes: cc.Nodes,
		Tenants: quota.NewManager(tenants, cc.Meta), Log: log,
	})

	var g service.Group
	service.StartDebug(ctx, &g, log, *debugAddr)
	g.Go("membership", func() error { cc.Watcher.Run(ctx, *memberEvery); return nil })
	g.Go("http", func() error {
		return service.Serve(ctx, log, *listen, gw.Handler(), service.TLS{Cert: *tlsCert, Key: *tlsKey}, 30*time.Second)
	})
	if err := g.Wait(); err != nil {
		service.Fatal(log, "exit", err)
	}
}
