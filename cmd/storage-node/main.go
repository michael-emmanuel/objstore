// Command storage-node runs one data-plane node: an on-disk blob engine
// served over HTTP, a heartbeat to the coordinator, and a rate-limited
// background scrubber that detects silent corruption.
//
// A storage node knows nothing about buckets, keys or tenants. It stores
// immutable blobs by ID and is told what to store by gateways, repairers
// and rebalancers.
package main

import (
	"flag"
	"os"
	"time"

	"github.com/michael-emmanuel/objstore/internal/clusterauth"
	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/service"
	"github.com/michael-emmanuel/objstore/internal/storage"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
)

func main() {
	host, _ := os.Hostname()
	var (
		id        = flag.String("id", host, "stable node ID (must survive restarts: placement and metadata refer to it)")
		listen    = flag.String("listen", ":8100", "listen address")
		advertise = flag.String("advertise", "", "URL other components use to reach this node (default http://<hostname><listen>)")
		zone      = flag.String("zone", "az-a", "availability zone (failure domain)")
		rack      = flag.String("rack", "rack-1", "rack (failure domain within a zone)")
		region    = flag.String("region", "local", "region")
		weight    = flag.Int("weight", 1, "relative placement weight (proportional to capacity)")

		dataDir   = flag.String("data-dir", "./data/node", "blob directory")
		capacity  = flag.Int64("capacity-bytes", 0, "artificial capacity limit in bytes (0 = filesystem limit)")
		fsync     = flag.Bool("fsync", true, "fsync blobs and directories before acknowledging a write")
		blockSize = flag.Int("block-size", storage.DefaultBlockSize, "integrity block size (CRC32C per block)")

		coordinator = flag.String("coordinator", "http://localhost:7000", "coordinator base URL")
		heartbeat   = flag.Duration("heartbeat", time.Second, "heartbeat interval")
		scrubEvery  = flag.Duration("scrub-interval", time.Hour, "pause between full scrub passes (0 disables)")
		scrubRate   = flag.Float64("scrub-bytes-per-sec", 20<<20, "scrubber read bandwidth budget")

		faults    = flag.Bool("enable-fault-injection", false, "expose /admin/faults and /admin/corrupt (never in production)")
		insecure  = flag.Bool("insecure-no-cluster-auth", false, "allow running without CLUSTER_TOKEN")
		debugAddr = flag.String("debug-listen", "", "optional pprof address, e.g. localhost:6060 (never expose publicly)")
		logLevel  = flag.String("log-level", "info", "debug|info|warn|error")
		tlsCert   = flag.String("tls-cert", "", "TLS certificate file")
		tlsKey    = flag.String("tls-key", "", "TLS key file")
	)
	flag.Parse()
	log := observability.NewLogger("storage-node", *logLevel).With("node", *id)
	if *advertise == "" {
		*advertise = "http://" + host + *listen
	}
	token, err := service.ClusterToken(log, *insecure)
	if err != nil {
		service.Fatal(log, "configuration", err)
	}
	ctx, stop := service.SignalContext()
	defer stop()

	eng, err := storage.Open(storage.Options{Root: *dataDir, CapacityBytes: *capacity, Fsync: *fsync, BlockSize: *blockSize})
	if err != nil {
		service.Fatal(log, "open engine", err)
	}
	st := eng.Stats()
	log.Info("engine recovered", "blobs", st.BlobCount, "used_bytes", st.UsedBytes, "quarantined", st.Quarantined)

	srv := storagenode.NewServer(storagenode.Config{
		Node:          placement.Node{ID: *id, Addr: *advertise, Zone: *zone, Rack: *rack, Region: *region, Weight: *weight},
		ScrubInterval: *scrubEvery, ScrubBytesPerSec: *scrubRate,
		// Repair and rebalance pull data node-to-node; those requests must
		// carry the cluster token like any other internal call.
		PeerHTTPClient: clusterauth.Wrap(storagenode.DefaultHTTPClient(), token),
	}, eng, log)

	coord := membership.NewClient(*coordinator)
	coord.HC = clusterauth.Wrap(coord.HC, token)

	var g service.Group
	service.StartDebug(ctx, &g, log, *debugAddr)
	g.Go("heartbeat", func() error { srv.RunHeartbeat(ctx, coord, *heartbeat); return nil })
	g.Go("scrubber", func() error { srv.RunScrubber(ctx); return nil })
	h := clusterauth.Middleware(token, service.GateFaults(*faults, srv.Handler()))
	g.Go("http", func() error {
		// Generous grace: let in-flight replica writes finish and commit
		// rather than forcing the gateway to retry them elsewhere.
		return service.Serve(ctx, log, *listen, h, service.TLS{Cert: *tlsCert, Key: *tlsKey}, 30*time.Second)
	})
	if err := g.Wait(); err != nil {
		service.Fatal(log, "exit", err)
	}
}
