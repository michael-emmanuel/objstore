// Package testcluster runs a complete cluster in one process for
// integration, failure and recovery tests: metadata shards and the
// coordinator served over real HTTP, storage nodes with real on-disk
// engines, a gateway, and the background workers (driven explicitly by
// tests so outcomes are deterministic).
package testcluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/objstore/internal/api"
	"github.com/michael-emmanuel/objstore/internal/config"
	"github.com/michael-emmanuel/objstore/internal/garbagecollector"
	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/quota"
	"github.com/michael-emmanuel/objstore/internal/rebalancer"
	"github.com/michael-emmanuel/objstore/internal/repair"
	"github.com/michael-emmanuel/objstore/internal/storage"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
	"github.com/michael-emmanuel/objstore/pkg/client"
)

// Well-known test credentials (test-only; never used outside tests).
const (
	AliceToken = "alice-test-token-0123456789"
	BobToken   = "bob-test-token-0123456789"
	AdminToken = "admin-test-token-0123456789"
)

type Options struct {
	Nodes       int
	Zones       []string
	Shards      int
	Gateway     api.Config
	AliceLimits config.Limits
	RepairDelay time.Duration
	GCGrace     time.Duration
	BlockSize   int
}

type Node struct {
	ID     string
	Zone   string
	Server *storagenode.Server
	Engine *storage.Engine
	TS     *httptest.Server
	cancel context.CancelFunc
	dir    string
	alive  bool
}

type Cluster struct {
	t          testing.TB
	opts       Options
	Shards     []*metadata.Shard
	MetaSrv    []*metadata.Server
	Meta       *metadata.Router
	Registry   *membership.Registry
	coordTS    *httptest.Server
	Coord      *membership.Client
	Watcher    *membership.Watcher
	Nodes      []*Node
	Gateway    *api.Gateway
	GatewayURL string
	Alice      *client.Client
	Bob        *client.Client
	Admin      *client.Client
	Anon       *client.Client
	NodeClient *storagenode.Client
	Repairer   *repair.Repairer
	Rebalancer *rebalancer.Rebalancer
	GC         *garbagecollector.Collector
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
}

func Start(t testing.TB, o Options) *Cluster {
	t.Helper()
	if o.Nodes == 0 {
		o.Nodes = 4
	}
	if len(o.Zones) == 0 {
		o.Zones = []string{"az-a", "az-b", "az-c"}
	}
	if o.Shards == 0 {
		o.Shards = 2
	}
	if o.RepairDelay == 0 {
		o.RepairDelay = 50 * time.Millisecond
	}
	if o.GCGrace == 0 {
		o.GCGrace = time.Millisecond
	}
	if o.BlockSize == 0 {
		o.BlockSize = 16 << 10
	}
	log := observability.DiscardLogger()
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{t: t, opts: o, ctx: ctx, cancel: cancel, NodeClient: storagenode.NewClient(nil)}

	var stores []metadata.Store
	for i := 0; i < o.Shards; i++ {
		sh, err := metadata.OpenShard(metadata.ShardOptions{Index: i, Dir: t.TempDir(), MinPartSize: 64 << 10, Log: log})
		if err != nil {
			t.Fatal(err)
		}
		srv := metadata.NewServer(sh)
		ts := httptest.NewServer(srv)
		c.Shards = append(c.Shards, sh)
		c.MetaSrv = append(c.MetaSrv, srv)
		stores = append(stores, metadata.NewClient(ts.URL, nil))
		t.Cleanup(ts.Close)
	}
	c.Meta = metadata.NewRouter(stores)

	reg, err := membership.NewRegistry(membership.Options{SuspectAfter: 300 * time.Millisecond, DownAfter: 600 * time.Millisecond, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	c.Registry = reg
	mux := http.NewServeMux()
	reg.Register(mux)
	c.coordTS = httptest.NewServer(mux)
	t.Cleanup(c.coordTS.Close)
	c.Coord = membership.NewClient(c.coordTS.URL)
	go reg.Run(ctx, 50*time.Millisecond)

	for i := 0; i < o.Nodes; i++ {
		c.AddNode(o.Zones[i%len(o.Zones)])
	}
	c.Watcher = membership.NewWatcher(c.Coord, placement.AlgoRing, log)
	c.Refresh()
	go c.Watcher.Run(ctx, 50*time.Millisecond)

	alice := o.AliceLimits
	tenants, err := config.ResolveTenants([]config.Tenant{
		{ID: "alice", TokenSHA256: config.HashToken(AliceToken), Limits: alice},
		{ID: "bob", TokenSHA256: config.HashToken(BobToken)},
		{ID: "admin", TokenSHA256: config.HashToken(AdminToken), Admin: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	qm := quota.NewManager(tenants, c.Meta)
	qm.UsageTTL = 0
	gcfg := o.Gateway
	if gcfg.SegmentSize == 0 {
		gcfg.SegmentSize = 1 << 20
	}
	if gcfg.MinPartSize == 0 {
		gcfg.MinPartSize = 64 << 10
	}
	c.Gateway = api.New(gcfg, api.Deps{Meta: c.Meta, Members: c.Watcher, Coord: c.Coord, Nodes: c.NodeClient, Tenants: qm, Log: log})
	gts := httptest.NewServer(c.Gateway.Handler())
	t.Cleanup(gts.Close)
	c.GatewayURL = gts.URL
	c.Alice = client.New(gts.URL, AliceToken)
	c.Bob = client.New(gts.URL, BobToken)
	c.Admin = client.New(gts.URL, AdminToken)
	c.Anon = client.New(gts.URL, "")

	c.Repairer = repair.New(repair.Config{RepairDelay: o.RepairDelay, Concurrency: 4}, c.Meta, c.Watcher, c.NodeClient, log, nil)
	c.Rebalancer = rebalancer.New(rebalancer.Config{}, c.Meta, c.Watcher, c.NodeClient, log, nil)
	c.GC = garbagecollector.New(garbagecollector.Config{Grace: o.GCGrace, Sweep: true, UploadTTL: time.Hour, TombstoneTTL: time.Hour},
		c.Meta, c.Watcher, c.NodeClient, log, nil)
	t.Cleanup(c.Close)
	return c
}

// AddNode starts a new storage node and waits until the coordinator sees it.
func (c *Cluster) AddNode(zone string) *Node {
	c.mu.Lock()
	idx := len(c.Nodes)
	c.mu.Unlock()
	id := fmt.Sprintf("node-%d", idx+1)
	n := &Node{ID: id, Zone: zone, dir: c.t.TempDir()}
	c.startNode(n)
	c.mu.Lock()
	c.Nodes = append(c.Nodes, n)
	c.mu.Unlock()
	if c.Watcher != nil {
		c.WaitFor("node joins", func() bool {
			c.Refresh()
			nn, ok := c.Watcher.Map().Node(id)
			return ok && nn.State == placement.StateActive
		})
	}
	return n
}

func (c *Cluster) startNode(n *Node) {
	eng, err := storage.Open(storage.Options{Root: n.dir, BlockSize: c.opts.BlockSize})
	if err != nil {
		c.t.Fatal(err)
	}
	n.Engine = eng
	log := observability.DiscardLogger()
	ts := httptest.NewUnstartedServer(nil)
	ts.Start()
	n.Server = storagenode.NewServer(storagenode.Config{Node: placement.Node{ID: n.ID, Addr: ts.URL, Zone: n.Zone,
		Rack: "rack-1", Region: "local", Weight: 1}}, eng, log)
	ts.Config.Handler = n.Server.Handler()
	n.TS = ts
	ctx, cancel := context.WithCancel(c.ctx)
	n.cancel = cancel
	n.alive = true
	go n.Server.RunHeartbeat(ctx, c.Coord, 50*time.Millisecond)
	// Wait for the first heartbeat to register.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, nodes := c.Registry.Snapshot()
		for _, x := range nodes {
			if x.ID == n.ID && x.Addr == ts.URL && x.State == placement.StateActive {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %s never registered", n.ID)
}

// Kill stops a node abruptly: no more heartbeats, connections refused.
func (c *Cluster) Kill(n *Node) {
	n.cancel()
	n.TS.CloseClientConnections()
	n.TS.Close()
	n.alive = false
}

// Restart brings a killed node back with its old disk (new address).
func (c *Cluster) Restart(n *Node) {
	c.startNode(n)
}

// WaitDown waits until the coordinator and the gateway see n as DOWN.
func (c *Cluster) WaitDown(n *Node) {
	c.WaitFor(n.ID+" down", func() bool {
		c.Refresh()
		x, _ := c.Watcher.Map().Node(n.ID)
		return x.State == placement.StateDown
	})
}

// Refresh pulls membership into the gateway/worker view.
func (c *Cluster) Refresh() {
	c.Watcher.Refresh(context.Background())
}

func (c *Cluster) WaitFor(what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s", what)
}

// NodeByID returns a node.
func (c *Cluster) NodeByID(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// BlobCopies counts physical copies of blobID across live nodes.
func (c *Cluster) BlobCopies(blobID string) []string {
	var out []string
	for _, n := range c.Nodes {
		if !n.alive {
			continue
		}
		if _, err := n.Engine.Stat(blobID); err == nil {
			out = append(out, n.ID)
		}
	}
	return out
}

func (c *Cluster) Close() {
	c.cancel()
	for _, n := range c.Nodes {
		if n.alive {
			n.cancel()
			n.TS.Close()
		}
	}
}
