package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/michael-emmanuel/objstore/internal/clusterauth"
	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
)

// ClusterClients are the control-plane and data-plane clients every
// cluster-side process (gateway, repairer, rebalancer) needs.
type ClusterClients struct {
	Meta    *metadata.Router
	Coord   *membership.Client
	Watcher *membership.Watcher
	Nodes   *storagenode.Client
}

// ClusterOptions configures Dial.
type ClusterOptions struct {
	MetadataURLs   []string // shard i at index i
	CoordinatorURL string
	Algorithm      placement.Algorithm
	Token          string
	Log            *slog.Logger
}

// Dial builds authenticated clients and loads the first membership view.
// It does not start background refresh: call RunWatcher from a Group.
func Dial(ctx context.Context, o ClusterOptions) (*ClusterClients, error) {
	if len(o.MetadataURLs) == 0 {
		return nil, fmt.Errorf("at least one metadata shard URL is required")
	}
	switch o.Algorithm {
	case placement.AlgoRing, placement.AlgoRendezvous:
	default:
		return nil, fmt.Errorf("unknown placement algorithm %q (ring|rendezvous)", o.Algorithm)
	}
	var shards []metadata.Store
	for _, u := range o.MetadataURLs {
		hc := clusterauth.Wrap(&http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}, o.Token)
		shards = append(shards, metadata.NewClient(u, hc))
	}
	coord := membership.NewClient(o.CoordinatorURL)
	coord.HC = clusterauth.Wrap(coord.HC, o.Token)
	c := &ClusterClients{
		Meta:    metadata.NewRouter(shards),
		Coord:   coord,
		Watcher: membership.NewWatcher(coord, o.Algorithm, o.Log),
		Nodes:   storagenode.NewClient(clusterauth.Wrap(storagenode.DefaultHTTPClient(), o.Token)),
	}
	// Retry the first membership fetch briefly so containers can start in
	// any order; afterwards the watcher keeps the last good view on errors.
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := c.Watcher.Refresh(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("initial membership fetch from %s: %w", o.CoordinatorURL, err)
		}
		o.Log.Warn("coordinator not reachable yet, retrying", "error", err)
		time.Sleep(time.Second)
	}
	return c, nil
}
