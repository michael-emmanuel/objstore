// Package membership tracks storage nodes, detects failures from missed
// heartbeats, and records operator-driven lifecycle states (DRAINING,
// DECOMMISSIONED).
//
// Failure detection is a timeout-based detector with two thresholds:
//
//	no heartbeat for SuspectAfter -> SUSPECT (stop placing new data)
//	no heartbeat for DownAfter    -> DOWN    (stop reading; repair after RepairDelay)
//
// The two-stage design trades detection speed for false-positive cost. A
// false DOWN triggers re-replication of everything on the node: that is the
// "repair storm" risk, so the repairer additionally waits RepairDelay after
// DOWN before copying data (see internal/repair).
//
// The registry is a single process (the "coordinator", co-located with
// metadata shard 0). It is not replicated; while it is unavailable, gateways
// keep using their last membership snapshot, so the data plane keeps
// working but failure detection and state changes pause.
package membership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/objstore/internal/placement"
)

var ErrUnknownNode = errors.New("unknown node")

// Options configures the registry.
type Options struct {
	SuspectAfter time.Duration
	DownAfter    time.Duration
	StatePath    string // persisted operator states; empty disables persistence
	Now          func() time.Time
	Log          *slog.Logger
}

// Registry is safe for concurrent use.
type Registry struct {
	opts  Options
	mu    sync.Mutex
	nodes map[string]*placement.Node
	epoch uint64
}

func NewRegistry(opts Options) (*Registry, error) {
	if opts.SuspectAfter <= 0 {
		opts.SuspectAfter = 3 * time.Second
	}
	if opts.DownAfter <= 0 {
		opts.DownAfter = 10 * time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	r := &Registry{opts: opts, nodes: map[string]*placement.Node{}, epoch: 1}
	if opts.StatePath != "" {
		b, err := os.ReadFile(opts.StatePath)
		if err == nil {
			var saved []placement.Node
			if err := json.Unmarshal(b, &saved); err != nil {
				return nil, fmt.Errorf("membership state: %w", err)
			}
			now := opts.Now()
			for i := range saved {
				n := saved[i]
				// Restarting the coordinator must not declare the whole
				// cluster dead: give every node a fresh heartbeat window.
				n.LastHeartbeat = now
				if n.State == placement.StateDown || n.State == placement.StateSuspect {
					n.State = placement.StateSuspect
				}
				r.nodes[n.ID] = &n
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return r, nil
}

// Heartbeat registers or refreshes a node. Operator states (DRAINING,
// DECOMMISSIONED) are preserved; failure states recover to ACTIVE.
func (r *Registry) Heartbeat(hb placement.Node) (placement.Node, error) {
	if hb.ID == "" || hb.Addr == "" {
		return placement.Node{}, errors.New("heartbeat requires id and addr")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.opts.Now()
	n, ok := r.nodes[hb.ID]
	if !ok {
		n = &placement.Node{ID: hb.ID, State: placement.StateActive, StateSince: now}
		r.nodes[hb.ID] = n
		r.epoch++
		r.opts.Log.Info("node joined", "node", hb.ID, "zone", hb.Zone, "rack", hb.Rack)
		defer r.persistLocked()
	}
	if n.Addr != hb.Addr || n.Zone != hb.Zone || n.Rack != hb.Rack || n.Region != hb.Region || n.Weight != hb.Weight {
		n.Addr, n.Zone, n.Rack, n.Region, n.Weight = hb.Addr, hb.Zone, hb.Rack, hb.Region, hb.Weight
		r.epoch++
	}
	n.CapacityBytes, n.UsedBytes, n.BlobCount = hb.CapacityBytes, hb.UsedBytes, hb.BlobCount
	n.LastHeartbeat = now
	if n.State == placement.StateSuspect || n.State == placement.StateDown {
		r.opts.Log.Info("node recovered", "node", n.ID, "from", n.State)
		r.setStateLocked(n, placement.StateActive)
	}
	return *n, nil
}

func (r *Registry) setStateLocked(n *placement.Node, s placement.NodeState) {
	if n.State == s {
		return
	}
	n.State = s
	n.StateSince = r.opts.Now()
	r.epoch++
}

// SetState applies an operator state change.
func (r *Registry) SetState(id string, s placement.NodeState) error {
	switch s {
	case placement.StateActive, placement.StateDraining, placement.StateDecommissioned:
	default:
		return fmt.Errorf("operator may only set ACTIVE, DRAINING or DECOMMISSIONED, not %s", s)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return ErrUnknownNode
	}
	r.opts.Log.Info("node state change (operator)", "node", id, "from", n.State, "to", s)
	r.setStateLocked(n, s)
	r.persistLocked()
	return nil
}

// Tick evaluates heartbeat timeouts. Run calls it periodically; tests call
// it directly with a fake clock.
func (r *Registry) Tick() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.opts.Now()
	for _, n := range r.nodes {
		if n.State == placement.StateDecommissioned {
			continue
		}
		since := now.Sub(n.LastHeartbeat)
		switch {
		case since >= r.opts.DownAfter && n.State != placement.StateDown:
			if n.State == placement.StateDraining {
				// A draining node that dies is still a failure: its data
				// must be repaired, not migrated.
				r.opts.Log.Warn("draining node failed", "node", n.ID)
			}
			r.opts.Log.Warn("node down", "node", n.ID, "silent_for", since.String())
			r.setStateLocked(n, placement.StateDown)
		case since >= r.opts.SuspectAfter && n.State == placement.StateActive:
			r.opts.Log.Warn("node suspect", "node", n.ID, "silent_for", since.String())
			r.setStateLocked(n, placement.StateSuspect)
		}
	}
}

// Run ticks until ctx is cancelled.
func (r *Registry) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.Tick()
		}
	}
}

// Snapshot returns the epoch and all nodes sorted by ID.
func (r *Registry) Snapshot() (uint64, []placement.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]placement.Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return r.epoch, out
}

func (r *Registry) persistLocked() {
	if r.opts.StatePath == "" {
		return
	}
	nodes := make([]placement.Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		nodes = append(nodes, *n)
	}
	b, _ := json.MarshalIndent(nodes, "", "  ")
	tmp := r.opts.StatePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err == nil {
		err = os.Rename(tmp, r.opts.StatePath)
		if err != nil {
			r.opts.Log.Warn("persist membership", "error", err)
		}
	}
}

// ---------------------------------------------------------------------------
// HTTP

// View is the wire form of a membership snapshot.
type View struct {
	Epoch uint64           `json:"epoch"`
	Nodes []placement.Node `json:"nodes"`
}

// Register mounts membership endpoints on mux.
func (r *Registry) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/nodes/heartbeat", func(w http.ResponseWriter, req *http.Request) {
		var hb placement.Node
		if err := json.NewDecoder(req.Body).Decode(&hb); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		n, err := r.Heartbeat(hb)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		json.NewEncoder(w).Encode(n)
	})
	mux.HandleFunc("GET /v1/nodes", func(w http.ResponseWriter, _ *http.Request) {
		e, nodes := r.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(View{Epoch: e, Nodes: nodes})
	})
	mux.HandleFunc("POST /v1/nodes/{id}/state", func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			State placement.NodeState `json:"state"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := r.SetState(req.PathValue("id"), body.State); err != nil {
			code := http.StatusBadRequest
			if errors.Is(err, ErrUnknownNode) {
				code = http.StatusNotFound
			}
			http.Error(w, err.Error(), code)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
