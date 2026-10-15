// Package rebalancer moves replicas to where the placement algorithm says
// they should live, after membership changes (node added, node draining).
//
// It never reduces durability: every move is copy-before-delete.
//
//  1. pull blob to the new node (verified against metadata SHA-256)
//  2. metadata: add new replica           -> temporarily RF+1 copies
//  3. metadata: remove old replica        -> readers stop choosing it
//  4. delete old physical copy (best effort; GC sweeps leftovers)
//
// It only touches fully replicated segments; under-replicated segments are
// the repairer's job. Moves per pass and bandwidth are bounded so that
// adding a node does not saturate the network: rebalancing is slow on
// purpose, while repair is prioritized.
package rebalancer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/ratelimit"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
)

type Config struct {
	MaxMovesPerPass      int     // default 1000
	BandwidthBytesPerSec float64 // 0 = unlimited
	TargetMaxFullness    float64 // never move data TO a node above this used fraction (default 0.90)
	ScanBatch            int
}

type Rebalancer struct {
	cfg     Config
	meta    metadata.Store
	members membership.Source
	nodes   *storagenode.Client
	log     *slog.Logger
	limiter *ratelimit.Bucket

	misplaced          *observability.Gauge
	moves, bytes, trim *observability.Counter
}

func New(cfg Config, meta metadata.Store, members membership.Source, nodes *storagenode.Client, log *slog.Logger, reg *observability.Registry) *Rebalancer {
	if cfg.MaxMovesPerPass <= 0 {
		cfg.MaxMovesPerPass = 1000
	}
	if cfg.TargetMaxFullness <= 0 {
		cfg.TargetMaxFullness = 0.90
	}
	if cfg.ScanBatch <= 0 {
		cfg.ScanBatch = 1000
	}
	if reg == nil {
		reg = observability.NewRegistry()
	}
	return &Rebalancer{cfg: cfg, meta: meta, members: members, nodes: nodes, log: log,
		limiter:   ratelimit.NewBucket(cfg.BandwidthBytesPerSec, cfg.BandwidthBytesPerSec),
		misplaced: reg.Gauge("rebalance_misplaced_segments", "Fully replicated segments not on their desired nodes.").With(),
		moves:     reg.Counter("rebalance_moves_total", "Replica moves completed.").With(),
		bytes:     reg.Counter("rebalance_bytes_total", "Bytes copied by rebalancing.").With(),
		trim:      reg.Counter("rebalance_trimmed_replicas_total", "Excess replicas removed (over-replication).").With(),
	}
}

type Report struct {
	Scanned   int   `json:"scanned"`
	Misplaced int   `json:"misplaced"`
	Moved     int   `json:"moved"`
	Trimmed   int   `json:"trimmed"`
	Failed    int   `json:"failed"`
	Bytes     int64 `json:"bytes"`
}

func (b *Rebalancer) RunOnce(ctx context.Context) (Report, error) {
	var rep Report
	m := b.members.Map()
	cursor := ""
	for {
		segs, next, err := b.meta.ScanSegments(ctx, cursor, b.cfg.ScanBatch)
		if err != nil {
			return rep, err
		}
		for _, s := range segs {
			rep.Scanned++
			if err := b.reconcile(ctx, m, s, &rep); err != nil {
				rep.Failed++
				b.log.Warn("rebalance move failed", "blob", s.BlobID, "error", err)
			}
			if ctx.Err() != nil {
				return rep, ctx.Err()
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	b.misplaced.Set(float64(rep.Misplaced))
	if rep.Misplaced > 0 || rep.Trimmed > 0 {
		b.log.Info("rebalance pass", "scanned", rep.Scanned, "misplaced", rep.Misplaced, "moved", rep.Moved,
			"trimmed", rep.Trimmed, "failed", rep.Failed, "bytes", rep.Bytes, "epoch", m.Epoch)
	}
	return rep, nil
}

func (b *Rebalancer) reconcile(ctx context.Context, m *placement.Map, s metadata.SegmentRef, rep *Report) error {
	cur := map[string]placement.Node{}
	for _, id := range s.Replicas {
		n, ok := m.Node(id)
		if !ok || !n.Readable() {
			return nil // degraded: repair first
		}
		cur[id] = n
	}
	if len(cur) < s.RF {
		return nil
	}
	desired := m.PlaceStable(s.BlobID, s.RF)
	if len(desired) < s.RF {
		return nil // not enough eligible nodes to define a better placement
	}
	want := map[string]bool{}
	var missing []placement.Node
	for _, n := range desired {
		want[n.ID] = true
		if _, ok := cur[n.ID]; !ok {
			missing = append(missing, n)
		}
	}
	var extra []placement.Node
	for id, n := range cur {
		if !want[id] {
			extra = append(extra, n)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	rep.Misplaced++
	// Prefer evicting from DRAINING nodes first.
	for i := range extra {
		if extra[i].State == placement.StateDraining {
			extra[0], extra[i] = extra[i], extra[0]
			break
		}
	}
	// Over-replicated with nothing missing (e.g. a node came back after its
	// replicas were already repaired elsewhere): trim the excess.
	if len(missing) == 0 {
		for _, x := range extra {
			if len(cur) <= s.RF {
				break
			}
			if err := b.removeReplica(ctx, s, x); err != nil {
				return err
			}
			delete(cur, x.ID)
			rep.Trimmed++
			b.trim.Inc()
		}
		return nil
	}
	if rep.Moved >= b.cfg.MaxMovesPerPass {
		return nil
	}
	dst := missing[0]
	if dst.CapacityBytes > 0 && float64(dst.UsedBytes) >= b.cfg.TargetMaxFullness*float64(dst.CapacityBytes) {
		return nil
	}
	if err := b.limiter.Wait(ctx, float64(s.Size)); err != nil {
		return err
	}
	var src placement.Node
	for _, n := range cur {
		if src.ID == "" || (n.State == placement.StateActive && src.State != placement.StateActive) {
			src = n
		}
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	_, err := b.nodes.Pull(pctx, dst.Addr, s.BlobID, src.Addr, s.SHA256, "rebalance")
	cancel()
	if err != nil {
		return err
	}
	if err := b.meta.UpdateReplicas(ctx, s.Bucket, s.BlobID, []string{dst.ID}, nil); err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return nil // deleted meanwhile; GC reclaims the copy
		}
		return err
	}
	rep.Moved++
	rep.Bytes += s.Size
	b.moves.Inc()
	b.bytes.Add(float64(s.Size))
	if len(extra) > 0 && len(cur)+1 > s.RF {
		return b.removeReplica(ctx, s, extra[0])
	}
	return nil
}

// removeReplica unpublishes first, then deletes. Readers that already chose
// this replica may get a 404 and fail over to another copy.
func (b *Rebalancer) removeReplica(ctx context.Context, s metadata.SegmentRef, n placement.Node) error {
	if err := b.meta.UpdateReplicas(ctx, s.Bucket, s.BlobID, nil, []string{n.ID}); err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			return nil
		}
		return err
	}
	if err := b.nodes.Delete(ctx, n.Addr, s.BlobID); err != nil {
		b.log.Debug("delete after move failed; GC will reclaim", "blob", s.BlobID, "node", n.ID, "error", err)
	}
	return nil
}

func (b *Rebalancer) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if _, err := b.RunOnce(ctx); err != nil && ctx.Err() == nil {
			b.log.Warn("rebalance pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
