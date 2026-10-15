// Package garbagecollector reclaims physical blobs that metadata no longer
// needs, plus abandoned multipart uploads and old tombstones.
//
// Two complementary mechanisms:
//
//  1. Queue (precise): metadata enqueues every blob it dereferences
//     (overwrite, delete, aborted upload, replaced part) together with the
//     replica list it knew. After a grace period the collector re-checks the
//     reference, deletes each copy, and acknowledges the queue entry.
//
//  2. Sweep (mark-and-sweep, catches everything else): page through every
//     node's blobs; a blob is an orphan if no shard references it and no
//     in-flight write has it pending, OR if it is referenced but this node is
//     not in its replica list (a stale copy left by a failed delete, a node
//     that came back after repair, an interrupted move). Orphans are deleted
//     only if (a) older than the grace period AND (b) seen as orphans in two
//     consecutive sweeps.
//
// Safety rules (why the collector never deletes live data):
//   - Never act on a single lookup: grace period + re-check + two sightings.
//   - An unreachable metadata shard fails CheckRefs: no answer means no delete.
//   - In-flight writes are protected by "pending" registrations that the
//     gateway makes BEFORE sending any byte to a node.
//   - Repair/rebalance copies land before metadata lists them; the grace
//     period covers that window.
package garbagecollector

import (
	"context"
	"log/slog"
	"time"

	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
)

type Config struct {
	Grace        time.Duration // minimum age before anything is reclaimed (default 1h)
	UploadTTL    time.Duration // abandoned multipart uploads are aborted after this (default 24h)
	TombstoneTTL time.Duration // delete markers in unversioned buckets are purged after this (default 24h)
	Sweep        bool          // run the orphan sweep
	Batch        int
}

type Collector struct {
	cfg     Config
	meta    metadata.Store
	members membership.Source
	nodes   *storagenode.Client
	log     *slog.Logger
	now     func() time.Time

	suspects map[string]time.Time // node|blob -> first sighting as orphan (only touched by RunOnce)

	deleted          *observability.CounterVec
	reclaimed        *observability.Counter
	queueDepth       *observability.Gauge
	uploads, markers *observability.Counter
}

func New(cfg Config, meta metadata.Store, members membership.Source, nodes *storagenode.Client, log *slog.Logger, reg *observability.Registry) *Collector {
	if cfg.Grace <= 0 {
		cfg.Grace = time.Hour
	}
	if cfg.UploadTTL <= 0 {
		cfg.UploadTTL = 24 * time.Hour
	}
	if cfg.TombstoneTTL <= 0 {
		cfg.TombstoneTTL = 24 * time.Hour
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 500
	}
	if reg == nil {
		reg = observability.NewRegistry()
	}
	return &Collector{cfg: cfg, meta: meta, members: members, nodes: nodes, log: log, now: time.Now,
		suspects:   map[string]time.Time{},
		deleted:    reg.Counter("gc_deleted_blobs_total", "Physical blob copies deleted by GC.", "source"),
		reclaimed:  reg.Counter("gc_reclaimed_bytes_total", "Bytes reclaimed by the orphan sweep.").With(),
		queueDepth: reg.Gauge("gc_queue_depth", "Garbage queue entries past their grace period at the last pass.").With(),
		uploads:    reg.Counter("gc_expired_uploads_total", "Abandoned multipart uploads aborted.").With(),
		markers:    reg.Counter("gc_purged_tombstones_total", "Delete markers purged.").With(),
	}
}

type Report struct {
	ExpiredUploads int `json:"expired_uploads"`
	PurgedMarkers  int `json:"purged_markers"`
	QueueDeleted   int `json:"queue_deleted"`
	QueueAcked     int `json:"queue_acked"`
	QueueDeferred  int `json:"queue_deferred"`
	OrphansSeen    int `json:"orphans_seen"`
	OrphansDeleted int `json:"orphans_deleted"`
}

func (c *Collector) RunOnce(ctx context.Context) (Report, error) {
	var rep Report
	now := c.now()
	n, err := c.meta.ExpireUploads(ctx, now.Add(-c.cfg.UploadTTL))
	if err != nil {
		return rep, err
	}
	rep.ExpiredUploads = n
	c.uploads.Add(float64(n))
	if n, err = c.meta.PurgeTombstones(ctx, now.Add(-c.cfg.TombstoneTTL)); err != nil {
		return rep, err
	}
	rep.PurgedMarkers = n
	c.markers.Add(float64(n))

	m := c.members.Map()
	if err := c.drainQueue(ctx, m, now, &rep); err != nil {
		return rep, err
	}
	if c.cfg.Sweep {
		if err := c.sweep(ctx, m, now, &rep); err != nil {
			return rep, err
		}
	}
	if rep.QueueDeleted+rep.OrphansDeleted+rep.ExpiredUploads+rep.PurgedMarkers > 0 {
		c.log.Info("gc pass", "queue_deleted", rep.QueueDeleted, "queue_deferred", rep.QueueDeferred,
			"orphans_deleted", rep.OrphansDeleted, "expired_uploads", rep.ExpiredUploads, "purged_markers", rep.PurgedMarkers)
	}
	return rep, nil
}

func (c *Collector) drainQueue(ctx context.Context, m *placement.Map, now time.Time, rep *Report) error {
	for {
		items, err := c.meta.PeekGarbage(ctx, now.Add(-c.cfg.Grace), c.cfg.Batch)
		if err != nil {
			return err
		}
		c.queueDepth.Set(float64(len(items)))
		if len(items) == 0 {
			return nil
		}
		ids := make([]string, len(items))
		for i, it := range items {
			ids[i] = it.BlobID
		}
		refs, err := c.meta.CheckRefs(ctx, ids)
		if err != nil {
			return err // no answer => no deletion
		}
		var ack []metadata.GarbageItem
		for _, it := range items {
			if ref := refs[it.BlobID]; ref.Referenced || ref.Pending {
				ack = append(ack, it) // re-referenced: drop the queue entry, keep data
				continue
			}
			complete := true
			for _, id := range it.Replicas {
				n, ok := m.Node(id)
				if !ok || n.State == placement.StateDecommissioned {
					continue // node gone for good
				}
				if !n.Readable() {
					complete = false // node down: retry later; the sweep is the backstop
					continue
				}
				if err := c.nodes.Delete(ctx, n.Addr, it.BlobID); err != nil {
					complete = false
					continue
				}
				rep.QueueDeleted++
				c.deleted.With("queue").Inc()
			}
			if complete {
				ack = append(ack, it)
			} else {
				rep.QueueDeferred++
			}
		}
		if err := c.meta.AckGarbage(ctx, ack); err != nil {
			return err
		}
		rep.QueueAcked += len(ack)
		if len(ack) == 0 {
			return nil // only deferred items remain; avoid spinning
		}
	}
}

func (c *Collector) sweep(ctx context.Context, m *placement.Map, now time.Time, rep *Report) error {
	seen := map[string]time.Time{}
	for _, n := range m.Nodes() {
		if !n.Readable() {
			continue
		}
		after := ""
		for {
			page, err := c.nodes.List(ctx, n.Addr, after, c.cfg.Batch)
			if err != nil {
				break // node unreachable: skip this pass
			}
			if len(page) == 0 {
				break
			}
			after = page[len(page)-1].ID
			var old []string
			sizes := map[string]int64{}
			for _, b := range page {
				if now.Sub(b.ModTime) >= c.cfg.Grace {
					old = append(old, b.ID)
					sizes[b.ID] = b.Size
				}
			}
			if len(old) == 0 {
				continue
			}
			refs, err := c.meta.CheckRefs(ctx, old)
			if err != nil {
				return err
			}
			for _, id := range old {
				ref := refs[id]
				orphan := (!ref.Referenced && !ref.Pending) || (ref.Referenced && !contains(ref.Replicas, n.ID))
				if !orphan {
					continue
				}
				rep.OrphansSeen++
				key := n.ID + "|" + id
				first, ok := c.suspects[key]
				if !ok {
					first = now
				}
				seen[key] = first
				if ok { // second consecutive sighting
					if err := c.nodes.Delete(ctx, n.Addr, id); err == nil {
						rep.OrphansDeleted++
						c.deleted.With("sweep").Inc()
						c.reclaimed.Add(float64(sizes[id]))
						delete(seen, key)
					}
				}
			}
		}
	}
	c.suspects = seen // suspects not re-sighted this pass are forgotten
	return nil
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func (c *Collector) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if _, err := c.RunOnce(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("gc pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
