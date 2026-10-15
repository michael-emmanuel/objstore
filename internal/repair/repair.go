// Package repair restores the replication factor of segments that lost
// replicas (dead nodes, failed disks, quarantined corrupt blobs, writes that
// were acknowledged at W < RF).
//
// One pass:
//
//  1. Drain quarantine reports from every reachable node: each corrupt blob
//     is removed from metadata's replica list for that node.
//  2. Scan all segments in metadata and classify each one:
//     healthy replicas      on ACTIVE / SUSPECT / DRAINING nodes
//     lost replicas         on DOWN nodes past RepairDelay, unknown or decommissioned nodes
//     pending-down replicas on DOWN nodes within RepairDelay
//  3. Queue a job when healthy < RF, EXCEPT when the deficit is covered by
//     pending-down replicas (a node rebooting for 2 minutes must not trigger
//     a cluster-wide copy storm). Exception to the exception: a segment with
//     a single healthy replica is repaired immediately.
//  4. Execute jobs fewest-healthy-replicas first, with bounded concurrency
//     and a bandwidth budget. The copy is node-to-node (target pulls from
//     source and verifies the SHA-256 from metadata); the repairer only
//     orchestrates.
//  5. Publish the new replica set to metadata (add new, drop lost).
//
// Limitation (documented): the scan is O(segments) per pass. At scale the
// repairer would be driven by a per-node index ("which blobs live on node
// X") so that a node failure enqueues exactly its blobs.
package repair

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/ratelimit"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
)

type Config struct {
	RepairDelay          time.Duration // wait after DOWN before copying (default 30s; production would be ~15m)
	Concurrency          int           // parallel copies (default 4)
	BandwidthBytesPerSec float64       // total repair bandwidth budget; 0 = unlimited
	ScanBatch            int
}

type Repairer struct {
	cfg     Config
	meta    metadata.Store
	members membership.Source
	nodes   *storagenode.Client
	log     *slog.Logger
	limiter *ratelimit.Bucket
	now     func() time.Time

	underRepl, unavailable, queueDepth  *observability.Gauge
	repaired, failed, bytes, quarantine *observability.Counter
	passSeconds                         *observability.Gauge
}

func New(cfg Config, meta metadata.Store, members membership.Source, nodes *storagenode.Client, log *slog.Logger, reg *observability.Registry) *Repairer {
	if cfg.RepairDelay <= 0 {
		cfg.RepairDelay = 30 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.ScanBatch <= 0 {
		cfg.ScanBatch = 1000
	}
	r := &Repairer{cfg: cfg, meta: meta, members: members, nodes: nodes, log: log, now: time.Now,
		limiter: ratelimit.NewBucket(cfg.BandwidthBytesPerSec, cfg.BandwidthBytesPerSec)}
	if reg == nil {
		reg = observability.NewRegistry()
	}
	r.underRepl = reg.Gauge("repair_under_replicated_segments", "Segments below RF at the last scan.").With()
	r.unavailable = reg.Gauge("repair_unavailable_segments", "Segments with no healthy replica at the last scan.").With()
	r.queueDepth = reg.Gauge("repair_queue_depth", "Repair jobs remaining in the current pass.").With()
	r.repaired = reg.Counter("repair_copies_total", "Replica copies created by repair.").With()
	r.failed = reg.Counter("repair_failures_total", "Repair attempts that failed.").With()
	r.bytes = reg.Counter("repair_bytes_total", "Bytes copied by repair (repair throughput).").With()
	r.quarantine = reg.Counter("repair_quarantine_processed_total", "Quarantined (corrupt) replicas removed from metadata.").With()
	r.passSeconds = reg.Gauge("repair_last_pass_seconds", "Duration of the last repair pass.").With()
	return r
}

// Report summarizes one pass.
type Report struct {
	Scanned         int   `json:"scanned"`
	UnderReplicated int   `json:"under_replicated"`
	Deferred        int   `json:"deferred"`
	Unavailable     int   `json:"unavailable"`
	Repaired        int   `json:"repaired"`
	Failed          int   `json:"failed"`
	Quarantined     int   `json:"quarantined"`
	BytesCopied     int64 `json:"bytes_copied"`
}

type job struct {
	seg     metadata.SegmentRef
	healthy []placement.Node
	lost    []string
	deficit int
}

// RunOnce executes one full pass.
func (r *Repairer) RunOnce(ctx context.Context) (Report, error) {
	start := r.now()
	defer func() { r.passSeconds.Set(r.now().Sub(start).Seconds()) }()
	var rep Report
	m := r.members.Map()
	r.processQuarantine(ctx, m, &rep)

	var jobs []job
	cursor := ""
	for {
		segs, next, err := r.meta.ScanSegments(ctx, cursor, r.cfg.ScanBatch)
		if err != nil {
			return rep, err
		}
		for _, s := range segs {
			rep.Scanned++
			if j, ok := r.classify(m, s, &rep); ok {
				jobs = append(jobs, j)
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}
	r.underRepl.Set(float64(rep.UnderReplicated))
	r.unavailable.Set(float64(rep.Unavailable))

	// Most endangered first: a segment with one copy left outranks one with two.
	sort.SliceStable(jobs, func(i, j int) bool { return len(jobs[i].healthy) < len(jobs[j].healthy) })
	r.queueDepth.Set(float64(len(jobs)))

	var mu sync.Mutex
	work := make(chan job)
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range work {
				n, bytes, err := r.repairOne(ctx, m, j)
				mu.Lock()
				rep.Repaired += n
				rep.BytesCopied += bytes
				if err != nil {
					rep.Failed++
					r.failed.Inc()
					r.log.Warn("repair failed", "blob", j.seg.BlobID, "error", err)
				}
				mu.Unlock()
				r.queueDepth.Add(-1)
			}
		}()
	}
	for _, j := range jobs {
		select {
		case work <- j:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(work)
	wg.Wait()
	if rep.UnderReplicated > 0 || rep.Quarantined > 0 {
		r.log.Info("repair pass", "scanned", rep.Scanned, "under_replicated", rep.UnderReplicated, "deferred", rep.Deferred,
			"unavailable", rep.Unavailable, "repaired", rep.Repaired, "failed", rep.Failed, "bytes", rep.BytesCopied,
			"quarantined", rep.Quarantined, "epoch", m.Epoch)
	}
	return rep, ctx.Err()
}

func (r *Repairer) classify(m *placement.Map, s metadata.SegmentRef, rep *Report) (job, bool) {
	var healthy []placement.Node
	var lost, pendingDown []string
	for _, id := range s.Replicas {
		n, ok := m.Node(id)
		switch {
		case !ok || n.State == placement.StateDecommissioned:
			lost = append(lost, id)
		case n.Readable():
			healthy = append(healthy, n)
		case n.State == placement.StateDown && r.now().Sub(n.StateSince) >= r.cfg.RepairDelay:
			lost = append(lost, id)
		default:
			pendingDown = append(pendingDown, id)
		}
	}
	if len(healthy) >= s.RF {
		return job{}, false
	}
	rep.UnderReplicated++
	if len(healthy) == 0 {
		rep.Unavailable++
		return job{}, false
	}
	deficit := s.RF - len(healthy)
	if len(healthy) > 1 {
		// Not urgent: give briefly-down nodes a chance to come back.
		deficit -= len(pendingDown)
		if deficit <= 0 {
			rep.Deferred++
			return job{}, false
		}
	}
	return job{seg: s, healthy: healthy, lost: lost, deficit: deficit}, true
}

func (r *Repairer) repairOne(ctx context.Context, m *placement.Map, j job) (int, int64, error) {
	targets := m.Complete(j.seg.BlobID, j.seg.RF, j.healthy)
	if len(targets) > j.deficit {
		targets = targets[:j.deficit]
	}
	if len(targets) == 0 {
		return 0, 0, errors.New("no eligible target node")
	}
	// Prefer ACTIVE sources; SUSPECT/DRAINING are fallbacks.
	sources := append([]placement.Node(nil), j.healthy...)
	sort.SliceStable(sources, func(a, b int) bool {
		return sources[a].State == placement.StateActive && sources[b].State != placement.StateActive
	})
	var added []string
	var copied int64
	var lastErr error
	for _, t := range targets {
		if err := r.limiter.Wait(ctx, float64(j.seg.Size)); err != nil {
			return len(added), copied, err
		}
		ok := false
		for _, src := range sources {
			pctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			_, err := r.nodes.Pull(pctx, t.Addr, j.seg.BlobID, src.Addr, j.seg.SHA256, "repair")
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			ok = true
			break
		}
		if ok {
			added = append(added, t.ID)
			copied += j.seg.Size
		}
	}
	if len(added) == 0 && len(j.lost) == 0 {
		return 0, 0, lastErr
	}
	err := r.meta.UpdateReplicas(ctx, j.seg.Bucket, j.seg.BlobID, added, j.lost)
	if errors.Is(err, metadata.ErrNotFound) {
		// The object was deleted while we copied; our new copies are
		// orphans that GC's sweep will reclaim.
		return 0, 0, nil
	}
	if err != nil {
		return 0, copied, err
	}
	r.repaired.Add(float64(len(added)))
	r.bytes.Add(float64(copied))
	if len(added) < len(targets) {
		return len(added), copied, lastErr
	}
	return len(added), copied, nil
}

// processQuarantine removes corrupt replicas from metadata so the scan sees
// the segment as under-replicated, then acknowledges the quarantine.
func (r *Repairer) processQuarantine(ctx context.Context, m *placement.Map, rep *Report) {
	for _, n := range m.Nodes() {
		if !n.Readable() {
			continue
		}
		q, err := r.nodes.Quarantined(ctx, n.Addr)
		if err != nil || len(q) == 0 {
			continue
		}
		ids := make([]string, 0, len(q))
		for id := range q {
			ids = append(ids, id)
		}
		refs, err := r.meta.CheckRefs(ctx, ids)
		if err != nil {
			continue
		}
		var done []string
		for _, id := range ids {
			ref := refs[id]
			if ref.Referenced && contains(ref.Replicas, n.ID) {
				if err := r.meta.UpdateReplicas(ctx, ref.Bucket, id, nil, []string{n.ID}); err != nil {
					continue
				}
				r.log.Warn("corrupt replica removed from metadata; will re-replicate", "blob", id, "node", n.ID, "reason", q[id])
				rep.Quarantined++
				r.quarantine.Inc()
			}
			done = append(done, id)
		}
		if len(done) > 0 {
			r.nodes.AckQuarantine(ctx, n.Addr, done)
		}
	}
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// Run executes passes until ctx is cancelled.
func (r *Repairer) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("repair pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
