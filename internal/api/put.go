package api

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/michael-emmanuel/objstore/internal/config"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/object"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/quota"
	"github.com/michael-emmanuel/objstore/internal/ratelimit"
	"github.com/michael-emmanuel/objstore/internal/replication"
)

func hashToken(t string) string { return config.HashToken(t) }

// writeQuorum returns W for an object with replication factor rf.
func (g *Gateway) writeQuorum(rf int) int {
	w := g.cfg.WriteQuorum
	if w <= 0 {
		w = rf/2 + 1
	}
	if w > rf {
		w = rf
	}
	return w
}

// putResult is returned to clients for PUT and part uploads.
type putResult struct {
	Bucket            string `json:"bucket"`
	Key               string `json:"key"`
	VersionID         string `json:"version_id,omitempty"`
	PartNumber        int    `json:"part_number,omitempty"`
	ETag              string `json:"etag"`
	Size              int64  `json:"size"`
	Segments          int    `json:"segments,omitempty"`
	ReplicationFactor int    `json:"replication_factor"`
	WriteQuorum       int    `json:"write_quorum"`
	MinReplicasAcked  int    `json:"min_replicas_acked"`
	UnderReplicated   bool   `json:"under_replicated"`
}

// writeBlob places, protects (pending), and replicates one blob from src.
func (g *Gateway) writeBlob(ctx context.Context, bucket, blobID string, rf int, src io.Reader) (replication.Result, error) {
	ctx, span := g.tracer.Start(ctx, "write_blob", "blob", blobID, "rf", rf)
	m := g.members.Map()
	nodes, err := m.PlaceExcluding(blobID, rf, g.slow.excluded())
	if len(nodes) < rf {
		// Penalized nodes are a performance preference, never a reason to
		// write fewer replicas: fall back to normal placement.
		nodes, err = m.Place(blobID, rf)
	}
	w := g.writeQuorum(rf)
	if len(nodes) < w {
		span.End(err)
		return replication.Result{}, err
	}
	span.SetAttr("epoch", m.Epoch)
	// Register before writing: if we crash after nodes commit but before the
	// metadata commit, GC still will not touch these blobs until PendingTTL.
	mctx, cancel := g.metaCtx(ctx)
	err = g.meta.RegisterPending(mctx, bucket, []string{blobID}, g.cfg.PendingTTL)
	cancel()
	if err != nil {
		span.End(err)
		return replication.Result{}, err
	}
	targets := make([]replication.Target, len(nodes))
	for i, n := range nodes {
		targets[i] = replication.Target{NodeID: n.ID, Addr: n.Addr}
	}
	res, err := g.writer.Write(ctx, blobID, targets, w, src, "client")
	// Penalize nodes that were too slow, whether they stalled mid-stream
	// (queue full) or straggled after the quorum was reached. Errors are not
	// penalized here: a failing node is the failure detector's job.
	for _, id := range res.Dropped {
		g.slow.mark(id)
	}
	for id, reason := range res.Failed {
		if strings.Contains(reason, "straggler") {
			g.slow.mark(id)
		}
	}
	for _, reason := range res.Failed {
		r := "error"
		switch {
		case strings.Contains(reason, "stalled"):
			r = "stalled"
		case strings.Contains(reason, "straggler"):
			r = "straggler"
		case strings.Contains(reason, "insufficient storage"):
			r = "disk_full"
		}
		g.m.replicaDrops.With(r).Inc()
	}
	g.m.replBytes.With().Add(float64(res.Size) * float64(len(res.Acked)))
	span.SetAttr("acked", len(res.Acked), "failed", len(res.Failed))
	span.End(err)
	if len(res.Failed) > 0 {
		g.log.Warn("replica write failures", "blob", blobID, "acked", res.Acked, "failed", res.Failed,
			"request_id", observability.RequestID(ctx))
	}
	return res, err
}

// stallReader enforces a client idle timeout on uploads: each Read must
// make progress within d, or the connection's read deadline fires. This is
// how a stalled or malicious slow client is prevented from pinning a
// gateway upload slot (and replica connections) forever.
type stallReader struct {
	r  io.Reader
	rc *http.ResponseController
	d  time.Duration
}

func (s *stallReader) Read(p []byte) (int, error) {
	if s.rc != nil {
		_ = s.rc.SetReadDeadline(time.Now().Add(s.d))
	}
	return s.r.Read(p)
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func userMeta(h http.Header) map[string]string {
	var m map[string]string
	for k, v := range h {
		if name, ok := strings.CutPrefix(strings.ToLower(k), "x-meta-"); ok && name != "" && len(v) > 0 {
			if m == nil {
				m = map[string]string{}
			}
			m[name] = v[0]
		}
	}
	return m
}

// acquireUpload takes the tenant and global upload slots.
func (g *Gateway) acquireUpload(p principal) (func(), error) {
	release, err := p.tenant.Uploads.TryAcquire()
	if err != nil {
		g.m.rateLimited.With(p.tenant.ID, "concurrent_uploads").Inc()
		return nil, quota.ErrTooManyUploads
	}
	rel2, err := g.uploadSlots.TryAcquire()
	if err != nil {
		release()
		return nil, errOverloaded
	}
	return func() { rel2(); release() }, nil
}

// uploadBody wraps the request body with every upload protection: max size,
// tenant bandwidth, client idle timeout, byte counting.
func (g *Gateway) uploadBody(w http.ResponseWriter, r *http.Request, p principal, max int64) *countingReader {
	var body io.Reader = r.Body
	if max > 0 {
		body = http.MaxBytesReader(w, r.Body, max)
	}
	body = &stallReader{r: body, rc: http.NewResponseController(w), d: g.cfg.ClientIdleTimeout}
	body = ratelimit.Reader(r.Context(), body, p.tenant.Upload)
	return &countingReader{r: body}
}

func (g *Gateway) handlePut(w http.ResponseWriter, r *http.Request) {
	p, b, err := g.prologue(r, true)
	if err == nil && p.anonymous {
		err = errUnauthenticated
	}
	if err != nil {
		g.fail(w, r, err)
		return
	}
	key := r.PathValue("key")
	if err := object.ValidateKey(key); err != nil {
		g.fail(w, r, err)
		return
	}
	ctype := r.Header.Get("Content-Type")
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	meta := userMeta(r.Header)
	if err := object.ValidateUserMeta(meta, ctype); err != nil {
		g.fail(w, r, err)
		return
	}
	wantSHA := strings.ToLower(r.Header.Get("X-Checksum-SHA256"))
	if wantSHA != "" && len(wantSHA) != 64 {
		g.fail(w, r, fmt.Errorf("%w: X-Checksum-SHA256 must be 64 hex chars", errBadRequest))
		return
	}
	if r.ContentLength > 0 {
		if err := p.tenant.CheckObjectSize(r.ContentLength); err != nil {
			g.fail(w, r, err)
			return
		}
		if err := g.tenants.PreCheck(r.Context(), p.tenant, r.ContentLength); err != nil {
			g.fail(w, r, err)
			return
		}
	}
	release, err := g.acquireUpload(p)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	defer release()

	ctx := r.Context()
	rf := b.ReplicationFactor
	vid := object.NewVersionID(time.Now())
	body := g.uploadBody(w, r, p, p.tenant.Limits.MaxObjectBytes)
	br := bufio.NewReaderSize(body, 64<<10)
	whole := sha256.New()
	v := object.Version{Bucket: b.Name, Key: key, VersionID: vid, ContentType: ctype, UserMeta: meta,
		ReplicationFactor: rf, StorageClass: b.StorageClass}
	minAcked := rf
	for i := 0; ; i++ {
		if _, err := br.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				break // no more data: also handles the zero-length object
			}
			g.fail(w, r, fmt.Errorf("%w: %w", replication.ErrSourceRead, err))
			return
		}
		blobID := object.SegmentBlobID(b.Name, key, vid, i)
		seg := io.TeeReader(io.LimitReader(br, g.cfg.SegmentSize), whole)
		res, err := g.writeBlob(ctx, b.Name, blobID, rf, seg)
		if err != nil {
			g.fail(w, r, err)
			return
		}
		v.Segments = append(v.Segments, object.Segment{Index: i, BlobID: blobID, Offset: v.Size, Size: res.Size,
			SHA256: res.SHA256, Replicas: res.Acked})
		v.Size += res.Size
		if len(res.Acked) < minAcked {
			minAcked = len(res.Acked)
		}
	}
	if r.ContentLength >= 0 && body.n != r.ContentLength {
		g.fail(w, r, fmt.Errorf("%w: received %d of %d bytes", replication.ErrSourceRead, body.n, r.ContentLength))
		return
	}
	sum := hex.EncodeToString(whole.Sum(nil))
	if wantSHA != "" && wantSHA != sum {
		// The written blobs are pending-protected, never referenced, and
		// will be reclaimed by GC: nothing partial is ever visible.
		g.fail(w, r, errBadDigest)
		return
	}
	v.Checksum, v.ETag = sum, sum
	g.m.bytesIn.With().Add(float64(v.Size))
	g.m.objSize.With().Observe(float64(v.Size))

	committed, err := g.commit(ctx, p, v)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	if minAcked < rf {
		g.m.underRepl.With().Inc()
	}
	g.audit(r, p, "put_object", b.Name, key, "version_id", committed.VersionID, "size", committed.Size)
	w.Header().Set("ETag", `"`+committed.ETag+`"`)
	w.Header().Set("X-Version-Id", committed.VersionID)
	w.Header().Set("X-Replicas-Acked", strconv.Itoa(minAcked)+"/"+strconv.Itoa(rf))
	writeJSON(w, http.StatusOK, putResult{Bucket: b.Name, Key: key, VersionID: committed.VersionID, ETag: committed.ETag,
		Size: committed.Size, Segments: len(committed.Segments), ReplicationFactor: rf, WriteQuorum: g.writeQuorum(rf),
		MinReplicasAcked: minAcked, UnderReplicated: minAcked < rf})
}

// commit publishes a version to metadata with the tenant's quota ceiling,
// then invalidates caches. This is the visibility point.
func (g *Gateway) commit(ctx context.Context, p principal, v object.Version) (object.Version, error) {
	ceiling, err := g.tenants.ShardCeiling(ctx, p.tenant, g.meta.ShardFor(v.Bucket))
	if err != nil {
		return v, err
	}
	// Commit gets its own deadline detached from the client: if the client
	// disconnects right now, finishing the commit is better than leaving the
	// outcome ambiguous for a write whose data is already durable.
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.MetadataTimeout)
	defer cancel()
	start := time.Now()
	out, err := g.meta.Commit(cctx, v, metadata.CommitOptions{QuotaBytes: ceiling})
	g.m.metaLatency.With("commit").Observe(time.Since(start).Seconds())
	if err != nil {
		return out, err
	}
	g.invalidate(v.Bucket, v.Key)
	p.tenant.Invalidate()
	return out, nil
}

func metaKey(bucket, key string) string { return bucket + "\x00" + key }

func (g *Gateway) invalidate(bucket, key string) { g.metaCache.Remove(metaKey(bucket, key)) }

func (g *Gateway) handleDelete(w http.ResponseWriter, r *http.Request) {
	p, b, err := g.prologue(r, true)
	if err == nil && p.anonymous {
		err = errUnauthenticated
	}
	if err != nil {
		g.fail(w, r, err)
		return
	}
	key := r.PathValue("key")
	if err := object.ValidateKey(key); err != nil {
		g.fail(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), g.cfg.MetadataTimeout)
	defer cancel()
	if vid := r.URL.Query().Get("version_id"); vid != "" {
		err = g.meta.DeleteVersion(ctx, b.Name, key, vid)
		g.invalidate(b.Name, key)
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			g.fail(w, r, err)
			return
		}
		g.audit(r, p, "delete_version", b.Name, key, "version_id", vid)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	marker, err := g.meta.Delete(ctx, b.Name, key)
	g.invalidate(b.Name, key)
	p.tenant.Invalidate()
	if err != nil && !errors.Is(err, metadata.ErrNotFound) {
		g.fail(w, r, err)
		return
	}
	if err == nil {
		w.Header().Set("X-Delete-Marker", "true")
		w.Header().Set("X-Version-Id", marker.VersionID)
	}
	g.audit(r, p, "delete_object", b.Name, key)
	w.WriteHeader(http.StatusNoContent) // deleting a missing key is not an error (idempotent)
}
