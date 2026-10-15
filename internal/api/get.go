package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/object"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/quorum"
	"github.com/michael-emmanuel/objstore/internal/quota"
	"github.com/michael-emmanuel/objstore/internal/ratelimit"
	"github.com/michael-emmanuel/objstore/internal/storage"
)

var errRangeNotSatisfiable = errors.New("range not satisfiable")

// lookup returns the requested version, consulting the metadata cache for
// "latest" lookups only. Cached entries may be up to MetaCacheTTL stale for
// writes made through OTHER gateways (docs/consistency.md).
func (g *Gateway) lookup(ctx context.Context, bucket, key, versionID string) (object.Version, error) {
	ck := metaKey(bucket, key)
	if versionID == "" {
		if v, ok := g.metaCache.Get(ck); ok {
			return v, nil
		}
	}
	mctx, cancel := g.metaCtx(ctx)
	defer cancel()
	start := time.Now()
	v, err := g.meta.Get(mctx, bucket, key, versionID)
	g.m.metaLatency.With("get").Observe(time.Since(start).Seconds())
	if err == nil && versionID == "" {
		g.metaCache.Add(ck, v, 1)
	}
	return v, err
}

// parseClientRange implements single byte ranges (RFC 9110 14.1.2). Multi-
// range and syntactically invalid headers are ignored (full response), as
// the RFC permits; unsatisfiable ranges are an error.
func parseClientRange(h string, size int64) (start, length int64, partial bool, err error) {
	spec, ok := strings.CutPrefix(h, "bytes=")
	if h == "" || !ok || strings.Contains(spec, ",") {
		return 0, size, false, nil
	}
	a, b, ok := strings.Cut(strings.TrimSpace(spec), "-")
	if !ok {
		return 0, size, false, nil
	}
	if a == "" { // suffix: last N bytes
		n, perr := strconv.ParseInt(b, 10, 64)
		if perr != nil || n <= 0 {
			return 0, size, false, nil
		}
		if size == 0 {
			return 0, 0, false, errRangeNotSatisfiable
		}
		if n > size {
			n = size
		}
		return size - n, n, true, nil
	}
	s, perr := strconv.ParseInt(a, 10, 64)
	if perr != nil || s < 0 {
		return 0, size, false, nil
	}
	if s >= size {
		return 0, 0, false, errRangeNotSatisfiable
	}
	e := size - 1
	if b != "" {
		x, perr := strconv.ParseInt(b, 10, 64)
		if perr != nil || x < s {
			return 0, size, false, nil
		}
		if x < e {
			e = x
		}
	}
	return s, e - s + 1, true, nil
}

func (g *Gateway) handleGet(w http.ResponseWriter, r *http.Request) {
	_, b, err := g.prologue(r, false)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	key := r.PathValue("key")
	if err := object.ValidateKey(key); err != nil {
		g.fail(w, r, err)
		return
	}
	ctx := r.Context()
	v, err := g.lookup(ctx, b.Name, key, r.URL.Query().Get("version_id"))
	if err != nil {
		g.fail(w, r, err)
		return
	}
	if v.DeleteMarker {
		w.Header().Set("X-Delete-Marker", "true")
		w.Header().Set("X-Version-Id", v.VersionID)
		g.fail(w, r, metadata.ErrNotFound)
		return
	}
	etag := `"` + v.ETag + `"`
	h := w.Header()
	h.Set("ETag", etag)
	h.Set("Last-Modified", v.UpdatedAt.UTC().Format(http.TimeFormat))
	h.Set("X-Version-Id", v.VersionID)
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Type", v.ContentType)
	h.Set("X-Storage-Class", string(v.StorageClass))
	for k, val := range v.UserMeta {
		h.Set("X-Meta-"+k, val)
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && (inm == etag || inm == "*") {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	start, length, partial, err := parseClientRange(r.Header.Get("Range"), v.Size)
	if err != nil {
		h.Set("Content-Range", fmt.Sprintf("bytes */%d", v.Size))
		h.Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if r.URL.Query().Get("consistency") == "quorum" {
		verified, err := g.verifyReplicas(ctx, v, start, length)
		if err != nil {
			g.fail(w, r, err)
			return
		}
		h.Set("X-Replicas-Verified", verified)
	}
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, v.Size))
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}

	// Downloads are charged to the bucket owner, also for anonymous reads of
	// public buckets: otherwise a public bucket is an unmetered firehose.
	owner, _ := g.tenants.ByID(b.Tenant)
	var limiter *ratelimit.Bucket
	if owner != nil {
		release, err := owner.Downloads.TryAcquire()
		if err != nil {
			g.m.rateLimited.With(owner.ID, "concurrent_downloads").Inc()
			h.Del("Content-Length")
			h.Del("Content-Range")
			g.fail(w, r, quota.ErrTooManyReads)
			return
		}
		defer release()
		limiter = owner.Download
	}

	if g.objCache != nil && v.Size <= g.cfg.ObjectCacheMaxObject {
		data, src, err := g.cachedObject(ctx, v)
		if err != nil {
			h.Del("Content-Length")
			h.Del("Content-Range")
			g.fail(w, r, err)
			return
		}
		h.Set("X-Cache", src)
		w.WriteHeader(status)
		n, _ := w.Write(data[start : start+length])
		g.m.bytesOut.With("cache").Add(float64(n))
		return
	}

	w.WriteHeader(status)
	out := ratelimit.Writer(ctx, &deadlineWriter{w: w, rc: http.NewResponseController(w), d: g.cfg.ClientIdleTimeout}, limiter)
	n, err := g.streamRange(ctx, out, v, start, length)
	g.m.bytesOut.With("storage").Add(float64(n))
	if err != nil {
		// Status and Content-Length are already on the wire. Aborting the
		// connection guarantees the client sees a truncated response
		// instead of a short body that looks complete.
		if !errors.Is(err, errClientGone) && !errors.Is(err, context.Canceled) {
			g.log.Error("download aborted mid-stream", "bucket", b.Name, "key", key, "sent", n, "error", err)
		}
		panic(http.ErrAbortHandler)
	}
}

// deadlineWriter bounds how long a single write to a slow client may block.
type deadlineWriter struct {
	w  io.Writer
	rc *http.ResponseController
	d  time.Duration
}

func (d *deadlineWriter) Write(p []byte) (int, error) {
	_ = d.rc.SetWriteDeadline(time.Now().Add(d.d))
	return d.w.Write(p)
}

var errClientGone = errors.New("client write failed")

// streamRange copies [start, start+length) of v to out, segment by segment.
func (g *Gateway) streamRange(ctx context.Context, out io.Writer, v object.Version, start, length int64) (int64, error) {
	end := start + length
	var sent int64
	for _, seg := range v.Segments {
		segEnd := seg.Offset + seg.Size
		if segEnd <= start || seg.Offset >= end {
			continue
		}
		from := max(start, seg.Offset) - seg.Offset
		to := min(end, segEnd) - seg.Offset
		n, err := g.streamSegment(ctx, out, seg, from, to-from)
		sent += n
		if err != nil {
			return sent, err
		}
	}
	return sent, nil
}

// replicaOrder ranks replicas for reading: same-zone healthy nodes first
// (avoids cross-AZ transfer cost), then other healthy nodes, then degraded
// (SUSPECT/DRAINING), then nodes believed DOWN as a last resort. Within a
// tier the order is randomized so a hot object's reads spread across all
// its replicas instead of hammering the first one.
func (g *Gateway) replicaOrder(ids []string) []placement.Node {
	m := g.members.Map()
	var tiers [4][]placement.Node
	for _, id := range ids {
		n, ok := m.Node(id)
		if !ok {
			continue
		}
		switch {
		case n.State == placement.StateActive && g.cfg.Zone != "" && n.Zone == g.cfg.Zone:
			tiers[0] = append(tiers[0], n)
		case n.State == placement.StateActive:
			tiers[1] = append(tiers[1], n)
		case n.Readable():
			tiers[2] = append(tiers[2], n)
		default:
			tiers[3] = append(tiers[3], n)
		}
	}
	var out []placement.Node
	for _, t := range tiers {
		rand.Shuffle(len(t), func(i, j int) { t[i], t[j] = t[j], t[i] })
		out = append(out, t...)
	}
	return out
}

// streamSegment reads [off, off+n) of one segment, failing over between
// replicas. Failover can resume mid-stream at the exact byte offset reached,
// because every replica holds byte-identical, CRC-verified content for the
// same immutable blob ID.
func (g *Gateway) streamSegment(ctx context.Context, out io.Writer, seg object.Segment, off, n int64) (int64, error) {
	var done int64
	var lastErr error = errNoReplicas
	order := g.replicaOrder(seg.Replicas)
	for i, node := range order {
		if done >= n {
			break
		}
		if i > 0 {
			g.m.failovers.With().Inc()
		}
		rc, err := g.nodes.Get(ctx, node.Addr, seg.BlobID, off+done, n-done, "client")
		if err != nil {
			lastErr = fmt.Errorf("node %s: %w", node.ID, err)
			continue
		}
		m, rerr, werr := copyTracked(out, rc)
		rc.Close()
		done += m
		if werr != nil {
			return done, fmt.Errorf("%w: %v", errClientGone, werr)
		}
		if done == n {
			return done, nil
		}
		if rerr == nil {
			rerr = io.ErrUnexpectedEOF
		}
		lastErr = fmt.Errorf("node %s at offset %d: %w", node.ID, off+done, rerr)
		g.log.Warn("replica read failed, failing over", "blob", seg.BlobID, "node", node.ID, "error", rerr)
	}
	if done == n {
		return done, nil
	}
	if ctx.Err() != nil {
		return done, ctx.Err()
	}
	return done, fmt.Errorf("%w: blob %s: %w", errNoReplicas, seg.BlobID, lastErr)
}

// copyTracked is io.Copy that reports read and write errors separately, so
// a slow/disconnected client is never mistaken for a bad replica.
func copyTracked(dst io.Writer, src io.Reader) (n int64, readErr, writeErr error) {
	buf := make([]byte, 64<<10)
	for {
		nr, rerr := src.Read(buf)
		if nr > 0 {
			nw, werr := dst.Write(buf[:nr])
			n += int64(nw)
			if werr != nil {
				return n, nil, werr
			}
		}
		if rerr == io.EOF {
			return n, nil, nil
		}
		if rerr != nil {
			return n, rerr, nil
		}
	}
}

// cachedObject returns a small object's bytes from the hot-object cache,
// loading it once per version under single-flight. Keyed by version ID:
// versions are immutable, so a cached entry can never be stale.
func (g *Gateway) cachedObject(ctx context.Context, v object.Version) ([]byte, string, error) {
	if data, ok := g.objCache.Get(v.VersionID); ok {
		return data, "HIT", nil
	}
	data, err, shared := g.objFlight.Do(v.VersionID, func() ([]byte, error) {
		// Detached from any single caller: one impatient client must not
		// fail every request coalesced onto this load.
		lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		buf := bytes.NewBuffer(make([]byte, 0, v.Size))
		if _, err := g.streamRange(lctx, buf, v, 0, v.Size); err != nil {
			return nil, err
		}
		g.objCache.Add(v.VersionID, buf.Bytes(), v.Size)
		return buf.Bytes(), nil
	})
	if shared {
		return data, "COALESCED", err
	}
	return data, "MISS", err
}

// verifyReplicas implements GET ?consistency=quorum: before serving, at
// least R replicas of each segment in range must report the expected
// checksum. It detects missing/corrupt replicas at read time at the cost of
// one HEAD round trip per replica.
func (g *Gateway) verifyReplicas(ctx context.Context, v object.Version, start, length int64) (string, error) {
	minOK := -1
	for _, seg := range v.Segments {
		if seg.Offset+seg.Size <= start || seg.Offset >= start+length {
			continue
		}
		need := g.cfg.ReadQuorum
		if need <= 0 {
			need = v.ReplicationFactor/2 + 1
		}
		if need > len(seg.Replicas) {
			return "", fmt.Errorf("%w: segment %d has %d recorded replicas, read quorum %d", quorum.ErrQuorumUnreachable, seg.Index, len(seg.Replicas), need)
		}
		order := g.replicaOrder(seg.Replicas)
		out, err := quorum.Do(ctx, len(order), need, false, func(ctx context.Context, i int) error {
			info, err := g.nodes.Head(ctx, order[i].Addr, seg.BlobID)
			if err != nil {
				return err
			}
			if info.SHA256 != seg.SHA256 {
				return storage.ErrCorrupt
			}
			return nil
		})
		if err != nil {
			return "", err
		}
		if minOK < 0 || len(out.Succeeded) < minOK {
			minOK = len(out.Succeeded)
		}
	}
	if minOK < 0 {
		minOK = 0
	}
	return fmt.Sprintf("%d/%d", minOK, v.ReplicationFactor), nil
}

// ---------------------------------------------------------------------------
// LIST

type listObject struct {
	Key          string              `json:"key"`
	Size         int64               `json:"size"`
	ETag         string              `json:"etag"`
	VersionID    string              `json:"version_id"`
	LastModified time.Time           `json:"last_modified"`
	StorageClass object.StorageClass `json:"storage_class"`
}

type listResponse struct {
	Bucket                string       `json:"bucket"`
	Prefix                string       `json:"prefix,omitempty"`
	Delimiter             string       `json:"delimiter,omitempty"`
	Objects               []listObject `json:"objects"`
	CommonPrefixes        []string     `json:"common_prefixes,omitempty"`
	IsTruncated           bool         `json:"is_truncated"`
	NextContinuationToken string       `json:"next_continuation_token,omitempty"`
}

func (g *Gateway) handleList(w http.ResponseWriter, r *http.Request) {
	_, b, err := g.prologue(r, false)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	q := r.URL.Query()
	req := metadata.ListRequest{Bucket: b.Name, Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter")}
	if len(req.Prefix) > object.MaxKeyBytes || len(req.Delimiter) > 16 {
		g.fail(w, r, fmt.Errorf("%w: prefix or delimiter too long", errBadRequest))
		return
	}
	if s := q.Get("max_keys"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			g.fail(w, r, fmt.Errorf("%w: max_keys", errBadRequest))
			return
		}
		req.Limit = min(n, metadata.MaxListPage)
	}
	if tok := q.Get("continuation_token"); tok != "" {
		raw, err := base64.RawURLEncoding.DecodeString(tok)
		if err != nil || len(raw) > 2*object.MaxKeyBytes {
			g.fail(w, r, fmt.Errorf("%w: continuation_token", errBadRequest))
			return
		}
		req.StartAt = string(raw)
	} else if sa := q.Get("start_after"); sa != "" {
		req.StartAt = sa + "\x00" // smallest key strictly greater than start_after
	}
	ctx, cancel := g.metaCtx(r.Context())
	defer cancel()
	start := time.Now()
	res, err := g.meta.List(ctx, req)
	g.m.metaLatency.With("list").Observe(time.Since(start).Seconds())
	if err != nil {
		g.fail(w, r, err)
		return
	}
	out := listResponse{Bucket: b.Name, Prefix: req.Prefix, Delimiter: req.Delimiter, Objects: []listObject{},
		CommonPrefixes: res.CommonPrefixes, IsTruncated: res.Truncated}
	for _, e := range res.Entries {
		out.Objects = append(out.Objects, listObject(e))
	}
	if res.Truncated {
		out.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(res.NextStartAt))
	}
	writeJSON(w, http.StatusOK, out)
}

func (g *Gateway) handleListVersions(w http.ResponseWriter, r *http.Request) {
	_, b, err := g.prologue(r, false)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	ctx, cancel := g.metaCtx(r.Context())
	defer cancel()
	vs, err := g.meta.ListVersions(ctx, b.Name, r.PathValue("key"))
	if err != nil {
		g.fail(w, r, err)
		return
	}
	type ver struct {
		VersionID    string    `json:"version_id"`
		IsLatest     bool      `json:"is_latest"`
		DeleteMarker bool      `json:"delete_marker"`
		Size         int64     `json:"size"`
		ETag         string    `json:"etag,omitempty"`
		LastModified time.Time `json:"last_modified"`
	}
	out := make([]ver, len(vs))
	for i, v := range vs {
		out[i] = ver{v.VersionID, i == 0, v.DeleteMarker, v.Size, v.ETag, v.UpdatedAt}
	}
	writeJSON(w, http.StatusOK, map[string]any{"bucket": b.Name, "key": r.PathValue("key"), "versions": out})
}
