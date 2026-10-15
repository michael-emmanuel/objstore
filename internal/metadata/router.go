package metadata

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/michael-emmanuel/objstore/internal/hashing"
	"github.com/michael-emmanuel/objstore/internal/object"
)

// Router presents a set of shards as one Store.
//
// Partitioning is by bucket: hash(bucket) mod len(shards). Consequences,
// all deliberate and documented in docs/metadata-scaling.md:
//
//   - Every per-object operation and every LIST touches exactly one shard,
//     so LIST is a single ordered range scan (no scatter-gather merge).
//   - A single huge or hot bucket cannot be spread across shards. Production
//     systems split by (bucket, key-range) with dynamic range splitting.
//   - The shard count is fixed for the lifetime of the cluster; changing it
//     requires a metadata migration. A directory/range map would remove that.
//
// Operations that are inherently global (repair scans, GC, usage) fan out.
type Router struct {
	shards []Store
}

func NewRouter(shards []Store) *Router {
	if len(shards) == 0 {
		panic("router needs at least one shard")
	}
	return &Router{shards: shards}
}

func (r *Router) NumShards() int { return len(r.shards) }

// ShardFor returns the shard index owning bucket.
func (r *Router) ShardFor(bucket string) int {
	return int(hashing.Hash64("bucket:"+bucket) % uint64(len(r.shards)))
}

func (r *Router) s(bucket string) Store { return r.shards[r.ShardFor(bucket)] }

func (r *Router) CreateBucket(ctx context.Context, b Bucket) error {
	return r.s(b.Name).CreateBucket(ctx, b)
}
func (r *Router) GetBucket(ctx context.Context, name string) (Bucket, error) {
	return r.s(name).GetBucket(ctx, name)
}
func (r *Router) RegisterPending(ctx context.Context, bucket string, ids []string, ttl time.Duration) error {
	return r.s(bucket).RegisterPending(ctx, bucket, ids, ttl)
}
func (r *Router) Commit(ctx context.Context, v object.Version, o CommitOptions) (object.Version, error) {
	return r.s(v.Bucket).Commit(ctx, v, o)
}
func (r *Router) Get(ctx context.Context, bucket, key, vid string) (object.Version, error) {
	return r.s(bucket).Get(ctx, bucket, key, vid)
}
func (r *Router) Delete(ctx context.Context, bucket, key string) (object.Version, error) {
	return r.s(bucket).Delete(ctx, bucket, key)
}
func (r *Router) DeleteVersion(ctx context.Context, bucket, key, vid string) error {
	return r.s(bucket).DeleteVersion(ctx, bucket, key, vid)
}
func (r *Router) List(ctx context.Context, req ListRequest) (ListResult, error) {
	return r.s(req.Bucket).List(ctx, req)
}
func (r *Router) ListVersions(ctx context.Context, bucket, key string) ([]object.Version, error) {
	return r.s(bucket).ListVersions(ctx, bucket, key)
}
func (r *Router) CreateUpload(ctx context.Context, u Upload) error {
	return r.s(u.Bucket).CreateUpload(ctx, u)
}

func (r *Router) uploadShard(id string) (Store, error) {
	b, ok := UploadBucket(id)
	if !ok {
		return nil, ErrNoSuchUpload
	}
	return r.s(b), nil
}

func (r *Router) GetUpload(ctx context.Context, id string) (Upload, error) {
	s, err := r.uploadShard(id)
	if err != nil {
		return Upload{}, err
	}
	return s.GetUpload(ctx, id)
}
func (r *Router) PutPart(ctx context.Context, id string, p Part) error {
	s, err := r.uploadShard(id)
	if err != nil {
		return err
	}
	return s.PutPart(ctx, id, p)
}
func (r *Router) CompleteUpload(ctx context.Context, id string, parts []CompletePart, vid string, o CommitOptions) (object.Version, error) {
	s, err := r.uploadShard(id)
	if err != nil {
		return object.Version{}, err
	}
	return s.CompleteUpload(ctx, id, parts, vid, o)
}
func (r *Router) AbortUpload(ctx context.Context, id string) error {
	s, err := r.uploadShard(id)
	if err != nil {
		return err
	}
	return s.AbortUpload(ctx, id)
}
func (r *Router) UpdateReplicas(ctx context.Context, bucket, blobID string, add, remove []string) error {
	return r.s(bucket).UpdateReplicas(ctx, bucket, blobID, add, remove)
}

// ScanSegments walks shards in order. Cursor: "<shard>|<inner cursor>".
func (r *Router) ScanSegments(ctx context.Context, cursor string, limit int) ([]SegmentRef, string, error) {
	shard, inner := 0, ""
	if cursor != "" {
		a, b, ok := strings.Cut(cursor, "|")
		n, err := strconv.Atoi(a)
		if !ok || err != nil || n < 0 || n >= len(r.shards) {
			return nil, "", fmt.Errorf("%w: cursor", ErrInvalidArgument)
		}
		shard, inner = n, b
	}
	for ; shard < len(r.shards); shard, inner = shard+1, "" {
		segs, next, err := r.shards[shard].ScanSegments(ctx, inner, limit)
		if err != nil {
			return nil, "", err
		}
		if next != "" {
			return segs, strconv.Itoa(shard) + "|" + next, nil
		}
		if len(segs) > 0 {
			if shard+1 < len(r.shards) {
				return segs, strconv.Itoa(shard+1) + "|", nil
			}
			return segs, "", nil
		}
	}
	return nil, "", nil
}

// CheckRefs asks every shard because blob IDs do not encode their bucket.
// A blob is referenced if ANY shard references it; an unreachable shard
// fails the whole call, which makes GC conservative by construction.
func (r *Router) CheckRefs(ctx context.Context, ids []string) (map[string]BlobRef, error) {
	out := make(map[string]BlobRef, len(ids))
	for _, id := range ids {
		out[id] = BlobRef{}
	}
	for _, s := range r.shards {
		m, err := s.CheckRefs(ctx, ids)
		if err != nil {
			return nil, err
		}
		for id, ref := range m {
			cur := out[id]
			if ref.Referenced {
				cur.Referenced, cur.Bucket, cur.Replicas = true, ref.Bucket, ref.Replicas
			}
			cur.Pending = cur.Pending || ref.Pending
			out[id] = cur
		}
	}
	return out, nil
}

func (r *Router) ReplicaCount(ctx context.Context, node string) (int, error) {
	total := 0
	for _, s := range r.shards {
		n, err := s.ReplicaCount(ctx, node)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (r *Router) PeekGarbage(ctx context.Context, olderThan time.Time, limit int) ([]GarbageItem, error) {
	var out []GarbageItem
	for i, s := range r.shards {
		if len(out) >= limit {
			break
		}
		items, err := s.PeekGarbage(ctx, olderThan, limit-len(out))
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			it.Shard = i
			out = append(out, it)
		}
	}
	return out, nil
}

func (r *Router) AckGarbage(ctx context.Context, items []GarbageItem) error {
	by := map[int][]GarbageItem{}
	for _, it := range items {
		by[it.Shard] = append(by[it.Shard], it)
	}
	keys := make([]int, 0, len(by))
	for k := range by {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		if k < 0 || k >= len(r.shards) {
			return fmt.Errorf("%w: shard %d", ErrInvalidArgument, k)
		}
		if err := r.shards[k].AckGarbage(ctx, by[k]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Router) ExpireUploads(ctx context.Context, olderThan time.Time) (int, error) {
	total := 0
	for _, s := range r.shards {
		n, err := s.ExpireUploads(ctx, olderThan)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (r *Router) PurgeTombstones(ctx context.Context, olderThan time.Time) (int, error) {
	total := 0
	for _, s := range r.shards {
		n, err := s.PurgeTombstones(ctx, olderThan)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// Usage returns per-shard usage for tenant, in shard order.
func (r *Router) Usage(ctx context.Context, tenant string) ([]int64, error) {
	out := make([]int64, len(r.shards))
	for i, s := range r.shards {
		u, err := s.Usage(ctx, tenant)
		if err != nil {
			return nil, err
		}
		for _, x := range u {
			out[i] += x
		}
	}
	return out, nil
}

func (r *Router) Stats(ctx context.Context) ([]Stats, error) {
	var out []Stats
	for i, s := range r.shards {
		st, err := s.Stats(ctx)
		if err != nil {
			return nil, err
		}
		for _, x := range st {
			x.Shard = i
			out = append(out, x)
		}
	}
	return out, nil
}

var _ Store = (*Router)(nil)
