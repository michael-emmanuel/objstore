package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/michael-emmanuel/objstore/internal/object"
)

// ShardOptions configures a shard.
type ShardOptions struct {
	Index        int
	Dir          string // empty: in-memory only (tests)
	Sync         bool   // fsync the WAL on every commit
	MinPartSize  int64  // minimum size of every multipart part except the last
	CompactEvery int    // snapshot + truncate WAL after this many records
	Now          func() time.Time
	Log          *slog.Logger
}

type keyEntry struct {
	CreatedAt time.Time         `json:"created_at"`
	Versions  []*object.Version `json:"versions"` // oldest -> newest
}

type bucketState struct {
	Bucket  Bucket               `json:"bucket"`
	keys    []string             // sorted; rebuilt on load
	Objects map[string]*keyEntry `json:"objects"`
}

// blobLoc says where a blob is referenced from.
type blobLoc struct {
	bucket, key, versionID string
	seg                    int
	uploadID               string
	part                   int
}

// Shard is one metadata partition. All methods are safe for concurrent use.
// Reads take a shared lock; mutations take the exclusive lock for the
// duration of validation + WAL append (+fsync) + apply. That serializes
// commits within a shard — the simplest way to make "current version"
// unambiguous — at the cost of per-shard write throughput being bounded by
// fsync latency. docs/performance.md discusses group commit as the fix.
type Shard struct {
	opts ShardOptions

	mu       sync.RWMutex
	buckets  map[string]*bucketState
	uploads  map[string]*Upload
	pending  map[string]time.Time
	refs     map[string]blobLoc
	garbage  []GarbageItem
	usage    map[string]int64
	seq      uint64
	gseq     uint64
	lsn      uint64
	wal      *wal
	readOnly bool

	pendingOps int
}

// record is one resolved mutation; see wal.go.
type record struct {
	LSN       uint64          `json:"lsn"`
	Op        string          `json:"op"`
	Time      time.Time       `json:"time"`
	Bucket    *Bucket         `json:"bucket,omitempty"`
	Version   *object.Version `json:"version,omitempty"`
	BlobIDs   []string        `json:"blob_ids,omitempty"`
	Expiry    time.Time       `json:"expiry,omitempty"`
	Upload    *Upload         `json:"upload,omitempty"`
	UploadID  string          `json:"upload_id,omitempty"`
	Part      *Part           `json:"part,omitempty"`
	UsedParts []int           `json:"used_parts,omitempty"`
	BlobID    string          `json:"blob_id,omitempty"`
	Add       []string        `json:"add,omitempty"`
	Remove    []string        `json:"remove,omitempty"`
	Garbage   []uint64        `json:"garbage,omitempty"`
	Purge     []versionRef    `json:"purge,omitempty"`
	Aborts    []string        `json:"aborts,omitempty"`
	BucketKey string          `json:"bucket_key,omitempty"`
}

type versionRef struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id"`
}

const (
	opCreateBucket   = "create_bucket"
	opPending        = "pending"
	opCommit         = "commit"
	opDeleteVersion  = "delete_version"
	opCreateUpload   = "create_upload"
	opPutPart        = "put_part"
	opCompleteUpload = "complete_upload"
	opAbortUpload    = "abort_upload"
	opUpdateReplicas = "update_replicas"
	opAckGarbage     = "ack_garbage"
	opPurge          = "purge"
)

type snapshotState struct {
	LSN     uint64                  `json:"lsn"`
	Seq     uint64                  `json:"seq"`
	GSeq    uint64                  `json:"gseq"`
	Buckets map[string]*bucketState `json:"buckets"`
	Uploads map[string]*Upload      `json:"uploads"`
	Pending map[string]time.Time    `json:"pending"`
	Garbage []GarbageItem           `json:"garbage"`
	Usage   map[string]int64        `json:"usage"`
}

// OpenShard loads the snapshot, replays the WAL and returns a ready shard.
func OpenShard(opts ShardOptions) (*Shard, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.CompactEvery <= 0 {
		opts.CompactEvery = 10000
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	s := &Shard{
		opts:    opts,
		buckets: map[string]*bucketState{},
		uploads: map[string]*Upload{},
		pending: map[string]time.Time{},
		refs:    map[string]blobLoc{},
		usage:   map[string]int64{},
	}
	if opts.Dir == "" {
		return s, nil
	}
	var snap snapshotState
	ok, err := readSnapshot(opts.Dir, &snap)
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	if ok {
		s.loadSnapshot(&snap)
	}
	w, err := openWAL(opts.Dir, opts.Sync)
	if err != nil {
		return nil, err
	}
	replayed := 0
	if err := w.replay(func(r *record) error {
		if r.LSN <= s.lsn {
			return nil // already in snapshot
		}
		s.apply(r)
		s.lsn = r.LSN
		replayed++
		return nil
	}); err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}
	s.wal = w
	opts.Log.Info("metadata shard recovered", "shard", opts.Index, "lsn", s.lsn, "replayed", replayed, "from_snapshot", ok)
	return s, nil
}

func (s *Shard) loadSnapshot(snap *snapshotState) {
	s.lsn, s.seq, s.gseq = snap.LSN, snap.Seq, snap.GSeq
	if snap.Buckets != nil {
		s.buckets = snap.Buckets
	}
	if snap.Uploads != nil {
		s.uploads = snap.Uploads
	}
	if snap.Pending != nil {
		s.pending = snap.Pending
	}
	if snap.Usage != nil {
		s.usage = snap.Usage
	}
	s.garbage = snap.Garbage
	for bname, bs := range s.buckets {
		if bs.Objects == nil {
			bs.Objects = map[string]*keyEntry{}
		}
		bs.keys = bs.keys[:0]
		for k, ke := range bs.Objects {
			bs.keys = append(bs.keys, k)
			for _, v := range ke.Versions {
				for i, seg := range v.Segments {
					s.refs[seg.BlobID] = blobLoc{bucket: bname, key: k, versionID: v.VersionID, seg: i}
				}
			}
		}
		sort.Strings(bs.keys)
	}
	for id, u := range s.uploads {
		for n, p := range u.Parts {
			s.refs[p.BlobID] = blobLoc{bucket: u.Bucket, uploadID: id, part: n}
		}
	}
}

// Close flushes and closes the WAL.
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal != nil {
		return s.wal.close()
	}
	return nil
}

// Snapshot forces a snapshot (used by tests and on graceful shutdown).
func (s *Shard) Snapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Shard) snapshotLocked() error {
	if s.wal == nil {
		return nil
	}
	return s.wal.writeSnapshot(&snapshotState{
		LSN: s.lsn, Seq: s.seq, GSeq: s.gseq, Buckets: s.buckets, Uploads: s.uploads,
		Pending: s.pending, Garbage: s.garbage, Usage: s.usage,
	})
}

// mutate makes rec durable, then applies it. Caller holds s.mu.
func (s *Shard) mutate(rec *record) error {
	if s.readOnly {
		return ErrReadOnly
	}
	rec.LSN = s.lsn + 1
	if rec.Time.IsZero() {
		rec.Time = s.opts.Now()
	}
	if s.wal != nil {
		if err := s.wal.append(rec); err != nil {
			s.readOnly = true
			s.opts.Log.Error("WAL append failed; shard is now read-only", "shard", s.opts.Index, "error", err)
			return fmt.Errorf("%w: %v", ErrReadOnly, err)
		}
	}
	s.lsn = rec.LSN
	s.apply(rec)
	if s.wal != nil && s.wal.records >= s.opts.CompactEvery {
		if err := s.snapshotLocked(); err != nil {
			s.opts.Log.Warn("snapshot failed", "shard", s.opts.Index, "error", err)
		}
	}
	return nil
}

// apply is infallible and deterministic: all validation happened before the
// record was written.
func (s *Shard) apply(r *record) {
	switch r.Op {
	case opCreateBucket:
		s.buckets[r.Bucket.Name] = &bucketState{Bucket: *r.Bucket, Objects: map[string]*keyEntry{}}
	case opPending:
		for _, id := range r.BlobIDs {
			s.pending[id] = r.Expiry
		}
	case opCommit:
		s.applyCommit(r.Version.Clone(), r.Time)
	case opDeleteVersion:
		for _, p := range r.Purge {
			s.removeVersion(p, "version deleted", r.Time)
		}
	case opPurge:
		for _, p := range r.Purge {
			s.removeVersion(p, "tombstone purged", r.Time)
		}
	case opCreateUpload:
		u := r.Upload.clone()
		s.uploads[u.ID] = &u
	case opPutPart:
		u := s.uploads[r.UploadID]
		if old, ok := u.Parts[r.Part.Number]; ok && old.BlobID != r.Part.BlobID {
			s.enqueueGarbage(old.BlobID, old.Replicas, "part replaced", r.Time)
			delete(s.refs, old.BlobID)
		}
		p := *r.Part
		p.Replicas = append([]string(nil), p.Replicas...)
		u.Parts[p.Number] = p
		s.refs[p.BlobID] = blobLoc{bucket: u.Bucket, uploadID: u.ID, part: p.Number}
		delete(s.pending, p.BlobID)
	case opCompleteUpload:
		u := s.uploads[r.UploadID]
		used := map[int]bool{}
		for _, n := range r.UsedParts {
			used[n] = true
		}
		for n, p := range u.Parts {
			delete(s.refs, p.BlobID)
			if !used[n] {
				s.enqueueGarbage(p.BlobID, p.Replicas, "part not in completion list", r.Time)
			}
		}
		delete(s.uploads, r.UploadID)
		s.applyCommit(r.Version.Clone(), r.Time)
	case opAbortUpload:
		for _, id := range append([]string{r.UploadID}, r.Aborts...) {
			u, ok := s.uploads[id]
			if !ok {
				continue
			}
			for _, p := range u.Parts {
				delete(s.refs, p.BlobID)
				s.enqueueGarbage(p.BlobID, p.Replicas, "upload aborted", r.Time)
			}
			delete(s.uploads, id)
		}
	case opUpdateReplicas:
		if loc, ok := s.refs[r.BlobID]; ok {
			s.setReplicas(r.BlobID, applyReplicaChange(s.replicasOfRO(loc), r.Add, r.Remove))
		}
	case opAckGarbage:
		drop := map[uint64]bool{}
		for _, id := range r.Garbage {
			drop[id] = true
		}
		kept := s.garbage[:0]
		for _, g := range s.garbage {
			if !drop[g.ID] {
				kept = append(kept, g)
			}
		}
		s.garbage = kept
	}
}

func applyReplicaChange(cur, add, remove []string) []string {
	rm := map[string]bool{}
	for _, x := range remove {
		rm[x] = true
	}
	out := make([]string, 0, len(cur)+len(add))
	seen := map[string]bool{}
	for _, x := range append(append([]string(nil), cur...), add...) {
		if rm[x] || seen[x] {
			continue
		}
		seen[x] = true
		out = append(out, x)
	}
	return out
}

func (s *Shard) applyCommit(v object.Version, t time.Time) {
	bs := s.buckets[v.Bucket]
	ke := bs.Objects[v.Key]
	if ke == nil {
		ke = &keyEntry{CreatedAt: v.CreatedAt}
		bs.Objects[v.Key] = ke
		i := sort.SearchStrings(bs.keys, v.Key)
		bs.keys = append(bs.keys, "")
		copy(bs.keys[i+1:], bs.keys[i:])
		bs.keys[i] = v.Key
	}
	if !bs.Bucket.Versioning {
		// Unversioned bucket: the new version (or delete marker) replaces
		// everything. Old data is NOT deleted here; it is queued for GC so
		// that in-flight readers holding the old metadata can finish.
		for _, old := range ke.Versions {
			s.dropVersionData(old, "overwritten", t)
		}
		ke.Versions = nil
	}
	vv := v
	ke.Versions = append(ke.Versions, &vv)
	if v.Seq > s.seq {
		s.seq = v.Seq
	}
	if !v.DeleteMarker {
		s.usage[v.Tenant] += v.Size
	}
	for i, seg := range vv.Segments {
		s.refs[seg.BlobID] = blobLoc{bucket: v.Bucket, key: v.Key, versionID: v.VersionID, seg: i}
		delete(s.pending, seg.BlobID)
	}
}

func (s *Shard) dropVersionData(v *object.Version, reason string, t time.Time) {
	for _, seg := range v.Segments {
		delete(s.refs, seg.BlobID)
		s.enqueueGarbage(seg.BlobID, seg.Replicas, reason, t)
	}
	if !v.DeleteMarker {
		s.usage[v.Tenant] -= v.Size
	}
}

func (s *Shard) removeVersion(p versionRef, reason string, t time.Time) {
	bs := s.buckets[p.Bucket]
	if bs == nil {
		return
	}
	ke := bs.Objects[p.Key]
	if ke == nil {
		return
	}
	for i, v := range ke.Versions {
		if v.VersionID == p.VersionID {
			s.dropVersionData(v, reason, t)
			ke.Versions = append(ke.Versions[:i], ke.Versions[i+1:]...)
			break
		}
	}
	if len(ke.Versions) == 0 {
		delete(bs.Objects, p.Key)
		i := sort.SearchStrings(bs.keys, p.Key)
		if i < len(bs.keys) && bs.keys[i] == p.Key {
			bs.keys = append(bs.keys[:i], bs.keys[i+1:]...)
		}
	}
}

func (s *Shard) enqueueGarbage(blobID string, replicas []string, reason string, t time.Time) {
	s.gseq++
	s.garbage = append(s.garbage, GarbageItem{ID: s.gseq, Shard: s.opts.Index, BlobID: blobID,
		Replicas: append([]string(nil), replicas...), Reason: reason, EnqueuedAt: t})
}

// setReplicas replaces the replica list of a referenced blob.
func (s *Shard) setReplicas(blobID string, next []string) {
	loc, ok := s.refs[blobID]
	if !ok {
		return
	}
	if loc.uploadID != "" {
		if u := s.uploads[loc.uploadID]; u != nil {
			p := u.Parts[loc.part]
			p.Replicas = next
			u.Parts[loc.part] = p
		}
		return
	}
	if bs := s.buckets[loc.bucket]; bs != nil {
		if ke := bs.Objects[loc.key]; ke != nil {
			for _, v := range ke.Versions {
				if v.VersionID == loc.versionID && loc.seg < len(v.Segments) {
					v.Segments[loc.seg].Replicas = next
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Bucket operations

func (s *Shard) CreateBucket(_ context.Context, b Bucket) error {
	if err := object.ValidateBucket(b.Name); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if b.Tenant == "" {
		return fmt.Errorf("%w: bucket must have a tenant", ErrInvalidArgument)
	}
	if b.ReplicationFactor <= 0 {
		b.ReplicationFactor = 3
	}
	if b.ReplicationFactor > 7 {
		return fmt.Errorf("%w: replication factor too large", ErrInvalidArgument)
	}
	if b.StorageClass == "" {
		b.StorageClass = object.ClassStandard
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[b.Name]; ok {
		return ErrBucketExists
	}
	b.CreatedAt = s.opts.Now().UTC()
	return s.mutate(&record{Op: opCreateBucket, Bucket: &b})
}

func (s *Shard) GetBucket(_ context.Context, name string) (Bucket, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bs, ok := s.buckets[name]
	if !ok {
		return Bucket{}, ErrNoSuchBucket
	}
	return bs.Bucket, nil
}

// ---------------------------------------------------------------------------
// Object operations

func (s *Shard) RegisterPending(_ context.Context, bucket string, blobIDs []string, ttl time.Duration) error {
	for _, id := range blobIDs {
		if !object.ValidBlobID(id) {
			return fmt.Errorf("%w: blob id", ErrInvalidArgument)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.buckets[bucket]; !ok {
		return ErrNoSuchBucket
	}
	s.prunePendingLocked()
	return s.mutate(&record{Op: opPending, BlobIDs: blobIDs, Expiry: s.opts.Now().Add(ttl)})
}

// prunePendingLocked drops expired pending registrations (writes that never
// committed). Pruning is not logged: an expired entry is semantically absent,
// so replay reaching a different-but-equivalent map is harmless.
func (s *Shard) prunePendingLocked() {
	s.pendingOps++
	if s.pendingOps%256 != 0 {
		return
	}
	now := s.opts.Now()
	for id, exp := range s.pending {
		if !exp.After(now) {
			delete(s.pending, id)
		}
	}
}

func validateSegments(v *object.Version) error {
	var off int64
	for i, seg := range v.Segments {
		if seg.Index != i || seg.Offset != off || seg.Size < 0 || !object.ValidBlobID(seg.BlobID) || len(seg.SHA256) != 64 {
			return fmt.Errorf("%w: segment %d malformed", ErrInvalidArgument, i)
		}
		off += seg.Size
	}
	if off != v.Size {
		return fmt.Errorf("%w: segments cover %d bytes, size is %d", ErrInvalidArgument, off, v.Size)
	}
	return nil
}

func (s *Shard) Commit(_ context.Context, v object.Version, opts CommitOptions) (object.Version, error) {
	if err := object.ValidateKey(v.Key); err != nil {
		return object.Version{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if v.VersionID == "" || v.DeleteMarker {
		return object.Version{}, fmt.Errorf("%w: version id required", ErrInvalidArgument)
	}
	if err := validateSegments(&v); err != nil {
		return object.Version{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(v, opts, &record{Op: opCommit})
}

// commitLocked fills in shard-assigned fields, checks quota and writes rec.
func (s *Shard) commitLocked(v object.Version, opts CommitOptions, rec *record) (object.Version, error) {
	bs, ok := s.buckets[v.Bucket]
	if !ok {
		return object.Version{}, ErrNoSuchBucket
	}
	now := s.opts.Now().UTC()
	v.Tenant = bs.Bucket.Tenant
	v.Seq = s.seq + 1
	v.UpdatedAt = now
	v.CreatedAt = now
	if ke := bs.Objects[v.Key]; ke != nil {
		v.CreatedAt = ke.CreatedAt
	}
	if v.ReplicationFactor == 0 {
		v.ReplicationFactor = bs.Bucket.ReplicationFactor
	}
	if v.StorageClass == "" {
		v.StorageClass = bs.Bucket.StorageClass
	}
	if !v.DeleteMarker && opts.QuotaBytes > 0 && v.Size > 0 {
		after := s.usage[v.Tenant] + v.Size
		if ke := bs.Objects[v.Key]; ke != nil && !bs.Bucket.Versioning {
			for _, old := range ke.Versions {
				if !old.DeleteMarker {
					after -= old.Size
				}
			}
		}
		if after > opts.QuotaBytes {
			return object.Version{}, ErrQuotaExceeded
		}
	}
	rec.Version = &v
	if err := s.mutate(rec); err != nil {
		return object.Version{}, err
	}
	return v.Clone(), nil
}

func (s *Shard) Get(_ context.Context, bucket, key, versionID string) (object.Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bs, ok := s.buckets[bucket]
	if !ok {
		return object.Version{}, ErrNoSuchBucket
	}
	ke := bs.Objects[key]
	if ke == nil || len(ke.Versions) == 0 {
		return object.Version{}, ErrNotFound
	}
	if versionID == "" {
		return ke.Versions[len(ke.Versions)-1].Clone(), nil
	}
	for _, v := range ke.Versions {
		if v.VersionID == versionID {
			return v.Clone(), nil
		}
	}
	return object.Version{}, ErrNotFound
}

func (s *Shard) ListVersions(_ context.Context, bucket, key string) ([]object.Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bs, ok := s.buckets[bucket]
	if !ok {
		return nil, ErrNoSuchBucket
	}
	ke := bs.Objects[key]
	if ke == nil {
		return nil, ErrNotFound
	}
	out := make([]object.Version, 0, len(ke.Versions))
	for i := len(ke.Versions) - 1; i >= 0; i-- { // newest first
		out = append(out, ke.Versions[i].Clone())
	}
	return out, nil
}

// Delete writes a delete marker (tombstone). Data is reclaimed later by GC.
func (s *Shard) Delete(_ context.Context, bucket, key string) (object.Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bs, ok := s.buckets[bucket]
	if !ok {
		return object.Version{}, ErrNoSuchBucket
	}
	ke := bs.Objects[key]
	if ke == nil || len(ke.Versions) == 0 || ke.Versions[len(ke.Versions)-1].DeleteMarker {
		return object.Version{}, ErrNotFound
	}
	marker := object.Version{Bucket: bucket, Key: key, VersionID: object.NewVersionID(s.opts.Now()), DeleteMarker: true}
	return s.commitLocked(marker, CommitOptions{}, &record{Op: opCommit})
}

// DeleteVersion permanently removes one version (versioned buckets).
func (s *Shard) DeleteVersion(_ context.Context, bucket, key, versionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	bs, ok := s.buckets[bucket]
	if !ok {
		return ErrNoSuchBucket
	}
	ke := bs.Objects[key]
	if ke == nil {
		return ErrNotFound
	}
	for _, v := range ke.Versions {
		if v.VersionID == versionID {
			return s.mutate(&record{Op: opDeleteVersion, Purge: []versionRef{{bucket, key, versionID}}})
		}
	}
	return ErrNotFound
}

// prefixSuccessor returns the smallest string greater than every string
// with prefix p, or "" if none exists.
func prefixSuccessor(p string) string {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

// MaxListPage bounds a LIST page; maxListScanFactor bounds work spent
// skipping delete markers so one call cannot scan an entire bucket.
const (
	MaxListPage       = 1000
	maxListScanFactor = 10
)

func (s *Shard) List(_ context.Context, req ListRequest) (ListResult, error) {
	limit := req.Limit
	if limit <= 0 || limit > MaxListPage {
		limit = MaxListPage
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	bs, ok := s.buckets[req.Bucket]
	if !ok {
		return ListResult{}, ErrNoSuchBucket
	}
	start := req.StartAt
	if req.Prefix > start {
		start = req.Prefix
	}
	res := ListResult{Entries: []ListEntry{}}
	keys := bs.keys
	i := sort.SearchStrings(keys, start)
	scanned := 0
	for i < len(keys) {
		k := keys[i]
		if !strings.HasPrefix(k, req.Prefix) {
			break
		}
		if len(res.Entries)+len(res.CommonPrefixes) >= limit || scanned >= limit*maxListScanFactor {
			res.Truncated, res.NextStartAt = true, k
			break
		}
		scanned++
		if req.Delimiter != "" {
			rest := k[len(req.Prefix):]
			if j := strings.Index(rest, req.Delimiter); j >= 0 {
				cp := req.Prefix + rest[:j+len(req.Delimiter)]
				res.CommonPrefixes = append(res.CommonPrefixes, cp)
				succ := prefixSuccessor(cp)
				if succ == "" {
					break
				}
				i = sort.SearchStrings(keys, succ)
				continue
			}
		}
		ke := bs.Objects[k]
		latest := ke.Versions[len(ke.Versions)-1]
		if !latest.DeleteMarker {
			res.Entries = append(res.Entries, ListEntry{Key: k, Size: latest.Size, ETag: latest.ETag,
				VersionID: latest.VersionID, LastModified: latest.UpdatedAt, StorageClass: latest.StorageClass})
		}
		i++
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// Multipart

func (s *Shard) CreateUpload(_ context.Context, u Upload) error {
	if err := object.ValidateKey(u.Key); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	if b, ok := UploadBucket(u.ID); !ok || b != u.Bucket {
		return fmt.Errorf("%w: upload id must encode bucket", ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	bs, ok := s.buckets[u.Bucket]
	if !ok {
		return ErrNoSuchBucket
	}
	if _, exists := s.uploads[u.ID]; exists {
		return fmt.Errorf("%w: duplicate upload id", ErrInvalidArgument)
	}
	u.Tenant = bs.Bucket.Tenant
	u.CreatedAt = s.opts.Now().UTC()
	u.Parts = map[int]Part{}
	if u.ReplicationFactor == 0 {
		u.ReplicationFactor = bs.Bucket.ReplicationFactor
	}
	if u.StorageClass == "" {
		u.StorageClass = bs.Bucket.StorageClass
	}
	return s.mutate(&record{Op: opCreateUpload, Upload: &u})
}

func (s *Shard) GetUpload(_ context.Context, id string) (Upload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.uploads[id]
	if !ok {
		return Upload{}, ErrNoSuchUpload
	}
	return u.clone(), nil
}

const MaxPartNumber = 10000

func (s *Shard) PutPart(_ context.Context, uploadID string, p Part) error {
	if p.Number < 1 || p.Number > MaxPartNumber || !object.ValidBlobID(p.BlobID) || len(p.SHA256) != 64 || p.Size < 0 {
		return fmt.Errorf("%w: malformed part", ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.uploads[uploadID]; !ok {
		return ErrNoSuchUpload
	}
	p.UploadedAt = s.opts.Now().UTC()
	return s.mutate(&record{Op: opPutPart, UploadID: uploadID, Part: &p})
}

func (s *Shard) CompleteUpload(_ context.Context, uploadID string, parts []CompletePart, versionID string, opts CommitOptions) (object.Version, error) {
	if len(parts) == 0 {
		return object.Version{}, fmt.Errorf("%w: empty part list", ErrInvalidPart)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.uploads[uploadID]
	if !ok {
		return object.Version{}, ErrNoSuchUpload
	}
	v := object.Version{
		Bucket: u.Bucket, Key: u.Key, VersionID: versionID, ContentType: u.ContentType,
		UserMeta: u.UserMeta, StorageClass: u.StorageClass, ReplicationFactor: u.ReplicationFactor,
	}
	var digests []string
	var used []int
	prev := 0
	for i, cp := range parts {
		if cp.Number <= prev {
			return object.Version{}, fmt.Errorf("%w: parts must be in strictly ascending order", ErrInvalidPart)
		}
		prev = cp.Number
		p, ok := u.Parts[cp.Number]
		if !ok {
			return object.Version{}, fmt.Errorf("%w: part %d was not uploaded", ErrInvalidPart, cp.Number)
		}
		if etag := strings.Trim(cp.ETag, `"`); etag != p.SHA256 {
			return object.Version{}, fmt.Errorf("%w: part %d etag mismatch", ErrInvalidPart, cp.Number)
		}
		if i < len(parts)-1 && p.Size < s.opts.MinPartSize {
			return object.Version{}, fmt.Errorf("%w: part %d smaller than minimum %d bytes", ErrInvalidPart, cp.Number, s.opts.MinPartSize)
		}
		v.Segments = append(v.Segments, object.Segment{Index: i, BlobID: p.BlobID, Offset: v.Size, Size: p.Size,
			SHA256: p.SHA256, Replicas: append([]string(nil), p.Replicas...)})
		v.Size += p.Size
		digests = append(digests, p.SHA256)
		used = append(used, cp.Number)
	}
	etag, err := object.MultipartETag(digests)
	if err != nil {
		return object.Version{}, fmt.Errorf("%w: %v", ErrInvalidPart, err)
	}
	v.ETag = etag
	return s.commitLocked(v, opts, &record{Op: opCompleteUpload, UploadID: uploadID, UsedParts: used})
}

func (s *Shard) AbortUpload(_ context.Context, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.uploads[uploadID]; !ok {
		return ErrNoSuchUpload
	}
	return s.mutate(&record{Op: opAbortUpload, UploadID: uploadID})
}

// ExpireUploads aborts uploads older than olderThan (abandoned uploads).
func (s *Shard) ExpireUploads(_ context.Context, olderThan time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for id, u := range s.uploads {
		if u.CreatedAt.Before(olderThan) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	sort.Strings(ids)
	return len(ids), s.mutate(&record{Op: opAbortUpload, Aborts: ids})
}

// PurgeTombstones removes delete markers in unversioned buckets that are the
// only remaining version of their key and are older than olderThan.
func (s *Shard) PurgeTombstones(_ context.Context, olderThan time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var purge []versionRef
	for name, bs := range s.buckets {
		if bs.Bucket.Versioning {
			continue
		}
		for _, k := range bs.keys {
			ke := bs.Objects[k]
			if len(ke.Versions) == 1 && ke.Versions[0].DeleteMarker && ke.Versions[0].UpdatedAt.Before(olderThan) {
				purge = append(purge, versionRef{name, k, ke.Versions[0].VersionID})
			}
		}
	}
	if len(purge) == 0 {
		return 0, nil
	}
	return len(purge), s.mutate(&record{Op: opPurge, Purge: purge})
}

// ---------------------------------------------------------------------------
// Background-job support

// ScanSegments iterates all committed segments in (bucket, key) order.
// The cursor is opaque; "" starts from the beginning and a returned ""
// means the scan is complete.
func (s *Shard) ScanSegments(_ context.Context, cursor string, limit int) ([]SegmentRef, string, error) {
	if limit <= 0 {
		limit = 1000
	}
	cb, ck, _ := strings.Cut(cursor, "\x00")
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.buckets))
	for n := range s.buckets {
		if n >= cb {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	var out []SegmentRef
	for _, name := range names {
		bs := s.buckets[name]
		i := 0
		if name == cb {
			i = sort.SearchStrings(bs.keys, ck)
		}
		for ; i < len(bs.keys); i++ {
			k := bs.keys[i]
			if len(out) >= limit {
				return out, name + "\x00" + k, nil
			}
			for _, v := range bs.Objects[k].Versions {
				for _, seg := range v.Segments {
					out = append(out, SegmentRef{Bucket: name, Key: k, VersionID: v.VersionID, Index: seg.Index,
						BlobID: seg.BlobID, Size: seg.Size, SHA256: seg.SHA256,
						Replicas: append([]string(nil), seg.Replicas...), RF: v.ReplicationFactor})
				}
			}
		}
	}
	return out, "", nil
}

// UpdateReplicas records that blobID gained/lost copies. It is how repair
// and rebalancing make their data-plane work visible to readers.
func (s *Shard) UpdateReplicas(_ context.Context, _ string, blobID string, add, remove []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	loc, ok := s.refs[blobID]
	if !ok {
		return ErrNotFound
	}
	cur := s.replicasOfRO(loc)
	if equalSets(cur, applyReplicaChange(cur, add, remove)) {
		return nil
	}
	return s.mutate(&record{Op: opUpdateReplicas, BlobID: blobID, Add: add, Remove: remove})
}

func equalSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]bool{}
	for _, x := range a {
		m[x] = true
	}
	for _, x := range b {
		if !m[x] {
			return false
		}
	}
	return true
}

func (s *Shard) CheckRefs(_ context.Context, blobIDs []string) (map[string]BlobRef, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.opts.Now()
	out := make(map[string]BlobRef, len(blobIDs))
	for _, id := range blobIDs {
		var r BlobRef
		if loc, ok := s.refs[id]; ok {
			r.Referenced, r.Bucket = true, loc.bucket
			if rp := s.replicasOfRO(loc); rp != nil {
				r.Replicas = append([]string(nil), rp...)
			}
		}
		if exp, ok := s.pending[id]; ok && exp.After(now) {
			r.Pending = true
		}
		out[id] = r
	}
	return out, nil
}

func (s *Shard) replicasOfRO(loc blobLoc) []string {
	if loc.uploadID != "" {
		if u := s.uploads[loc.uploadID]; u != nil {
			return u.Parts[loc.part].Replicas
		}
		return nil
	}
	if bs := s.buckets[loc.bucket]; bs != nil {
		if ke := bs.Objects[loc.key]; ke != nil {
			for _, v := range ke.Versions {
				if v.VersionID == loc.versionID && loc.seg < len(v.Segments) {
					return v.Segments[loc.seg].Replicas
				}
			}
		}
	}
	return nil
}

func (s *Shard) ReplicaCount(_ context.Context, nodeID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, loc := range s.refs {
		for _, r := range s.replicasOfRO(loc) {
			if r == nodeID {
				n++
			}
		}
	}
	return n, nil
}

func (s *Shard) PeekGarbage(_ context.Context, olderThan time.Time, limit int) ([]GarbageItem, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []GarbageItem
	for _, g := range s.garbage {
		if len(out) >= limit {
			break
		}
		if g.EnqueuedAt.Before(olderThan) {
			g.Replicas = append([]string(nil), g.Replicas...)
			out = append(out, g)
		}
	}
	return out, nil
}

func (s *Shard) AckGarbage(_ context.Context, items []GarbageItem) error {
	if len(items) == 0 {
		return nil
	}
	ids := make([]uint64, len(items))
	for i, g := range items {
		ids[i] = g.ID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mutate(&record{Op: opAckGarbage, Garbage: ids})
}

func (s *Shard) Usage(_ context.Context, tenant string) ([]int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return []int64{s.usage[tenant]}, nil
}

func (s *Shard) Stats(_ context.Context) ([]Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := Stats{Shard: s.opts.Index, Buckets: len(s.buckets), Uploads: len(s.uploads),
		Pending: len(s.pending), GarbageQueue: len(s.garbage), LSN: s.lsn}
	for _, bs := range s.buckets {
		st.Keys += len(bs.keys)
		for _, ke := range bs.Objects {
			st.Versions += len(ke.Versions)
			for _, v := range ke.Versions {
				st.Segments += len(v.Segments)
			}
		}
	}
	if s.wal != nil {
		st.WALRecords = s.wal.records
	}
	return []Stats{st}, nil
}

var _ Store = (*Shard)(nil)
