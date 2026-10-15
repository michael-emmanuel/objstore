// Package metadata is the metadata (control) plane: buckets, object
// versions, multipart uploads, blob references, replica locations, the
// garbage queue and per-tenant usage.
//
// Architecture (honest summary, expanded in docs/metadata-scaling.md):
//
//   - The keyspace is partitioned by bucket across a fixed number of shards
//     (hash(bucket) mod S). Each shard is a single process that is the sole
//     authority for its buckets.
//   - A shard keeps its state in memory and makes every mutation durable in a
//     write-ahead log (fsync per commit) before applying it, with periodic
//     snapshots for bounded recovery time.
//   - Shards are NOT replicated and there is NO consensus protocol. A shard
//     outage makes its buckets unavailable (data is not lost; the WAL is on
//     disk). Production systems replicate each shard with Raft/Paxos or use a
//     strongly consistent database (FoundationDB, Spanner, TiKV). The Store
//     interface is the seam where that replacement would go.
package metadata

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/michael-emmanuel/objstore/internal/object"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrBucketExists    = errors.New("bucket already exists")
	ErrNoSuchBucket    = errors.New("no such bucket")
	ErrNoSuchUpload    = errors.New("no such upload")
	ErrInvalidPart     = errors.New("invalid part")
	ErrQuotaExceeded   = errors.New("storage quota exceeded")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrUnavailable     = errors.New("metadata unavailable")
	ErrReadOnly        = errors.New("metadata shard is read-only after a WAL failure")
)

// Bucket configuration. Owned by exactly one tenant.
type Bucket struct {
	Name              string              `json:"name"`
	Tenant            string              `json:"tenant"`
	Versioning        bool                `json:"versioning"`
	PublicRead        bool                `json:"public_read"`
	ReplicationFactor int                 `json:"replication_factor"`
	StorageClass      object.StorageClass `json:"storage_class"`
	CreatedAt         time.Time           `json:"created_at"`
}

// Part is one uploaded part of a multipart upload.
type Part struct {
	Number     int       `json:"part_number"`
	BlobID     string    `json:"blob_id"`
	Size       int64     `json:"size"`
	SHA256     string    `json:"sha256"`
	Replicas   []string  `json:"replicas"`
	UploadedAt time.Time `json:"uploaded_at"`
}

// Upload is an in-progress multipart upload.
type Upload struct {
	ID                string              `json:"id"`
	Bucket            string              `json:"bucket"`
	Key               string              `json:"key"`
	Tenant            string              `json:"tenant"`
	ContentType       string              `json:"content_type,omitempty"`
	UserMeta          map[string]string   `json:"user_meta,omitempty"`
	StorageClass      object.StorageClass `json:"storage_class"`
	ReplicationFactor int                 `json:"replication_factor"`
	CreatedAt         time.Time           `json:"created_at"`
	Parts             map[int]Part        `json:"parts"`
}

func (u Upload) clone() Upload {
	out := u
	out.Parts = make(map[int]Part, len(u.Parts))
	for k, p := range u.Parts {
		p.Replicas = append([]string(nil), p.Replicas...)
		out.Parts[k] = p
	}
	return out
}

// UploadBucket extracts the bucket from an upload ID ("<bucket>~<random>").
// Encoding the bucket lets any gateway route upload calls to the right shard
// without a global upload directory.
func UploadBucket(id string) (string, bool) {
	i := strings.LastIndexByte(id, '~')
	if i <= 0 || i == len(id)-1 {
		return "", false
	}
	return id[:i], true
}

// CompletePart is a client-supplied entry of the completion list.
type CompletePart struct {
	Number int    `json:"part_number"`
	ETag   string `json:"etag"`
}

// GarbageItem is a blob that is no longer referenced by metadata and whose
// physical copies may be reclaimed after a grace period.
type GarbageItem struct {
	ID         uint64    `json:"id"`
	Shard      int       `json:"shard"`
	BlobID     string    `json:"blob_id"`
	Replicas   []string  `json:"replicas"`
	Reason     string    `json:"reason"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// SegmentRef is a flattened view of one stored segment used by repair and
// rebalancing scans.
type SegmentRef struct {
	Bucket    string   `json:"bucket"`
	Key       string   `json:"key"`
	VersionID string   `json:"version_id"`
	Index     int      `json:"index"`
	BlobID    string   `json:"blob_id"`
	Size      int64    `json:"size"`
	SHA256    string   `json:"sha256"`
	Replicas  []string `json:"replicas"`
	RF        int      `json:"rf"`
}

// BlobRef answers "is this blob still needed, and where should it live?"
type BlobRef struct {
	Referenced bool     `json:"referenced"`
	Pending    bool     `json:"pending"`
	Bucket     string   `json:"bucket,omitempty"`
	Replicas   []string `json:"replicas,omitempty"`
}

// ListRequest lists the current versions of keys >= StartAt with Prefix.
type ListRequest struct {
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix"`
	Delimiter string `json:"delimiter"`
	StartAt   string `json:"start_at"`
	Limit     int    `json:"limit"`
}

type ListEntry struct {
	Key          string              `json:"key"`
	Size         int64               `json:"size"`
	ETag         string              `json:"etag"`
	VersionID    string              `json:"version_id"`
	LastModified time.Time           `json:"last_modified"`
	StorageClass object.StorageClass `json:"storage_class"`
}

type ListResult struct {
	Entries        []ListEntry `json:"entries"`
	CommonPrefixes []string    `json:"common_prefixes,omitempty"`
	Truncated      bool        `json:"truncated"`
	NextStartAt    string      `json:"next_start_at,omitempty"`
}

// CommitOptions carries policy decided by the gateway.
type CommitOptions struct {
	// QuotaBytes is the maximum tenant usage allowed on this shard after the
	// commit. <= 0 disables the check. See internal/quota for how the gateway
	// derives a per-shard limit from a global quota.
	QuotaBytes int64 `json:"quota_bytes"`
}

// Stats summarizes a shard.
type Stats struct {
	Shard        int    `json:"shard"`
	Buckets      int    `json:"buckets"`
	Keys         int    `json:"keys"`
	Versions     int    `json:"versions"`
	Segments     int    `json:"segments"`
	Uploads      int    `json:"uploads"`
	Pending      int    `json:"pending"`
	GarbageQueue int    `json:"garbage_queue"`
	LSN          uint64 `json:"lsn"`
	WALRecords   int    `json:"wal_records"`
}

// Store is the metadata plane contract. *Shard implements it for one
// partition, *Client implements it over HTTP, and *Router implements it for
// a whole cluster by routing to shards.
type Store interface {
	CreateBucket(ctx context.Context, b Bucket) error
	GetBucket(ctx context.Context, name string) (Bucket, error)

	// RegisterPending protects blobs that are being written but not yet
	// committed from garbage collection.
	RegisterPending(ctx context.Context, bucket string, blobIDs []string, ttl time.Duration) error
	// Commit publishes a new version. This is THE visibility point of a PUT.
	Commit(ctx context.Context, v object.Version, opts CommitOptions) (object.Version, error)
	// Get returns a version (latest if versionID == ""). A delete marker is
	// returned as a normal Version with DeleteMarker set.
	Get(ctx context.Context, bucket, key, versionID string) (object.Version, error)
	Delete(ctx context.Context, bucket, key string) (object.Version, error)
	DeleteVersion(ctx context.Context, bucket, key, versionID string) error
	List(ctx context.Context, req ListRequest) (ListResult, error)
	ListVersions(ctx context.Context, bucket, key string) ([]object.Version, error)

	CreateUpload(ctx context.Context, u Upload) error
	GetUpload(ctx context.Context, id string) (Upload, error)
	PutPart(ctx context.Context, uploadID string, p Part) error
	CompleteUpload(ctx context.Context, uploadID string, parts []CompletePart, versionID string, opts CommitOptions) (object.Version, error)
	AbortUpload(ctx context.Context, uploadID string) error

	ScanSegments(ctx context.Context, cursor string, limit int) ([]SegmentRef, string, error)
	UpdateReplicas(ctx context.Context, bucket, blobID string, add, remove []string) error
	CheckRefs(ctx context.Context, blobIDs []string) (map[string]BlobRef, error)
	ReplicaCount(ctx context.Context, nodeID string) (int, error)

	PeekGarbage(ctx context.Context, olderThan time.Time, limit int) ([]GarbageItem, error)
	AckGarbage(ctx context.Context, items []GarbageItem) error
	ExpireUploads(ctx context.Context, olderThan time.Time) (int, error)
	PurgeTombstones(ctx context.Context, olderThan time.Time) (int, error)

	Usage(ctx context.Context, tenant string) ([]int64, error) // per shard
	Stats(ctx context.Context) ([]Stats, error)
}
