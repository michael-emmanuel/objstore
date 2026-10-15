// Package object defines the logical object model shared by the metadata
// plane and the data plane, and the rules for turning a logical identity
// (bucket, key, version) into a physical blob identity.
//
// The key design rule: the user-supplied object key is NEVER used as a
// filesystem path. Storage nodes only ever see opaque, fixed-length, hex
// blob IDs derived by hashing the logical identity. This removes path
// traversal, case-folding collisions, reserved names, and path-length limits
// from the data plane entirely.
package object

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// StorageClass selects durability/cost policy. Only replication-based classes
// are implemented; see docs/tradeoffs.md for the erasure-coding discussion.
type StorageClass string

const (
	ClassStandard StorageClass = "STANDARD"           // RF from bucket (default 3)
	ClassReduced  StorageClass = "REDUCED_REDUNDANCY" // RF=2, for reproducible data
)

// Segment is one physically-stored, immutable piece of an object. A simple
// PUT is split into fixed-size segments; a multipart object has one segment
// per part. Each segment is placed independently on the cluster.
type Segment struct {
	Index    int      `json:"index"`
	BlobID   string   `json:"blob_id"`
	Offset   int64    `json:"offset"` // logical offset of this segment within the object
	Size     int64    `json:"size"`
	SHA256   string   `json:"sha256"`
	Replicas []string `json:"replicas"` // node IDs believed to hold a verified copy
}

// Version is the metadata record for one immutable version of an object.
// It references data; it never contains object bytes.
type Version struct {
	Bucket            string            `json:"bucket"`
	Key               string            `json:"key"`
	VersionID         string            `json:"version_id"`
	Seq               uint64            `json:"seq"` // commit order within the metadata shard
	Tenant            string            `json:"tenant"`
	Size              int64             `json:"size"`
	ETag              string            `json:"etag"`
	ContentType       string            `json:"content_type,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`         // when the key first came into existence
	UpdatedAt         time.Time         `json:"updated_at"`         // when this version was committed
	Checksum          string            `json:"checksum,omitempty"` // sha256 of full object; empty for multipart (composite ETag instead)
	StorageClass      StorageClass      `json:"storage_class"`
	ReplicationFactor int               `json:"replication_factor"`
	DeleteMarker      bool              `json:"delete_marker,omitempty"`
	Segments          []Segment         `json:"segments,omitempty"`
	UserMeta          map[string]string `json:"user_meta,omitempty"`
}

// Clone returns a deep copy so callers can never mutate store-owned state.
func (v Version) Clone() Version {
	out := v
	if v.Segments != nil {
		out.Segments = make([]Segment, len(v.Segments))
		for i, s := range v.Segments {
			s.Replicas = append([]string(nil), s.Replicas...)
			out.Segments[i] = s
		}
	}
	if v.UserMeta != nil {
		out.UserMeta = make(map[string]string, len(v.UserMeta))
		for k, val := range v.UserMeta {
			out.UserMeta[k] = val
		}
	}
	return out
}

// Limits bound metadata size. Bounded metadata is what keeps the metadata
// plane's memory and WAL growth predictable (see docs/metadata-scaling.md).
const (
	MaxKeyBytes         = 1024
	MaxUserMetaEntries  = 16
	MaxUserMetaBytes    = 2048
	MaxContentTypeBytes = 256
	BlobIDLen           = 32 // hex chars (128 bits)
)

var (
	ErrInvalidBucket   = errors.New("invalid bucket name")
	ErrInvalidKey      = errors.New("invalid object key")
	ErrInvalidMetadata = errors.New("invalid object metadata")
)

// ValidateBucket enforces DNS-compatible bucket names (3-63 chars,
// lowercase letters, digits, '-' and '.', alphanumeric at both ends).
func ValidateBucket(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("%w: length must be 3-63", ErrInvalidBucket)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		alnum := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !alnum && c != '-' && c != '.' {
			return fmt.Errorf("%w: illegal character %q", ErrInvalidBucket, c)
		}
		if (i == 0 || i == len(name)-1) && !alnum {
			return fmt.Errorf("%w: must start and end with a letter or digit", ErrInvalidBucket)
		}
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("%w: consecutive dots", ErrInvalidBucket)
	}
	return nil
}

// ValidateKey accepts any valid UTF-8 up to 1024 bytes without control
// characters. Keys like "../../etc/passwd" are legal *keys*: they are safe
// because keys are never interpreted as paths anywhere in the system.
func ValidateKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty", ErrInvalidKey)
	}
	if len(key) > MaxKeyBytes {
		return fmt.Errorf("%w: longer than %d bytes", ErrInvalidKey, MaxKeyBytes)
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("%w: not valid UTF-8", ErrInvalidKey)
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: control character", ErrInvalidKey)
		}
	}
	return nil
}

// ValidateUserMeta bounds user-defined metadata.
func ValidateUserMeta(m map[string]string, contentType string) error {
	if len(contentType) > MaxContentTypeBytes {
		return fmt.Errorf("%w: content type too long", ErrInvalidMetadata)
	}
	if len(m) > MaxUserMetaEntries {
		return fmt.Errorf("%w: more than %d entries", ErrInvalidMetadata, MaxUserMetaEntries)
	}
	total := 0
	for k, v := range m {
		total += len(k) + len(v)
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return fmt.Errorf("%w: not valid UTF-8", ErrInvalidMetadata)
		}
	}
	if total > MaxUserMetaBytes {
		return fmt.Errorf("%w: larger than %d bytes", ErrInvalidMetadata, MaxUserMetaBytes)
	}
	return nil
}

// NewVersionID returns a 128-bit identifier: 48 bits of millisecond time
// (so IDs are roughly time-ordered when debugging) plus 80 random bits.
// Ordering of versions is NOT derived from IDs; the metadata shard assigns
// a commit sequence number (Version.Seq) which is the authoritative order.
func NewVersionID(now time.Time) string {
	var b [16]byte
	ms := uint64(now.UnixMilli())
	binary.BigEndian.PutUint64(b[:8], ms<<16)
	if _, err := rand.Read(b[6:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// RandomID returns n random bytes hex-encoded.
func RandomID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// SegmentBlobID derives the physical blob ID for a segment of an object
// version. Because versionID is unique per write, blob IDs are unique per
// write: blobs are immutable and never overwritten in place. This is the
// property that makes replica divergence detectable (same ID must mean same
// bytes) and makes caching by blob ID safe without invalidation.
func SegmentBlobID(bucket, key, versionID string, index int) string {
	return hashID("seg", bucket, key, versionID, fmt.Sprint(index))
}

// PartBlobID derives the blob ID for one attempt of a multipart part. The
// attempt nonce makes retries of the same part number write a new blob, so a
// retried part never collides with a half-acknowledged earlier attempt.
func PartBlobID(bucket, key, uploadID string, partNumber int, attempt string) string {
	return hashID("part", bucket, key, uploadID, fmt.Sprint(partNumber), attempt)
}

func hashID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], uint32(len(p))) // length-prefix: no ambiguity between ("ab","c") and ("a","bc")
		h.Write(l[:])
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))[:BlobIDLen]
}

// ValidBlobID reports whether id is exactly BlobIDLen lowercase hex chars.
// Storage nodes reject anything else, which is the data plane's path-safety
// guarantee.
func ValidBlobID(id string) bool {
	if len(id) != BlobIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// MultipartETag computes an S3-style composite ETag: hash of the
// concatenated binary part digests, suffixed with the part count. It proves
// the part list, not the whole-object bytes; see docs/tradeoffs.md (checksums).
func MultipartETag(partSHA256Hex []string) (string, error) {
	h := sha256.New()
	for _, p := range partSHA256Hex {
		b, err := hex.DecodeString(p)
		if err != nil {
			return "", fmt.Errorf("bad part digest: %w", err)
		}
		h.Write(b)
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(partSHA256Hex)), nil
}
