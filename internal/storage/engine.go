// Package storage is the single-node blob engine: it stores immutable blobs
// on a local filesystem with an explicit commit protocol and block-level
// integrity checking.
//
// Commit protocol for a blob (see docs/architecture.md, "Write path"):
//
//  1. Stream bytes into tmp/<id>.<rand>.tmp while computing SHA-256 and a
//     CRC32C per 64 KiB block. Nothing in tmp/ is ever visible to readers.
//  2. Verify size and (optional) expected SHA-256.
//  3. fsync the data file.
//  4. Write the sidecar (size, sha256, block CRCs) to tmp/, fsync it, and
//     rename it into place.
//  5. rename the data file into place  <-- the blob becomes visible here.
//  6. fsync the parent directory so the renames themselves are durable.
//
// A crash at any point leaves either nothing visible, or an orphan sidecar
// (removed at startup), or a complete blob. A half-written blob can never be
// served. The metadata commit that makes the *object* visible happens later,
// in the metadata plane, only after a write quorum of blobs is committed.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/michael-emmanuel/objstore/internal/object"
)

var (
	ErrNotFound         = errors.New("blob not found")
	ErrConflict         = errors.New("blob exists with different content")
	ErrBusy             = errors.New("blob is being written concurrently")
	ErrDiskFull         = errors.New("insufficient storage")
	ErrCorrupt          = errors.New("blob failed integrity check")
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrSizeMismatch     = errors.New("size mismatch (truncated or oversized transfer)")
	ErrInvalidID        = errors.New("invalid blob id")
	ErrRange            = errors.New("range not satisfiable")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const (
	DefaultBlockSize = 64 << 10
	ioBufSize        = 256 << 10
	sidecarMagic     = "OSB1"
	sidecarExt       = ".sum"
)

// Options configures an Engine.
type Options struct {
	Root          string
	CapacityBytes int64 // artificial capacity limit; 0 means only the real filesystem limits
	Fsync         bool  // fsync data, sidecar and directories on commit
	BlockSize     int
}

// BlobInfo describes a committed blob.
type BlobInfo struct {
	ID      string    `json:"id"`
	Size    int64     `json:"size"`
	SHA256  string    `json:"sha256,omitempty"`
	ModTime time.Time `json:"mod_time"`
}

// Engine is safe for concurrent use.
type Engine struct {
	opts                         Options
	blobDir, tmpDir, quarDir     string
	capacity, used, reserved, nb atomic.Int64
	bufPool, blockPool           sync.Pool

	mu      sync.Mutex
	writing map[string]struct{}

	qmu         sync.Mutex
	quarantined map[string]string // id -> reason

	// OnCorrupt is called (synchronously) when a blob is quarantined.
	OnCorrupt func(id, reason string)
}

// Open creates or recovers an engine rooted at opts.Root.
func Open(opts Options) (*Engine, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	e := &Engine{
		opts:        opts,
		blobDir:     filepath.Join(opts.Root, "blobs"),
		tmpDir:      filepath.Join(opts.Root, "tmp"),
		quarDir:     filepath.Join(opts.Root, "quarantine"),
		writing:     map[string]struct{}{},
		quarantined: map[string]string{},
	}
	e.capacity.Store(opts.CapacityBytes)
	e.bufPool.New = func() any { b := make([]byte, ioBufSize); return &b }
	bs := opts.BlockSize
	e.blockPool.New = func() any { b := make([]byte, bs); return &b }
	for _, d := range []string{e.blobDir, e.tmpDir, e.quarDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if err := e.recover(); err != nil {
		return nil, fmt.Errorf("recover: %w", err)
	}
	return e, nil
}

// recover discards in-progress writes from a previous run and rebuilds
// usage counters. Temporary files are never resumed: the writer (gateway,
// repairer) still holds the authoritative bytes and will retry.
func (e *Engine) recover() error {
	tmps, err := os.ReadDir(e.tmpDir)
	if err != nil {
		return err
	}
	for _, t := range tmps {
		_ = os.Remove(filepath.Join(e.tmpDir, t.Name()))
	}
	shards, err := os.ReadDir(e.blobDir)
	if err != nil {
		return err
	}
	for _, sh := range shards {
		dir := filepath.Join(e.blobDir, sh.Name())
		ents, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		names := map[string]bool{}
		for _, en := range ents {
			names[en.Name()] = true
		}
		for name := range names {
			if strings.HasSuffix(name, sidecarExt) {
				if !names[strings.TrimSuffix(name, sidecarExt)] {
					_ = os.Remove(filepath.Join(dir, name)) // sidecar committed, data rename never happened
				}
				continue
			}
			if !names[name+sidecarExt] {
				e.moveToQuarantine(name, "missing sidecar")
				continue
			}
			if fi, err := os.Stat(filepath.Join(dir, name)); err == nil {
				e.used.Add(fi.Size())
				e.nb.Add(1)
			}
		}
	}
	qs, err := os.ReadDir(e.quarDir)
	if err != nil {
		return err
	}
	for _, q := range qs {
		if !strings.HasSuffix(q.Name(), sidecarExt) {
			e.quarantined[q.Name()] = "found at startup"
		}
	}
	return nil
}

func (e *Engine) path(id string) string { return filepath.Join(e.blobDir, id[:2], id) }

// SetCapacity changes the artificial capacity limit (fault injection).
func (e *Engine) SetCapacity(n int64) { e.capacity.Store(n) }

// Put streams r into a new blob. expectedSize < 0 means unknown.
// If the blob already exists with identical content the call succeeds
// (idempotent retries); with different content it fails with ErrConflict.
func (e *Engine) Put(ctx context.Context, id string, r io.Reader, expectedSize int64, expectedSHA string) (BlobInfo, error) {
	if !object.ValidBlobID(id) {
		return BlobInfo{}, ErrInvalidID
	}
	e.mu.Lock()
	if _, busy := e.writing[id]; busy {
		e.mu.Unlock()
		return BlobInfo{}, ErrBusy
	}
	e.writing[id] = struct{}{}
	e.mu.Unlock()
	defer func() { e.mu.Lock(); delete(e.writing, id); e.mu.Unlock() }()

	existing, statErr := e.Stat(id)
	if statErr == nil && expectedSHA != "" {
		if existing.SHA256 == expectedSHA {
			return existing, nil
		}
		return BlobInfo{}, ErrConflict
	}

	f, err := os.CreateTemp(e.tmpDir, id+".*.tmp")
	if err != nil {
		return BlobInfo{}, mapFSErr(err)
	}
	committed := false
	defer func() {
		if !committed {
			f.Close()
			os.Remove(f.Name())
		}
	}()

	sc, err := e.writeData(ctx, f, r)
	if err != nil {
		return BlobInfo{}, err
	}
	if expectedSize >= 0 && sc.size != expectedSize {
		return BlobInfo{}, fmt.Errorf("%w: got %d bytes, expected %d", ErrSizeMismatch, sc.size, expectedSize)
	}
	sum := hex.EncodeToString(sc.sha[:])
	if expectedSHA != "" && sum != expectedSHA {
		return BlobInfo{}, fmt.Errorf("%w: computed %s, expected %s", ErrChecksumMismatch, sum, expectedSHA)
	}
	if statErr == nil { // existed, caller gave no checksum: compare content
		if existing.SHA256 == sum {
			return existing, nil
		}
		return BlobInfo{}, ErrConflict
	}

	if e.opts.Fsync {
		if err := f.Sync(); err != nil {
			return BlobInfo{}, mapFSErr(err)
		}
	}
	if err := f.Close(); err != nil {
		return BlobInfo{}, mapFSErr(err)
	}
	dir := filepath.Join(e.blobDir, id[:2])
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return BlobInfo{}, mapFSErr(err)
	}
	if err := e.writeSidecar(id, sc); err != nil {
		return BlobInfo{}, err
	}
	if err := os.Rename(f.Name(), e.path(id)); err != nil {
		os.Remove(e.path(id) + sidecarExt)
		return BlobInfo{}, mapFSErr(err)
	}
	committed = true
	if e.opts.Fsync {
		if err := syncDir(dir); err != nil {
			return BlobInfo{}, mapFSErr(err)
		}
	}
	e.used.Add(sc.size)
	e.nb.Add(1)
	return BlobInfo{ID: id, Size: sc.size, SHA256: sum, ModTime: time.Now()}, nil
}

type sidecar struct {
	blockSize uint32
	size      int64
	sha       [32]byte
	crcs      []uint32
}

func (e *Engine) writeData(ctx context.Context, f *os.File, r io.Reader) (*sidecar, error) {
	bp := e.bufPool.Get().(*[]byte)
	defer e.bufPool.Put(bp)
	buf := *bp
	h := sha256.New()
	sc := &sidecar{blockSize: uint32(e.opts.BlockSize)}
	var cur uint32
	inBlock := 0
	bs := e.opts.BlockSize
	var reservedHere int64
	defer func() { e.reserved.Add(-reservedHere) }()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, rerr := r.Read(buf)
		if n > 0 {
			if c := e.capacity.Load(); c > 0 && e.used.Load()+e.reserved.Load()+int64(n) > c {
				return nil, ErrDiskFull
			}
			e.reserved.Add(int64(n))
			reservedHere += int64(n)
			if _, err := f.Write(buf[:n]); err != nil {
				return nil, mapFSErr(err)
			}
			h.Write(buf[:n])
			for p := buf[:n]; len(p) > 0; {
				take := bs - inBlock
				if take > len(p) {
					take = len(p)
				}
				cur = crc32.Update(cur, castagnoli, p[:take])
				inBlock += take
				p = p[take:]
				if inBlock == bs {
					sc.crcs = append(sc.crcs, cur)
					cur, inBlock = 0, 0
				}
			}
			sc.size += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
	}
	if inBlock > 0 {
		sc.crcs = append(sc.crcs, cur)
	}
	copy(sc.sha[:], h.Sum(nil))
	return sc, nil
}

func (e *Engine) writeSidecar(id string, sc *sidecar) error {
	b := make([]byte, 0, 4+4+8+32+4+4*len(sc.crcs)+4)
	b = append(b, sidecarMagic...)
	b = binary.BigEndian.AppendUint32(b, sc.blockSize)
	b = binary.BigEndian.AppendUint64(b, uint64(sc.size))
	b = append(b, sc.sha[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(sc.crcs)))
	for _, c := range sc.crcs {
		b = binary.BigEndian.AppendUint32(b, c)
	}
	b = binary.BigEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))

	f, err := os.CreateTemp(e.tmpDir, id+".*.sum.tmp")
	if err != nil {
		return mapFSErr(err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(f.Name())
		return mapFSErr(err)
	}
	if e.opts.Fsync {
		if err := f.Sync(); err != nil {
			f.Close()
			os.Remove(f.Name())
			return mapFSErr(err)
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return mapFSErr(err)
	}
	if err := os.Rename(f.Name(), e.path(id)+sidecarExt); err != nil {
		os.Remove(f.Name())
		return mapFSErr(err)
	}
	return nil
}

func (e *Engine) readSidecar(id string) (*sidecar, error) {
	b, err := os.ReadFile(e.path(id) + sidecarExt)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if len(b) < 4+4+8+32+4+4 || string(b[:4]) != sidecarMagic {
		return nil, fmt.Errorf("%w: malformed sidecar", ErrCorrupt)
	}
	body, tail := b[:len(b)-4], b[len(b)-4:]
	if crc32.Checksum(body, castagnoli) != binary.BigEndian.Uint32(tail) {
		return nil, fmt.Errorf("%w: sidecar checksum", ErrCorrupt)
	}
	sc := &sidecar{
		blockSize: binary.BigEndian.Uint32(b[4:8]),
		size:      int64(binary.BigEndian.Uint64(b[8:16])),
	}
	copy(sc.sha[:], b[16:48])
	n := int(binary.BigEndian.Uint32(b[48:52]))
	if len(body) != 52+4*n || sc.blockSize == 0 {
		return nil, fmt.Errorf("%w: sidecar length", ErrCorrupt)
	}
	sc.crcs = make([]uint32, n)
	for i := range sc.crcs {
		sc.crcs[i] = binary.BigEndian.Uint32(b[52+4*i:])
	}
	return sc, nil
}

// Stat returns blob info from the sidecar without reading data.
func (e *Engine) Stat(id string) (BlobInfo, error) {
	if !object.ValidBlobID(id) {
		return BlobInfo{}, ErrInvalidID
	}
	fi, err := os.Stat(e.path(id))
	if err != nil {
		return BlobInfo{}, ErrNotFound
	}
	sc, err := e.readSidecar(id)
	if err != nil {
		if errors.Is(err, ErrCorrupt) {
			e.Quarantine(id, err.Error())
		}
		return BlobInfo{}, err
	}
	if fi.Size() != sc.size {
		e.Quarantine(id, "data length differs from sidecar")
		return BlobInfo{}, fmt.Errorf("%w: truncated data file", ErrCorrupt)
	}
	return BlobInfo{ID: id, Size: sc.size, SHA256: hex.EncodeToString(sc.sha[:]), ModTime: fi.ModTime()}, nil
}

// Get opens a verified reader over [off, off+length). length < 0 reads to
// the end. Every block is CRC-checked before any of its bytes are returned,
// so corrupted data is never sent to a client; the reader instead fails with
// ErrCorrupt and the blob is quarantined.
func (e *Engine) Get(id string, off, length int64) (io.ReadCloser, BlobInfo, error) {
	info, err := e.Stat(id)
	if err != nil {
		return nil, BlobInfo{}, err
	}
	if off < 0 || off > info.Size || (off == info.Size && info.Size > 0 && length != 0) {
		return nil, info, ErrRange
	}
	if length < 0 || off+length > info.Size {
		length = info.Size - off
	}
	sc, err := e.readSidecar(id)
	if err != nil {
		return nil, info, err
	}
	f, err := os.Open(e.path(id))
	if err != nil {
		return nil, info, ErrNotFound
	}
	bs := int64(sc.blockSize)
	bp := e.blockPool.Get().(*[]byte)
	if int64(len(*bp)) < bs { // blob written with a larger block size than current config
		b := make([]byte, bs)
		bp = &b
	}
	return &verifyingReader{
		e: e, id: id, f: f, sc: sc, bp: bp,
		block: off / bs, skip: int(off % bs), remaining: length,
	}, info, nil
}

type verifyingReader struct {
	e         *Engine
	id        string
	f         *os.File
	sc        *sidecar
	bp        *[]byte
	block     int64
	skip      int
	pos, end  int
	remaining int64
	err       error
}

func (r *verifyingReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if r.pos == r.end {
		if err := r.load(); err != nil {
			r.err = err
			return 0, err
		}
	}
	n := copy(p, (*r.bp)[r.pos:r.end])
	if int64(n) > r.remaining {
		n = int(r.remaining)
	}
	r.pos += n
	r.remaining -= int64(n)
	return n, nil
}

func (r *verifyingReader) load() error {
	bs := int64(r.sc.blockSize)
	start := r.block * bs
	n := bs
	if start+n > r.sc.size {
		n = r.sc.size - start
	}
	if r.block >= int64(len(r.sc.crcs)) || n <= 0 {
		return fmt.Errorf("%w: block %d out of range", ErrCorrupt, r.block)
	}
	buf := (*r.bp)[:n]
	if _, err := r.f.ReadAt(buf, start); err != nil {
		r.e.Quarantine(r.id, "short read: "+err.Error())
		return fmt.Errorf("%w: short read at block %d", ErrCorrupt, r.block)
	}
	if crc32.Checksum(buf, castagnoli) != r.sc.crcs[r.block] {
		r.e.Quarantine(r.id, fmt.Sprintf("crc mismatch in block %d", r.block))
		return fmt.Errorf("%w: crc mismatch in block %d", ErrCorrupt, r.block)
	}
	r.pos, r.end = r.skip, int(n)
	r.skip = 0
	r.block++
	return nil
}

func (r *verifyingReader) Close() error {
	if r.bp != nil && int64(len(*r.bp)) == int64(r.e.opts.BlockSize) {
		r.e.blockPool.Put(r.bp)
	}
	r.bp = nil
	return r.f.Close()
}

// Verify reads the whole blob, checking every block CRC and the SHA-256.
// Used by the scrubber and by explicit replica verification.
func (e *Engine) Verify(id string) (BlobInfo, error) {
	rc, info, err := e.Get(id, 0, -1)
	if err != nil {
		return info, err
	}
	defer rc.Close()
	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return info, err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != info.SHA256 {
		e.Quarantine(id, "sha256 mismatch")
		return info, fmt.Errorf("%w: sha256 %s != %s", ErrCorrupt, got, info.SHA256)
	}
	return info, nil
}

// Delete removes a committed blob. Open readers keep working (POSIX unlink
// semantics), which is why GC can delete without coordinating with reads.
func (e *Engine) Delete(id string) error {
	if !object.ValidBlobID(id) {
		return ErrInvalidID
	}
	p := e.path(id)
	fi, err := os.Stat(p)
	if err != nil {
		return ErrNotFound
	}
	if err := os.Remove(p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return err
	}
	_ = os.Remove(p + sidecarExt)
	e.used.Add(-fi.Size())
	e.nb.Add(-1)
	return nil
}

// List returns up to limit committed blobs with ID > after, in ID order.
// SHA256 is not populated (that would require reading every sidecar).
func (e *Engine) List(after string, limit int) ([]BlobInfo, error) {
	shards, err := os.ReadDir(e.blobDir)
	if err != nil {
		return nil, err
	}
	var out []BlobInfo
	for _, sh := range shards {
		if len(after) >= 2 && sh.Name() < after[:2] {
			continue
		}
		ents, err := os.ReadDir(filepath.Join(e.blobDir, sh.Name()))
		if err != nil {
			return nil, err
		}
		for _, en := range ents {
			name := en.Name()
			if !object.ValidBlobID(name) || name <= after {
				continue
			}
			fi, err := en.Info()
			if err != nil {
				continue // deleted concurrently
			}
			out = append(out, BlobInfo{ID: name, Size: fi.Size(), ModTime: fi.ModTime()})
			if len(out) >= limit {
				return out, nil
			}
		}
	}
	return out, nil
}

// Quarantine moves a blob out of the serving path. It is not deleted: an
// operator may want to inspect it, and the repairer will recreate a good
// copy from another replica, then acknowledge (delete) the quarantined one.
func (e *Engine) Quarantine(id, reason string) {
	if e.moveToQuarantine(id, reason) && e.OnCorrupt != nil {
		e.OnCorrupt(id, reason)
	}
}

func (e *Engine) moveToQuarantine(id, reason string) bool {
	p := e.path(id)
	fi, err := os.Stat(p)
	if err != nil {
		return false
	}
	if err := os.Rename(p, filepath.Join(e.quarDir, id)); err != nil {
		return false
	}
	_ = os.Rename(p+sidecarExt, filepath.Join(e.quarDir, id+sidecarExt))
	e.used.Add(-fi.Size())
	e.nb.Add(-1)
	e.qmu.Lock()
	e.quarantined[id] = reason
	e.qmu.Unlock()
	return true
}

// Quarantined returns quarantined blob IDs and reasons.
func (e *Engine) Quarantined() map[string]string {
	e.qmu.Lock()
	defer e.qmu.Unlock()
	out := make(map[string]string, len(e.quarantined))
	for k, v := range e.quarantined {
		out[k] = v
	}
	return out
}

// AckQuarantine permanently deletes quarantined blobs once the cluster has
// re-replicated them elsewhere.
func (e *Engine) AckQuarantine(ids []string) {
	e.qmu.Lock()
	defer e.qmu.Unlock()
	for _, id := range ids {
		if !object.ValidBlobID(id) {
			continue
		}
		os.Remove(filepath.Join(e.quarDir, id))
		os.Remove(filepath.Join(e.quarDir, id+sidecarExt))
		delete(e.quarantined, id)
	}
}

// CorruptByte flips one byte of a stored blob without touching its
// checksums. Fault injection only.
func (e *Engine) CorruptByte(id string, off int64) error {
	if !object.ValidBlobID(id) {
		return ErrInvalidID
	}
	f, err := os.OpenFile(e.path(id), os.O_RDWR, 0)
	if err != nil {
		return ErrNotFound
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		return ErrRange
	}
	b[0] ^= 0xff
	_, err = f.WriteAt(b, off)
	return err
}

// Stats is a point-in-time usage snapshot.
type Stats struct {
	CapacityBytes int64 `json:"capacity_bytes"`
	UsedBytes     int64 `json:"used_bytes"`
	InflightBytes int64 `json:"inflight_bytes"`
	BlobCount     int64 `json:"blob_count"`
	FSFreeBytes   int64 `json:"fs_free_bytes"`
	Quarantined   int   `json:"quarantined"`
}

func (e *Engine) Stats() Stats {
	st := Stats{
		CapacityBytes: e.capacity.Load(),
		UsedBytes:     e.used.Load(),
		InflightBytes: e.reserved.Load(),
		BlobCount:     e.nb.Load(),
		FSFreeBytes:   fsFree(e.opts.Root),
	}
	if st.CapacityBytes == 0 {
		st.CapacityBytes = st.UsedBytes + st.FSFreeBytes
	}
	e.qmu.Lock()
	st.Quarantined = len(e.quarantined)
	e.qmu.Unlock()
	return st
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func mapFSErr(err error) error {
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return fmt.Errorf("%w: %v", ErrDiskFull, err)
	}
	return err
}
