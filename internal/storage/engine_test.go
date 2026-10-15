package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/michael-emmanuel/objstore/internal/object"
)

func newEngine(t testing.TB, opts Options) *Engine {
	t.Helper()
	if opts.Root == "" {
		opts.Root = t.TempDir()
	}
	if opts.BlockSize == 0 {
		opts.BlockSize = 4096 // small blocks exercise block boundaries
	}
	e, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func randBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func shaHex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func id(i int) string { return object.SegmentBlobID("b", "k", "v", i) }

func TestPutGetRoundTripAndRanges(t *testing.T) {
	e := newEngine(t, Options{})
	data := randBytes(100_000, 1)
	info, err := e.Put(context.Background(), id(1), bytes.NewReader(data), int64(len(data)), shaHex(data))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(data)) || info.SHA256 != shaHex(data) {
		t.Fatalf("bad info %+v", info)
	}
	cases := [][2]int64{{0, -1}, {0, 1}, {4095, 2}, {4096, 4096}, {99_999, 1}, {12345, 50000}, {100_000, 0}}
	for _, c := range cases {
		rc, _, err := e.Get(id(1), c[0], c[1])
		if err != nil {
			t.Fatalf("range %v: %v", c, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		end := int64(len(data))
		if c[1] >= 0 {
			end = c[0] + c[1]
		}
		if !bytes.Equal(got, data[c[0]:end]) {
			t.Fatalf("range %v: content mismatch (%d bytes)", c, len(got))
		}
	}
	if _, _, err := e.Get(id(1), 100_001, 1); !errors.Is(err, ErrRange) {
		t.Fatalf("want ErrRange, got %v", err)
	}
}

func TestEmptyBlob(t *testing.T) {
	e := newEngine(t, Options{})
	if _, err := e.Put(context.Background(), id(0), bytes.NewReader(nil), 0, ""); err != nil {
		t.Fatal(err)
	}
	rc, info, err := e.Get(id(0), 0, -1)
	if err != nil || info.Size != 0 {
		t.Fatal(err, info)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if len(b) != 0 {
		t.Fatal("expected empty")
	}
}

func TestIdempotentPutAndConflict(t *testing.T) {
	e := newEngine(t, Options{})
	a := randBytes(5000, 2)
	ctx := context.Background()
	if _, err := e.Put(ctx, id(1), bytes.NewReader(a), -1, ""); err != nil {
		t.Fatal(err)
	}
	// Same bytes, with and without a checksum: success (retry-safe).
	if _, err := e.Put(ctx, id(1), bytes.NewReader(a), -1, ""); err != nil {
		t.Fatalf("idempotent retry failed: %v", err)
	}
	if _, err := e.Put(ctx, id(1), bytes.NewReader(a), -1, shaHex(a)); err != nil {
		t.Fatalf("idempotent retry with sha failed: %v", err)
	}
	// Different bytes under the same immutable id: conflict.
	b := randBytes(5000, 3)
	if _, err := e.Put(ctx, id(1), bytes.NewReader(b), -1, ""); !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if e.Stats().BlobCount != 1 {
		t.Fatalf("blob count %d", e.Stats().BlobCount)
	}
}

// A reader that fails mid-stream models a client disconnect or a dropped
// replication connection.
type failingReader struct {
	r     io.Reader
	after int
	read  int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.read >= f.after {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) > f.after-f.read {
		p = p[:f.after-f.read]
	}
	n, err := f.r.Read(p)
	f.read += n
	return n, err
}

func TestPartialWriteNeverVisible(t *testing.T) {
	e := newEngine(t, Options{})
	data := randBytes(50_000, 4)
	_, err := e.Put(context.Background(), id(1), &failingReader{r: bytes.NewReader(data), after: 20_000}, -1, "")
	if err == nil {
		t.Fatal("expected error")
	}
	if _, err := e.Stat(id(1)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial blob is visible: %v", err)
	}
	if ents, _ := os.ReadDir(e.tmpDir); len(ents) != 0 {
		t.Fatalf("temporary files leaked: %d", len(ents))
	}
	// Truncation detected by expected size.
	if _, err := e.Put(context.Background(), id(2), bytes.NewReader(data[:100]), 200, ""); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("want size mismatch, got %v", err)
	}
	// Corruption in transit detected by expected checksum.
	if _, err := e.Put(context.Background(), id(3), bytes.NewReader(data), -1, shaHex(data[1:])); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("want checksum mismatch, got %v", err)
	}
	if e.Stats().BlobCount != 0 || e.Stats().UsedBytes != 0 {
		t.Fatalf("stats after failed writes: %+v", e.Stats())
	}
}

func TestContextCancellationAbortsWrite(t *testing.T) {
	e := newEngine(t, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		_, err := e.Put(ctx, id(1), pr, -1, "")
		done <- err
	}()
	pw.Write(randBytes(1000, 5))
	cancel()
	// Feed more bytes from another goroutine so the blocked Read returns and the
	// copy loop observes ctx. Put may return before consuming them, so closing
	// the reader side afterwards guarantees the writer goroutine is released.
	wdone := make(chan struct{})
	go func() {
		defer close(wdone)
		pw.Write(randBytes(10, 6))
		pw.Close()
	}()
	err := <-done
	pr.Close()
	<-wdone
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if _, err := e.Stat(id(1)); !errors.Is(err, ErrNotFound) {
		t.Fatal("cancelled blob visible")
	}
}

func TestCorruptionDetectedBeforeBytesServed(t *testing.T) {
	e := newEngine(t, Options{})
	var corrupted []string
	e.OnCorrupt = func(id, _ string) { corrupted = append(corrupted, id) }
	data := randBytes(20_000, 7)
	e.Put(context.Background(), id(1), bytes.NewReader(data), -1, "")
	if err := e.CorruptByte(id(1), 9000); err != nil { // block 2 (4096-byte blocks)
		t.Fatal(err)
	}
	// A range entirely inside an intact block still succeeds.
	rc, _, err := e.Get(id(1), 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(rc); err != nil || !bytes.Equal(b, data[:4096]) {
		t.Fatalf("intact block read failed: %v", err)
	}
	rc.Close()
	// Reading through the corrupted block fails and never returns its bytes.
	rc, _, err = e.Get(id(1), 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
	if len(got) != 8192 || !bytes.Equal(got, data[:8192]) {
		t.Fatalf("served %d bytes; only the two verified blocks may be served", len(got))
	}
	if len(corrupted) != 1 || e.Quarantined()[id(1)] == "" {
		t.Fatal("blob not quarantined")
	}
	if _, err := e.Stat(id(1)); !errors.Is(err, ErrNotFound) {
		t.Fatal("quarantined blob still served")
	}
	e.AckQuarantine([]string{id(1)})
	if len(e.Quarantined()) != 0 {
		t.Fatal("ack did not clear quarantine")
	}
}

func TestVerifyDetectsTruncation(t *testing.T) {
	e := newEngine(t, Options{})
	data := randBytes(10_000, 8)
	e.Put(context.Background(), id(1), bytes.NewReader(data), -1, "")
	if err := os.Truncate(e.path(id(1)), 5000); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Verify(id(1)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestDiskFull(t *testing.T) {
	e := newEngine(t, Options{CapacityBytes: 30_000})
	ctx := context.Background()
	if _, err := e.Put(ctx, id(1), bytes.NewReader(randBytes(20_000, 9)), -1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put(ctx, id(2), bytes.NewReader(randBytes(20_000, 10)), -1, ""); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("want ErrDiskFull, got %v", err)
	}
	if _, err := e.Stat(id(2)); !errors.Is(err, ErrNotFound) {
		t.Fatal("failed write visible")
	}
	// Existing data stays readable on a full disk.
	if _, err := e.Verify(id(1)); err != nil {
		t.Fatal(err)
	}
	e.SetCapacity(0)
	if _, err := e.Put(ctx, id(2), bytes.NewReader(randBytes(20_000, 10)), -1, ""); err != nil {
		t.Fatalf("after freeing capacity: %v", err)
	}
}

func TestRecoveryCleansCrashLeftovers(t *testing.T) {
	root := t.TempDir()
	e := newEngine(t, Options{Root: root})
	e.Put(context.Background(), id(1), bytes.NewReader(randBytes(1000, 11)), -1, "")
	// Simulate crashes at different points of the commit protocol.
	os.WriteFile(filepath.Join(root, "tmp", id(2)+".123.tmp"), []byte("partial"), 0o644) // crashed mid-write
	sidecarOnly := id(3)
	os.MkdirAll(filepath.Join(root, "blobs", sidecarOnly[:2]), 0o755)
	os.WriteFile(filepath.Join(root, "blobs", sidecarOnly[:2], sidecarOnly+sidecarExt), []byte("x"), 0o644) // crashed between renames

	e2 := newEngine(t, Options{Root: root})
	if st := e2.Stats(); st.BlobCount != 1 || st.UsedBytes != 1000 {
		t.Fatalf("recovered stats %+v", st)
	}
	if ents, _ := os.ReadDir(filepath.Join(root, "tmp")); len(ents) != 0 {
		t.Fatal("tmp not cleaned")
	}
	if _, err := os.Stat(filepath.Join(root, "blobs", sidecarOnly[:2], sidecarOnly+sidecarExt)); !os.IsNotExist(err) {
		t.Fatal("orphan sidecar not removed")
	}
	if _, err := e2.Verify(id(1)); err != nil {
		t.Fatal(err)
	}
}

func TestListAndDelete(t *testing.T) {
	e := newEngine(t, Options{})
	ids := map[string]bool{}
	for i := 0; i < 50; i++ {
		e.Put(context.Background(), id(i), bytes.NewReader([]byte(fmt.Sprint(i))), -1, "")
		ids[id(i)] = true
	}
	var all []BlobInfo
	after := ""
	for {
		page, err := e.List(after, 7)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		after = page[len(page)-1].ID
	}
	if len(all) != 50 {
		t.Fatalf("listed %d", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].ID >= all[i].ID {
			t.Fatal("list not ordered")
		}
	}
	if err := e.Delete(id(3)); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete(id(3)); !errors.Is(err, ErrNotFound) {
		t.Fatal("double delete should be not found")
	}
	if e.Stats().BlobCount != 49 {
		t.Fatal("count after delete")
	}
	if _, err := e.Put(context.Background(), "../../etc/passwd", bytes.NewReader(nil), -1, ""); !errors.Is(err, ErrInvalidID) {
		t.Fatal("path traversal id accepted")
	}
}

func TestDeleteWhileReading(t *testing.T) {
	e := newEngine(t, Options{})
	data := randBytes(50_000, 12)
	e.Put(context.Background(), id(1), bytes.NewReader(data), -1, "")
	rc, _, err := e.Get(id(1), 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Delete(id(1)); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc) // POSIX: open handle survives unlink
	rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reader broken by concurrent delete: %v", err)
	}
}

func TestConcurrentWritersSameAndDifferentIDs(t *testing.T) {
	e := newEngine(t, Options{})
	data := randBytes(30_000, 13)
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Half write the same id (busy/idempotent), half distinct ids.
			target := id(1000)
			if i%2 == 0 {
				target = id(i)
			}
			if _, err := e.Put(context.Background(), target, bytes.NewReader(data), -1, ""); err != nil && !errors.Is(err, ErrBusy) {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if _, err := e.Verify(id(1000)); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPut1MiB(b *testing.B)         { benchPut(b, 1<<20, false) }
func BenchmarkPut1MiBFsync(b *testing.B)    { benchPut(b, 1<<20, true) }
func BenchmarkPut64KiB(b *testing.B)        { benchPut(b, 64<<10, false) }
func BenchmarkPut64KiBFsync(b *testing.B)   { benchPut(b, 64<<10, true) }
func BenchmarkGetVerified1MiB(b *testing.B) { benchGet(b, 1<<20) }

func benchPut(b *testing.B, size int, fsync bool) {
	e := newEngine(b, Options{Fsync: fsync, BlockSize: DefaultBlockSize})
	data := randBytes(size, 1)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Put(context.Background(), id(i), bytes.NewReader(data), int64(size), ""); err != nil {
			b.Fatal(err)
		}
	}
}

func benchGet(b *testing.B, size int) {
	e := newEngine(b, Options{BlockSize: DefaultBlockSize})
	data := randBytes(size, 1)
	e.Put(context.Background(), id(0), bytes.NewReader(data), -1, "")
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc, _, err := e.Get(id(0), 0, -1)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
}
