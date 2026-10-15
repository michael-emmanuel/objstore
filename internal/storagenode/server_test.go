package storagenode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math/rand"
	"net/http/httptest"
	"testing"

	"github.com/michael-emmanuel/objstore/internal/object"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/storage"
)

type testNode struct {
	srv *Server
	ts  *httptest.Server
	eng *storage.Engine
}

func startNode(t *testing.T) *testNode {
	t.Helper()
	eng, err := storage.Open(storage.Options{Root: t.TempDir(), BlockSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{Node: placement.Node{ID: "n"}}, eng, observability.DiscardLogger())
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return &testNode{srv: s, ts: ts, eng: eng}
}

func data(n int) ([]byte, string) {
	b := make([]byte, n)
	rand.New(rand.NewSource(int64(n))).Read(b)
	s := sha256.Sum256(b)
	return b, hex.EncodeToString(s[:])
}

var bctx = context.Background()

func TestPutGetHeadDeleteOverHTTP(t *testing.T) {
	n := startNode(t)
	c := NewClient(nil)
	id := object.SegmentBlobID("b", "k", "v", 0)
	b, sum := data(100_000)
	info, err := c.Put(bctx, n.ts.URL, id, bytes.NewReader(b), -1, "", "")
	if err != nil || info.SHA256 != sum {
		t.Fatal(err, info)
	}
	rc, err := c.Get(bctx, n.ts.URL, id, 1000, 5000, "")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, b[1000:6000]) {
		t.Fatal("range mismatch")
	}
	h, err := c.Head(bctx, n.ts.URL, id)
	if err != nil || h.Size != 100_000 || h.SHA256 != sum {
		t.Fatal(err, h)
	}
	if err := c.Delete(bctx, n.ts.URL, id); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(bctx, n.ts.URL, id); err != nil {
		t.Fatal("delete must be idempotent:", err)
	}
	if _, err := c.Head(bctx, n.ts.URL, id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("blob still present")
	}
	if _, err := c.Put(bctx, n.ts.URL, id, bytes.NewReader(b), -1, sum[:63]+"0", ""); !errors.Is(err, storage.ErrChecksumMismatch) {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}
}

func TestPullVerifiesChecksum(t *testing.T) {
	a, b := startNode(t), startNode(t)
	c := NewClient(nil)
	id := object.SegmentBlobID("b", "k", "v", 1)
	d, sum := data(50_000)
	c.Put(bctx, a.ts.URL, id, bytes.NewReader(d), -1, "", "")
	if _, err := c.Pull(bctx, b.ts.URL, id, a.ts.URL, sum, "repair"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.eng.Verify(id); err != nil {
		t.Fatal(err)
	}
	// A pull from a corrupt source must fail, not create a bad copy.
	id2 := object.SegmentBlobID("b", "k", "v", 2)
	c.Put(bctx, a.ts.URL, id2, bytes.NewReader(d), -1, "", "")
	c.Corrupt(bctx, a.ts.URL, id2, 30_000)
	if _, err := c.Pull(bctx, b.ts.URL, id2, a.ts.URL, sum, "repair"); err == nil {
		t.Fatal("pull from corrupt source succeeded")
	}
	if _, err := b.eng.Stat(id2); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("corrupt copy created")
	}
	if _, ok := a.eng.Quarantined()[id2]; !ok {
		t.Fatal("source did not quarantine the corrupt blob")
	}
}

func TestCorruptionMidStreamAbortsConnection(t *testing.T) {
	n := startNode(t)
	c := NewClient(nil)
	id := object.SegmentBlobID("b", "k", "v", 3)
	d, _ := data(500_000)
	c.Put(bctx, n.ts.URL, id, bytes.NewReader(d), -1, "", "")
	c.Corrupt(bctx, n.ts.URL, id, 400_000)
	rc, err := c.Get(bctx, n.ts.URL, id, 0, -1, "")
	if err != nil {
		t.Fatal("headers should be sent before the corrupt block is reached:", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err == nil {
		t.Fatalf("truncated stream ended cleanly after %d bytes", len(got))
	}
	if len(got) >= 400_000 || !bytes.Equal(got, d[:len(got)]) {
		t.Fatal("corrupt or unverified bytes were served")
	}
	// Corruption at the start of a range is a clean error response.
	c.Put(bctx, n.ts.URL, object.SegmentBlobID("b", "k", "v", 4), bytes.NewReader(d), -1, "", "")
	c.Corrupt(bctx, n.ts.URL, object.SegmentBlobID("b", "k", "v", 4), 10)
	if _, err := c.Get(bctx, n.ts.URL, object.SegmentBlobID("b", "k", "v", 4), 0, -1, ""); err == nil {
		t.Fatal("expected error")
	}
}

func TestFaults(t *testing.T) {
	n := startNode(t)
	c := NewClient(nil)
	id := object.SegmentBlobID("b", "k", "v", 5)
	d, _ := data(1000)
	c.SetFaults(bctx, n.ts.URL, Faults{FailWrites: true})
	if _, err := c.Put(bctx, n.ts.URL, id, bytes.NewReader(d), -1, "", ""); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("got %v", err)
	}
	capacity := int64(500)
	c.SetFaults(bctx, n.ts.URL, Faults{Capacity: &capacity})
	if _, err := c.Put(bctx, n.ts.URL, id, bytes.NewReader(d), -1, "", ""); !errors.Is(err, storage.ErrDiskFull) {
		t.Fatalf("got %v", err)
	}
	c.SetFaults(bctx, n.ts.URL, Faults{Partitioned: true})
	if _, err := c.Head(bctx, n.ts.URL, id); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("partitioned node answered: %v", err)
	}
}
