package replication

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/michael-emmanuel/objstore/internal/object"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/quorum"
	"github.com/michael-emmanuel/objstore/internal/storage"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
)

type node struct {
	id  string
	srv *storagenode.Server
	eng *storage.Engine
	ts  *httptest.Server
}

func nodes(t *testing.T, n int) []*node {
	var out []*node
	for i := 0; i < n; i++ {
		eng, err := storage.Open(storage.Options{Root: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		id := string(rune('a' + i))
		srv := storagenode.NewServer(storagenode.Config{Node: placement.Node{ID: id}}, eng, observability.DiscardLogger())
		ts := httptest.NewServer(srv.Handler())
		t.Cleanup(ts.Close)
		out = append(out, &node{id: id, srv: srv, eng: eng, ts: ts})
	}
	return out
}

func targets(ns []*node) []Target {
	var out []Target
	for _, n := range ns {
		out = append(out, Target{NodeID: n.id, Addr: n.ts.URL})
	}
	return out
}

func payload(n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(1)).Read(b)
	return b
}

func blob(i int) string { return object.SegmentBlobID("b", "k", "v", i) }

func TestAllReplicasAck(t *testing.T) {
	ns := nodes(t, 3)
	w := NewWriter(storagenode.NewClient(nil), Options{ChunkSize: 8 << 10})
	data := payload(300_000)
	res, err := w.Write(context.Background(), blob(1), targets(ns), 2, bytes.NewReader(data), "client")
	if err != nil || len(res.Acked) != 3 || res.Size != int64(len(data)) {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	for _, n := range ns {
		if info, err := n.eng.Verify(blob(1)); err != nil || info.SHA256 != res.SHA256 {
			t.Fatalf("node %s: %v", n.id, err)
		}
	}
}

// Replica 3 fails: with W=2 the write succeeds but is under-replicated;
// with W=3 it fails. This is the configurable durability policy.
func TestQuorumPolicyOnReplicaFailure(t *testing.T) {
	ns := nodes(t, 3)
	ns[2].srv.SetFaults(storagenode.Faults{FailWrites: true})
	w := NewWriter(storagenode.NewClient(nil), Options{})
	res, err := w.Write(context.Background(), blob(2), targets(ns), 2, bytes.NewReader(payload(50_000)), "client")
	if err != nil || len(res.Acked) != 2 || res.Failed["c"] == "" {
		t.Fatalf("W=2: res=%+v err=%v", res, err)
	}
	_, err = w.Write(context.Background(), blob(3), targets(ns), 3, bytes.NewReader(payload(50_000)), "client")
	if !errors.Is(err, quorum.ErrQuorumUnreachable) {
		t.Fatalf("W=3 should fail, got %v", err)
	}
}

func TestSlowReplicaDroppedWithoutBlockingWrite(t *testing.T) {
	ns := nodes(t, 3)
	ns[1].srv.SetFaults(storagenode.Faults{ChunkDelayMS: 500})
	t.Cleanup(func() { ns[1].srv.SetFaults(storagenode.Faults{}) }) // runs before the servers close
	w := NewWriter(storagenode.NewClient(nil), Options{ChunkSize: 64 << 10, QueueDepth: 2, StallTimeout: 200 * time.Millisecond, StragglerTimeout: 200 * time.Millisecond})
	start := time.Now()
	// Large enough to overflow kernel socket buffers, so the slow replica's
	// queue really fills and the stall detector (not the straggler timer) acts.
	res, err := w.Write(context.Background(), blob(4), targets(ns), 2, bytes.NewReader(payload(32<<20)), "client")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Acked) != 2 || res.Failed["b"] == "" {
		t.Fatalf("expected slow replica b excluded: %+v", res)
	}
	t.Logf("slow replica outcome: dropped=%v reason=%q elapsed=%v", res.Dropped, res.Failed["b"], time.Since(start))
	if el := time.Since(start); el > 10*time.Second {
		t.Fatalf("write took %v: slow replica set the pace", el)
	}
	if _, err := ns[1].eng.Stat(blob(4)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("excluded replica committed a blob")
	}
}

// With a small object the slow replica never fills its queue (socket buffers
// absorb it); the straggler timeout bounds latency instead.
func TestStragglerCancelledAfterQuorum(t *testing.T) {
	ns := nodes(t, 3)
	ns[2].srv.SetFaults(storagenode.Faults{WriteDelayMS: 3000})
	t.Cleanup(func() { ns[2].srv.SetFaults(storagenode.Faults{}) })
	w := NewWriter(storagenode.NewClient(nil), Options{StragglerTimeout: 100 * time.Millisecond})
	start := time.Now()
	res, err := w.Write(context.Background(), blob(5), targets(ns), 2, bytes.NewReader(payload(10_000)), "client")
	if err != nil || len(res.Acked) != 2 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("waited %v for a straggler", el)
	}
}

type brokenReader struct {
	r     io.Reader
	limit int
	n     int
}

func (b *brokenReader) Read(p []byte) (int, error) {
	if b.n >= b.limit {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) > b.limit-b.n {
		p = p[:b.limit-b.n]
	}
	n, err := b.r.Read(p)
	b.n += n
	if b.n >= b.limit {
		return n, errors.New("client disconnected")
	}
	return n, err
}

func TestClientDisconnectLeavesNothingAndNoLeaks(t *testing.T) {
	ns := nodes(t, 3)
	w := NewWriter(storagenode.NewClient(nil), Options{ChunkSize: 8 << 10})
	// Warm up connections so goroutine counts are comparable.
	w.Write(context.Background(), blob(9), targets(ns), 3, bytes.NewReader(payload(1000)), "client")
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()
	for i := 0; i < 10; i++ {
		_, err := w.Write(context.Background(), blob(10+i), targets(ns), 2, &brokenReader{r: bytes.NewReader(payload(200_000)), limit: 100_000}, "client")
		if !errors.Is(err, ErrSourceRead) {
			t.Fatalf("got %v", err)
		}
	}
	for _, n := range ns {
		for i := 0; i < 10; i++ {
			if _, err := n.eng.Stat(blob(10 + i)); !errors.Is(err, storage.ErrNotFound) {
				t.Fatal("partial upload became visible on a node")
			}
		}
	}
	time.Sleep(200 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+10 {
		t.Fatalf("goroutine leak: %d -> %d", before, after)
	}
}

func TestContextCancel(t *testing.T) {
	ns := nodes(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	w := NewWriter(storagenode.NewClient(nil), Options{})
	done := make(chan error, 1)
	go func() {
		_, err := w.Write(ctx, blob(30), targets(ns), 2, pr, "client")
		done <- err
	}()
	pw.Write(payload(1000))
	cancel()
	pw.CloseWithError(errors.New("gone"))
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write did not observe cancellation")
	}
}

func BenchmarkReplicatedWrite1MiB_RF3(b *testing.B) {
	var ns []*node
	for i := 0; i < 3; i++ {
		eng, _ := storage.Open(storage.Options{Root: b.TempDir()})
		srv := storagenode.NewServer(storagenode.Config{}, eng, observability.DiscardLogger())
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()
		ns = append(ns, &node{id: string(rune('a' + i)), eng: eng, ts: ts})
	}
	w := NewWriter(storagenode.NewClient(nil), Options{})
	data := payload(1 << 20)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Write(context.Background(), object.SegmentBlobID("b", "k", "bench", i), targets(ns), 2, bytes.NewReader(data), "client"); err != nil {
			b.Fatal(err)
		}
	}
}
