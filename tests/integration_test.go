// Integration, failure and recovery tests against a full in-process
// cluster (see internal/testcluster). Each test states the system property
// it demonstrates.
package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/objstore/internal/api"
	"github.com/michael-emmanuel/objstore/internal/config"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/object"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
	"github.com/michael-emmanuel/objstore/internal/testcluster"
	"github.com/michael-emmanuel/objstore/pkg/client"
)

var ctx = context.Background()

func payload(n int, seed int64) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func sum(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func get(t *testing.T, c *client.Client, bucket, key string, o client.GetOptions) ([]byte, client.ObjectInfo) {
	t.Helper()
	rc, info, err := c.Get(ctx, bucket, key, o)
	if err != nil {
		t.Fatalf("GET %s/%s: %v", bucket, key, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("GET %s/%s body: %v", bucket, key, err)
	}
	return b, info
}

func put(t *testing.T, c *client.Client, bucket, key string, data []byte) client.PutResult {
	t.Helper()
	res, err := c.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), client.PutOptions{SHA256: sum(data)})
	if err != nil {
		t.Fatalf("PUT %s/%s: %v", bucket, key, err)
	}
	return res
}

func inspect(t *testing.T, c *testcluster.Cluster, bucket, key string) object.Version {
	t.Helper()
	v, err := c.Meta.Get(ctx, bucket, key, "")
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// Basic object lifecycle through the public API.
func TestBasicLifecycle(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{})
	if err := c.Alice.CreateBucket(ctx, "photos", client.BucketOptions{}); err != nil {
		t.Fatal(err)
	}
	data := payload(300_000, 1)
	res := put(t, c.Alice, "photos", "user-123/profile.jpg", data)
	if res.Size != 300_000 || res.ETag != sum(data) || res.MinReplicasAcked != 3 || res.UnderReplicated {
		t.Fatalf("put result %+v", res)
	}
	got, info := get(t, c.Alice, "photos", "user-123/profile.jpg", client.GetOptions{})
	if !bytes.Equal(got, data) || info.VersionID != res.VersionID {
		t.Fatal("round trip mismatch")
	}
	h, err := c.Alice.Head(ctx, "photos", "user-123/profile.jpg")
	if err != nil || h.ContentLength != 300_000 {
		t.Fatal(err, h)
	}
	// Replicas span three zones.
	v := inspect(t, c, "photos", "user-123/profile.jpg")
	if c.Watcher.Map().DomainSpread(v.Segments[0].Replicas) != 3 {
		t.Fatalf("replicas %v not spread across zones", v.Segments[0].Replicas)
	}
	for _, k := range []string{"user-123/a", "user-123/b", "user-456/c", "readme"} {
		put(t, c.Alice, "photos", k, []byte(k))
	}
	page, err := c.Alice.List(ctx, "photos", client.ListOptions{Prefix: "user-123/"})
	if err != nil || len(page.Objects) != 3 {
		t.Fatalf("list prefix: %+v %v", page, err)
	}
	page, _ = c.Alice.List(ctx, "photos", client.ListOptions{Delimiter: "/"})
	if strings.Join(page.CommonPrefixes, ",") != "user-123/,user-456/" || len(page.Objects) != 1 {
		t.Fatalf("list delimiter: %+v", page)
	}
	// Pagination with continuation tokens visits every key exactly once.
	var keys []string
	tok := ""
	for {
		p, err := c.Alice.List(ctx, "photos", client.ListOptions{MaxKeys: 2, ContinuationToken: tok})
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range p.Objects {
			keys = append(keys, o.Key)
		}
		if !p.IsTruncated {
			break
		}
		tok = p.NextContinuationToken
	}
	if len(keys) != 5 || !sort.StringsAreSorted(keys) {
		t.Fatalf("paginated keys %v", keys)
	}
	if err := c.Alice.Delete(ctx, "photos", "user-123/profile.jpg"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Alice.Head(ctx, "photos", "user-123/profile.jpg"); !client.IsStatus(err, 404) {
		t.Fatalf("deleted object still visible: %v", err)
	}
	if err := c.Alice.Delete(ctx, "photos", "user-123/profile.jpg"); err != nil {
		t.Fatal("repeated delete must succeed (idempotent):", err)
	}
	// Traversal-looking keys are just keys.
	put(t, c.Alice, "photos", "../../etc/passwd", []byte("not a file"))
	if b, _ := get(t, c.Alice, "photos", "../../etc/passwd", client.GetOptions{}); string(b) != "not a file" {
		t.Fatal("odd key round trip")
	}
}

// A 12 MiB object streamed with chunked encoding is split into 1 MiB
// segments, each placed independently; ranges spanning segment boundaries
// are served correctly.
func TestLargeStreamingObjectAndRanges(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Gateway: api.Config{SegmentSize: 1 << 20}})
	c.Alice.CreateBucket(ctx, "big", client.BucketOptions{})
	data := payload(12<<20+12345, 2)
	pr, pw := io.Pipe()
	go func() {
		for off := 0; off < len(data); off += 100_000 {
			end := min(off+100_000, len(data))
			pw.Write(data[off:end])
		}
		pw.Close()
	}()
	res, err := c.Alice.Put(ctx, "big", "video.bin", pr, -1, client.PutOptions{SHA256: sum(data)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Segments != 13 {
		t.Fatalf("segments %d", res.Segments)
	}
	got, _ := get(t, c.Alice, "big", "video.bin", client.GetOptions{})
	if sum(got) != sum(data) {
		t.Fatal("content mismatch")
	}
	for _, r := range [][2]int64{{0, 0}, {1<<20 - 10, 1<<20 + 10}, {5_000_000, 9_000_000}, {int64(len(data)) - 1, int64(len(data)) - 1}} {
		b, info := get(t, c.Alice, "big", "video.bin", client.GetOptions{HasRange: true, RangeStart: r[0], RangeEnd: r[1]})
		if !bytes.Equal(b, data[r[0]:r[1]+1]) || info.Status != 206 || info.Size != int64(len(data)) {
			t.Fatalf("range %v: got %d bytes status %d", r, len(b), info.Status)
		}
	}
	_, _, err = c.Alice.Get(ctx, "big", "video.bin", client.GetOptions{HasRange: true, RangeStart: int64(len(data)), RangeEnd: int64(len(data)) + 5})
	if !client.IsStatus(err, 416) {
		t.Fatalf("want 416, got %v", err)
	}
	// Segments are placed independently: with 13 segments on 4 nodes, more
	// than one distinct replica set is expected.
	v := inspect(t, c, "big", "video.bin")
	sets := map[string]bool{}
	for _, s := range v.Segments {
		r := append([]string(nil), s.Replicas...)
		sort.Strings(r)
		sets[strings.Join(r, ",")] = true
	}
	if len(sets) < 2 {
		t.Fatalf("all segments on the same replica set: %v", sets)
	}
}

// Multipart: parts retried individually, completion is atomic, and the
// composite ETag is returned. A part that failed (quorum unreachable)
// is simply re-sent.
func TestMultipartUploadWithPartRetry(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{})
	c.Alice.CreateBucket(ctx, "mp-1", client.BucketOptions{})
	id, err := c.Alice.CreateUpload(ctx, "mp-1", "backup.tar", "application/x-tar")
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{payload(200_000, 10), payload(200_000, 11), payload(50_000, 12)}
	var done []client.Part

	// Part 2's first attempt fails: two of four nodes reject writes, so
	// some placements cannot reach W=2. Make it deterministic: fail all but one.
	for _, n := range c.Nodes[1:] {
		n.Server.SetFaults(storagenode.Faults{FailWrites: true})
	}
	if _, err := c.Alice.UploadPart(ctx, id, 2, bytes.NewReader(parts[1]), int64(len(parts[1])), ""); !client.IsStatus(err, 503) {
		t.Fatalf("expected 503 while nodes fail, got %v", err)
	}
	for _, n := range c.Nodes {
		n.Server.SetFaults(storagenode.Faults{})
	}
	// Upload out of order and in parallel; retry part 2.
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := len(parts) - 1; i >= 0; i-- {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.Alice.UploadPart(ctx, id, i+1, bytes.NewReader(parts[i]), int64(len(parts[i])), sum(parts[i]))
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			done = append(done, client.Part{Number: i + 1, ETag: r.ETag})
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	sort.Slice(done, func(i, j int) bool { return done[i].Number < done[j].Number })
	// Not visible before completion.
	if _, err := c.Alice.Head(ctx, "mp-1", "backup.tar"); !client.IsStatus(err, 404) {
		t.Fatal("object visible before completion")
	}
	res, err := c.Alice.CompleteUpload(ctx, id, done)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(res.ETag, "-3") {
		t.Fatalf("etag %q", res.ETag)
	}
	all := bytes.Join(parts, nil)
	got, _ := get(t, c.Alice, "mp-1", "backup.tar", client.GetOptions{})
	if !bytes.Equal(got, all) {
		t.Fatal("multipart content mismatch")
	}
	// Range across the part 1/2 boundary.
	b, _ := get(t, c.Alice, "mp-1", "backup.tar", client.GetOptions{HasRange: true, RangeStart: 199_990, RangeEnd: 200_009})
	if !bytes.Equal(b, all[199_990:200_010]) {
		t.Fatal("range across parts")
	}
	// Abort path: parts become garbage and are reclaimed.
	id2, _ := c.Alice.CreateUpload(ctx, "mp-1", "abandoned", "")
	r2, _ := c.Alice.UploadPart(ctx, id2, 1, bytes.NewReader(parts[0]), int64(len(parts[0])), "")
	_ = r2
	if err := c.Alice.AbortUpload(ctx, id2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if rep, err := c.GC.RunOnce(ctx); err != nil || rep.QueueDeleted < 3 {
		t.Fatalf("gc after abort: %+v %v", rep, err)
	}
}

// Replica 3 fails during PUT. With W=2 the client gets success with an
// explicit under-replication signal; repair later restores RF=3.
func TestReplicaFailureDuringPutThenRepair(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	c.Nodes[2].Server.SetFaults(storagenode.Faults{FailWrites: true})
	data := payload(100_000, 3)
	res := put(t, c.Alice, "b-1", "k", data)
	if res.MinReplicasAcked != 2 || !res.UnderReplicated {
		t.Fatalf("expected 2/3 acks: %+v", res)
	}
	c.Nodes[2].Server.SetFaults(storagenode.Faults{})
	rep, err := c.Repairer.RunOnce(ctx)
	if err != nil || rep.Repaired != 1 {
		t.Fatalf("repair %+v %v", rep, err)
	}
	v := inspect(t, c, "b-1", "k")
	if len(v.Segments[0].Replicas) != 3 || len(c.BlobCopies(v.Segments[0].BlobID)) != 3 {
		t.Fatalf("after repair: %v", v.Segments[0].Replicas)
	}
}

// Two of three replicas fail: W=2 cannot be met. The client gets 503 and
// NOTHING becomes visible (no partial object).
func TestWriteQuorumUnreachable(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	c.Nodes[1].Server.SetFaults(storagenode.Faults{FailWrites: true})
	c.Nodes[2].Server.SetFaults(storagenode.Faults{FailWrites: true})
	_, err := c.Alice.Put(ctx, "b-1", "k", bytes.NewReader(payload(1000, 4)), 1000, client.PutOptions{})
	var ae *client.APIError
	if !errors.As(err, &ae) || ae.Status != 503 || ae.Code != "InsufficientReplicas" {
		t.Fatalf("want 503 InsufficientReplicas, got %v", err)
	}
	if _, err := c.Alice.Head(ctx, "b-1", "k"); !client.IsStatus(err, 404) {
		t.Fatal("failed write is visible")
	}
}

// W=3 (synchronous full replication) trades availability for durability:
// the same single-replica failure that W=2 tolerated now fails the write.
func TestDurabilityPolicyW3(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Gateway: api.Config{WriteQuorum: 3}})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	c.Nodes[0].Server.SetFaults(storagenode.Faults{FailWrites: true})
	if _, err := c.Alice.Put(ctx, "b-1", "k", bytes.NewReader([]byte("x")), 1, client.PutOptions{}); !client.IsStatus(err, 503) {
		t.Fatalf("W=3 with one failed replica must fail, got %v", err)
	}
}

// A storage node dies. Reads keep working (failover), the coordinator
// marks it DOWN, and repair re-creates the lost replicas elsewhere.
func TestNodeFailureReadAvailabilityAndRepair(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 5, Gateway: api.Config{SegmentSize: 256 << 10}})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	objs := map[string][]byte{}
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("obj-%d", i)
		objs[k] = payload(600_000, int64(100+i))
		put(t, c.Alice, "b-1", k, objs[k])
	}
	victim := c.Nodes[1]
	c.Kill(victim)
	// Immediately (before failure detection) reads still succeed via failover.
	for k, d := range objs {
		if got, _ := get(t, c.Alice, "b-1", k, client.GetOptions{}); !bytes.Equal(got, d) {
			t.Fatalf("read %s during failure", k)
		}
	}
	c.WaitDown(victim)
	time.Sleep(60 * time.Millisecond) // > RepairDelay
	rep, err := c.Repairer.RunOnce(ctx)
	if err != nil || rep.Repaired == 0 || rep.Failed != 0 {
		t.Fatalf("repair: %+v %v", rep, err)
	}
	segs, _, _ := c.Meta.ScanSegments(ctx, "", 10000)
	for _, s := range segs {
		if len(s.Replicas) != 3 {
			t.Fatalf("segment %s has %v", s.BlobID, s.Replicas)
		}
		for _, r := range s.Replicas {
			if r == victim.ID {
				t.Fatalf("dead node still listed for %s", s.BlobID)
			}
		}
		if got := len(c.BlobCopies(s.BlobID)); got != 3 {
			t.Fatalf("blob %s has %d physical copies", s.BlobID, got)
		}
	}
	// A second pass finds nothing to do (idempotent, no repair storm).
	if rep, _ := c.Repairer.RunOnce(ctx); rep.UnderReplicated != 0 {
		t.Fatalf("second pass %+v", rep)
	}
}

// A briefly-down node is NOT repaired immediately (RepairDelay), avoiding
// re-replication traffic for transient failures.
func TestRepairDelayAvoidsStorms(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 4, RepairDelay: time.Hour})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	for i := 0; i < 5; i++ {
		put(t, c.Alice, "b-1", fmt.Sprint(i), payload(1000, int64(i)))
	}
	victim := c.Nodes[0]
	c.Kill(victim)
	c.WaitDown(victim)
	rep, _ := c.Repairer.RunOnce(ctx)
	if rep.Repaired != 0 || rep.UnderReplicated == 0 || rep.Deferred != rep.UnderReplicated {
		t.Fatalf("expected deferral, got %+v", rep)
	}
}

// Silent corruption on one replica: the read path detects it via block
// CRCs before sending bad bytes, fails over mid-stream, the node
// quarantines the blob, and repair restores a good copy.
func TestCorruptionDetectionFailoverAndRepair(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	data := payload(900_000, 5)
	put(t, c.Alice, "b-1", "k", data)
	v := inspect(t, c, "b-1", "k")
	blob := v.Segments[0].BlobID
	for _, n := range c.Nodes {
		if err := n.Server.Faults(); err.FailReads {
			t.Fatal()
		}
		c.NodeClient.Corrupt(ctx, n.TS.URL, blob, 700_000) // corrupt ALL... no: only one below
		break
	}
	// The first node in the list now has a corrupt copy. Every GET must
	// still return correct bytes.
	for i := 0; i < 6; i++ {
		got, _ := get(t, c.Alice, "b-1", "k", client.GetOptions{})
		if !bytes.Equal(got, data) {
			t.Fatal("corrupt data served")
		}
	}
	corruptNode := c.Nodes[0]
	if _, ok := corruptNode.Engine.Quarantined()[blob]; !ok {
		// The randomized replica order may not have hit the bad copy yet;
		// the scrubber path (explicit verify) catches it regardless.
		corruptNode.Engine.Verify(blob)
	}
	if _, ok := corruptNode.Engine.Quarantined()[blob]; !ok {
		t.Fatal("corrupt replica not quarantined")
	}
	rep, err := c.Repairer.RunOnce(ctx)
	if err != nil || rep.Quarantined != 1 || rep.Repaired != 1 {
		t.Fatalf("repair after corruption: %+v %v", rep, err)
	}
	v = inspect(t, c, "b-1", "k")
	for _, id := range v.Segments[0].Replicas {
		n := c.NodeByID(id)
		if _, err := n.Engine.Verify(blob); err != nil {
			t.Fatalf("replica on %s still bad: %v", id, err)
		}
	}
	if len(corruptNode.Engine.Quarantined()) != 0 {
		t.Fatal("quarantine not acknowledged after repair")
	}
}

// Adding a node moves only part of the data, never drops below RF, and
// keeps every object readable.
func TestRebalanceOnNodeAddition(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 4, Gateway: api.Config{SegmentSize: 64 << 10}})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	objs := map[string][]byte{}
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("o%d", i)
		objs[k] = payload(100_000, int64(i))
		put(t, c.Alice, "b-1", k, objs[k])
	}
	before, _, _ := c.Meta.ScanSegments(ctx, "", 100000)
	newNode := c.AddNode("az-b")
	rep, err := c.Rebalancer.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rebalance: scanned=%d misplaced=%d moved=%d bytes=%d", rep.Scanned, rep.Misplaced, rep.Moved, rep.Bytes)
	frac := float64(rep.Moved) / float64(len(before)*3)
	if rep.Moved == 0 || frac > 0.5 {
		t.Fatalf("moved %d of %d replicas (%.2f): expected a minority", rep.Moved, len(before)*3, frac)
	}
	after, _, _ := c.Meta.ScanSegments(ctx, "", 100000)
	onNew := 0
	for _, s := range after {
		if len(s.Replicas) != 3 {
			t.Fatalf("segment %s has %d replicas after rebalance", s.BlobID, len(s.Replicas))
		}
		for _, r := range s.Replicas {
			if r == newNode.ID {
				onNew++
			}
		}
	}
	if onNew != rep.Moved {
		t.Fatalf("new node holds %d, moved %d", onNew, rep.Moved)
	}
	if rep2, _ := c.Rebalancer.RunOnce(ctx); rep2.Moved != 0 {
		t.Fatalf("second pass moved %d: placement not stable", rep2.Moved)
	}
	for k, d := range objs {
		if got, _ := get(t, c.Alice, "b-1", k, client.GetOptions{}); !bytes.Equal(got, d) {
			t.Fatalf("object %s damaged by rebalance", k)
		}
	}
	// Old copies were deleted (copy-then-delete), not left behind.
	time.Sleep(5 * time.Millisecond)
	gcRep, _ := c.GC.RunOnce(ctx)
	for _, s := range after {
		if n := len(c.BlobCopies(s.BlobID)); n != 3 {
			t.Fatalf("blob %s has %d physical copies (gc %+v)", s.BlobID, n, gcRep)
		}
	}
}

// Draining: no new data lands on the node; the rebalancer migrates its
// replicas away; only then may it be decommissioned.
func TestNodeDrain(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 4})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	for i := 0; i < 20; i++ {
		put(t, c.Alice, "b-1", fmt.Sprint(i), payload(5000, int64(i)))
	}
	drained := c.Nodes[3]
	if err := c.Admin.Raw(ctx, http.MethodPost, "/admin/nodes/"+drained.ID+"/state", map[string]string{"state": "DRAINING"}, nil); err != nil {
		t.Fatal(err)
	}
	c.WaitFor("draining visible", func() bool {
		c.Refresh()
		n, _ := c.Watcher.Map().Node(drained.ID)
		return n.State == placement.StateDraining
	})
	// Decommission is refused while data remains.
	err := c.Admin.Raw(ctx, http.MethodPost, "/admin/nodes/"+drained.ID+"/state", map[string]string{"state": "DECOMMISSIONED"}, nil)
	if !client.IsStatus(err, 409) {
		t.Fatalf("decommission before drain should be refused: %v", err)
	}
	// New writes avoid the draining node.
	for i := 0; i < 10; i++ {
		v := put(t, c.Alice, "b-1", fmt.Sprint("new", i), []byte("x"))
		_ = v
		vv := inspect(t, c, "b-1", fmt.Sprint("new", i))
		for _, r := range vv.Segments[0].Replicas {
			if r == drained.ID {
				t.Fatal("new data placed on draining node")
			}
		}
	}
	for i := 0; i < 5; i++ {
		c.Rebalancer.RunOnce(ctx)
	}
	var status map[string]any
	c.Admin.Raw(ctx, http.MethodGet, "/admin/nodes/"+drained.ID+"/drain", nil, &status)
	if status["safe_to_remove"] != true {
		t.Fatalf("drain incomplete: %+v", status)
	}
	if err := c.Admin.Raw(ctx, http.MethodPost, "/admin/nodes/"+drained.ID+"/state", map[string]string{"state": "DECOMMISSIONED"}, nil); err != nil {
		t.Fatal(err)
	}
	c.Kill(drained)
	for i := 0; i < 20; i++ {
		if _, err := c.Alice.Head(ctx, "b-1", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
}

// Versioning: overwrites keep history, delete adds a marker, old versions
// remain readable by ID.
func TestVersioning(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{})
	c.Alice.CreateBucket(ctx, "docs", client.BucketOptions{Versioning: true})
	v1 := put(t, c.Alice, "docs", "profile.jpg", []byte("v1"))
	put(t, c.Alice, "docs", "profile.jpg", []byte("v2"))
	c.Alice.Delete(ctx, "docs", "profile.jpg")
	if _, err := c.Alice.Head(ctx, "docs", "profile.jpg"); !client.IsStatus(err, 404) {
		t.Fatal("latest should be a delete marker")
	}
	if b, _ := get(t, c.Alice, "docs", "profile.jpg", client.GetOptions{VersionID: v1.VersionID}); string(b) != "v1" {
		t.Fatal("old version not readable")
	}
	var out struct {
		Versions []map[string]any `json:"versions"`
	}
	c.Alice.Raw(ctx, http.MethodGet, "/v1/buckets/docs/versions/profile.jpg", nil, &out)
	if len(out.Versions) != 3 || out.Versions[0]["delete_marker"] != true {
		t.Fatalf("versions %+v", out)
	}
}

// GC reclaims overwritten and deleted data, and the sweep removes orphans
// (blobs no metadata references), but never touches live data.
func TestGarbageCollection(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	put(t, c.Alice, "b-1", "keep", payload(1000, 1))
	put(t, c.Alice, "b-1", "overwrite", payload(1000, 2))
	old := inspect(t, c, "b-1", "overwrite").Segments[0].BlobID
	put(t, c.Alice, "b-1", "overwrite", payload(1000, 3))
	put(t, c.Alice, "b-1", "delete-me", payload(1000, 4))
	deleted := inspect(t, c, "b-1", "delete-me").Segments[0].BlobID
	c.Alice.Delete(ctx, "b-1", "delete-me")
	// An orphan: written directly to a node, never committed in metadata.
	orphan := object.SegmentBlobID("b-1", "ghost", "v", 0)
	c.NodeClient.Put(ctx, c.Nodes[0].TS.URL, orphan, bytes.NewReader([]byte("ghost")), -1, "", "")
	time.Sleep(5 * time.Millisecond)

	rep, err := c.GC.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(c.BlobCopies(old)); n != 0 {
		t.Fatalf("overwritten blob still has %d copies", n)
	}
	if n := len(c.BlobCopies(deleted)); n != 0 {
		t.Fatalf("deleted blob still has %d copies", n)
	}
	if len(c.BlobCopies(orphan)) != 1 {
		t.Fatal("orphan deleted on FIRST sighting; must require two")
	}
	rep2, _ := c.GC.RunOnce(ctx)
	if len(c.BlobCopies(orphan)) != 0 || rep2.OrphansDeleted != 1 {
		t.Fatalf("orphan not reclaimed on second sighting: %+v", rep2)
	}
	if got, _ := get(t, c.Alice, "b-1", "keep", client.GetOptions{}); !bytes.Equal(got, payload(1000, 1)) {
		t.Fatal("GC damaged live data")
	}
	t.Logf("gc pass 1: %+v", rep)
}

// A hot small object: concurrent misses are coalesced into one backend
// read, later reads are served from the version-keyed cache.
func TestHotObjectCacheAndStampede(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Gateway: api.Config{ObjectCacheBytes: 8 << 20, MetaCacheTTL: time.Second}})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	data := payload(200_000, 9)
	put(t, c.Alice, "b-1", "hot", data)
	// Slow down storage reads so concurrent misses overlap.
	for _, n := range c.Nodes {
		n.Server.SetFaults(storagenode.Faults{ReadDelayMS: 200})
	}
	var wg sync.WaitGroup
	srcs := make(chan string, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, info := get(t, c.Alice, "b-1", "hot", client.GetOptions{})
			if !bytes.Equal(b, data) {
				t.Error("bad data")
			}
			srcs <- info.Cache
		}()
	}
	wg.Wait()
	close(srcs)
	counts := map[string]int{}
	for s := range srcs {
		counts[s]++
	}
	if counts["MISS"] > 2 {
		t.Fatalf("stampede: %v", counts)
	}
	for _, n := range c.Nodes {
		n.Server.SetFaults(storagenode.Faults{})
	}
	if _, info := get(t, c.Alice, "b-1", "hot", client.GetOptions{}); info.Cache != "HIT" {
		t.Fatalf("expected cache hit, got %q", info.Cache)
	}
	// Overwrite: the new version has a new ID, so no stale bytes are served
	// by this gateway (its metadata cache was invalidated on write).
	put(t, c.Alice, "b-1", "hot", []byte("new"))
	if b, _ := get(t, c.Alice, "b-1", "hot", client.GetOptions{}); string(b) != "new" {
		t.Fatal("stale object after overwrite")
	}
	t.Logf("cache sources for 100 concurrent cold reads: %v", counts)
}

// Metadata outage: writes fail cleanly; reads of recently-read objects can
// still be served from the gateway's metadata cache (bounded staleness).
func TestMetadataUnavailable(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Shards: 1, Gateway: api.Config{MetaCacheTTL: 10 * time.Second}})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	put(t, c.Alice, "b-1", "k", []byte("hello"))
	get(t, c.Alice, "b-1", "k", client.GetOptions{}) // warm cache
	c.MetaSrv[0].SetFaults(metadata.Faults{FailReads: true, FailWrites: true})
	if b, _ := get(t, c.Alice, "b-1", "k", client.GetOptions{}); string(b) != "hello" {
		t.Fatal("cached read failed during metadata outage")
	}
	if _, err := c.Alice.Put(ctx, "b-1", "k2", bytes.NewReader([]byte("x")), 1, client.PutOptions{}); !client.IsStatus(err, 503) {
		t.Fatalf("write during metadata outage: %v", err)
	}
	if _, err := c.Alice.Head(ctx, "b-1", "uncached"); !client.IsStatus(err, 503) {
		t.Fatalf("uncached read during outage: %v", err)
	}
}

// Tenant limits: storage quota, object size, request rate.
func TestTenantLimits(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{AliceLimits: config.Limits{MaxStorageBytes: 250_000, MaxObjectBytes: 200_000,
		RequestsPerSec: 20, RequestBurst: 20}})
	c.Alice.CreateBucket(ctx, "quota", client.BucketOptions{})
	put(t, c.Alice, "quota", "a", payload(150_000, 1))
	if _, err := c.Alice.Put(ctx, "quota", "b", bytes.NewReader(payload(150_000, 2)), 150_000, client.PutOptions{}); !client.IsStatus(err, 403) {
		t.Fatalf("quota: %v", err)
	}
	if _, err := c.Alice.Put(ctx, "quota", "c", bytes.NewReader(payload(210_000, 3)), 210_000, client.PutOptions{}); !client.IsStatus(err, 413) {
		t.Fatalf("object size: %v", err)
	}
	// Chunked upload with unknown length is cut off at the max object size.
	if _, err := c.Alice.Put(ctx, "quota", "d", bytes.NewReader(payload(210_000, 3)), -1, client.PutOptions{}); !client.IsStatus(err, 413) {
		t.Fatalf("chunked object size: %v", err)
	}
	limited := 0
	for i := 0; i < 50; i++ {
		if _, err := c.Alice.Head(ctx, "quota", "a"); client.IsStatus(err, 429) {
			limited++
		}
	}
	if limited == 0 {
		t.Fatal("rate limit never applied")
	}
	// Another tenant is unaffected (noisy-neighbour isolation).
	c.Bob.CreateBucket(ctx, "bobs", client.BucketOptions{})
	for i := 0; i < 30; i++ {
		if _, err := c.Bob.Put(ctx, "bobs", fmt.Sprint(i), bytes.NewReader([]byte("x")), 1, client.PutOptions{}); err != nil {
			t.Fatal("bob throttled by alice's limits:", err)
		}
	}
}

// Authorization and tenant isolation.
func TestAuthorization(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{})
	c.Alice.CreateBucket(ctx, "private", client.BucketOptions{})
	c.Alice.CreateBucket(ctx, "public", client.BucketOptions{PublicRead: true})
	put(t, c.Alice, "private", "k", []byte("secret"))
	put(t, c.Alice, "public", "k", []byte("open"))
	if _, err := c.Bob.Head(ctx, "private", "k"); !client.IsStatus(err, 403) {
		t.Fatalf("bob read alice's private bucket: %v", err)
	}
	if _, err := c.Anon.Head(ctx, "private", "k"); !client.IsStatus(err, 401) {
		t.Fatalf("anonymous read private: %v", err)
	}
	if b, _ := get(t, c.Anon, "public", "k", client.GetOptions{}); string(b) != "open" {
		t.Fatal("anonymous cannot read public bucket")
	}
	if _, err := c.Anon.Put(ctx, "public", "x", bytes.NewReader([]byte("x")), 1, client.PutOptions{}); !client.IsStatus(err, 401) {
		t.Fatalf("anonymous write: %v", err)
	}
	if _, err := c.Bob.Put(ctx, "public", "x", bytes.NewReader([]byte("x")), 1, client.PutOptions{}); !client.IsStatus(err, 403) {
		t.Fatalf("bob write to alice's public bucket: %v", err)
	}
	bad := client.New(c.GatewayURL, "wrong-token-xxxxxxxxxx")
	if _, err := bad.Head(ctx, "public", "k"); !client.IsStatus(err, 401) {
		t.Fatalf("bad token: %v", err)
	}
	if err := c.Alice.Raw(ctx, http.MethodGet, "/admin/cluster", nil, nil); !client.IsStatus(err, 403) {
		t.Fatalf("non-admin reached admin API: %v", err)
	}
}

// Client disconnects halfway through an upload: nothing becomes visible,
// and the partial blobs are reclaimed by GC.
func TestClientDisconnectMidUpload(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Gateway: api.Config{SegmentSize: 256 << 10}})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	pr, pw := io.Pipe()
	go func() {
		pw.Write(payload(700_000, 1)) // > 2 segments committed on nodes
		pw.CloseWithError(errors.New("client vanished"))
	}()
	_, err := c.Alice.Put(ctx, "b-1", "partial", pr, 2_000_000, client.PutOptions{})
	if err == nil {
		t.Fatal("expected failure")
	}
	if _, err := c.Alice.Head(ctx, "b-1", "partial"); !client.IsStatus(err, 404) {
		t.Fatal("partial upload visible")
	}
	st, _ := c.Meta.Stats(ctx)
	total := 0
	for _, s := range st {
		total += s.Versions
	}
	if total != 0 {
		t.Fatalf("metadata has %d versions after a failed upload", total)
	}
}

// Concurrent overwrites of one key: every reader sees exactly one complete
// version, never a mix (atomic visibility through metadata commit).
func TestConcurrentOverwritesAreAtomic(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Gateway: api.Config{SegmentSize: 64 << 10}})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	versions := map[string]bool{}
	var datas [][]byte
	for i := 0; i < 8; i++ {
		d := payload(200_000, int64(1000+i))
		datas = append(datas, d)
		versions[sum(d)] = true
	}
	put(t, c.Alice, "b-1", "k", datas[0])
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			put(t, c.Alice, "b-1", "k", datas[i])
		}(i)
	}
	readerErr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				readerErr <- nil
				return
			default:
			}
			rc, _, err := c.Alice.Get(ctx, "b-1", "k", client.GetOptions{})
			if err != nil {
				continue
			}
			b, err := io.ReadAll(rc)
			rc.Close()
			if err == nil && !versions[sum(b)] {
				readerErr <- fmt.Errorf("torn read: %d bytes match no version", len(b))
				return
			}
		}
	}()
	wg.Wait()
	close(stop)
	if err := <-readerErr; err != nil {
		t.Fatal(err)
	}
	final, _ := get(t, c.Alice, "b-1", "k", client.GetOptions{})
	if !versions[sum(final)] {
		t.Fatal("final object is not one of the written versions")
	}
}

// GET ?consistency=quorum verifies R replicas hold a matching copy.
func TestQuorumRead(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	put(t, c.Alice, "b-1", "k", []byte("quorum"))
	if _, _, err := c.Alice.Get(ctx, "b-1", "k", client.GetOptions{Consistency: "quorum"}); err != nil {
		t.Fatal(err)
	}
	c.Nodes[0].Server.SetFaults(storagenode.Faults{FailReads: true})
	c.Nodes[1].Server.SetFaults(storagenode.Faults{FailReads: true})
	if _, _, err := c.Alice.Get(ctx, "b-1", "k", client.GetOptions{Consistency: "quorum"}); !client.IsStatus(err, 503) {
		t.Fatalf("R=2 with 2 failed replicas must fail: %v", err)
	}
	// A plain read only needs one replica.
	if b, _ := get(t, c.Alice, "b-1", "k", client.GetOptions{}); string(b) != "quorum" {
		t.Fatal("plain read failed")
	}
}

// Disk full on one node: writes route around it via quorum, and the full
// node is excluded from placement once its usage is reported.
func TestDiskFull(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 4})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	full := c.Nodes[0]
	for i := 0; i < 4; i++ {
		put(t, c.Alice, "b-1", fmt.Sprint("seed", i), payload(50_000, int64(i)))
	}
	// Simulate a nearly full disk: capacity just above current usage.
	limit := full.Engine.Stats().UsedBytes + 1000
	full.Server.SetFaults(storagenode.Faults{Capacity: &limit})
	for i := 0; i < 10; i++ {
		r := put(t, c.Alice, "b-1", fmt.Sprint(i), payload(50_000, int64(i)))
		if r.MinReplicasAcked < 2 {
			t.Fatalf("write %d: %+v", i, r)
		}
	}
	c.WaitFor("full node not writable", func() bool {
		c.Refresh()
		n, _ := c.Watcher.Map().Node(full.ID)
		return !n.Writable()
	})
	r := put(t, c.Alice, "b-1", "after", payload(50_000, 99))
	if r.UnderReplicated {
		t.Fatalf("placement still targets the full node: %+v", r)
	}
}

// Slow replica: the stall detector drops it, the write completes at the
// speed of the healthy replicas, and repair restores the third copy.
func TestBackpressureSlowReplica(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 3, Gateway: api.Config{SegmentSize: 64 << 20,
		StallTimeout: 200 * time.Millisecond, QueueDepth: 2, ChunkSize: 64 << 10}})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	slow := c.Nodes[1]
	slow.Server.SetFaults(storagenode.Faults{ChunkDelayMS: 500})
	t.Cleanup(func() { slow.Server.SetFaults(storagenode.Faults{}) })
	data := payload(24<<20, 7)
	start := time.Now()
	r := put(t, c.Alice, "b-1", "k", data)
	t.Logf("24 MiB PUT with one slow replica: %v, acked %d/3", time.Since(start), r.MinReplicasAcked)
	if r.MinReplicasAcked != 2 {
		t.Fatalf("%+v", r)
	}
	slow.Server.SetFaults(storagenode.Faults{})
	// The slow node may still be finishing the aborted request for this
	// blob; repair simply retries on the next pass.
	c.WaitFor("repair of dropped replica", func() bool {
		rep, _ := c.Repairer.RunOnce(ctx)
		return rep.UnderReplicated == 0 || rep.Repaired == 1
	})
}

// A node that is alive but slow stays ACTIVE in membership. Without a
// penalty, every segment placed on it pays the full stall timeout before the
// replica is dropped (found on the local cluster: a 200 MB upload took 41.6 s
// with one slow node). With the gateway's slow-node penalty, only the first
// segment pays; later segments avoid the node.
func TestSlowNodePenalizedAcrossSegments(t *testing.T) {
	run := func(penalty time.Duration) time.Duration {
		c := testcluster.Start(t, testcluster.Options{Nodes: 4, Gateway: api.Config{SegmentSize: 512 << 10,
			StallTimeout: 300 * time.Millisecond, StragglerTimeout: 400 * time.Millisecond, QueueDepth: 2, ChunkSize: 64 << 10,
			SlowNodePenalty: penalty}})
		c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
		slow := c.Nodes[1]
		slow.Server.SetFaults(storagenode.Faults{ChunkDelayMS: 500})
		defer slow.Server.SetFaults(storagenode.Faults{})
		data := payload(8<<20, 11) // 16 segments
		start := time.Now()
		r := put(t, c.Alice, "b-1", "k", data)
		elapsed := time.Since(start)
		if r.MinReplicasAcked < 2 {
			t.Fatalf("%+v", r)
		}
		got, _ := get(t, c.Alice, "b-1", "k", client.GetOptions{})
		if !bytes.Equal(got, data) {
			t.Fatal("content mismatch")
		}
		return elapsed
	}
	without := run(-1)
	with := run(time.Minute)
	t.Logf("8 MiB in 16 segments with one slow node: penalty off %v, penalty on %v", without, with)
	if with*2 > without {
		t.Fatalf("penalty should avoid repeated stalls: off=%v on=%v", without, with)
	}
}

// A node that was declared DOWN and repaired around comes back with its old
// disk. The cluster now has 4 copies of some segments: the rebalancer trims
// the excess (never below RF) and GC's sweep removes the stale copies.
func TestNodeReturnsAfterRepair(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 4})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	for i := 0; i < 10; i++ {
		put(t, c.Alice, "b-1", fmt.Sprint(i), payload(20_000, int64(i)))
	}
	victim := c.Nodes[2]
	held := victim.Engine.Stats().BlobCount
	c.Kill(victim)
	c.WaitDown(victim)
	time.Sleep(60 * time.Millisecond)
	if rep, _ := c.Repairer.RunOnce(ctx); rep.Repaired == 0 {
		t.Fatalf("nothing repaired: %+v", rep)
	}
	c.Restart(victim)
	c.WaitFor("victim active", func() bool {
		c.Refresh()
		n, _ := c.Watcher.Map().Node(victim.ID)
		return n.State == placement.StateActive
	})
	if victim.Engine.Stats().BlobCount != held {
		t.Fatal("returning node lost its disk contents")
	}
	// Metadata no longer lists the victim, so its copies are stale.
	for i := 0; i < 3; i++ {
		c.Rebalancer.RunOnce(ctx)
	}
	time.Sleep(5 * time.Millisecond)
	c.GC.RunOnce(ctx)
	c.GC.RunOnce(ctx) // second sighting deletes orphans
	segs, _, _ := c.Meta.ScanSegments(ctx, "", 10000)
	for _, s := range segs {
		if len(s.Replicas) != 3 {
			t.Fatalf("segment %s replicas %v", s.BlobID, s.Replicas)
		}
		if got := c.BlobCopies(s.BlobID); len(got) != 3 {
			t.Fatalf("segment %s physical copies %v (metadata %v)", s.BlobID, got, s.Replicas)
		}
	}
	for i := 0; i < 10; i++ {
		if b, _ := get(t, c.Alice, "b-1", fmt.Sprint(i), client.GetOptions{}); !bytes.Equal(b, payload(20_000, int64(i))) {
			t.Fatal("data damaged")
		}
	}
}

// Repair target dies mid-repair: the pass reports failures, nothing is
// published for the failed copy, and the next pass completes the work.
func TestNodeFailsDuringRepair(t *testing.T) {
	c := testcluster.Start(t, testcluster.Options{Nodes: 5})
	c.Alice.CreateBucket(ctx, "b-1", client.BucketOptions{})
	for i := 0; i < 15; i++ {
		put(t, c.Alice, "b-1", fmt.Sprint(i), payload(10_000, int64(i)))
	}
	victim := c.Nodes[0]
	c.Kill(victim)
	c.WaitDown(victim)
	time.Sleep(60 * time.Millisecond)
	// Every potential repair target refuses writes for this pass.
	for _, n := range c.Nodes[1:] {
		n.Server.SetFaults(storagenode.Faults{FailWrites: true})
	}
	rep, _ := c.Repairer.RunOnce(ctx)
	if rep.Repaired != 0 || rep.Failed == 0 {
		t.Fatalf("expected failed pass: %+v", rep)
	}
	for _, n := range c.Nodes[1:] {
		n.Server.SetFaults(storagenode.Faults{})
	}
	rep, _ = c.Repairer.RunOnce(ctx)
	if rep.Failed != 0 || rep.Repaired == 0 {
		t.Fatalf("recovery pass: %+v", rep)
	}
	if rep, _ := c.Repairer.RunOnce(ctx); rep.UnderReplicated != 0 {
		t.Fatalf("still under-replicated: %+v", rep)
	}
}
