package metadata

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/objstore/internal/object"
)

var ctx = context.Background()

func sha(i int) string { return fmt.Sprintf("%064x", i) }

func newShard(t *testing.T, dir string) *Shard {
	t.Helper()
	s, err := OpenShard(ShardOptions{Dir: dir, MinPartSize: 10, CompactEvery: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mkVersion(bucket, key string, size int64, nseg int) object.Version {
	vid := object.NewVersionID(time.Now())
	v := object.Version{Bucket: bucket, Key: key, VersionID: vid, Size: size, ETag: "e"}
	per := size / int64(nseg)
	var off int64
	for i := 0; i < nseg; i++ {
		sz := per
		if i == nseg-1 {
			sz = size - off
		}
		v.Segments = append(v.Segments, object.Segment{Index: i, BlobID: object.SegmentBlobID(bucket, key, vid, i),
			Offset: off, Size: sz, SHA256: sha(i), Replicas: []string{"n1", "n2", "n3"}})
		off += sz
	}
	return v
}

func mustBucket(t *testing.T, s Store, name string, versioning bool) {
	t.Helper()
	if err := s.CreateBucket(ctx, Bucket{Name: name, Tenant: "t1", Versioning: versioning}); err != nil {
		t.Fatal(err)
	}
}

func TestCommitGetOverwriteGarbage(t *testing.T) {
	s := newShard(t, "")
	mustBucket(t, s, "photos", false)
	if err := s.CreateBucket(ctx, Bucket{Name: "photos", Tenant: "t1"}); !errors.Is(err, ErrBucketExists) {
		t.Fatal("duplicate bucket allowed")
	}
	v1, err := s.Commit(ctx, mkVersion("photos", "a.jpg", 100, 2), CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := s.Commit(ctx, mkVersion("photos", "a.jpg", 50, 1), CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if v2.Seq <= v1.Seq || v2.CreatedAt != v1.CreatedAt || v2.Tenant != "t1" || v2.ReplicationFactor != 3 {
		t.Fatalf("shard-assigned fields wrong: %+v", v2)
	}
	got, _ := s.Get(ctx, "photos", "a.jpg", "")
	if got.VersionID != v2.VersionID {
		t.Fatal("latest is not v2")
	}
	if _, err := s.Get(ctx, "photos", "a.jpg", v1.VersionID); !errors.Is(err, ErrNotFound) {
		t.Fatal("unversioned bucket kept old version")
	}
	// v1's two segments must be queued for GC, not deleted.
	g, _ := s.PeekGarbage(ctx, time.Now().Add(time.Second), 100)
	if len(g) != 2 || g[0].BlobID != v1.Segments[0].BlobID || g[0].Reason != "overwritten" {
		t.Fatalf("garbage %+v", g)
	}
	if u, _ := s.Usage(ctx, "t1"); u[0] != 50 {
		t.Fatalf("usage %v", u)
	}
	refs, _ := s.CheckRefs(ctx, []string{v1.Segments[0].BlobID, v2.Segments[0].BlobID})
	if refs[v1.Segments[0].BlobID].Referenced || !refs[v2.Segments[0].BlobID].Referenced {
		t.Fatalf("refs %+v", refs)
	}
	s.AckGarbage(ctx, g)
	if g, _ := s.PeekGarbage(ctx, time.Now().Add(time.Second), 100); len(g) != 0 {
		t.Fatal("ack did not remove garbage")
	}
}

func TestVersioningAndDeleteMarkers(t *testing.T) {
	s := newShard(t, "")
	mustBucket(t, s, "docs", true)
	v1, _ := s.Commit(ctx, mkVersion("docs", "k", 10, 1), CommitOptions{})
	v2, _ := s.Commit(ctx, mkVersion("docs", "k", 20, 1), CommitOptions{})
	m, err := s.Delete(ctx, "docs", "k")
	if err != nil || !m.DeleteMarker {
		t.Fatal(err, m)
	}
	latest, _ := s.Get(ctx, "docs", "k", "")
	if !latest.DeleteMarker {
		t.Fatal("latest should be delete marker")
	}
	if old, err := s.Get(ctx, "docs", "k", v1.VersionID); err != nil || old.Size != 10 {
		t.Fatal("historical version lost")
	}
	vs, _ := s.ListVersions(ctx, "docs", "k")
	if len(vs) != 3 || !vs[0].DeleteMarker || vs[2].VersionID != v1.VersionID {
		t.Fatalf("versions %+v", vs)
	}
	if u, _ := s.Usage(ctx, "t1"); u[0] != 30 {
		t.Fatalf("versioned usage must count all versions: %v", u)
	}
	if _, err := s.Delete(ctx, "docs", "k"); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleting a deleted key should be not found")
	}
	if err := s.DeleteVersion(ctx, "docs", "k", v2.VersionID); err != nil {
		t.Fatal(err)
	}
	if u, _ := s.Usage(ctx, "t1"); u[0] != 10 {
		t.Fatalf("usage after version delete %v", u)
	}
	r, _ := s.List(ctx, ListRequest{Bucket: "docs"})
	if len(r.Entries) != 0 {
		t.Fatal("list must hide keys whose latest version is a delete marker")
	}
}

func TestUnversionedTombstonePurge(t *testing.T) {
	now := time.Now()
	s, _ := OpenShard(ShardOptions{Now: func() time.Time { return now }})
	mustBucket(t, s, "b-1", false)
	v, _ := s.Commit(ctx, mkVersion("b-1", "k", 10, 1), CommitOptions{})
	s.Delete(ctx, "b-1", "k")
	g, _ := s.PeekGarbage(ctx, now.Add(time.Second), 10)
	if len(g) != 1 || g[0].BlobID != v.Segments[0].BlobID {
		t.Fatal("deleted data not queued for GC")
	}
	if n, _ := s.PurgeTombstones(ctx, now); n != 0 {
		t.Fatal("fresh tombstone purged")
	}
	now = now.Add(time.Hour)
	if n, _ := s.PurgeTombstones(ctx, now.Add(-time.Minute)); n != 1 {
		t.Fatal("old tombstone not purged")
	}
	if st, _ := s.Stats(ctx); st[0].Keys != 0 {
		t.Fatal("key not removed after purge")
	}
}

func TestListPrefixDelimiterPagination(t *testing.T) {
	s := newShard(t, "")
	mustBucket(t, s, "b-1", false)
	keys := []string{"a/1", "a/2", "a/b/3", "b/1", "c", "user-123/x", "user-123/y", "user-124/z"}
	for _, k := range keys {
		s.Commit(ctx, mkVersion("b-1", k, 1, 1), CommitOptions{})
	}
	r, _ := s.List(ctx, ListRequest{Bucket: "b-1", Prefix: "user-123/"})
	if len(r.Entries) != 2 || r.Entries[0].Key != "user-123/x" {
		t.Fatalf("prefix: %+v", r)
	}
	r, _ = s.List(ctx, ListRequest{Bucket: "b-1", Delimiter: "/"})
	if strings.Join(r.CommonPrefixes, ",") != "a/,b/,user-123/,user-124/" || len(r.Entries) != 1 || r.Entries[0].Key != "c" {
		t.Fatalf("delimiter: %+v", r)
	}
	r, _ = s.List(ctx, ListRequest{Bucket: "b-1", Prefix: "a/", Delimiter: "/"})
	if strings.Join(r.CommonPrefixes, ",") != "a/b/" || len(r.Entries) != 2 {
		t.Fatalf("nested delimiter: %+v", r)
	}
	// Paginate with page size 3 and collect everything.
	var got []string
	start := ""
	for pages := 0; ; pages++ {
		r, err := s.List(ctx, ListRequest{Bucket: "b-1", StartAt: start, Limit: 3})
		if err != nil || pages > 10 {
			t.Fatal(err)
		}
		for _, e := range r.Entries {
			got = append(got, e.Key)
		}
		if !r.Truncated {
			break
		}
		start = r.NextStartAt
	}
	if strings.Join(got, ",") != strings.Join(keys, ",") {
		t.Fatalf("pagination got %v", got)
	}
}

func TestMultipartLifecycle(t *testing.T) {
	s := newShard(t, "")
	mustBucket(t, s, "b-1", false)
	uid := "b-1~" + object.RandomID(8)
	if err := s.CreateUpload(ctx, Upload{ID: uid, Bucket: "b-1", Key: "big"}); err != nil {
		t.Fatal(err)
	}
	part := func(n int, attempt string, size int64) Part {
		return Part{Number: n, BlobID: object.PartBlobID("b-1", "big", uid, n, attempt), Size: size, SHA256: sha(n*10 + len(attempt)), Replicas: []string{"n1"}}
	}
	p1a := part(1, "a", 20)
	s.PutPart(ctx, uid, p1a)
	p1b := part(1, "bb", 20) // retry of part 1 replaces the first attempt
	s.PutPart(ctx, uid, p1b)
	p2 := part(2, "a", 5)
	s.PutPart(ctx, uid, p2)
	p3 := part(3, "a", 7) // uploaded but not used in completion
	s.PutPart(ctx, uid, p3)

	if _, err := s.CompleteUpload(ctx, uid, []CompletePart{{2, p2.SHA256}, {1, p1b.SHA256}}, "v", CommitOptions{}); !errors.Is(err, ErrInvalidPart) {
		t.Fatal("out-of-order parts accepted")
	}
	if _, err := s.CompleteUpload(ctx, uid, []CompletePart{{1, p1a.SHA256}, {2, p2.SHA256}}, "v", CommitOptions{}); !errors.Is(err, ErrInvalidPart) {
		t.Fatal("stale etag accepted")
	}
	if _, err := s.CompleteUpload(ctx, uid, []CompletePart{{2, p2.SHA256}, {3, p3.SHA256}}, "v", CommitOptions{}); !errors.Is(err, ErrInvalidPart) {
		t.Fatal("undersized non-final part accepted")
	}
	vid := object.NewVersionID(time.Now())
	v, err := s.CompleteUpload(ctx, uid, []CompletePart{{1, `"` + p1b.SHA256 + `"`}, {2, p2.SHA256}}, vid, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if v.Size != 25 || len(v.Segments) != 2 || v.Segments[1].Offset != 20 || !strings.HasSuffix(v.ETag, "-2") {
		t.Fatalf("completed version %+v", v)
	}
	if _, err := s.GetUpload(ctx, uid); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatal("upload survives completion")
	}
	g, _ := s.PeekGarbage(ctx, time.Now().Add(time.Second), 10)
	reasons := map[string]string{}
	for _, it := range g {
		reasons[it.BlobID] = it.Reason
	}
	if reasons[p1a.BlobID] != "part replaced" || reasons[p3.BlobID] != "part not in completion list" || len(g) != 2 {
		t.Fatalf("garbage %+v", g)
	}
	refs, _ := s.CheckRefs(ctx, []string{p1b.BlobID})
	if !refs[p1b.BlobID].Referenced {
		t.Fatal("used part no longer referenced")
	}
}

func TestAbortAndExpireUploads(t *testing.T) {
	now := time.Now()
	s, _ := OpenShard(ShardOptions{Now: func() time.Time { return now }})
	mustBucket(t, s, "b-1", false)
	for i := 0; i < 2; i++ {
		uid := fmt.Sprintf("b-1~%d", i)
		s.CreateUpload(ctx, Upload{ID: uid, Bucket: "b-1", Key: "k"})
		s.PutPart(ctx, uid, Part{Number: 1, BlobID: object.PartBlobID("b-1", "k", uid, 1, "x"), Size: 1, SHA256: sha(1)})
	}
	if err := s.AbortUpload(ctx, "b-1~0"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(48 * time.Hour)
	if n, _ := s.ExpireUploads(ctx, now.Add(-24*time.Hour)); n != 1 {
		t.Fatalf("expired %d", n)
	}
	if g, _ := s.PeekGarbage(ctx, now.Add(time.Second), 10); len(g) != 2 {
		t.Fatalf("aborted parts not queued: %d", len(g))
	}
}

func TestQuota(t *testing.T) {
	s := newShard(t, "")
	mustBucket(t, s, "b-1", false)
	if _, err := s.Commit(ctx, mkVersion("b-1", "k", 80, 1), CommitOptions{QuotaBytes: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(ctx, mkVersion("b-1", "k2", 30, 1), CommitOptions{QuotaBytes: 100}); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatal("quota not enforced")
	}
	// Overwriting k with a same-size object fits because the old version is released.
	if _, err := s.Commit(ctx, mkVersion("b-1", "k", 90, 1), CommitOptions{QuotaBytes: 100}); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateReplicasAndScan(t *testing.T) {
	s := newShard(t, "")
	mustBucket(t, s, "b-1", false)
	mustBucket(t, s, "b-2", false)
	for i := 0; i < 5; i++ {
		s.Commit(ctx, mkVersion("b-1", fmt.Sprint("k", i), 30, 3), CommitOptions{})
		s.Commit(ctx, mkVersion("b-2", fmt.Sprint("k", i), 10, 1), CommitOptions{})
	}
	var all []SegmentRef
	cursor := ""
	for {
		segs, next, err := s.ScanSegments(ctx, cursor, 4)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, segs...)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(all) != 20 {
		t.Fatalf("scanned %d segments, want 20", len(all))
	}
	blob := all[0].BlobID
	if err := s.UpdateReplicas(ctx, "b-1", blob, []string{"n4"}, []string{"n2"}); err != nil {
		t.Fatal(err)
	}
	refs, _ := s.CheckRefs(ctx, []string{blob})
	if strings.Join(refs[blob].Replicas, ",") != "n1,n3,n4" {
		t.Fatalf("replicas %v", refs[blob].Replicas)
	}
	if n, _ := s.ReplicaCount(ctx, "n4"); n != 1 {
		t.Fatalf("replica count %d", n)
	}
	if err := s.UpdateReplicas(ctx, "b-1", object.SegmentBlobID("x", "y", "z", 0), []string{"n4"}, nil); !errors.Is(err, ErrNotFound) {
		t.Fatal("update on unknown blob should fail")
	}
}

func TestPendingProtection(t *testing.T) {
	now := time.Now()
	s, _ := OpenShard(ShardOptions{Now: func() time.Time { return now }})
	mustBucket(t, s, "b-1", false)
	id := object.SegmentBlobID("b-1", "k", "v", 0)
	s.RegisterPending(ctx, "b-1", []string{id}, time.Hour)
	if r, _ := s.CheckRefs(ctx, []string{id}); !r[id].Pending || r[id].Referenced {
		t.Fatal("pending not reported")
	}
	now = now.Add(2 * time.Hour)
	if r, _ := s.CheckRefs(ctx, []string{id}); r[id].Pending {
		t.Fatal("expired pending still protects blob")
	}
}

func TestWALRecoveryAndTornTail(t *testing.T) {
	dir := t.TempDir()
	s := newShard(t, dir)
	mustBucket(t, s, "b-1", true)
	var last object.Version
	for i := 0; i < 20; i++ {
		last, _ = s.Commit(ctx, mkVersion("b-1", fmt.Sprint("k", i%5), 10, 1), CommitOptions{})
	}
	s.Delete(ctx, "b-1", "k0")
	s.Close()

	// Append garbage to simulate a crash mid-append.
	f, _ := os.OpenFile(filepath.Join(dir, walName), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0, 0, 1, 0, 9, 9, 9, 9, '{'})
	f.Close()

	s2 := newShard(t, dir)
	got, err := s2.Get(ctx, "b-1", "k4", "")
	if err != nil || got.VersionID != last.VersionID || got.Seq != last.Seq {
		t.Fatalf("recovered %+v err=%v", got, err)
	}
	if m, _ := s2.Get(ctx, "b-1", "k0", ""); !m.DeleteMarker {
		t.Fatal("delete marker lost")
	}
	st, _ := s2.Stats(ctx)
	if st[0].Versions != 21 {
		t.Fatalf("versions after replay %d", st[0].Versions)
	}
	// New writes after recovery must land after the truncated tail.
	if _, err := s2.Commit(ctx, mkVersion("b-1", "after", 1, 1), CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	s3 := newShard(t, dir)
	if _, err := s3.Get(ctx, "b-1", "after", ""); err != nil {
		t.Fatal("write after torn-tail recovery lost")
	}
}

func TestSnapshotThenReplay(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenShard(ShardOptions{Dir: dir, CompactEvery: 7})
	mustBucket(t, s, "b-1", false)
	uid := "b-1~u"
	s.CreateUpload(ctx, Upload{ID: uid, Bucket: "b-1", Key: "mp"})
	s.PutPart(ctx, uid, Part{Number: 1, BlobID: object.PartBlobID("b-1", "mp", uid, 1, "a"), Size: 1, SHA256: sha(1), Replicas: []string{"n1"}})
	for i := 0; i < 30; i++ {
		s.Commit(ctx, mkVersion("b-1", fmt.Sprint("k", i%7), int64(i+1), 2), CommitOptions{})
	}
	before, _ := s.Stats(ctx)
	g1, _ := s.PeekGarbage(ctx, time.Now().Add(time.Hour), 1000)
	u1, _ := s.Usage(ctx, "t1")
	s.Close()
	if _, err := os.Stat(filepath.Join(dir, snapshotName)); err != nil {
		t.Fatal("no snapshot written")
	}
	s2 := newShard(t, dir)
	after, _ := s2.Stats(ctx)
	g2, _ := s2.PeekGarbage(ctx, time.Now().Add(time.Hour), 1000)
	u2, _ := s2.Usage(ctx, "t1")
	if before[0].Keys != after[0].Keys || before[0].Segments != after[0].Segments || before[0].LSN != after[0].LSN || len(g1) != len(g2) || u1[0] != u2[0] {
		t.Fatalf("state differs after snapshot+replay: %+v vs %+v", before, after)
	}
	if _, err := s2.GetUpload(ctx, uid); err != nil {
		t.Fatal("upload lost in snapshot")
	}
	partBlob := object.PartBlobID("b-1", "mp", uid, 1, "a")
	if err := s2.UpdateReplicas(ctx, "b-1", partBlob, []string{"n2"}, nil); err != nil {
		t.Fatal("part refs not rebuilt from snapshot:", err)
	}
}

func TestConcurrentCommitsSameKey(t *testing.T) {
	s := newShard(t, "")
	mustBucket(t, s, "b-1", true)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Commit(ctx, mkVersion("b-1", "hot", 1, 1), CommitOptions{}); err != nil {
				t.Error(err)
			}
			s.Get(ctx, "b-1", "hot", "")
			s.List(ctx, ListRequest{Bucket: "b-1"})
		}()
	}
	wg.Wait()
	vs, _ := s.ListVersions(ctx, "b-1", "hot")
	if len(vs) != 50 {
		t.Fatalf("got %d versions", len(vs))
	}
	for i := 1; i < len(vs); i++ {
		if vs[i-1].Seq <= vs[i].Seq {
			t.Fatal("versions not totally ordered by commit sequence")
		}
	}
}

func TestRouterAndRPC(t *testing.T) {
	var shards []Store
	for i := 0; i < 3; i++ {
		sh, _ := OpenShard(ShardOptions{Index: i})
		srv := httptest.NewServer(NewServer(sh))
		t.Cleanup(srv.Close)
		shards = append(shards, NewClient(srv.URL, nil))
	}
	r := NewRouter(shards)
	used := map[int]bool{}
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("bucket-%d", i)
		mustBucket(t, r, name, false)
		used[r.ShardFor(name)] = true
		if _, err := r.Commit(ctx, mkVersion(name, "k", 10, 2), CommitOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if len(used) != 3 {
		t.Fatal("buckets did not spread over shards")
	}
	var n int
	cursor := ""
	for {
		segs, next, err := r.ScanSegments(ctx, cursor, 5)
		if err != nil {
			t.Fatal(err)
		}
		n += len(segs)
		if next == "" {
			break
		}
		cursor = next
	}
	if n != 24 {
		t.Fatalf("router scan found %d segments", n)
	}
	u, _ := r.Usage(ctx, "t1")
	if u[0]+u[1]+u[2] != 120 {
		t.Fatalf("usage %v", u)
	}
	// Error mapping survives the wire.
	if _, err := r.Get(ctx, "bucket-0", "missing", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	if _, err := r.GetBucket(ctx, "nope-nope"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("got %v", err)
	}
	r.Delete(ctx, "bucket-0", "k")
	g, err := r.PeekGarbage(ctx, time.Now().Add(time.Second), 10)
	if err != nil || len(g) != 2 {
		t.Fatalf("garbage %v %v", g, err)
	}
	if err := r.AckGarbage(ctx, g); err != nil {
		t.Fatal(err)
	}
}

func TestInjectedFaults(t *testing.T) {
	sh, _ := OpenShard(ShardOptions{})
	srv := NewServer(sh)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	c := NewClient(ts.URL, nil)
	mustBucket(t, c, "b-1", false)
	srv.SetFaults(Faults{FailReads: true})
	if _, err := c.Get(ctx, "b-1", "k", ""); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want unavailable, got %v", err)
	}
	if _, err := c.Commit(ctx, mkVersion("b-1", "k", 1, 1), CommitOptions{}); err != nil {
		t.Fatal("writes should still work:", err)
	}
}
