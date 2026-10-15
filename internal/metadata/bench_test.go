package metadata

import (
	"fmt"
	"testing"
)

// Commit throughput of one shard. Commits are serialized per shard and each
// one appends to the WAL; with Sync the fsync dominates. This is the
// per-shard write ceiling discussed in docs/performance.md and
// docs/metadata-scaling.md.
func BenchmarkCommitNoSync(b *testing.B) { benchCommit(b, false) }
func BenchmarkCommitFsync(b *testing.B)  { benchCommit(b, true) }

func benchCommit(b *testing.B, sync bool) {
	s, err := OpenShard(ShardOptions{Dir: b.TempDir(), Sync: sync, MinPartSize: 10, CompactEvery: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	if err := s.CreateBucket(ctx, Bucket{Name: "bench", Tenant: "t1", Versioning: true}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Commit(ctx, mkVersion("bench", fmt.Sprintf("k/%08d", i), 4096, 1), CommitOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

// Latest-version lookups take only the shared lock.
func BenchmarkGetParallel(b *testing.B) {
	s, err := OpenShard(ShardOptions{MinPartSize: 10})
	if err != nil {
		b.Fatal(err)
	}
	s.CreateBucket(ctx, Bucket{Name: "bench", Tenant: "t1"})
	for i := 0; i < 10000; i++ {
		s.Commit(ctx, mkVersion("bench", fmt.Sprintf("k/%08d", i), 4096, 1), CommitOptions{})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := s.Get(ctx, "bench", fmt.Sprintf("k/%08d", i%10000), ""); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
