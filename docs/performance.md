# Performance

Rule for this document: **no number without a measurement**. Every figure
below was produced by the commands shown, on the environment described. They
are useful for *relative* comparisons (fsync vs no fsync, ring vs rendezvous,
cache vs no cache) and for finding bottlenecks. They say nothing about what
the design would do on real servers.

## Measurement environment

| | |
|---|---|
| CPU | 2 vCPU, Intel Xeon @ 2.10 GHz (SHA-NI available) |
| Memory | 7 GiB |
| Disk | one shared virtual disk for every process |
| OS | Linux 6.18, Go 1.24.7 |
| Topology | every process of the cluster (2 metadata shards, 4-5 storage nodes, gateway, repairer, rebalancer) **and** the load generator on the same VM |

Consequence: all "replicas" share one disk and one CPU pair. Network is
loopback. Absolute throughput is therefore pessimistic for disk-bound
paths and optimistic for network latency.

## How to reproduce

```bash
go test -run '^$' -bench . -benchmem ./internal/...      # micro-benchmarks
scripts/local-cluster.sh up && source .local/env           # real processes
go run ./cmd/loadgen -workload write-only -size 4KiB -keys 5000 -concurrency 32 -duration 12s \
   -scrape http://localhost:8101/metrics,http://localhost:8102/metrics
go tool pprof -top http://localhost:6060/debug/pprof/profile?seconds=10   # gateway CPU profile
go test -race ./...                                         # correctness under concurrency
```

Binaries accept `-debug-listen localhost:PORT` to expose pprof on a separate,
local-only address (the local cluster uses :6060 for the gateway and :600N for
node N).

## Micro-benchmarks

### Storage engine (`internal/storage`)

| Benchmark | Result | Notes |
|---|---|---|
| Put 1 MiB, no fsync | 639 MB/s | tmp write + SHA-256 + CRC32C + sidecar + rename |
| Put 1 MiB, fsync | 213 MB/s | **3.0x slower** |
| Put 64 KiB, no fsync | 144 MB/s (2,200 ops/s) | per-blob fixed costs dominate |
| Put 64 KiB, fsync | 24 MB/s (360 ops/s) | **6.1x slower**: fsync per file + directory |
| Get 1 MiB with CRC verification | 8.5 GB/s | page cache hit; verification cost included |
| SHA-256, 1 MiB | 1.48 GB/s | hardware SHA extensions |
| CRC32C, 1 MiB | 21.2 GB/s | 14x faster than SHA-256 |

Reading: small objects are dominated by per-object filesystem work
(create, two renames, three fsyncs), not by bytes. This is the quantitative
case for append-only volume files for small objects
([tradeoffs.md](tradeoffs.md#1-local-filesystem-vs-custom-storage-engine)).

### Metadata shard (`internal/metadata`)

| Benchmark | Result |
|---|---|
| Commit, WAL fsync per commit | 267 µs/op (~3,750 commits/s per shard) |
| Commit, no fsync | 12.8 µs/op (~78,000 commits/s) |
| Get latest version, parallel readers | 0.48 µs/op |

Commits are serialized per shard (exclusive lock held across WAL append and
fsync), so the per-shard commit ceiling is ~1 / fsync latency. **Group commit**
(let concurrent commits queue, write them as one WAL batch, fsync once,
acknowledge all) is the standard fix and is not implemented. Reads take a
shared lock and are not a bottleneck.

### Placement (`internal/hashing`)

| Benchmark | 100 nodes | 1,000 nodes |
|---|---|---|
| Ring (256 vnodes/weight), top-3 | 0.29 µs | 0.66 µs |
| Rendezvous, top-3 | 11.7 µs | 169 µs |

Movement when adding an 11th node to 10: ring 8.8%, rendezvous 9.0%,
modulo 90.7% (ideal 9.1%). Balance across 20 nodes (relative stddev of
ownership): ring 6.2% at 256 vnodes, 18.7% at 16, 79.5% at 1; rendezvous 1.5%.

### Other

| Benchmark | Result |
|---|---|
| LRU Get, parallel | 49 ns/op, 0 allocs |
| Replicated write 1 MiB to 3 in-process nodes (RF 3) | 115 MB/s of logical data (345 MB/s written) |

## End-to-end measurements (local cluster, real processes)

Unless stated, 4 storage nodes in zones a/b/c/a, RF 3, W 2, fsync on,
16 MiB segments, gateway hot-object cache 256 MiB for objects <= 1 MiB.

### Large object streaming (curl, 1 GiB random data)

| | Result |
|---|---|
| Upload 1 GiB (64 segments x 3 replicas = 3 GiB written, fsync on) | 10.7 s (~100 MB/s logical) |
| Download 1 GiB with SHA-256 check by client | 2.3 s (~460 MB/s) |
| **Gateway peak RSS during the upload** | **18.9 MB** |
| End-to-end SHA-256 | matched |
| `Range: bytes=1000000-1000099` | 206, correct 100 bytes |

Peak RSS of ~19 MB while streaming 1 GiB is the evidence for bounded-memory
streaming: memory is set by `replicas x (queue depth + 2) x chunk size` per
upload, not by object size.

### Load generator runs (12-15 s each, client on the same VM)

| Workload | Size | Conc. | PUT ops/s | PUT p50 / p95 / p99 ms | GET ops/s | GET p50 / p95 / p99 ms | GET MiB/s |
|---|---|---|---|---|---|---|---|
| write-only | 4 KiB | 32 | 285 | 110 / 164 / 192 | - | - | - |
| write-heavy (20/80) | 1 MiB | 16 | 75 | 200 / 291 / 347 | 20 | 39 / 115 / 134 | 20 |
| mixed (50/50) | 1-64 KiB | 16 | 207 | 65 / 102 / 122 | 209 | 10 / 25 / 36 | 7 |
| read-heavy (95/5) | 4 MiB | 16 | 3.7 | 196 / 334 / 415 | 101 | 145 / 269 / 334 | 406 |
| read-only, 90% hot key | 64 KiB | 32 | - | - | 3,180 | 9.5 / 18.9 / 25.0 | 199 |

Server-side counters during the runs (from `-scrape`):

- **Write amplification is exactly RF.** In the 1 MiB write-heavy run, storage nodes received 2.84 GB for 0.94 GB of logical writes (3.0x).
- **Zone imbalance is visible.** With zones a={node-1}, b={node-2,node-5}, c={node-3} (node-4 drained), node-1 and node-3 received 946 MB each and node-2/node-5 ~470 MB each: one replica per zone means the single-node zones take every object.
- **Hot-object cache.** 99.6% hits (47,495 of 47,707 GETs); 12 coalesced misses; nodes served 13 MB while clients received ~3 GB.

### Gateway CPU profile (1 MiB write-heavy load, ~88 MiB/s)

```
38.5%  syscall (socket writes: 3 replica streams per upload + client reads)
30.6%  sha256.blockSHANI (end-to-end checksum of every byte)
 8.1%  memmove
 ~3%   select / scheduling (fan-out goroutines)
```

The gateway's own logic is negligible; the cost is moving bytes three times
and hashing them once. Paths to reduce it: chain replication (gateway sends
one copy), `splice`/`sendfile` are not applicable because bytes must be hashed,
and the hash cannot be dropped without losing end-to-end integrity.

### Failure-path measurements

| Scenario | Result |
|---|---|
| SIGKILL a node, read 30 objects immediately | 30/30 succeeded |
| PUT during node failure | 200, `X-Replicas-Acked: 2/3` |
| Detection | SUSPECT at ~3 s, DOWN at ~6-8 s (configured 3 s / 6 s, heartbeat 500 ms) |
| Repair of 99 segments (1.08 GB) | ~21 s at the 50 MB/s budget (budget-bound, as intended) |
| Rebalance after adding a node | 99 moves, 1.08 GB, at the 20 MB/s budget |
| Drain of a node with 49 replicas | ~35 s to `safe_to_remove` |
| Slow replica (one node delayed 500 ms per 64 KiB chunk, stall timeout 200 ms) | 24 MiB PUT in 0.30 s, acked 2/3; the slow replica was dropped, not waited for (integration test) |
| Slow node on the live cluster (300 ms per 64 KiB), 200 MB PUT, 16 MiB segments | **41.6 s before the fix** (8 segments x 5 s stall timeout); **6.9 s after** adding the gateway's slow-node penalty (1 stall, then the node is avoided). Gateway RSS 15-18 MB in both |
| Slow node, 8 MiB in 16 small segments (straggler path) | 6.5 s without penalty vs 0.49 s with (`TestSlowNodePenalizedAcrossSegments`) |
| Corruption: flip one byte, then read | read succeeded via mid-stream failover; repaired within ~160 ms |

## Discussion

**Sequential vs random I/O.** Blobs are written sequentially and read
sequentially or by range; the random I/O is filesystem metadata (create,
rename, directory fsync). On HDDs that metadata I/O, not bandwidth, limits
small-object throughput ([capacity-planning.md](capacity-planning.md#disk-throughput)).

**Filesystem (page) cache.** Reads after writes hit the page cache (8.5 GB/s
verified reads). The engine relies on it rather than an application cache on
nodes; the gateway caches only small hot objects to avoid the network hop.
Double caching (page cache + application cache) is avoided on nodes.

**Direct I/O.** Not used. It would avoid polluting the page cache with large
cold scans (scrubber, repair) at the cost of alignment constraints and losing
read-ahead. Worth it once scrub/repair traffic is shown to evict hot data;
`posix_fadvise(DONTNEED)` after scrubbing is a cheaper first step.

**fsync.** The single largest cost: 3x for 1 MiB blobs, 6x for 64 KiB blobs,
21x for metadata commits. Mitigations in order of value: group commit for the
metadata WAL; batching small objects into volume files (one fsync per batch);
NVMe/NVRAM for journals.

**Serialization.** Metadata RPCs and the WAL use JSON. Commit at 12.8 µs
without fsync shows JSON is not the bottleneck while fsync costs 267 µs.
Protobuf or a binary WAL would matter after group commit.

**Checksums.** SHA-256 is ~31% of gateway CPU under write load even with
hardware acceleration, and each replica computes it again. CRC32C is 14x
cheaper and is used for block verification on reads. Keeping SHA-256 is a
deliberate choice: it is the object's identity across the system.

**Network bandwidth.** The gateway sends RF copies: gateway egress = 3x
ingress. In the profile, socket writes are the largest CPU item.

**CPU.** On the gateway: hashing and syscalls. On nodes: hashing, CRC, file
I/O syscalls.

**Memory.** Bounded per upload; measured gateway RSS 19 MB for a 1 GiB
upload and a 2-3.5 MiB loadgen heap across runs.

**Goroutine scheduling.** Each replica write uses two goroutines (uploader and
pump) plus the HTTP transport's; with RF 3 and 256 concurrent uploads that is
~2,000 goroutines, cheap for Go. `selectgo` and `casgstatus` appear at ~3% in
the profile.

**Lock contention.** The metadata shard's exclusive commit lock is the only
contended lock by design (it *is* the serialization point). Caches use one
mutex each (49 ns parallel Get); sharding them would only matter at very high
core counts. Storage nodes hold no lock during I/O.

## Correctness under concurrency

`go test -race ./...` passes, including 25 integration tests that run a full
cluster in-process with concurrent writers, readers, repair, rebalancing and
GC. `TestConcurrentOverwritesAreAtomic` hammers one key with concurrent PUTs
and GETs and checks every read returns one complete version.
