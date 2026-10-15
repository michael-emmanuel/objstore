# Capacity Planning

Worked example: **10 PB of logical data**, RF 3, in one region with three
availability zones. All arithmetic is shown so the assumptions can be changed.
Hardware figures are typical planning values, not measurements; measured
figures from this codebase are marked *(measured)* and come from
[performance.md](performance.md).

## Assumptions

| Parameter | Value | Why |
|---|---|---|
| Logical data | 10 PB | given |
| Replication | RF 3 (one copy per zone) | default storage class |
| Disk | 20 TB HDD, ~150 MB/s sequential, ~150 random IOPS | capacity-optimized |
| Node | 24 disks, 2 x 25 Gb/s NICs | common storage chassis |
| Target fill | 80% | headroom for failures, rebalancing, growth |
| Average object size | 1 MB (sensitivity: 256 KB and 4 MB) | dominates metadata sizing |
| Disk AFR | 1.5% | typical published fleet rates |

## Total object storage and replication overhead

```
raw       = logical x RF              = 10 PB x 3   = 30 PB
installed = raw / target fill         = 30 PB / 0.8 = 37.5 PB
disks     = 37.5 PB / 20 TB           = 1,875 disks
nodes     = 1,875 / 24                = 78 nodes  (26 per zone)
```

Replication overhead is 20 PB of the 30 PB raw. Per-zone capacity must be
equal: with one replica per zone, each zone stores a full 10 PB copy, and a
smaller zone fills first. (Observed in miniature on the local cluster: with
zones of 1, 2 and 1 nodes, the single-node zones took 2x the write bytes.)

### How erasure coding changes the equation

| Scheme | Raw | Installed at 80% | 20 TB disks | 24-disk nodes | Tolerates |
|---|---|---|---|---|---|
| RF 3 | 30.0 PB | 37.5 PB | 1,875 | 78 | 2 failures |
| RS(6,3) | 15.0 PB | 18.8 PB | 938 | 39 | 3 failures |
| RS(10,4) | 14.0 PB | 17.5 PB | 875 | 36 | 4 failures |

RS(10,4) halves hardware for the same logical data and tolerates more
failures, but needs 14 failure domains for full spread (with 3 zones, place
at most 5 fragments per zone so a zone loss leaves >= 10 readable), and repair
reads 10x the lost data. Realistic plan: hot/small data at RF 3, cold large
data at RS(10,4). If 70% of bytes are cold:
`3 PB x 3 + 7 PB x 1.4 = 18.8 PB raw` instead of 30 PB.

## Metadata storage

Per-version metadata in this implementation is a JSON record: identifiers,
size, ETag, checksum, timestamps, content type and one entry per segment
(blob ID, offset, size, SHA-256, replica list). For a single-segment object it
is about **600 bytes**; each additional 64 MiB segment adds ~150 bytes.

| Avg object size | Objects for 10 PB | Metadata (600 B each) | x3 if replicated (Raft) |
|---|---|---|---|
| 256 KB | 39 billion | 23 TB | 70 TB |
| 1 MB | 10 billion | 6 TB | 18 TB |
| 4 MB | 2.5 billion | 1.5 TB | 4.5 TB |

Before indexes and storage-engine overhead (an LSM adds roughly 1.1-1.5x).
**Object count, not bytes, sizes the metadata plane.** The canonical example:
1 billion objects x 500 B = 500 GB before indexes, replication and overhead.

## Metadata memory

This implementation keeps each shard's entire state in memory (Go maps and
slices, roughly 2-3x the JSON size). At 10 billion objects that is
impossible, which is exactly why a production metadata store keeps the data
in an LSM on SSD and only caches hot ranges and index blocks in memory:

```
metadata on SSD     ~ 18 TB (replicated) over, e.g., 30 metadata servers = 600 GB each
memory per server   block cache for hot ranges + index/filter blocks:
                    ~5-10% of data = 30-60 GB per server
```

## Request throughput

Assume 50,000 GET/s and 10,000 PUT/s at peak (1 MB average).

| Resource | Demand | Supply / notes |
|---|---|---|
| Metadata reads | 50,000/s (one per GET; fewer with the opt-in metadata cache) | ~0.5 µs per in-memory lookup *(measured)*; RPC/network bound in practice |
| Metadata commits | 10,000/s | ~3,700 commits/s per shard with fsync per commit *(measured)* -> at least 3 shards, 10+ with headroom, or group commit (batching N commits per fsync multiplies this) |
| Small-write ceiling today | | 285 PUT/s for 4 KiB objects on the 2-vCPU test VM *(measured)*: fsync-bound end to end |

## Network bandwidth

```
client ingress (PUT)      10,000/s x 1 MB          = 10 GB/s
gateway -> nodes          ingress x RF             = 30 GB/s   (gateway fan-out)
cross-zone write traffic  2 of 3 replicas cross    = 20 GB/s   (if gateways are zonal)
client egress (GET)       50,000/s x 1 MB          = 50 GB/s   (minus hot-object cache hits)
```

A 25 Gb/s NIC moves about 3.1 GB/s. Gateway egress alone (30 GB/s) needs
~10 gateway NICs at line rate; plan 2x for headroom. Fan-out from the gateway
is the cost of the synchronous-parallel design; chain replication would put
2/3 of that traffic on the storage network instead.

Reads are served zone-locally when the gateway knows its zone (`-zone`),
avoiding cross-zone egress charges for reads.

## Disk throughput

```
aggregate sequential   1,875 disks x 150 MB/s  = 281 GB/s
write demand           30 GB/s (all replicas)  = ~11% of sequential capacity
read demand            50 GB/s                 = ~18%
```

Bandwidth is not the limit; **IOPS are**. Each PUT costs several random I/Os
per replica (data file create, sidecar, two renames, fsyncs): roughly 6-8
I/Os. 10,000 PUT/s x 3 replicas x 7 = 210,000 IOPS against
1,875 x 150 = 281,000 IOPS available: the cluster is near its IOPS ceiling
for small writes while using a tenth of its bandwidth. This is the
quantitative argument for append-only volume files for small objects
([tradeoffs.md](tradeoffs.md#1-local-filesystem-vs-custom-storage-engine)) and
for SSD/NVMe journals.

## Repair bandwidth

```
disk failures per year  = 1,875 x 1.5%     = ~28 (one every two weeks)
data on a failed disk   = 20 TB x 0.8      = 16 TB
data on a failed node   = 24 x 16 TB       = 384 TB
```

Because placement is per segment, a failed disk's data has replicas spread
over essentially every other disk: each of ~1,874 disks contributes ~8.5 GB.
Repair is therefore limited by the bandwidth budget, not by any single disk.

| Repair budget | One disk (16 TB) | One node (384 TB) | Whole zone (10 PB: one full replica) |
|---|---|---|---|
| 1 GB/s | 4.4 h | 107 h | ~116 days |
| 10 GB/s | 0.44 h | 10.7 h | ~278 h (11.6 days) |

A zone failure is therefore not "repaired"; the system runs on two copies
(still tolerating one more failure) and waits for the zone to return. Repair
after a zone loss should be limited to segments that dropped to a single
copy. This is why the repair delay matters most for large failures.
**This implementation does not make that distinction**: after the repair
delay it re-replicates every affected segment into the surviving zones. During
a zone outage an operator should raise `-repair-delay` (segments down to a
single healthy copy are still repaired immediately, regardless of the delay);
an automatic "correlated-failure mode" is a production feature this project
lacks.

The repair window is the period of reduced redundancy. The probability of
losing data is roughly the probability that the *remaining* replicas of some
segment fail within that window, so the window should be small relative to
disk MTTF, and the repair budget must still leave room for client traffic.
The defaults in this repository (50 MB/s per repairer) are sized for a laptop.

## Cache memory

Hot-object cache sizing: the hot set, not the total. If 1 million objects
of 256 KB receive most reads, that is 256 GB: spread across 16 gateways at
16 GB each. Objects larger than `-object-cache-max-object` (default 1 MiB)
are never cached; they rely on replica spreading and the OS page cache on
storage nodes. Measured: with a 90%-hot workload, 99.6% hit rate and backend
traffic of ~0.4% of client traffic *(measured)*.

## Summary for 10 PB, RF 3

| Item | Estimate |
|---|---|
| Raw / installed storage | 30 PB / 37.5 PB |
| Storage nodes | ~78 x 24 x 20 TB (26 per zone) |
| Metadata (1 MB avg) | 6 TB, 18 TB replicated; LSM on SSD |
| Metadata commits | >= 3 shards at today's per-shard rate; group commit strongly advised |
| Gateway egress | 30 GB/s for 10 GB/s ingest |
| Repair | one disk failure every ~2 weeks; 16 TB per disk; 10 GB/s budget = < 30 min |
| With RS(10,4) for cold data | ~17.5 PB installed instead of 37.5 PB |
