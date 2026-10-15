# Architectural Tradeoffs

A study document. Every decision is tied to this system's code. Each entry
covers: **Problem**, **Chosen approach**, **Alternatives**, **Why chosen**,
**Benefits**, **Costs**, **Failure modes**, **Scaling implications**, and
**When another design would be preferable**.

Measured numbers come from [performance.md](performance.md) (2-vCPU VM, one
shared disk; treat them as relative, not absolute).

Contents: [1 Filesystem](#1-local-filesystem-vs-custom-storage-engine) ·
[2 Planes](#2-metadata-plane-vs-data-plane) ·
[3 Hash vs consistent](#3-hash-partitioning-vs-consistent-hashing) ·
[4 Ring vs rendezvous](#4-consistent-hashing-vs-rendezvous-hashing) ·
[5 RF](#5-replication-factor) · [6 Quorum reads](#6-quorum-reads) ·
[7 Quorum writes](#7-quorum-writes) · [8 Strong vs eventual](#8-strong-vs-eventual-consistency) ·
[9 Sync replication](#9-synchronous-replication) · [10 Async replication](#10-asynchronous-replication) ·
[11 Checksums](#11-checksums) · [12 Content addressing](#12-content-addressing) ·
[13 Dedup](#13-deduplication) · [14 Multipart](#14-multipart-uploads) ·
[15 Streaming](#15-streaming-vs-buffering) · [16 Segment size](#16-segment--chunk-size) ·
[17 Atomic rename](#17-atomic-rename) · [18 fsync](#18-fsync-policy) ·
[19 Versioning](#19-versioning) · [20 Tombstones](#20-tombstones) ·
[21 GC](#21-garbage-collection) · [22 Repair](#22-repair) ·
[23 Rebalancing](#23-rebalancing) · [24 Draining](#24-node-draining) ·
[25 Failure domains](#25-failure-domains) · [26 Hot-object cache](#26-hot-object-caching) ·
[27 LRU](#27-lru-caching) · [28 Metadata cache](#28-metadata-caching) ·
[29 Rate limiting](#29-rate-limiting) · [30 Backpressure](#30-backpressure) ·
[31 Quotas](#31-tenant-quotas) · [32 Pagination](#32-pagination) ·
[33 Metadata sharding](#33-metadata-sharding) · [34 Multi-region](#34-multi-region-replication) ·
[35 Active-active vs active-passive](#35-active-active-vs-active-passive) ·
[36 Erasure coding](#36-erasure-coding-vs-replication) ·
[Final review](#final-tradeoff-review)

---

## 1. Local filesystem vs custom storage engine

- **Problem:** Each node must store millions of immutable blobs durably and serve random ranges of them.
- **Chosen approach:** One file per blob on the local filesystem (`blobs/<id[0:2]>/<id>`), plus a small binary sidecar (size, SHA-256, CRC32C per 64 KiB block, its own CRC). Commit by tmp-write, fsync, rename, directory fsync.
- **Alternatives:** (a) Append-only volume files with an in-memory index (Haystack/SeaweedFS style); (b) an embedded key-value store (RocksDB/Pebble) holding small blobs; (c) raw block device with a custom allocator (Ceph BlueStore).
- **Why chosen:** Correctness is easy to reason about and easy to inspect (`ls`, `sha256sum`). The commit protocol is short and well known. Deletes are an `unlink`, so GC needs no compaction.
- **Benefits:** Simple crash recovery; OS page cache for free; independent blobs mean no shared on-disk structure to corrupt.
- **Costs:** One inode and at least two directory entries per blob; small-object overhead (a 4 KiB object costs a 4 KiB block + sidecar + inode); several fsyncs per write (measured: 64 KiB writes are 6x slower with fsync). Filesystem metadata becomes the node's bottleneck at hundreds of millions of files.
- **Failure modes:** Filesystem corruption affects many blobs; inode exhaustion before byte exhaustion with small objects.
- **Scaling implications:** Fine to tens of millions of blobs per node. Beyond that, directory sizes and inode caches hurt.
- **When another design would be preferable:** Small-object-heavy workloads (append-only volumes amortize fsync and inodes: one fsync for many objects); very high IOPS (direct I/O and a custom allocator avoid double caching).

## 2. Metadata plane vs data plane

- **Problem:** Objects need both bytes and facts about bytes (existence, version, location, checksum). These have opposite scaling and consistency needs.
- **Chosen approach:** Separate services. Metadata shards are the single authority for versions and replica locations; storage nodes store opaque immutable blobs and know nothing else.
- **Alternatives:** (a) Store metadata next to the data on the storage nodes (each node indexes its own objects; listing = scatter-gather); (b) a single combined service.
- **Why chosen:** Immutable data needs no coordination; mutable metadata needs linearizability. Separating them puts coordination only where it is required ([architecture.md](architecture.md#2-metadata-plane-vs-data-plane)).
- **Benefits:** Data plane scales by adding nodes; repair and rebalancing never touch object identity; listing is a single ordered scan.
- **Costs:** Two hops per request; the metadata service is a critical dependency and the coordination bottleneck; two stores can disagree (orphans, lost replicas) and need reconciliation.
- **Failure modes:** Metadata outage = unavailability for its buckets even with all data intact.
- **Scaling implications:** Metadata must be sharded and replicated by itself; it scales with object count and op rate, not bytes.
- **When another design would be preferable:** Very small deployments (one box) where the extra hop and service are pure overhead; append-only log stores (Kafka-like) where position is the metadata.

## 3. Hash partitioning vs consistent hashing

- **Problem:** Map each blob to storage nodes deterministically so any gateway agrees, while letting the cluster grow.
- **Chosen approach:** Consistent hashing (ring with virtual nodes) produces a preference list; placement walks it applying failure-domain rules.
- **Alternatives:** `hash(key) % N`; a directory (lookup table) of placement groups (Ceph PGs, HDFS NameNode block map); random placement recorded in metadata.
- **Why chosen:** With `% N`, changing N remaps almost everything: measured **90.7%** of keys move when going from 10 to 11 nodes, versus **8.8%** with the ring (ideal 1/11 = 9.1%). Rebalancing cost is proportional to data moved.
- **Benefits:** Deterministic, no coordination needed to compute; minimal movement on membership change.
- **Costs:** Imperfect balance (needs vnodes); weight changes move data; no explicit control over individual placements.
- **Failure modes:** Inconsistent membership views across components produce different placements. Mitigated because **metadata records the replicas actually written**: placement is an intention, not the source of truth for reads.
- **Scaling implications:** Placement is recomputed for every segment on every repair and rebalance scan; lookup cost matters (next entry).
- **When another design would be preferable:** When you need to move individual hot or misbehaving placement units by hand, a directory (placement groups) is better, at the cost of maintaining the map.

Note the important subtlety: because the replica list is recorded per segment,
this system could use *any* placement function, even random, for correctness.
The hash only determines where data *should* converge and how much moves when
membership changes.

## 4. Consistent hashing vs rendezvous hashing

- **Problem:** Choose the preference-list algorithm.
- **Chosen approach:** Ring by default (`-placement=ring`); rendezvous (highest-random-weight) is implemented behind the same interface and selectable.
- **Alternatives:** Rendezvous; jump consistent hash (no weights, no removals); Maglev hashing; CRUSH.
- **Why chosen (measured, not assumed):**

  | | Ring (256 vnodes/weight, default) | Rendezvous |
  |---|---|---|
  | Keys moved adding 11th node | 8.8% | 9.0% |
  | Ownership balance over 20 nodes (rel. stddev) | 6.2% (1.09x max/mean) | 1.5% (1.02x) |
  | Top-3 lookup, 100 nodes | 0.29 µs | 11.7 µs |
  | Top-3 lookup, 1000 nodes | 0.66 µs | 169 µs |

  Placement is evaluated for every segment in the cluster by every repair and
  rebalance pass. At 1,000 nodes and 1 billion segments, rendezvous costs about
  47 CPU-hours per full pass versus about 11 CPU-minutes for the ring. That
  decides it for this design. Rendezvous is better balanced with zero tuning.
- **Benefits:** O(log V) lookup; balance tunable with vnodes (1 vnode: 3.2x max/mean; 256: 1.09x).
- **Costs:** Memory for vnodes (16 B each); balance is never perfect; more code.
- **Failure modes:** Too few vnodes produce hot nodes and uneven fill.
- **Scaling implications:** Ring lookup stays sub-microsecond at 10,000 nodes.
- **When another design would be preferable:** Small clusters (under ~100 nodes) where rendezvous's O(N) is negligible and its perfect balance and simplicity win; or when placement is evaluated only on writes.

## 5. Replication factor

- **Problem:** How many copies?
- **Chosen approach:** Per-bucket RF (default 3); `REDUCED_REDUNDANCY` class uses RF 2.
- **Alternatives:** RF 2; RF 4+; erasure coding (#36).
- **Why chosen:** RF 3 across 3 zones survives a full zone loss plus continued writes with W=2; it is the industry default for hot data.
- **Benefits:** Simple; reads from any copy; fast repair (copy one blob).
- **Costs:** 3x raw storage; 3x write bandwidth from the gateway (fan-out).
- **Failure modes:** RF copies on correlated hardware fail together.
- **Scaling implications:** At 10 PB logical, RF 3 = 30 PB raw ([capacity-planning.md](capacity-planning.md)).
- **When another design would be preferable:** Cold/large data: erasure coding at 1.2-1.5x overhead. Reproducible data (thumbnails): RF 2.

## 6. Quorum reads

- **Problem:** Should reads consult multiple replicas?
- **Chosen approach:** Default reads one replica (ordered: same zone, healthy, randomized). `?consistency=quorum` checks R replicas' checksums via HEAD before streaming.
- **Alternatives:** Dynamo-style R-of-N read with version comparison and read repair.
- **Why chosen:** Blobs are immutable; a replica cannot return a stale value, only no value. Version freshness comes from metadata ([consistency.md](consistency.md#quorum-reads-and-why-r--w--n-is-not-what-provides-consistency-here)).
- **Benefits:** Read latency of one replica; no read amplification.
- **Costs:** Missing replicas are discovered on demand (failover) or by the scrubber rather than on every read.
- **Failure modes:** None for consistency; quorum mode reduces availability (needs R reachable replicas).
- **Scaling implications:** Read throughput scales with replicas, not divided by R.
- **When another design would be preferable:** When replicas hold mutable values with no central version authority (Cassandra-style), R+W>N is what provides read-your-writes.

## 7. Quorum writes

- **Problem:** When is a write durable enough to acknowledge?
- **Chosen approach:** W of RF replicas must fsync and return a matching SHA-256 (default majority). After W acks, stragglers get `-straggler-timeout` and are then cancelled and repaired later.
- **Alternatives:** W=RF (all); W=1 plus async copies; chain replication.
- **Why chosen:** Tail latency is set by the W-th fastest replica, not the slowest, while an ack still means "survives RF-W failures".
- **Benefits:** Writes keep working with one node down; one slow disk does not slow the cluster.
- **Costs:** Acknowledged writes can be under-replicated until repair; the gateway must send RF copies (3x egress).
- **Failure modes:** If W nodes fail before repair, data is lost; W=1 makes that a single failure.
- **Scaling implications:** Gateway NIC egress = RF x ingress; chain replication would move that cost onto the storage network.
- **When another design would be preferable:** Very large writes over constrained gateway links: chain or primary-copy replication (client -> primary -> secondaries) halves gateway egress.

## 8. Strong vs eventual consistency

- **Problem:** What can a reader see after a write?
- **Chosen approach:** Linearizable per key for metadata (single authority per shard, no cache by default); immutable data. Bounded staleness is opt-in via the metadata cache.
- **Alternatives:** Eventual consistency with replicated metadata and conflict resolution (S3 before 2020, Dynamo).
- **Why chosen:** Read-after-write and list-after-write are what users expect and what makes applications simple; S3 moved to strong consistency in 2020 for this reason.
- **Benefits:** No anomalies to explain; no conflict resolution.
- **Costs:** Metadata unavailability during partitions/outages of a shard (CP choice); per-shard write throughput limited by serialized commits.
- **Failure modes:** Shard down = its buckets down.
- **Scaling implications:** Strong consistency per shard scales by sharding; cross-shard operations get no ordering.
- **When another design would be preferable:** Multi-region active-active with partition tolerance for writes: you must accept eventual consistency and resolve conflicts.

## 9. Synchronous replication

- **Problem:** When do replicas receive data?
- **Chosen approach:** Synchronous fan-out of one stream to all RF nodes in parallel; ack after W.
- **Alternatives:** Asynchronous replication after acking the first copy.
- **Why chosen:** An ack must mean durable on W independent nodes.
- **Benefits:** No replica lag; no window where an acknowledged write has a single copy (for W>=2).
- **Costs:** Write latency includes the W-th slowest node's fsync; write availability needs W reachable nodes.
- **Failure modes:** Slow replicas (handled by stall/straggler timeouts).
- **Scaling implications:** Within a region, network latency is small relative to fsync, so synchronous is cheap. Across regions it is not (#34).
- **When another design would be preferable:** Cross-region copies.

## 10. Asynchronous replication

- **Problem:** Some copies need not be on the request path.
- **Chosen approach:** Used only for *repair*: replicas missing after a write (W < RF acks) are created asynchronously by the repairer.
- **Alternatives:** Async for all secondary copies.
- **Why chosen:** Keeps acknowledged durability at W while still converging to RF.
- **Benefits:** Write availability during partial failures.
- **Costs:** A window of reduced redundancy; reconciliation machinery.
- **Failure modes:** Repair backlog grows if failures outpace repair bandwidth.
- **Scaling implications:** Repair bandwidth must be provisioned ([capacity-planning.md](capacity-planning.md#repair-bandwidth)).
- **When another design would be preferable:** Cross-region (#34) is the natural place for async replication.

## 11. Checksums

- **Problem:** Detect corruption in transit, at rest, and in software.
- **Chosen approach:** Two layers. SHA-256 of each segment (end to end: client header, gateway, node, metadata, repair). CRC32C per 64 KiB block, verified on the node before any block's bytes are returned, so range reads are verified without reading the whole blob.
- **Alternatives:** Only TCP checksums (weak, 16-bit); only a whole-object hash (range reads unverified); filesystem checksums (ZFS/btrfs).
- **Why chosen:** SHA-256 identifies content; CRC32C is ~14x faster (measured 21 GB/s vs 1.5 GB/s) and block-local.
- **Benefits:** Corrupt bytes are never served; replica divergence is detectable by comparing one hash.
- **Costs:** SHA-256 costs ~0.7 ms of CPU per MiB per hash on this CPU; it is computed at the gateway and on each replica.
- **Failure modes:** Checksum and data corrupted consistently (sidecar has its own CRC; SHA-256 in metadata is independent of the node).
- **Scaling implications:** At 10 GB/s ingest, SHA-256 alone needs ~7 cores across gateways (plus per replica); hardware SHA extensions reduce this.
- **When another design would be preferable:** Checksums are not authentication. An attacker who can modify data can recompute CRCs and SHA-256 (they are unkeyed). Integrity against an adversary needs a MAC (HMAC with a secret key) or authenticated encryption (AES-GCM) so a modification cannot be made to verify.

## 12. Content addressing

- **Problem:** How are physical blobs named?
- **Chosen approach:** Not content-addressed. Blob ID = SHA-256(bucket, key, version, segment index), unique per write; content SHA-256 stored alongside.
- **Alternatives:** Blob ID = SHA-256(content).
- **Why chosen:** See [architecture.md](architecture.md#content-addressing-considered-not-chosen): names must be known before the upload ends (streaming placement); shared blobs need distributed reference counting; cross-tenant dedup leaks information.
- **Benefits:** Per-write blobs are exclusively owned: deleting a version can only free its own blobs. GC needs a reference *existence* check, not a count.
- **Costs:** No deduplication; identical uploads store twice.
- **Failure modes:** None specific.
- **Scaling implications:** Storage grows with writes, not unique content.
- **When another design would be preferable:** Backup or artifact stores with heavy exact duplication (container layers, VM images): content addressing with refcounts in a strongly consistent store pays off.

## 13. Deduplication

- **Problem:** Avoid storing identical content twice.
- **Chosen approach:** None.
- **Alternatives:** Whole-object dedup via content addressing; block-level (fixed or content-defined chunking) dedup.
- **Why chosen:** Complexity (refcounts, GC races, cross-tenant leaks) outweighs savings for typical object workloads (compressed media, encrypted backups dedupe poorly).
- **Benefits:** Simple, exclusive ownership.
- **Costs:** Storage for duplicates.
- **Failure modes:** n/a. (With dedup: a refcount bug deletes another tenant's data.)
- **Scaling implications:** n/a.
- **When another design would be preferable:** Per-tenant dedup in backup products, where duplication ratios of 10x+ are common.

## 14. Multipart uploads

- **Problem:** A 100 GB upload over one connection fails too often and cannot be parallelised.
- **Chosen approach:** S3-style: initiate, upload parts independently (each part is its own blob with RF replicas and its own SHA-256), complete with an ordered list of part ETags. Retrying a part writes a fresh blob (attempt nonce). Completion is one atomic metadata commit.
- **Alternatives:** Resumable single-stream upload (offset-based, tus/GCS-style); client-side chunking into many objects.
- **Why chosen:** Parallelism and part-level retry; parts land on different nodes, so a large object is spread across the cluster.
- **Benefits:** A failed 10 MiB part costs a 10 MiB retry; uploads can be parallel; reads of a multipart object are spread across nodes.
- **Costs:** Upload state in metadata; abandoned uploads must be expired; composite ETag instead of a whole-object SHA-256.
- **Failure modes:** Abandoned uploads consume space until `-upload-ttl`.
- **Scaling implications:** Max object size = parts x max part size.
- **When another design would be preferable:** Mobile clients on flaky links benefit from byte-offset resumability within a part.

## 15. Streaming vs buffering

- **Problem:** Objects can be larger than memory.
- **Chosen approach:** End-to-end streaming in 256 KiB chunks with bounded per-replica queues; nothing is buffered whole except small objects admitted to the hot-object cache.
- **Alternatives:** Buffer to the gateway's disk then replicate; buffer in memory.
- **Why chosen:** Memory per upload is bounded (`replicas x (queue depth + 2) x chunk`, about 7.5 MiB at defaults); measured gateway RSS stayed under 19 MB while streaming a 1 GiB object.
- **Benefits:** Arbitrary object sizes; TCP flow control propagates backpressure to the client.
- **Costs:** Cannot retry a failed replica write from the start without the client resending (dropped replicas are repaired later instead); the client's speed paces the write.
- **Failure modes:** Slow clients hold slots (bounded by the idle timeout).
- **Scaling implications:** Concurrent uploads x per-upload bound = gateway memory; capped by `-max-inflight-uploads`.
- **When another design would be preferable:** Spooling to gateway disk lets the gateway retry replicas independently and decouples slow clients from storage nodes, at the cost of extra I/O.

## 16. Segment / chunk size

- **Problem:** Large simple PUTs need to be split; buffers need a size.
- **Chosen approach:** Simple PUTs split into 64 MiB segments (independently placed blobs); 256 KiB chunks in flight; 64 KiB CRC blocks.
- **Alternatives:** One blob per object; smaller (4-8 MiB) segments; larger (256 MiB+).
- **Why chosen:** 64 MiB spreads large objects across nodes and bounds repair unit size while keeping per-segment metadata small (a 1 TiB object = 16,384 segments).
- **Benefits:** Repair copies in bounded units; large reads can be served by many nodes.
- **Costs:** More metadata per object; a segment is the unit of loss.
- **Failure modes:** Very small segments inflate metadata; very large ones make repair coarse.
- **Scaling implications:** Metadata size per object = segments x ~150 B.
- **When another design would be preferable:** Erasure-coded classes use stripes, with segment size tied to the stripe width.

## 17. Atomic rename

- **Problem:** A reader must never see a partially written blob.
- **Chosen approach:** Write to `tmp/`, fsync, write and rename the sidecar, rename the data file into place, fsync the directory.
- **Alternatives:** Write in place with a "complete" flag; a log-structured store.
- **Why chosen:** POSIX `rename` within a filesystem is atomic; the visibility point is a single syscall.
- **Benefits:** Crash leaves either nothing or a complete blob; startup recovery is a directory scan.
- **Costs:** Directory fsync per write.
- **Failure modes:** Rename atomicity does not imply durability without the directory fsync.
- **Scaling implications:** Directory contention under very high create rates.
- **When another design would be preferable:** Append-only volumes make visibility an index update.

## 18. fsync policy

- **Problem:** When is a write on stable storage?
- **Chosen approach:** fsync data, sidecar and directory before acking (storage); fsync the WAL on every commit (metadata). Both can be disabled for experiments.
- **Alternatives:** Periodic fsync; rely on replication across nodes for durability ("replicated in memory").
- **Why chosen:** RF copies in page caches are not durable against a correlated power loss.
- **Benefits:** An ack means durable on W disks.
- **Costs (measured):** 1 MiB writes 3x slower (213 vs 639 MB/s), 64 KiB writes 6x slower; metadata commits 21x slower (267 µs vs 13 µs; ~3,700/s per shard).
- **Failure modes:** Disks that lie about cache flushes.
- **Scaling implications:** Group commit (batching concurrent commits behind one fsync) is the standard fix for the metadata WAL; not implemented.
- **When another design would be preferable:** Batteries/NVRAM make fsync nearly free; then the policy matters less.

## 19. Versioning

- **Problem:** Overwrites and deletes lose data; users want history.
- **Chosen approach:** Every write creates a version. Versioned buckets keep all versions; unversioned buckets dereference the previous version on overwrite (its blobs go to the garbage queue).
- **Alternatives:** In-place overwrite; snapshots.
- **Why chosen:** Immutability is also what makes the data plane simple. Versioning costs nothing extra in the data plane.
- **Benefits:** Undelete; audit; overwrite never races with a reader.
- **Costs:** Storage amplification in versioned buckets (N versions = N x size); no lifecycle rule to expire noncurrent versions yet.
- **Failure modes:** Unbounded growth of versioned buckets.
- **Scaling implications:** Versions count toward metadata size and quota.
- **When another design would be preferable:** Append-heavy workloads might prefer appendable objects.

## 20. Tombstones

- **Problem:** Delete must be atomic, replicated and safe for concurrent readers.
- **Chosen approach:** Delete commits a delete marker (a version). Physical deletion happens later.
- **Alternatives:** Immediate physical delete across replicas.
- **Why chosen:** Immediate deletion complicates: **replication** (a node down during delete keeps its copy forever unless tracked); **retries** (a retried delete races with a re-create); **stale replicas** (a returning node resurrects data); **concurrent readers** (a GET streaming the old version breaks); **recovery** (no undo).
- **Benefits:** Delete is one metadata commit; readers holding old blob IDs finish.
- **Costs:** Markers and dereferenced blobs consume space until purged.
- **Failure modes:** Purging markers too early in a replicated metadata system would allow resurrection; here the single-authority shard avoids that.
- **Scaling implications:** Tombstone purge (`-tombstone-ttl`) bounds metadata growth.
- **When another design would be preferable:** Compliance deletion ("right to be forgotten") needs a bounded-time physical delete path.

## 21. Garbage collection

- **Problem:** Reclaim blobs no longer needed without ever deleting live data.
- **Chosen approach:** Two mechanisms. A precise **queue** (metadata enqueues every dereferenced blob with its replica list; GC re-checks the reference after a grace period, deletes, acks). A **mark-and-sweep** over every node's blobs catching everything else (failed writes, stale copies on returned nodes, interrupted moves), deleting only after grace + two consecutive sightings.
- **Alternatives:** Reference counting; only sweep; only queue.
- **Why chosen:** The queue is cheap and precise for the common case; the sweep is the safety net for anything the queue cannot know about.
- **Safety rules:** never act on a single lookup; no answer from metadata means no delete; in-flight writes register "pending" before sending a byte; the grace period covers copies made before metadata lists them.
- **Race: GC vs a new reference.** A blob is enqueued, then (in a versioned world) re-referenced: the re-check after grace sees the reference and drops the queue entry, keeping the data. Repair copies a blob to node X; before metadata lists X, the sweep sees an orphan on X: the grace period and second sighting prevent deletion.
- **Costs:** Delayed reclamation (space); sweep is O(blobs) per pass.
- **Failure modes:** Grace shorter than the longest write or copy window would delete live data; observed in the demo that a returning node's stale copies are deleted and later moved back by the rebalancer, which wastes bandwidth.
- **Scaling implications:** The sweep needs to be incremental and per node at scale.
- **When another design would be preferable:** Content-addressed systems need refcounts or epoch-based GC.

## 22. Repair

- **Problem:** Restore RF after losses.
- **Chosen approach:** Periodic scan of all segments; classify replicas (healthy / lost / pending-down); queue fewest-copies-first; node-to-node pull verified against SHA-256; then update metadata. Repair waits `-repair-delay` after DOWN unless a segment has a single copy. Bandwidth- and concurrency-limited.
- **Alternatives:** Event-driven repair from a per-node index; immediate repair; no delay.
- **Why chosen:** Simple and self-healing: any divergence is found eventually.
- **Benefits:** Prioritizes the most endangered data; avoids repair storms from reboots; the repairer never carries bytes itself.
- **Costs:** O(segments) per pass; reduced-redundancy window = delay + queue time.
- **Failure modes:** Repair storm (many nodes falsely DOWN), repair starvation (bandwidth too low for failure rate), copying from a corrupt source (prevented by SHA-256 verification).
- **Scaling implications:** At billions of segments the scan must be replaced by "list blobs on node X" from a per-node index; repair parallelism must scale with cluster size (every node contributes).
- **When another design would be preferable:** Large clusters: declustered placement groups so a failed node's data is rebuilt from and to many nodes at once.

## 23. Rebalancing

- **Problem:** After membership changes, data is not where placement wants it.
- **Chosen approach:** Copy-before-delete: copy to the desired node, add to metadata (RF+1), remove the old replica from metadata, then delete the old copy. Only fully replicated segments; bounded moves per pass and bandwidth.
- **Alternatives:** Move-then-delete without the metadata step; no rebalancing (only new data goes to new nodes).
- **Why chosen:** Never reduces redundancy; temporary over-replication is the safe intermediate.
- **Benefits:** Adding a node moves only its share (~1/N); drains reuse the same mechanism.
- **Costs:** Extra copy traffic; slow by design.
- **Failure modes:** Oscillation if "desired" depends on fullness (avoided: stable placement ignores fullness).
- **Scaling implications:** Moves proportional to capacity added.
- **When another design would be preferable:** Very heterogeneous clusters need weight-aware and fullness-aware balancing.

## 24. Node draining

- **Problem:** Remove a node without a recovery event.
- **Chosen approach:** Mark DRAINING: no new placements, still readable; the rebalancer moves its replicas off; decommission is refused until `ReplicaCount(node) == 0` unless forced.
- **Alternatives:** Just turn it off and let repair handle it.
- **Why chosen:** An abrupt removal makes every segment on the node under-replicated at once; repair must copy everything urgently while redundancy is reduced. Draining copies while redundancy is still full, at a chosen pace.
- **Benefits:** No durability window; controlled bandwidth.
- **Costs:** Slower; requires operator steps.
- **Failure modes:** A draining node that fails mid-drain becomes a normal failure.
- **Scaling implications:** Drain time = node data / rebalance bandwidth.
- **When another design would be preferable:** n/a; abrupt removal is always worse when there is a choice.

## 25. Failure domains

- **Problem:** Copies on the same rack/zone fail together.
- **Chosen approach:** Nodes carry region, zone and rack. Placement prefers distinct zones, then distinct racks, then any.
- **Alternatives:** CRUSH-style hierarchical rules; ignoring topology.
- **Why chosen:** The cheapest defence against correlated failures.
- **Benefits:** RF 3 over 3 zones survives a zone loss.
- **Costs:** Cross-zone write bandwidth (paid per GB in clouds); load skew when zones are unequal (observed: single-node zones took 2x the write bytes).
- **Failure modes:** If zones are imbalanced in capacity, the small zone fills first and writes degrade to fewer zones.
- **Scaling implications:** Capacity must be planned per zone.
- **When another design would be preferable:** Latency-sensitive single-zone deployments accept rack-level diversity only.

## 26. Hot-object caching

- **Problem:** One object at 100,000 GET/s overwhelms the three nodes that hold it.
- **Chosen approach:** Gateway LRU of small objects (default <= 1 MiB, 256 MiB total) keyed by version ID, filled with single-flight.
- **Alternatives:** Increase RF for hot objects; CDN; node-side cache only (page cache).
- **Why chosen:** Immutable versions make invalidation unnecessary; single-flight prevents stampedes.
- **Benefits:** Measured 99.6% hit rate on a hot workload; backend traffic dropped to ~0.4% of client traffic.
- **Costs:** Memory per gateway; each gateway has its own cache (N gateways = N cold misses).
- **Failure modes:** Large hot objects are not cached (by design); cache cold after restarts.
- **Scaling implications:** Throughput for a hot object scales with gateways.
- **When another design would be preferable:** Large hot objects: CDN or temporary extra replicas.

## 27. LRU caching

- **Problem:** Which entries to keep.
- **Chosen approach:** Cost-aware LRU (bytes), optional TTL, mutex-protected (measured 49 ns per parallel Get).
- **Alternatives:** LFU/TinyLFU (better hit rate under scans), CLOCK, sharded LRU.
- **Why chosen:** Simple and predictable.
- **Benefits:** Bounded memory by bytes.
- **Costs:** A large scan evicts the hot set (no scan resistance); one lock.
- **Failure modes:** Thrashing when the working set exceeds capacity.
- **Scaling implications:** Lock contention at very high core counts: shard the cache.
- **When another design would be preferable:** Mixed workloads with scans: W-TinyLFU.

## 28. Metadata caching

- **Problem:** Every GET costs a metadata RPC.
- **Chosen approach:** Optional gateway cache of latest-version lookups with TTL; local invalidation on writes; can serve during metadata outages.
- **Alternatives:** No cache (default); leases/invalidation pushed from metadata.
- **Why chosen:** Off by default to keep read-after-write across gateways; available when load or availability matters more.
- **Benefits:** Fewer metadata RPCs; reads survive short metadata outages.
- **Costs:** Bounded staleness across gateways.
- **Failure modes:** Serving a deleted object for up to TTL.
- **Scaling implications:** Reduces metadata read load by the hit rate.
- **When another design would be preferable:** Leases (metadata promises not to change a key without notifying holders) give caching without staleness at the cost of server-side state.

## 29. Rate limiting

- **Problem:** One tenant can starve others.
- **Chosen approach:** Per-tenant token buckets for requests/s, upload bytes/s and download bytes/s, plus concurrency semaphores for uploads and downloads, a max object size and a storage quota.
- **Alternatives:** Only requests/s; fair queuing.
- **Why chosen:** **Request rate and bandwidth are separate dimensions.** 10 requests/s of 1 GB objects is 10 GB/s; a request limit alone would allow it. Bandwidth is charged per byte as it flows (throttled reader/writer), so a large request is slowed, not rejected.
- **Benefits:** Noisy-neighbour protection along each resource.
- **Costs:** Limits are per gateway (N gateways admit N x).
- **Failure modes:** Burst sizes set too low make large objects slow to start.
- **Scaling implications:** A global limiter needs a shared counter service or quota leasing.
- **When another design would be preferable:** Weighted fair queuing for best-effort sharing instead of hard caps.

## 30. Backpressure

- **Problem:** A slow component must not cause unbounded memory growth.
- **Chosen approach:** Bounded queues everywhere: per-replica chunk queue (write waits for the slowest live replica, or drops it after the stall timeout); per-write deadlines to slow clients; global and per-tenant upload slots (503/429 when full); repair and rebalance token buckets; scrub rate limit.
- **Alternatives:** Unbounded buffering; dropping work silently.
- **Why chosen:** Memory is bounded by configuration, and overload becomes explicit errors clients can retry.
- **Benefits:** Measured: a 24 MiB PUT with one slow replica finished in 0.3 s at 2/3 acks instead of waiting for the slow node.
- **Costs:** More errors (429/503) under load; tuning.
- **Failure modes:** Stall timeout too short drops healthy-but-busy replicas. A slow-but-alive node stays ACTIVE, so without memory every new segment re-discovers it and pays the full timeout: measured 41.6 s for a 200 MB upload. The gateway therefore penalizes nodes that stalled or straggled for `-slow-node-penalty` (30 s), avoiding them for new placements (6.9 s after the fix). When too few unpenalized nodes remain, penalized nodes are re-admitted fewest-strikes first rather than all at once: the first version fell back to unrestricted placement, and a single false-positive straggler on a 4-node CI cluster put the slow node back into every placement. It is a per-gateway circuit breaker, and it never reduces the replica count.
- **Scaling implications:** Client backpressure (TCP), storage backpressure (queues), replication backpressure (stall drops), repair backpressure (bandwidth budget) compose independently.
- **When another design would be preferable:** n/a.

## 31. Tenant quotas

- **Problem:** Bound each tenant's storage.
- **Chosen approach:** Usage tracked per tenant per shard in metadata; the gateway passes a per-shard ceiling (global quota minus cached usage on other shards); the shard enforces it atomically in the commit.
- **Alternatives:** Gateway-only checks (racy); a central usage counter (bottleneck).
- **Why chosen:** Exact within a shard without cross-shard coordination.
- **Benefits:** No coordination on the write path.
- **Costs:** Soft across shards (overshoot bounded by concurrent commits within the 2 s usage cache window).
- **Failure modes:** Large concurrent uploads to buckets on different shards can overshoot.
- **Scaling implications:** Reserve-then-commit with per-shard quota leases makes it exact.
- **When another design would be preferable:** Billing-grade enforcement.

## 32. Pagination

- **Problem:** A bucket can hold billions of keys.
- **Chosen approach:** Ordered listing with opaque continuation tokens (a key position), max 1000 per page, at most 10x page size entries scanned per call.
- **Alternatives:** Offset pagination; snapshot listing.
- **Why chosen:** Stable cost per page regardless of bucket size; no server state between pages.
- **Benefits:** Bounded work and memory per request.
- **Costs:** No snapshot across pages.
- **Failure modes:** Pages with many delete markers can return few entries.
- **Scaling implications:** A range-sharded bucket would merge-scan shards.
- **When another design would be preferable:** Analytics over all keys: an inventory export.

## 33. Metadata sharding

- **Problem:** One metadata server cannot hold billions of objects or absorb all commits.
- **Chosen approach:** `hash(bucket) mod S` with a fixed S; upload IDs embed the bucket for routing.
- **Alternatives:** Range sharding on (bucket, key) with splits (Bigtable/Spanner/FoundationDB); consistent hashing on (bucket, key).
- **Why chosen:** Simplest routing; LIST stays single-shard.
- **Benefits:** Per-bucket operations never cross shards.
- **Costs:** A hot or huge bucket is bounded by one shard; S cannot change without migration.
- **Failure modes:** Hot shard.
- **Scaling implications:** See [metadata-scaling.md](metadata-scaling.md).
- **When another design would be preferable:** Any production scale: range sharding with automatic splitting.

## 34. Multi-region replication

- **Problem:** Survive a region loss.
- **Chosen approach:** Not implemented; designed in [multi-region.md](multi-region.md): asynchronous replication of committed versions to a remote region.
- **Alternatives:** Synchronous cross-region (latency of the slowest region per write).
- **Why chosen (design):** Synchronous cross-region adds tens to hundreds of ms per write.
- **Benefits:** Region-local write latency.
- **Costs:** RPO > 0 (replication lag).
- **Failure modes:** Lag growth; conflicting writes in active-active.
- **Scaling implications:** Inter-region bandwidth cost.
- **When another design would be preferable:** Zero-RPO requirements justify synchronous replication for a subset of buckets.

## 35. Active-active vs active-passive

- **Problem:** Which regions accept writes?
- **Chosen approach (design):** Active-passive per bucket (a bucket has a home region), with read replicas elsewhere.
- **Alternatives:** Active-active with conflict resolution.
- **Why chosen:** Keeps the single-authority-per-key property; failover is a controlled change of home region.
- **Benefits:** No conflicts.
- **Costs:** Remote writers pay cross-region latency; failover RTO.
- **Failure modes:** Failover with lag loses the un-replicated tail (RPO).
- **Scaling implications:** Simple to reason about.
- **When another design would be preferable:** Globally distributed writers that cannot tolerate cross-region latency: active-active with last-writer-wins by hybrid logical clock, or keeping both as versions.

## 36. Erasure coding vs replication

- **Problem:** Replication costs 3x raw storage.
- **Chosen approach:** Replication only. Erasure coding is analysed here, not implemented.
- **Alternatives:** Reed-Solomon RS(k, m): split a stripe into k data fragments and compute m parity fragments; any k of k+m reconstruct the data.

### Comparison (per logical byte)

| | RF 3 | RS(6,3) | RS(10,4) |
|---|---|---|---|
| Raw storage | 3.0x | 1.5x | 1.4x |
| Failures tolerated | 2 | 3 | 4 |
| Nodes touched by a full read | 1 | 6 | 10 |
| Bytes read to repair one lost fragment | 1x the fragment | 6x | 10x |
| CPU on write | none beyond checksums | encode | encode |
| Degraded read (one fragment missing) | read another copy | read k fragments + decode | read k + decode |

- **Storage efficiency:** RS(10,4) stores 10 PB in 14 PB raw vs 30 PB with RF 3 and tolerates more failures.
- **CPU cost:** Encoding with SIMD runs at multiple GB/s per core; small compared to network but non-zero, and decoding on degraded reads adds latency.
- **Network cost:** Writes send (k+m)/k x the data (1.4x) instead of 3x: EC is *cheaper* on write bandwidth. Repair is far more expensive: rebuilding one 64 MiB fragment of RS(10,4) reads 10 fragments (640 MiB) across 10 nodes.
- **Repair complexity:** Repair is a distributed computation, not a copy; placement must keep k+m fragments on distinct failure domains (14 domains for RS(10,4)).
- **Small objects:** A 4 KiB object split into 10 fragments + 4 parity creates 14 tiny blobs, 14 metadata entries and 14 I/Os: overhead dominates. Small objects should be replicated, or packed into large containers that are then erasure coded.
- **Large objects:** Ideal: fragments are large, sequential, and reads can be parallel across k nodes.
- **Read performance:** Healthy reads of a byte range can still touch a single fragment if the layout is contiguous per fragment; full-object reads fan out to k nodes (more parallelism, more tail-latency exposure).
- **Degraded reads:** A missing fragment means fetching k others and decoding: higher latency and k x read amplification during failures.

**Why production systems mix strategies:** a typical design writes new objects
replicated (fast writes, cheap degraded reads, fast repair, good for small and
hot data) and converts objects that are large and cold to erasure coding in the
background (lifecycle transition). Storage classes express this:
STANDARD = RF 3, a cold class = RS(10,4), with a minimum object size or
packing for the EC class.

- **When replication is preferable:** small objects; hot data; low node counts (fewer than k+m failure domains); when repair speed dominates risk.
- **When EC is preferable:** large, cold data at scale, where 1.4x vs 3x is the difference of tens of PB of hardware.
- **How this codebase would add it:** a new storage class whose segments carry `k+m` fragment references instead of a replica list; the gateway encodes per segment; the repairer reconstructs a missing fragment by reading k; placement asks for k+m distinct domains. Migration from replication to EC is a background rewrite of segments followed by a metadata swap and GC of the old replicas.

---

# Final Tradeoff Review

## 10x Scale

(40 nodes, ~100 M objects.) The design holds with configuration changes:
more metadata shards (S fixed at creation, so choose it for the target
scale); group commit in the WAL; repair and GC scans take minutes rather
than seconds; the gateway fan-out moves 3x ingress over gateway NICs, so add
gateways. Rebalancing after adding nodes runs for hours at the default
bandwidth caps.

## 100x Scale

(400 nodes, ~1 B objects, ~10 PB.) Breaks without redesign:
- Metadata in memory: about 0.5-1 TB of state across shards; a shard must become an LSM-backed store, replicated (Raft).
- Full scans for repair and GC are too slow and too heavy: need a per-node blob index so a node failure enqueues exactly its blobs, and incremental sweeps.
- Fixed bucket-hash sharding: large buckets need range sharding.
- Single coordinator: replicate it; heartbeat fan-in of 400 nodes is fine, but the snapshot should be versioned and pushed rather than polled.

## 1,000x Scale

(4,000 nodes, ~10 B objects, ~100 PB.) A different system in the details:
- Erasure coding for cold data is mandatory economically (100 PB at 3x is 300 PB raw).
- Placement groups instead of per-segment placement, so failures and moves are tracked per group, not per blob.
- Multiple disks per node managed individually; repair declustered across thousands of disks.
- Metadata is a distributed database in its own right (range-sharded, Raft per range, automatic splitting and balancing).
- Multi-region.

## 10 PB

What becomes difficult: raw capacity (30 PB at RF 3: ~1,900 x 20 TB disks at 80% fill); disk failures become routine (~28 per year, one every two weeks, at 1.5% AFR); repair bandwidth for a whole node (hundreds of TB) takes many hours; metadata for billions of objects. See [capacity-planning.md](capacity-planning.md).

## 100 PB

What becomes difficult: the cost of replication (EC required); correlated failures across thousands of disks; datacentre network (repair and rebalance traffic must not collide with client traffic: QoS classes); metadata in the tens of TB; operational tooling (rolling upgrades across thousands of nodes, automatic remediation).

## What Breaks First?

In order, for this implementation:
1. **Metadata shard commit throughput** (serialized commits with one fsync each: ~3,700/s per shard measured). Small-object write workloads hit this first.
2. **Metadata memory** (all state in RAM per shard).
3. **Repair/GC full scans** (O(segments) per pass).
4. **Gateway egress** (RF x ingress) for write-heavy large-object workloads.
5. **Filesystem inode/directory scaling** on nodes with many small objects.

## What Must Be Sharded?

- Metadata (by bucket today; by (bucket, key) range at scale).
- Garbage queues and usage counters (already per shard).
- The repair and GC work (by metadata shard and by node).
- Data (already sharded by placement).

## What Must Be Replicated?

Critical state that cannot be reconstructed:
- **Metadata shards** (versions, segment lists, replica lists, uploads, usage). Not replicated in this project; the top production requirement.
- **Blobs** (RF copies).
- **Operator node states** (DRAINING/DECOMMISSIONED) persisted by the coordinator.

State that can be **reconstructed** and need not be replicated: heartbeats and node liveness (soft state), all caches, membership snapshots, repair queues, rebalance plans, scrub progress, the garbage queue can be rebuilt by a sweep.

## What Can Be Eventually Consistent?

- Membership views at gateways and workers (metadata records actual replicas).
- Replica lists converging to RF (repair).
- Placement converging to the hash (rebalance).
- Physical deletion (GC).
- Usage/quota across shards.
- Metrics, cache contents, cross-region copies.

## What Requires Stronger Guarantees?

- The latest-version pointer per key (linearizable).
- Multipart completion (atomic).
- Replica list updates relative to deletes (never delete before unpublishing).
- Pending registrations relative to GC.
- Quota within a shard.

## What Would I Replace?

| Educational component | Mature replacement |
|---|---|
| In-memory metadata shard + WAL | FoundationDB, TiKV, CockroachDB/Spanner, or Raft-replicated Pebble per range |
| Single coordinator | etcd / the metadata store itself |
| Shared cluster token | mTLS with per-service identities (SPIFFE) |
| Span logging | OpenTelemetry SDK and collector |
| Hand-written Prometheus exposition | prometheus/client_golang |
| One-file-per-blob engine | append-only volume files with an index (for small objects) |
| Tenant file | identity provider + policy engine |

## What Would I Keep?

- Separation of metadata and data planes.
- Immutable, per-write blob IDs (no stale replicas, no cache invalidation, resumable failover).
- Metadata as the commit point; ordering rules that make orphans and lost replicas the only possible divergences.
- Copy-before-delete for every move; delayed, re-checked deletion.
- Bounded queues and explicit 429/503 instead of unbounded buffering.
- Failure-domain-aware placement with recorded replica lists.

## What Should NOT Be Optimized Yet?

- Erasure coding before there are cold, large objects and enough failure domains.
- Direct I/O / custom allocators before the filesystem is shown to be the bottleneck.
- Zero-copy networking: the measured bottleneck is fsync and SHA-256, not memcpy.
- Global rate limiting before per-gateway limits are shown to be insufficient.
- Content addressing / dedup without evidence of duplication.
- Fancy cache algorithms before measuring hit rates on real traffic.
