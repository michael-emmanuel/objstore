# Design Evolution

How the design gets from "an HTTP server that saves files" to a distributed
storage system, one problem at a time. Each stage names the problem that
forced it, the change, what it buys, and what it costs, including the new
failure modes it introduces. The final code contains all stages; package
names show where each lives.

---

## V1: single-node filesystem object store

- **Original problem:** store and retrieve named byte blobs over HTTP.
- **Architectural change:** one process; `PUT /key` writes `data/<key>`.
- **Benefit:** trivial.
- **Complexity introduced:** none.
- **Failure modes introduced:** key `../../etc/passwd` writes outside the data dir; a crash mid-write leaves a half file that GET serves; the disk is a single point of failure; whole objects buffered in memory.
- **What survives into the final design:** the storage engine idea, but with keys never used as paths (`internal/object` blob IDs), tmp + fsync + rename commit, and streaming (`internal/storage`).

## V2: metadata abstraction

- **Original problem:** V1 conflates "what exists" with "where bytes are". Overwrite is a destructive in-place write; there is no checksum, size, content type or version; listing walks the filesystem.
- **Architectural change:** a metadata record per object (size, checksum, content type, blob reference) behind a `Store` interface; bytes stored as opaque blobs named by IDs, not keys.
- **Benefit:** atomic visibility (commit the record after the blob is complete); listing is an index scan; path traversal impossible by construction.
- **Complexity introduced:** two stores that can disagree.
- **Failure modes introduced:** orphaned blobs (blob written, metadata commit failed); dangling references (metadata points at a missing blob). The ordering rule "blob first, then metadata; unpublish before delete" makes only orphans possible, and orphans need GC.

## V3: multiple storage nodes

- **Original problem:** one disk is neither big enough nor durable enough.
- **Architectural change:** storage nodes as separate processes (`internal/storagenode`); a gateway that routes; metadata records which node holds each blob.
- **Benefit:** capacity scales with nodes.
- **Complexity introduced:** network failures, partial failures, a placement decision per blob, node identity and membership (`internal/membership`).
- **Failure modes introduced:** a node down makes its blobs unavailable; the network can fail mid-transfer; nodes can be slow rather than dead.

## V4: consistent hashing

- **Original problem:** the first placement function, `hash % N`, remaps ~N/(N+1) of keys whenever N changes (measured 90.7% for 10 -> 11 nodes).
- **Architectural change:** a ring with virtual nodes (`internal/hashing`) produces a preference list per blob ID; rendezvous implemented for comparison.
- **Benefit:** adding a node moves ~1/(N+1) of data (measured 8.8%).
- **Complexity introduced:** vnode tuning for balance; weights.
- **Failure modes introduced:** components with different membership views compute different placements. Resolved by recording actual replicas in metadata, so placement is an intention, not a lookup path.

## V5: replication

- **Original problem:** a single node failure loses data.
- **Architectural change:** RF copies on distinct zones/racks (`internal/placement`); a fan-out writer streams one body to RF nodes in parallel with bounded memory (`internal/replication`).
- **Benefit:** durability and read availability through node failures.
- **Complexity introduced:** RF x write bandwidth; what to do when some replicas fail; replica lists in metadata.
- **Failure modes introduced:** partial replication (2 of 3 succeeded), slow replicas stalling every write, replicas diverging silently (addressed by per-write immutable blob IDs and SHA-256 per segment).

## V6: quorum writes and reads

- **Original problem:** requiring all RF acks makes every write as slow as the slowest disk and fails writes whenever any replica is down; requiring one ack loses data on a single failure.
- **Architectural change:** W-of-RF acknowledgement with straggler and stall timeouts (`internal/quorum`, `internal/replication`); optional R-replica read verification. Version resolution stays in metadata, so R is about verification, not freshness.
- **Benefit:** tail latency set by the W-th replica; writes survive RF - W failures.
- **Complexity introduced:** acknowledged-but-under-replicated segments.
- **Failure modes introduced:** the window between ack and full replication; operators must understand W vs RF.

## V7: repair

- **Original problem:** after V5/V6, replicas get lost (dead nodes, corrupt blobs, W < RF writes) and nothing restores them.
- **Architectural change:** failure detection by heartbeats with SUSPECT/DOWN thresholds; quarantine of corrupt blobs; a scrubber; a repairer that scans metadata, prioritizes segments with fewest copies, waits a delay for DOWN nodes, and copies node-to-node with checksum verification (`internal/repair`).
- **Benefit:** the system converges back to RF on its own.
- **Complexity introduced:** reconciliation loops, bandwidth budgets, priorities.
- **Failure modes introduced:** repair storms from false DOWN detections; repair traffic overloading survivors; copying corrupt data (prevented by verification); repair racing with deletes (copies become orphans).

## V8: rebalancing (and draining)

- **Original problem:** new nodes receive only new data; old nodes stay full. Removing a node abruptly triggers a recovery event.
- **Architectural change:** a rebalancer that moves replicas toward the placement function with copy-before-delete and temporary over-replication; DRAINING state; decommission refused until drained (`internal/rebalancer`).
- **Benefit:** balanced fill; planned removals with no redundancy window.
- **Complexity introduced:** moves concurrent with reads, writes, repair and GC.
- **Failure modes introduced:** oscillation (avoided by fullness-independent "stable" placement); moving data back to a node whose stale copies GC just deleted (observed; production re-adopts verified copies).

## V9: multipart uploads

- **Original problem:** a 100 GB upload over one connection fails too often and restarts from zero.
- **Architectural change:** upload sessions in metadata; each part is an independently replicated blob with its own checksum; atomic completion (`internal/api/multipart.go`, metadata upload records).
- **Benefit:** part-level retry and parallel upload; parts spread over the cluster.
- **Complexity introduced:** upload state; composite ETags; minimum part size.
- **Failure modes introduced:** abandoned uploads consuming space (expired by GC); retried parts leaving garbage (attempt nonces + garbage queue).

## V10: versioning

- **Original problem:** overwrite and delete destroy data; physically deleting immediately races with readers and replicas.
- **Architectural change:** every write is a version ordered by commit sequence; deletes are delete markers; dereferenced blobs go to a garbage queue; GC with grace periods and two-sighting sweeps (`internal/garbagecollector`).
- **Benefit:** undelete, history, safe overwrite under concurrent readers.
- **Complexity introduced:** storage amplification; lifecycle (tombstone purge, upload expiry).
- **Failure modes introduced:** GC deleting live data if its safety rules are wrong; unbounded growth of versioned buckets.

## V11: caching

- **Original problem:** a hot object saturates its three nodes; every GET costs a metadata RPC.
- **Architectural change:** gateway LRU of small objects keyed by immutable version ID with single-flight; optional metadata cache with TTL (`internal/cache`).
- **Benefit:** measured 99.6% hit rate on a hot workload; stampedes coalesced.
- **Complexity introduced:** cache sizing; staleness of the metadata cache.
- **Failure modes introduced:** bounded-stale reads across gateways (metadata cache only); cold-cache load spikes after restarts.

(Alongside V11: tenant isolation, i.e. per-tenant token buckets for requests
and bandwidth, concurrency limits and quotas in `internal/ratelimit` and
`internal/quota`, and observability in `internal/observability`.)

## V12: multi-region (conceptual)

- **Original problem:** a region failure loses everything; distant users pay latency.
- **Architectural change (designed, not built):** asynchronous replication of committed versions to other regions with a per-bucket home region; see [multi-region.md](multi-region.md).
- **Benefit:** region-level durability and read locality.
- **Complexity introduced:** replication lag, failover, conflict handling for active-active.
- **Failure modes introduced:** data loss bounded by lag (RPO) on failover; split-brain if two regions accept writes for the same bucket without conflict resolution.

---

## What the sequence teaches

Each stage fixes a real failure of the previous one and introduces a new
class of failure. The final shape (immutable blobs, a single metadata
authority per key, reconciliation loops for everything else) is what makes
the later stages tractable: repair, rebalancing, GC and caching are all simple
because data never changes after commit.
