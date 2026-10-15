# Consistency Model

This document states exactly what a client can observe, operation by
operation, and the assumptions each guarantee depends on. It avoids the word
"strong" except where the implementation provides linearizability, and it
names the configuration that weakens each guarantee.

## The one idea that determines everything

**Bytes are immutable; only metadata changes.** Every write creates new blobs
with IDs unique to that write. A blob is never modified after its commit, and
its SHA-256 is recorded in metadata. Therefore:

- A data replica can be *missing* or *corrupt* (detected by CRC/SHA-256) but
  never *stale*. There is no "old value" of a blob.
- Every question of the form "which value is current?" is answered by the
  metadata plane, and only there.

Each metadata shard is a single process that applies mutations one at a time
under an exclusive lock, assigns a sequence number (`Seq`), appends to its WAL
and fsyncs before acknowledging. For the keys of one bucket, that is a
single-copy linearizable register: every operation takes effect at one
instant between its invocation and its response.

That guarantee rests on assumptions, stated plainly:

1. **One authority per shard.** There is exactly one process per shard. There
   is no replication of the shard, so there is no failover and no possibility
   of two leaders. The price is availability: if the shard is down, its
   buckets are unavailable (section "Failures").
2. **The WAL is honest.** `fsync` returns only when data is on stable storage.
   With `-fsync=false`, a crash can lose acknowledged commits (durability loss,
   and after restart a client can observe an older version than one it was
   told was committed).
3. **The gateway metadata cache is disabled** (`-metadata-cache-ttl=0`, the
   default). With a TTL, see "Caching".

## Per-operation guarantees (default configuration)

| Operation | Guarantee |
|---|---|
| PUT | Atomic: the new version becomes visible all at once at the metadata commit, or not at all. A 200 response means the version is committed and each segment is durable on at least W replicas. |
| GET / HEAD | Returns the latest committed version at the moment metadata is read (linearizable read of the version pointer). Bytes are then read from immutable blobs, so the body always matches that version's checksum. |
| GET ?version_id | Returns exactly that version, forever, until it is permanently deleted. |
| DELETE | Commits a delete marker atomically. Subsequent GETs return 404. |
| DELETE ?version_id | Removes that version from metadata atomically. Its blobs are reclaimed later. |
| LIST | Each page is a consistent view of the shard at one instant. Pagination is not a snapshot (see below). |
| Multipart complete | Atomic: the assembled object appears at once; until then nothing is visible under the key. |

### Read-after-write

After a PUT returns 200, **any** GET through **any** gateway observes that
version or a later one. Reason: all gateways read the version pointer from the
same single authoritative shard, which applied the commit before responding.

Two caveats that are not specific to this system but must be said:

- **Ambiguous outcome.** If the PUT response is lost (timeout, gateway crash
  after commit), the client cannot know whether the commit happened. The
  gateway mitigates one source of this: once data is durable, the commit runs
  on a context detached from the client connection, so a client disconnect at
  that moment does not abort an almost-finished commit. Retrying a PUT is safe
  (it creates another version with the same content), but it is not
  idempotent in the sense of "exactly one version".
- **Concurrent writers.** Two PUTs to the same key both write their own blobs;
  the shard serializes their commits. Commit order (`Seq`), not client
  timestamps, decides which is latest. The "loser" becomes a noncurrent
  version (versioned bucket) or garbage (unversioned bucket). Neither client
  sees a mix of both bodies (`TestConcurrentOverwritesAreAtomic`).

### Overwrite consistency

A GET that has already resolved version `v1` keeps streaming `v1` even if
`v2` is committed mid-stream: it holds blob IDs, not a key. Blobs of a
superseded version are only physically deleted after the GC grace period
(default 1 h), and an open file on a storage node keeps working after unlink
(POSIX semantics). A sufficiently long-running download that outlives the
grace period *and* fails over to another replica after that replica's copy was
deleted would fail rather than return wrong data.

### Delete consistency

Delete is a metadata commit, so it has the same visibility guarantee as PUT.
Physical deletion is deliberately delayed and is invisible to clients.

### Listing consistency

- A key committed before a LIST request starts is included (if it matches the
  prefix and is not deleted); a key deleted before is excluded.
- Each page is computed under the shard's read lock: within a page there are no
  torn results.
- **Across pages there is no snapshot.** The continuation token is a key
  position. A key inserted behind the cursor after page 1 will not appear; a
  key inserted ahead of it will. A key is never returned twice, and keys are
  always in ascending order.
- To bound the cost of one request, a page scans at most 10 x `max_keys`
  entries (skipping delete markers); it may return fewer entries than
  requested with `is_truncated=true`.

### HEAD

Identical to GET without the body. It shares the metadata lookup and cache.

## Replica lag

Replication is **synchronous** for the W replicas that acknowledge a write.
There is no asynchronous replica stream to lag behind. Replicas that did not
acknowledge (slow, failed, dropped as stragglers) are *absent*, not lagging:
metadata lists only the replicas that acknowledged, readers never ask the
others, and the repairer creates the missing copies by node-to-node copy
verified against the SHA-256 in metadata.

The observable form of "replica lag" is therefore **under-replication**:
`gateway_under_replicated_writes_total` and `repair_under_replicated_segments`.

## Quorum reads and why R + W > N is not what provides consistency here

In a Dynamo-style system (Cassandra, Riak) each replica holds a mutable value
with a version, and R + W > N guarantees that a read quorum overlaps the latest
write quorum in at least one replica, which then has the newest version. That
argument has assumptions that are easy to forget: no sloppy quorum / hinted
handoff, a correct "newest" resolution (clocks or vector clocks), and the same
N nodes on both sides even during membership changes.

This system takes a different route:

- **W is a durability threshold.** A PUT is acknowledged only after W replicas
  durably committed matching bytes. With W=2 of 3, an acknowledged write
  survives any single node loss.
- **Version resolution is not done by replicas at all.** It is done by the
  single metadata authority. That is why read-after-write holds even with R=1.
- **R is used for read verification.** `GET ?consistency=quorum` requires R
  replicas of each segment in range to confirm the expected checksum (HEAD)
  before serving. It detects missing or quarantined replicas at read time, at
  the cost of one extra round trip per replica. It does not change which
  version is returned.

In short: we moved the hard part of the consistency problem into a small,
single-writer metadata service, and kept the byte-heavy plane free of
coordination. The cost of that choice is that metadata availability is the
system's availability ([distributed-consistency.md](distributed-consistency.md)).

## Caching

| Cache | Key | Can it be stale? |
|---|---|---|
| Hot-object cache (gateway) | version ID | No. Versions are immutable. |
| Metadata cache (gateway, opt-in) | bucket + key -> latest version | Yes, up to `-metadata-cache-ttl` for writes made through **other** gateways. The writing gateway invalidates its own entry, so read-your-writes holds through the same gateway. |
| Bucket config cache (with metadata cache) | bucket | Yes, up to 5 x TTL. |
| Membership snapshot | epoch | Yes, up to the poll interval; affects placement and replica choice, never correctness of data. |

With `-metadata-cache-ttl=T > 0` the guarantees become:

- read-your-writes per gateway;
- across gateways, an overwrite or delete becomes visible within T
  (bounded staleness);
- during a metadata outage, a gateway may serve cached versions (stale but
  internally consistent). This is the deliberate availability trade.

## Stale metadata

Metadata can be out of date with respect to the physical world in two ways,
both safe by design:

| Divergence | Example | Effect | Converged by |
|---|---|---|---|
| Metadata lists a replica that no longer exists | disk died; blob quarantined | a read tries it, fails, fails over | repairer (adds new replica, drops lost one) |
| A blob exists that metadata does not reference | failed write; node returned after repair; interrupted move | wastes space only | GC (queue and two-sighting sweep) |

The dangerous divergence, metadata referencing a blob that was never durably
written, is prevented by ordering: blobs are committed on W nodes *before*
metadata references them, and metadata drops a replica *before* the copy is
deleted.

## Failures and consistency

| Situation | Behaviour |
|---|---|
| Metadata shard unreachable | Its buckets: all operations fail with 503 (unless served from the opt-in metadata cache). Consistency is preserved by refusing to answer. |
| All replicas of a segment unreachable | GET fails with 503 `DataUnavailable`. Never a partial or wrong body: an in-progress response is aborted so the client sees a truncated transfer, not a short "successful" one. |
| Fewer than W writable nodes | PUT fails with 503 `InsufficientReplicas`; the previous version stays current. |
| Coordinator unreachable | Gateways keep the last membership snapshot. Placement may target a node that has since died; the write then needs W of the remaining targets. |

## What is NOT guaranteed

- No multi-key transactions or atomic rename of keys.
- No conditional writes (`If-Match` on PUT). Clients cannot do compare-and-swap on a key.
- No global ordering across shards (buckets on different shards are independent).
- Storage quota is exact within a shard but soft across shards: the gateway derives a per-shard ceiling from cached usage of other shards (refreshed every 2 s), so concurrent uploads to buckets on different shards can overshoot by the bytes committed within that window.
- Rate limits are per gateway: N gateways admit up to N times a tenant's configured rate.
