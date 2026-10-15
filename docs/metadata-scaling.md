# Metadata Scaling

Object storage scales horizontally in bytes almost for free: add nodes,
placement spreads new blobs, rebalancing moves a fraction of old ones. The
metadata plane does not get that for free. This document explains why, what
this implementation does, and what production would do.

## Why metadata becomes the bottleneck

1. **It scales with object count, not bytes.** A 4 KB object and a 4 GB
   object each cost one version record. 1 billion objects x 500 B = 500 GB of
   metadata before indexes, replication and storage-engine overhead. With
   small objects, metadata can outgrow what one machine holds long before the
   data plane is large.
2. **Every request touches it.** A GET needs the version pointer and segment
   list; a PUT commits a version; a LIST is pure metadata. Data requests can
   be spread over hundreds of nodes; metadata requests for one key all go to
   one owner.
3. **It needs ordering.** "Which version is latest" requires a single
   serialization point per key. Ordering is coordination, and coordination
   does not parallelize within its scope.
4. **Background work multiplies it.** Repair, rebalancing and GC read every
   segment record per pass in this implementation.

## What this implementation does

```
         gateway / workers
               |
     metadata.Router  -- hash("bucket:"+name) mod S --+
               |                                       |
   +-----------+-----------+-------------+             |
   | shard 0   | shard 1   | ... shard S-1             |
   | (+ coord) |           |                           |
   +-----------+-----------+-------------+             |
   each shard: in-memory maps + sorted key index per bucket,
               WAL (CRC-framed, fsync per commit), snapshot every N records,
               exclusive lock for commits, shared lock for reads
```

| Aspect | Implementation | Consequence |
|---|---|---|
| Partitioning | `hash(bucket) mod S`, S fixed | A bucket never spans shards: LIST is one ordered scan, and per-object ops touch one shard. A huge or hot bucket is bounded by one shard. Changing S requires migrating metadata. |
| Upload routing | upload ID = `<bucket>~<random>` | Any gateway routes multipart calls without a global directory. |
| Storage | in memory; WAL + periodic snapshot; deterministic replay of effect records | Recovery time grows with WAL length (bounded by `-compact-every`); capacity bounded by RAM. |
| Concurrency | commits serialized per shard; reads concurrent | Measured: ~3,700 commits/s with fsync, ~78,000/s without; 0.48 µs per parallel read. |
| Key index | sorted slice per bucket, binary search | Ordered LIST and prefix scans; insertion is O(n) memmove, acceptable to millions of keys per bucket, not billions. |
| Global operations | fan-out to all shards (`ScanSegments`, garbage queue, usage, stats) | Repair/GC cost grows with total metadata. |
| Replication | **none** | A shard is a single point of failure for its buckets. |

## Metadata sharding options

| Strategy | Pros | Cons | Used by |
|---|---|---|---|
| Hash(bucket) | trivial routing; LIST is local | hot or huge bucket cannot be split; S fixed | this project |
| Hash(bucket, key) | spreads a hot bucket evenly | LIST must merge-scan every shard | some KV-backed stores |
| Range(bucket, key) with automatic splitting | ordered LIST; hot ranges split and move; shards can be added | needs a range directory and a split/merge protocol | Bigtable, Spanner, FoundationDB, TiKV, CockroachDB; S3's index is widely described as range-partitioned with automatic splits on hot prefixes |

The production answer is range partitioning on `(bucket, key)` with each range
replicated by Raft (3 or 5 replicas), a placement driver that splits ranges
by size and by load, and a small replicated directory mapping ranges to
replica groups.

## Metadata caching

| Cache | Where | Staleness | Status |
|---|---|---|---|
| Latest-version lookups | gateway LRU with TTL | bounded by TTL across gateways; local writes invalidate | implemented, off by default |
| Bucket configuration | gateway | 5 x TTL | implemented with the above |
| Range directory | gateway | refreshed on a "wrong shard" error | needed with range sharding |
| Block cache of the storage engine | metadata server | none (same process) | needed with an LSM |

Caching reduces read load but not commit load. Commits only scale by
sharding and by batching (group commit: N concurrent commits share one fsync,
which turns the per-shard ceiling from ~1/fsync-latency into
~N/fsync-latency).

## Hot prefixes

Keys like `logs/2026-10-14/...` concentrate all writes on the newest prefix.
With hash(bucket) sharding the whole bucket is already on one shard, so the
prefix makes no difference to which shard is hot; the bucket is the unit of
heat. With range sharding, sequential prefixes make the *last* range hot;
range splitting by load fixes it only if the split point keeps moving
forward, which is why systems recommend high-cardinality prefixes (or apply a
hash prefix internally).

## Listing operations

GET by exact key is a point lookup: O(log n) in an ordered index, one owner,
independent of bucket size. LIST is a **range scan**:

- Its cost is proportional to the entries examined, not returned (delete
  markers, noncurrent versions and keys outside a delimiter group are
  skipped but still read).
- In a range-sharded store, a prefix may span many ranges on many servers.
- It cannot be cached effectively because any write in the range invalidates it.
- Listing a billion-key bucket in 1,000-key pages is a million sequential
  requests: clients that need everything should use an inventory (a periodic
  export of the index to objects), not LIST.

This implementation bounds each call: at most 1,000 entries per page, at most
10x that many entries scanned (to skip delete markers), and a stateless
continuation token (the next key position). See
[consistency.md](consistency.md#listing-consistency) for what pagination
guarantees.

## Pagination

Continuation tokens encode the key position, not an offset: inserting keys
before the cursor does not shift later pages, and the server holds no
per-client state. Offset pagination would cost O(offset) per page and shift
under concurrent writes.

## Scaling the metadata plane: a plan

| Stage | Objects | Change |
|---|---|---|
| Today | up to ~10^7 per shard | in-memory shards, S fixed |
| 10^8 | | group commit; more shards chosen up front; incremental repair/GC scans by shard |
| 10^9 | 0.5-1 TB of metadata | LSM-backed shards on SSD; per-node blob index (node -> blobs) for repair; Raft-replicated shards |
| 10^10-10^11 | 5-50 TB | range sharding with automatic split/merge and load-based placement; inventory exports for bulk listing |

### Migrating the shard count (or the sharding scheme)

Because routing is a pure function of S, changing S is a migration:

1. Introduce a routing table (bucket -> shard) defaulting to `hash mod S_old`.
2. For each bucket to move: block writes briefly (or dual-write), copy its records to the new shard, flip the table entry, drain the old copy.
3. Gateways refresh the table and retry on a "moved" error.

That is precisely the directory that range sharding uses, which is the argument
for starting with a directory instead of a hash at production scale.
