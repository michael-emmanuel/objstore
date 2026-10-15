# Architecture

This document explains how the system is put together and, above all, why
the **metadata plane** and the **data plane** are separated. Every other
document builds on the vocabulary defined here.

## 1. Components

```
                                Clients
                                   |
                         HTTPS, bearer token (tenant)
                                   v
                     +----------------------------+
                     |  Gateway (stateless, N of) |  auth, quotas, rate limits,
                     |  - placement (ring)        |  fan-out writes, failover reads,
                     |  - replication writer      |  hot-object + metadata caches
                     +-------------+--------------+
            control plane          |            data plane
     (small, consistent, few ops)  |   (large, parallel, most bytes)
              +--------------------+--------------------+
              |                                         |
              v                                         v
 +-------------------------+          +----------+ +----------+ +----------+
 | Metadata shards 0..S-1  |          | Storage  | | Storage  | | Storage  |
 |  bucket -> shard        |          | node 1   | | node 2   | | node 3   | ...
 |  versions, segments,    |          | az-a     | | az-b     | | az-c     |
 |  replica lists, uploads,|          | blobs on | | blobs on | | blobs on |
 |  garbage queue, usage   |          | local FS | | local FS | | local FS |
 |  WAL + snapshots        |          +----+-----+ +----+-----+ +----+-----+
 +------------+------------+               ^  node-to-node pulls  ^
              |                            +-----------+----------+
 +------------+------------+                           |
 | Coordinator (on shard 0)|<-- heartbeats ------------+
 |  membership, node state,|
 |  failure detection      |      +--------------------------------------+
 +-------------------------+      | Background workers                   |
                                  |  repairer: restore RF, process       |
                                  |            quarantined (corrupt) data|
                                  |  GC:       queue + mark/sweep        |
                                  |  rebalancer: move toward placement,  |
                                  |            drain nodes               |
                                  +--------------------------------------+
```

| Component | Binary | State | Authoritative for |
|---|---|---|---|
| Gateway | `cmd/gateway` | caches only | nothing (stateless) |
| Metadata shard | `cmd/metadata` | in-memory + WAL + snapshot on local disk | buckets, versions, which blobs make up an object, where replicas are, uploads, usage |
| Coordinator | `cmd/metadata -coordinator` | heartbeat table (soft) + operator states (persisted) | node lifecycle state (ACTIVE/SUSPECT/DOWN/DRAINING/DECOMMISSIONED) |
| Storage node | `cmd/storage-node` | blobs + sidecars on local FS | the bytes of the blobs it holds |
| Repairer + GC | `cmd/repairer` | none | nothing (reconciles toward metadata) |
| Rebalancer | `cmd/rebalancer` | none | nothing (reconciles toward placement) |

## 2. Metadata plane vs data plane

**Metadata** answers questions *about* objects:

- Does `photos/user-123/profile.jpg` exist? Which version is current?
- How big is it, what is its checksum and ETag, what content type?
- Which segments (blobs) make up version `v7`, in which order, at which offsets?
- Which storage nodes hold a verified copy of each segment? (replica state)
- Which blobs are no longer referenced and may be reclaimed?
- How much is tenant `alice` storing?

**Data** is the bytes, stored as immutable *blobs* named by opaque 128-bit
IDs. A storage node does not know what a bucket, key, version or tenant is.

### Why separate them

1. **They scale along different axes.** Data volume scales with bytes
   (petabytes); it is embarrassingly parallel because every blob is
   independent. Metadata scales with *object count* and *operation rate*, and
   it needs ordering ("which version is latest") which is inherently
   coordinated. Mixing them forces the coordination cost onto every byte.

2. **They need different consistency.** A blob is immutable: once written it
   is either present and byte-identical everywhere, or absent. There is
   nothing to keep consistent except existence, which is verifiable by
   checksum. Metadata is mutable (overwrites, deletes, replica moves) and
   must be linearizable per key so that two writers do not both "win". By
   putting all mutability in the metadata plane, the data plane needs no
   consensus at all.

3. **They fail differently**, and the separation lets us reason about each
   failure on its own (section 5).

4. **Moving data does not require rewriting object identity.** Repair and
   rebalancing copy a blob and then update one replica list in metadata.
   Nothing about the object changes from the client's point of view.

### The cost of separating them

- Every request needs a metadata lookup *and* a data access: two hops.
- The metadata plane becomes the coordination bottleneck and the most
  critical availability dependency (section 5).
- Two stores can disagree. A blob can exist with no metadata pointing at it
  (orphan) or metadata can point at a replica that is gone (lost replica).
  The system is designed so that both are *safe* states that background
  reconciliation converges: orphans are garbage collected, lost replicas are
  repaired. The ordering rules in section 4 guarantee we never create the
  dangerous state, metadata pointing at a blob that was never durably
  written on W nodes.

## 3. Object identity and naming

Logical identity: `(bucket, key, version_id)`.

- Bucket: DNS-style name, 3-63 chars, `[a-z0-9.-]`.
- Key: any valid UTF-8 up to 1024 bytes without control characters. `../../etc/passwd` is a *legal key*.
- Version ID: 128 bits, 48-bit millisecond timestamp prefix + 80 random bits.
  Ordering comes from the shard's commit sequence number (`Seq`), never from IDs or clocks.

Physical identity: a **blob ID**, 32 lowercase hex characters:

```
segment blob = SHA-256( len|"seg" | len|bucket | len|key | len|version_id | len|index )[:32]
part blob    = SHA-256( len|"part"| len|bucket | len|key | len|upload_id  | len|part | len|attempt )[:32]
```

The user's key **never touches a filesystem path**. Storage nodes accept only
IDs matching `^[0-9a-f]{32}$` and store them at `blobs/<id[0:2]>/<id>`. That
removes path traversal, case-insensitive filesystem collisions, reserved names
(`CON`, `.`), Unicode normalization collisions and path-length limits from the
data plane entirely, rather than trying to sanitize them. Length-prefixing
the hash inputs means `("ab","c")` and `("a","bc")` cannot collide.

Because the version ID is fresh on every write, **blob IDs are unique per
write**: blobs are never overwritten. This is the property that makes the rest
of the design simple: caching by blob ID or version needs no invalidation,
replicas cannot diverge silently (same ID must mean same bytes, enforced by
SHA-256), and failover can resume a read at an exact byte offset on another
replica.

### Content addressing: considered, not chosen

Naming blobs by `SHA-256(content)` would give free deduplication and
self-verifying names. We deliberately do not, because:

- The name is only known after the whole object has been read, so a streaming
  upload cannot be placed until it ends (or must be written to a temp name
  and renamed across nodes).
- Deduplicated blobs are shared, which requires **reference counting** across
  tenants and buckets. Every delete becomes a distributed refcount decrement,
  and GC must prove a count is zero across all metadata shards. A refcount bug
  deletes someone else's data.
- Cross-tenant dedup leaks information (a tenant can probe whether some
  content exists by timing uploads).
- Typical object workloads (photos, logs, backups with encryption) dedupe
  poorly at whole-object granularity.

We still get the integrity benefit: every segment's SHA-256 is recorded in
metadata and checked end to end. See [tradeoffs.md](tradeoffs.md#12-content-addressing).

## 4. Object lifecycle

### Write path (simple PUT)

```
client           gateway                          metadata shard       storage nodes (RF=3)
  | PUT body ------>|                                    |                     |
  |                 | auth, bucket lookup, quota precheck|                     |
  |                 | acquire tenant + global upload slot|                     |
  |                 | for each 64 MiB segment:           |                     |
  |                 |   place(blob_id) -> [n1,n2,n3]     |                     |
  |                 |   RegisterPending(blob) ---------->| (GC protection)     |
  |                 |   stream chunks to n1,n2,n3 in parallel ---------------->| tmp file,
  |                 |                                    |                     | SHA-256 + CRC32C
  |                 |                                    |                     | fsync, sidecar,
  |                 |                                    |                     | rename, dir fsync
  |                 |<--------------------------------------- acks (sha,size) -|
  |                 |   need W matching acks; stragglers get 2s then dropped   |
  |                 | verify X-Checksum-SHA256 if given  |                     |
  |                 | Commit(version, segments, replicas, quota ceiling) ->|   |
  |                 |                         WAL append + fsync, apply  |     |
  |<-- 200 ---------|<----------------- committed (Seq) -|                     |
```

The **commit point is the metadata commit**. Before it, nothing is visible;
after it, the whole object is visible at once. A crash or error anywhere
earlier leaves only unreferenced blobs, which are protected from GC for
`PendingTTL` (6 h) and then reclaimed.

**Replica 1 and 2 succeed, replica 3 fails: does the client get success?**
It depends on W (`-write-quorum`, default majority = 2 for RF 3):

| Policy | Outcome | Durability at ack | Notes |
|---|---|---|---|
| W=2 (default) | 200, `X-Replicas-Acked: 2/3`, `under_replicated: true` | survives 1 node loss | repairer adds the third copy without waiting for any node to be declared DOWN |
| W=3 | 503 `InsufficientReplicas` | n/a | the two written blobs become orphans and are GC'd; client retries |
| W=1 | 200 | a single disk failure before repair loses the object | not recommended; shown for contrast |

Metadata records only the replicas that actually acknowledged, so a read
never tries a replica that was not written.

### Storage node commit protocol

```
1  create tmp/<id>.<rand>.tmp        (never readable)
2  stream bytes: SHA-256 over all, CRC32C per 64 KiB block, capacity check per chunk
3  verify size and expected SHA-256  (mismatch -> delete tmp, error)
4  fsync data file
5  write sidecar (size, sha256, block CRCs, own CRC) to tmp, fsync, rename into place
6  rename data file into place       <- blob visible on this node
7  fsync parent directory            <- the renames are durable
```

Crash analysis: before 5, nothing visible and tmp files are deleted on
startup; between 5 and 6, an orphan sidecar is removed on startup; after 6 the
blob is complete. A data file without a sidecar is quarantined at startup.
A half-written blob can never be served because readers only see renamed
files and verify every block CRC before returning its bytes.

With `-fsync=false` steps 4, 5 (sync) and 7 are skipped: faster (measured in
[performance.md](performance.md)), but an acknowledged blob can vanish on
power loss. The replica count at ack time then overstates durability.

### Read path

```
client         gateway                       metadata             storage nodes
  | GET -------->| auth, rate limit            |                      |
  |              | Get(bucket,key) ----------->| (or metadata cache)  |
  |              |<--- version: segments + replicas                   |
  |              | small object? hot-object cache (single-flight)     |
  |              | for each segment in range:                         |
  |              |   order replicas: same zone, ACTIVE, then degraded |
  |              |   GET blob [off,len] ---------------------------------->| CRC check per
  |<--- stream --|<----------------------------------- bytes ------------| block before
  |              |   on error: resume on next replica at exact offset |      | serving it
```

A corrupt block is detected on the node *before* its bytes leave the node; the
node quarantines the blob and aborts the stream; the gateway resumes from the
same byte offset on another replica. The client sees a correct stream.

### Delete

`DELETE` without `version_id` commits a **delete marker** as the latest
version. No data is touched. Dereferenced blobs (from overwrites in
unversioned buckets, permanent version deletes, aborted uploads) are put on the
shard's **garbage queue** together with their replica list, and the GC deletes
the physical copies after a grace period. See
[tradeoffs.md](tradeoffs.md#20-tombstones) for why deletion is logical first.

### Multipart

```
POST /v1/buckets/b/multipart {key}              -> upload_id "<bucket>~<random>"
POST /v1/multipart/{id}/parts?part_number=N     -> each part is its own blob, RF replicas, W quorum
     (retry of part N writes a new blob with a fresh attempt nonce; the old one is garbage)
POST /v1/multipart/{id}/complete {parts:[{n, etag}]}
     -> metadata validates every part (exists, ETag matches, size >= min except last)
        and atomically commits one Version whose segments are the parts.
DELETE /v1/multipart/{id}                       -> parts go to the garbage queue
```

The upload ID embeds the bucket so any gateway can route part calls to the
owning shard without a global upload directory. Abandoned uploads are expired
by the GC after `-upload-ttl`.

## 5. Failure behaviour by plane

| Failure | Data plane effect | Metadata plane effect | Client-visible |
|---|---|---|---|
| 1 storage node down (RF 3) | reads fail over; writes place on other nodes | coordinator marks SUSPECT then DOWN; repair after delay | none (latency blip) |
| 2 nodes down in different zones | segments with 1 remaining copy are read-only-available and repaired first | as above | writes with W=2 still succeed if 2 writable nodes remain |
| Metadata shard down | storage untouched | buckets on that shard unavailable for read and write | 503 `MetadataUnavailable` for those buckets; other shards unaffected |
| Coordinator down | unaffected | membership frozen at last snapshot; no failure detection, no new nodes | none unless a node also fails |
| Gateway down | none | none | load balancer retries another gateway |

The asymmetry is the point: **data-plane failures are absorbed by
redundancy; metadata-plane failures are absorbed only by replicating the
metadata service**, which this project does not do (no consensus). A
production deployment must replicate each shard (Raft group per shard, or a
database such as FoundationDB/Spanner/TiKV). See
[metadata-scaling.md](metadata-scaling.md).

"Can objects still be read when metadata is unavailable?" Only from the
gateway's metadata cache, if enabled (`-metadata-cache-ttl`), and only for
versions it has cached: the gateway serves stale-but-valid versions within
the TTL. Without metadata we do not know which blobs make up an object, and
we will not guess.

## 6. Metadata bottlenecks, caching and sharding (summary)

- **Bottleneck:** each shard serializes commits through one lock and one WAL
  fsync. Measured ceiling: about 3,700 commits/s per shard with fsync on the
  test VM ([performance.md](performance.md)). Reads take a shared lock and are
  sub-microsecond.
- **Caching:** the gateway can cache latest-version lookups (TTL, invalidated
  locally on writes). Version-addressed data is cached without invalidation.
- **Sharding:** by `hash(bucket) mod S`. Simple, keeps LIST single-shard, but
  a single huge or hot bucket cannot be spread. Production would range-shard
  `(bucket, key)` with splitting. Full discussion in
  [metadata-scaling.md](metadata-scaling.md).

## 7. Security

| Concern | Implementation | Limitation / production step |
|---|---|---|
| Authentication (clients) | bearer tokens; config stores only SHA-256 hashes or env-var names; constant-time lookup by hash | no expiry/rotation; production: OIDC/JWT or signed requests (SigV4-style) |
| Authorization | bucket owned by one tenant; only owner (or admin) may access; optional `public_read` | no per-object ACLs or policies |
| Tenant isolation | per-tenant token buckets (req/s, upload B/s, download B/s), concurrency semaphores, object size cap, storage quota | gateway-local limits: N gateways allow N x the rate |
| Internal authentication | shared cluster token (`X-Cluster-Token`, from `CLUSTER_TOKEN`) on every internal call incl. node-to-node pulls; `/healthz` and `/metrics` exempt | single shared secret, no per-service identity; production: mTLS with per-service certs |
| TLS | every binary accepts `-tls-cert/-tls-key` | not enabled in the local cluster |
| Request validation | bucket/key/metadata limits (key 1 KiB, 16 user-meta entries / 2 KiB), header size cap 64 KiB, JSON bodies bounded | |
| Path traversal | impossible by construction: keys never become paths, nodes accept only 32-hex IDs | |
| Secrets | generated by `scripts/dev-config.sh` / `local-cluster.sh` into git-ignored files with mode 0600; never logged | production: a secret manager (Vault, KMS) |
| Audit logging | structured `audit` records for every mutation (tenant, op, bucket, key, version, request ID) | goes to the same log stream; production: separate tamper-evident sink |
| Fault injection | routes exist only when a process starts with `-enable-fault-injection` | |
| Encryption at rest | not implemented | per-object data keys wrapped by a KMS |

## 8. Honest scope statement

This is an educational system that exercises the real problems. It is not a
replacement for S3, Ceph or HDFS. Specifically:

- **No consensus.** Metadata shards and the coordinator are single processes.
- **Local filesystem** per node, one directory per node (no multi-disk JBOD
  management, no direct I/O, no custom on-disk format beyond the sidecar).
- **Replication only.** Erasure coding is analysed, not implemented.
- **Repair and GC scan all metadata** each pass. Fine for millions of
  segments, not billions.
- **Single region.** Multi-region is a design document.
- **Tracing** is W3C-traceparent-compatible span logging, not a full
  OpenTelemetry exporter.
