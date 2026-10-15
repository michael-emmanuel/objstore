# System Design

A one-document design review: requirements, API, data model, the key flows,
and how the system behaves as it scales from 1 to 10,000 nodes. Deeper
treatment of each topic is linked.

## 1. Requirements

**Functional:** buckets; PUT/GET/HEAD/DELETE/LIST objects; multipart upload;
range reads; object versions and delete markers; per-tenant quotas and rate
limits; node addition, draining and failure recovery without downtime.

**Non-functional:**

| Property | Target in this design |
|---|---|
| Durability | an acknowledged write survives RF - W node losses immediately and RF - 1 after repair; corruption never served |
| Availability | reads survive RF - 1 replica losses; writes survive RF - W; metadata shard outage = that shard's buckets unavailable (no consensus) |
| Consistency | linearizable per key (single metadata authority per shard); immutable data |
| Object size | unbounded by memory (streaming); multipart for large objects |
| Isolation | per-tenant request rate, bandwidth, concurrency, object size and storage |
| Operability | metrics on every component, structured logs, trace propagation, fault injection |

**Out of scope (documented):** consensus-replicated metadata, erasure coding,
multi-region, per-object ACL policies, encryption at rest.

## 2. API

See [api/openapi/openapi.yaml](../api/openapi/openapi.yaml).

```
PUT    /v1/buckets/{bucket}                          create bucket
PUT    /v1/buckets/{bucket}/objects/{key}            upload (streamed)
GET    /v1/buckets/{bucket}/objects/{key}            download (Range, version_id, consistency=quorum)
HEAD   /v1/buckets/{bucket}/objects/{key}
DELETE /v1/buckets/{bucket}/objects/{key}            delete marker (or ?version_id= permanent)
GET    /v1/buckets/{bucket}/objects                  list (prefix, delimiter, max_keys, continuation_token)
GET    /v1/buckets/{bucket}/versions/{key}           list versions of a key
POST   /v1/buckets/{bucket}/multipart                initiate
POST   /v1/multipart/{upload_id}/parts?part_number=N upload part
POST   /v1/multipart/{upload_id}/complete            complete
DELETE /v1/multipart/{upload_id}                     abort
GET    /admin/cluster | /admin/objects/{b}/{k} | /admin/nodes/{id}/drain
POST   /admin/nodes/{id}/state                       DRAINING / DECOMMISSIONED / ACTIVE
```

Every response has `X-Request-Id`; every request has a context with a
deadline for metadata calls (`-metadata-timeout`), a progress deadline for
the client (`-client-idle-timeout`), and cancellation propagated to replica
streams.

## 3. Data model

```
Bucket   { name, tenant, versioning, public_read, replication_factor, storage_class }
Version  { bucket, key, version_id, seq, tenant, size, etag, checksum, content_type,
           created_at, updated_at, storage_class, replication_factor,
           delete_marker, user_meta, segments[] }
Segment  { index, blob_id, offset, size, sha256, replicas[] }      <- reference to data
Upload   { id, bucket, key, tenant, parts{n -> Part} }
Part     { number, blob_id, size, sha256, replicas[] }
Blob     (storage node) id -> bytes + sidecar{size, sha256, crc32c per 64 KiB}
```

Metadata references data; it never contains object bytes. Identity and
naming rules: [architecture.md](architecture.md#3-object-identity-and-naming).

## 4. Where does an object live?

For `PUT /v1/buckets/photos/objects/user-123/profile.jpg`:

1. **Metadata owner:** shard `hash("bucket:photos") mod S`.
2. **Version:** a new version ID; the commit assigns `seq`.
3. **Blobs:** the body is cut into 64 MiB segments; segment i gets
   `blob_id = H(photos, user-123/profile.jpg, version_id, i)`.
4. **Replicas:** `placement.Place(blob_id, RF)` walks the consistent-hash
   preference list for `blob_id`, skipping nodes that are not ACTIVE or are
   over 95% full, choosing distinct zones, then racks.
5. **Durability decision:** at least W replicas return a matching SHA-256
   after fsync; then the version (with the acked replica list per segment) is
   committed. `X-Replicas-Acked` reports the result.

Inspect any object's placement: `GET /admin/objects/{bucket}/{key}`
(shows actual replicas, desired placement, zones spanned).

Placement uses the *blob ID*, not `bucket+key`, as the hash input. Hashing
`bucket+key` would put every version and every segment of a key on the same
three nodes: a large object could not be read in parallel and a hot key's
history would pile up on the same disks.

## 5. Concurrency design

| Concern | Decision |
|---|---|
| Goroutine lifecycle | Every background loop takes a context and is started from `service.Group` in `main`; `Wait` returns only when all have exited. Per-request goroutines (replica uploader and pump) are joined before the handler returns; a test asserts no goroutine leaks after client disconnects. |
| Shared state | No mutable globals. Membership is an immutable snapshot swapped atomically (`atomic.Pointer`). Faults are an atomic pointer. |
| Locks | Metadata shard: RWMutex (commits exclusive, reads shared). Storage engine: a small map of in-progress writes per blob ID; no lock held during I/O. Caches: one mutex each. |
| Channels | All bounded (per-replica chunk queue depth, semaphores). |
| Buffers | Pooled chunks shared by reference count across replicas, not copied per replica. |
| Shutdown | SIGTERM: stop accepting, give in-flight requests a grace period (30 s for nodes/gateways), then close. The commit protocol makes cut-off transfers invisible. |

## 6. Scale model

What changes, and what breaks first, as the cluster grows. Assumes 24 x 20 TB
disks per node at 80% fill (~384 TB usable per node, ~128 TB logical at RF 3)
and 1 MB average objects.

| | 1 node | 10 nodes | 100 nodes | 1,000 nodes | 10,000 nodes (conceptual) |
|---|---|---|---|---|---|
| Logical capacity | ~128 TB (RF 1!) | ~1.3 PB | ~13 PB | ~130 PB | ~1.3 EB |
| Objects (1 MB) | ~10^8 | ~10^9 | ~10^10 | ~10^11 | ~10^12 |
| Disk failures | rare | ~4/yr | ~36/yr | ~1/day | ~10/day |
| Metadata | fits in memory | needs several shards; memory tight | LSM + Raft, range-sharded | dedicated metadata cluster | metadata is a planet-scale database |
| Placement | trivial | ring in microseconds | ring sub-µs; rendezvous ~12 µs | ring ~0.7 µs; rendezvous ~170 µs/lookup (too slow for full scans) | placement groups instead of per-blob placement |
| Rebalancing (add 1 node) | n/a | moves ~1/11 of data (~120 TB) | ~1/101 per node; add nodes in batches | continuous background activity | always running; needs QoS classes |
| Repair traffic | n/a (no redundancy) | one node = 384 TB to re-replicate | routine | continuous; must be declustered | dominant background load |
| Network topology | n/a | one switch | leaf-spine; cross-zone links matter | oversubscription matters; repair must stay zone-local where possible | multiple fabrics; regions |
| Hot objects | page cache | 3 nodes' NICs | gateway caches | CDN tier | CDN tier |
| Hot prefixes | n/a | whole bucket on one shard | hash(bucket) shards become uneven | range splitting required | range splitting + load-based placement |
| **Likely first bottleneck** | single disk, no redundancy | metadata commit rate (small writes); gateway egress (large writes) | metadata memory and full-scan repair/GC | metadata cluster; repair bandwidth; ops automation | everything is a distributed system: rollout safety, correlated failures |

At 1 node the system is a single-disk object store with no redundancy: RF
cannot exceed the number of nodes, so placement returns fewer replicas and W
must be 1 for writes to succeed.

## 7. Where to read more

| Topic | Document |
|---|---|
| Components, planes, lifecycle, security | [architecture.md](architecture.md) |
| Guarantees per operation | [consistency.md](consistency.md) |
| Partitions, CAP, durability vs availability | [distributed-consistency.md](distributed-consistency.md) |
| Every decision with alternatives | [tradeoffs.md](tradeoffs.md) |
| What fails and how it recovers | [failure-modes.md](failure-modes.md) |
| Sizing 10 PB | [capacity-planning.md](capacity-planning.md) |
| Metadata at scale | [metadata-scaling.md](metadata-scaling.md) |
