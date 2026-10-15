# Distributed Object Storage

An educational distributed object store written in Go from first principles:
a simplified S3/HDFS/Ceph-style system with a separate metadata plane and
data plane, consistent-hash placement across failure domains, quorum
replication, streaming I/O, multipart uploads, versioning, repair,
rebalancing, garbage collection, caching, tenant isolation, observability,
fault injection and a load generator.

Not a replacement for S3, Ceph or HDFS, and it says plainly where it is
simplified (see [Limitations](#limitations)).

## Architecture

```
                               Clients
                                  |   bearer token -> tenant
                                  v
                    +----------------------------+
                    |  Gateway (stateless, N)    |  auth, rate limits, quotas,
                    |  placement (ring + zones)  |  streaming fan-out writes,
                    |  replication writer        |  failover reads, caches
                    +-------------+--------------+
        CONTROL / METADATA PLANE  |  DATA PLANE
          +-----------------------+------------------------+
          v                                                v
+--------------------------+         +-----------+ +-----------+ +-----------+
| Metadata shards 0..S-1   |         | Storage   | | Storage   | | Storage   |
| hash(bucket) mod S       |         | node az-a | | node az-b | | node az-c | ...
| versions, segments,      |         | immutable | | blobs +   | | CRC/64KiB |
| replica lists, uploads,  |         | blobs     | | SHA-256   | | scrubber  |
| GC queue, usage; WAL     |         +-----+-----+ +-----+-----+ +-----+-----+
+------------+-------------+               ^  node-to-node copies  ^
             |                             +-----------+-----------+
+------------+-------------+                           |
| Coordinator: heartbeats, |<---- heartbeats ----------+
| ACTIVE/SUSPECT/DOWN,     |
| DRAINING/DECOMMISSIONED  |     Background: repairer (+GC), rebalancer
+--------------------------+
```

## Core capabilities

| Capability                                                                           | Where                                            |
| ------------------------------------------------------------------------------------ | ------------------------------------------------ |
| PUT / GET / HEAD / DELETE / LIST, Range reads, versions                              | `internal/api`                                   |
| Streaming with bounded memory (1 GiB upload at < 19 MB gateway RSS, measured)        | `internal/replication`, `internal/api`           |
| Multipart upload with per-part retry and atomic completion                           | `internal/api/multipart.go`, `internal/metadata` |
| Path-safe blob naming (keys never become paths)                                      | `internal/object`                                |
| tmp + fsync + rename commit, SHA-256 + CRC32C per 64 KiB block                       | `internal/storage`                               |
| Consistent hashing (ring with vnodes) and rendezvous, failure-domain-aware placement | `internal/hashing`, `internal/placement`         |
| Replication with write quorum W, stall/straggler handling, slow-node penalty         | `internal/replication`, `internal/quorum`        |
| Sharded metadata with WAL, snapshots and deterministic replay                        | `internal/metadata`                              |
| Failure detection (SUSPECT/DOWN), draining, decommissioning                          | `internal/membership`                            |
| Repair (priority, delay, bandwidth cap), rebalancing (copy-before-delete)            | `internal/repair`, `internal/rebalancer`         |
| Garbage collection (queue + two-sighting mark/sweep), tombstones, upload expiry      | `internal/garbagecollector`                      |
| Hot-object LRU with single-flight, optional metadata cache                           | `internal/cache`                                 |
| Token-bucket rate limits (requests and bandwidth), concurrency limits, quotas        | `internal/ratelimit`, `internal/quota`           |
| Metrics, structured logs, trace propagation, pprof                                   | `internal/observability`, `internal/service`     |
| Fault injection, load generator                                                      | `internal/storagenode`, `cmd/loadgen`            |
| Cluster-internal authentication                                                      | `internal/clusterauth`                           |

## Data plane

Storage nodes store **immutable blobs** named by 128-bit IDs derived from
`(bucket, key, version, segment)`. A node knows nothing about buckets, keys or
tenants. Writes go to `tmp/`, are hashed (SHA-256) and block-checksummed
(CRC32C), fsynced, then renamed into place. Reads verify each block before
sending it. Because every write produces new blob IDs, a replica can be
missing or corrupt but never stale.

## Metadata plane

Metadata shards own buckets (`hash(bucket) mod S`). A shard stores bucket
config, versions (with segment lists, checksums and the replicas that actually
acknowledged), multipart uploads, the garbage queue and per-tenant usage, in
memory with a CRC-framed write-ahead log fsynced per commit. Commits are
serialized per shard, which makes the latest version of every key
unambiguous. Details: [docs/architecture.md](docs/architecture.md),
[docs/metadata-scaling.md](docs/metadata-scaling.md).

## Object lifecycle

```
PUT:    auth -> limits -> per 64 MiB segment: place -> register pending -> stream to RF nodes
        -> W fsynced, checksum-matched acks -> commit version (quota-checked)  <- visibility point
GET:    auth -> metadata lookup (or cache) -> per segment: best replica, block-verified,
        mid-stream failover at exact byte offset
DELETE: commit a delete marker; blobs reclaimed later by GC after a grace period
MPU:    initiate -> parts (each an independent replicated blob, retryable) -> complete (atomic)
```

## Replication

RF per bucket (default 3), one replica per zone where possible, then distinct
racks. The gateway streams one body to all RF nodes in parallel through
bounded per-replica queues; the slowest live replica sets the pace (TCP
backpressure), a stalled replica is dropped after `-replica-stall-timeout`,
and a node that stalls or straggles is avoided for new placements for
`-slow-node-penalty`. Metadata records only replicas that acknowledged.

## Quorum

**W** (`-write-quorum`, default majority) is a durability threshold: a PUT
succeeds only when W replicas have fsynced bytes matching the gateway's
SHA-256. With RF 3 and W 2, replica 1 and 2 succeeding while replica 3 fails
returns 200 with `X-Replicas-Acked: 2/3`, and repair adds the third copy;
with W 3 it returns 503 and nothing is visible.

**R** is read verification (`GET ?consistency=quorum`), not version
resolution: freshness comes from the single metadata authority per shard,
so read-after-write holds with R = 1. Why R + W > N is not the mechanism
here, and the assumptions behind the guarantees: [docs/consistency.md](docs/consistency.md).

## Failure recovery

| Event                              | Response                                                                                                              |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Node crash                         | reads fail over immediately; SUSPECT -> DOWN; repair after `-repair-delay` (single-copy segments immediately)         |
| Corrupt block                      | detected by CRC before bytes leave the node; quarantine; gateway resumes on another replica; repair replaces the copy |
| Slow node                          | replica dropped after the stall timeout; node penalized for new placements                                            |
| Disk full                          | replica fails, write succeeds at W; placement avoids nodes over 95%                                                   |
| Client disconnect / partial upload | replica streams aborted with an error; nothing visible; GC reclaims                                                   |
| Node added                         | rebalancer moves its share, copy-before-delete                                                                        |
| Node drained                       | migrated off while still serving reads; decommission allowed only at zero references                                  |
| Metadata shard down                | its buckets return 503 (fail closed); other shards unaffected                                                         |

Full catalogue: [docs/failure-modes.md](docs/failure-modes.md).

## Quick start

Requirements: Go 1.24+, `curl`, `python3` (for JSON in examples).

```bash
go test ./...            # unit + integration tests (full in-process cluster)
go test -race ./...
scripts/local-cluster.sh up
source .local/env        # generated secrets: OBJSTORE_TOKEN, CLUSTER_TOKEN, GW
scripts/smoke.sh         # end-to-end check against the running cluster
```

## Local cluster

`scripts/local-cluster.sh` runs real processes (no Docker needed):
2 metadata shards (`:7000` also runs the coordinator, `:7001`), 4 storage
nodes (`:8101-8104`, zones a/b/c/a), gateway (`:8080`), repairer + GC
(`:9100`), rebalancer (`:9200`). Timers are shortened (DOWN after 6 s, repair
after 10 s). Logs are in `.local/logs`.

```bash
scripts/local-cluster.sh status
scripts/local-cluster.sh kill-node 2       # SIGKILL
scripts/local-cluster.sh start-node 2      # restart with its disk
scripts/local-cluster.sh start-node 5      # add a node
scripts/local-cluster.sh down | clean
```

Docker Compose (3 metadata shards, 4 nodes, node-5 under the `expansion`
profile, optional Prometheus under `observability`):

```bash
scripts/dev-config.sh                                   # writes deploy/.env (git-ignored)
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml --profile expansion up -d node-5
```

## Example commands

```bash
A="Authorization: Bearer $OBJSTORE_TOKEN"; ADM="Authorization: Bearer $OBJSTORE_TOKEN_ADMIN"
curl -X PUT -H "$A" $GW/v1/buckets/photos -d '{"versioning":true}'
curl -X PUT -H "$A" -T profile.jpg $GW/v1/buckets/photos/objects/user-123/profile.jpg
curl -H "$A" $GW/v1/buckets/photos/objects/user-123/profile.jpg -o out.jpg
curl -I -H "$A" $GW/v1/buckets/photos/objects/user-123/profile.jpg
curl -H "$A" -r 0-99 $GW/v1/buckets/photos/objects/user-123/profile.jpg
curl -H "$A" "$GW/v1/buckets/photos/objects?prefix=user-123/&max_keys=100"
curl -H "$A" $GW/v1/buckets/photos/versions/user-123/profile.jpg
curl -X DELETE -H "$A" $GW/v1/buckets/photos/objects/user-123/profile.jpg
curl -H "$ADM" $GW/admin/objects/photos/user-123/profile.jpg   # replicas, zones, desired placement
curl -H "$ADM" $GW/admin/cluster
```

API reference: [api/openapi/openapi.yaml](api/openapi/openapi.yaml). Go
client: `pkg/client`.

## Fault demonstrations

Fault endpoints exist only on processes started with
`-enable-fault-injection`. Examples (internal endpoints need the cluster
token):

```bash
CT="X-Cluster-Token: $CLUSTER_TOKEN"
curl -X POST -H "$CT" localhost:8101/admin/faults -d '{"chunk_delay_ms":300}'     # slow disk
curl -X POST -H "$CT" localhost:8101/admin/faults -d '{"partitioned":true}'       # partition
curl -X POST -H "$CT" "localhost:8101/admin/corrupt/<blob_id>?offset=100"         # bit rot
curl -X POST -H "$CT" localhost:7000/admin/faults -d '{"fail_reads":true}'        # metadata outage
```

Twelve experiments with expected behaviour: [docs/fault-injection.md](docs/fault-injection.md).
Fifteen recorded demonstrations: [docs/demo.md](docs/demo.md).

## Load testing

```bash
go run ./cmd/loadgen -workload mixed -size small -keys 2000 -concurrency 16 -duration 30s
go run ./cmd/loadgen -workload read-only -size 64KiB -keys 200 -hot-ratio 0.9 \
   -scrape http://localhost:8080/metrics -scrape-match gateway_object_cache
```

Workloads: write-only, read-only, read-heavy, write-heavy, mixed; sizes fixed
or small/medium/large distributions; hot-key ratio. Reports ops/s, MiB/s,
p50/p95/p99/max per operation, cache sources, and server counter deltas.
Measured results and their caveats: [docs/performance.md](docs/performance.md).

## Observability

Every process serves Prometheus metrics at `/metrics` and logs JSON. Key
signals: request rate and latency by operation, replica write failures by
reason, under-replicated and unavailable segments, repair and rebalance
throughput, GC backlog, cache hit/eviction rates, per-shard metadata size and
commit rate, per-node capacity and corruption counts. Traces propagate via
W3C `traceparent` from gateway to metadata and storage nodes. pprof is
available on a separate local address with `-debug-listen`.
Details: [docs/observability.md](docs/observability.md).

## Scaling

Planes scale independently: storage by adding nodes (consistent hashing
moves ~1/N), gateways freely (stateless), metadata by shards. What breaks
first in this implementation: per-shard commit rate (~3,700/s with fsync,
measured), metadata memory, full-scan repair/GC, and gateway egress (RF x
ingress). Scale model from 1 to 10,000 nodes:
[docs/system-design.md](docs/system-design.md#6-scale-model). Sizing 10 PB:
[docs/capacity-planning.md](docs/capacity-planning.md).

## Tradeoffs

36 decisions, each with problem, choice, alternatives, costs, failure modes
and when another design wins, plus the 10x/100x/1,000x review:
[docs/tradeoffs.md](docs/tradeoffs.md). Highlights:

- Ring over rendezvous hashing because placement is evaluated for every segment on every reconcile pass (0.66 µs vs 169 µs per lookup at 1,000 nodes, measured).
- No content addressing: per-write blob IDs make every blob exclusively owned, so GC needs an existence check rather than distributed reference counts.
- Linearizable metadata per shard, paid for with metadata availability (no consensus).
- Replication, not erasure coding; EC analysed for large cold data.

## Documentation index

| Document                                                      | Contents                                                                            |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| [architecture.md](docs/architecture.md)                       | components, metadata vs data plane, identity, lifecycle, commit protocols, security |
| [system-design.md](docs/system-design.md)                     | requirements, API, data model, concurrency design, scale model                      |
| [consistency.md](docs/consistency.md)                         | per-operation guarantees and assumptions                                            |
| [distributed-consistency.md](docs/distributed-consistency.md) | partitions, CAP per plane, durability vs availability scenarios                     |
| [tradeoffs.md](docs/tradeoffs.md)                             | 36 decisions; erasure coding; final scale review                                    |
| [failure-modes.md](docs/failure-modes.md)                     | 17 failure modes: detection, recovery, risks, operator response                     |
| [capacity-planning.md](docs/capacity-planning.md)             | 10 PB sizing: storage, metadata, network, IOPS, repair, cache                       |
| [metadata-scaling.md](docs/metadata-scaling.md)               | sharding, caching, hot prefixes, LIST, migration                                    |
| [evolution.md](docs/evolution.md)                             | V1-V12: how each stage fixes and introduces failure modes                           |
| [observability.md](docs/observability.md)                     | metrics, logs, tracing, alerts                                                      |
| [performance.md](docs/performance.md)                         | measured benchmarks, load tests, profiles                                           |
| [multi-region.md](docs/multi-region.md)                       | options, RPO/RTO, what would change (design only)                                   |
| [fault-injection.md](docs/fault-injection.md)                 | 12 experiments with expected behaviour                                              |
| [demo.md](docs/demo.md)                                       | 15 demonstrations with observed results                                             |
| [engineering-review.md](docs/engineering-review.md)           | explanations at 30 s / 2 min / 10 min, 65 questions, staff-level discussion         |

## Engineering Review

Start with [docs/engineering-review.md](docs/engineering-review.md): the 30-second
and 2-minute explanations, a 10-minute whiteboard walkthrough, 65 questions
(each with what is being tested, a strong answer, a follow-up, a common weak
answer and the tradeoff) and 24 open-ended staff-level prompts. Then rehearse
demos 4-8 from [docs/demo.md](docs/demo.md) live.

## Repository layout

```
cmd/            gateway, metadata, storage-node, repairer, rebalancer, loadgen
internal/       api, metadata, membership, placement, hashing, storage, storagenode, object,
                replication, quorum, repair, rebalancer, garbagecollector, cache, ratelimit,
                quota, config, observability, clusterauth, service, testcluster
pkg/client      Go client
api/openapi     OpenAPI 3 spec
docs/           design documents
scripts/        local-cluster.sh, smoke.sh, dev-config.sh
deploy/         docker-compose.yml, tenants.json, prometheus.yml
docker/         Dockerfile
tests/          integration, failure and recovery tests (full cluster in-process)
.github/        CI: gofmt, vet, test, race, benchmarks, cluster smoke test
```

## Limitations

Stated plainly, and discussed in the documents linked:

- **No consensus.** Each metadata shard and the coordinator is a single process. A shard outage makes its buckets unavailable; losing a shard's disk loses that shard's metadata.
- **Local filesystem**, one data directory per node; no multi-disk management, no direct I/O.
- **Replication only**; erasure coding is analysed, not implemented.
- **Repair and GC scan all metadata** each pass: fine to millions of segments, not billions.
- **Metadata in memory** per shard; fixed shard count.
- **Single region**; multi-region is a design.
- **Rate limits are per gateway**; quotas are exact within a shard and soft across shards.
- **Security:** a single shared cluster token instead of per-service mTLS; TLS supported but not enabled in the local setups; no encryption at rest; no per-object ACLs.
- **Tracing** is span logging with W3C propagation, not an OpenTelemetry exporter.
- **Measurements** come from a single 2-vCPU VM running every process; they show relative effects, not production performance.
- **Docker image:** the Compose wiring was verified with locally built binaries; the Dockerfile's base-image pull could not be exercised in the build environment.

## Roadmap

1. Raft-replicated metadata shards (or a FoundationDB/TiKV backend behind the `metadata.Store` interface).
2. Group commit for the metadata WAL.
3. Per-node blob index to drive repair and GC incrementally.
4. Re-adopt valid copies on rejoining nodes instead of deleting and re-copying.
5. Correlated-failure mode: suppress bulk repair during zone outages.
6. Erasure-coded storage class with background conversion.
7. Range-sharded metadata with automatic splitting.
8. mTLS between services; encryption at rest.
9. Multi-region asynchronous replication with per-bucket home regions.
