# Multi-Region Design

**Status: design only.** The implementation is single-region. Nodes carry a
`region` attribute and placement can see it, but there is no cross-region
replication, no region failover and no global metadata. This document
describes what would be built and what would have to change.

## Goals and constraints

| Goal | Constraint |
|---|---|
| Survive the loss of a region | cross-region RTT 30-150 ms; synchronous writes would add it to every PUT |
| Serve reads near users | inter-region bandwidth is expensive (cloud egress pricing) |
| Respect data sovereignty | some buckets must never leave a jurisdiction |
| Keep per-key consistency where promised | partitions between regions are normal events |

## Options

```
Option 1: primary + remote replicas      Option 2: active-passive           Option 3: active-active
  Region A (writes, reads)                 Region A (active)                  Region A <---> Region B
     |  async copy of committed versions      | async                           \          /
     v                                        v                                  Region C
  Region B, C (reads, stale)               Region B (standby, promoted        every region accepts
                                            on failover)                      writes; conflicts resolved
```

### 1. Single-region primary with remote replicas

Each bucket has a home region holding its metadata authority and RF copies.
A replication worker tails the home shard's committed versions (its WAL is an
ordered change stream; `Seq` gives the order) and copies the version record
plus blobs to remote regions, which serve **read-only, possibly stale**
copies.

- Consistency: linearizable in the home region; remote reads are stale by the replication lag but never torn (a version is published remotely only after all its blobs have arrived and verified).
- Cost: inter-region bandwidth = write ingress x number of remote regions (one copy per region, then RF locally from that copy).
- Failure: the home region failing makes the bucket read-only elsewhere.

### 2. Active-passive

Option 1 plus **failover**: when the home region is lost, an operator (or an
automated controller with fencing) promotes a standby region to home.

- **RPO** = replication lag at the moment of failure: commits not yet shipped are lost (or recovered later if the region returns, as conflicting versions).
- **RTO** = detection + decision + promotion (re-pointing routing, starting to accept writes): minutes with automation, longer with a human decision, which is often the right call because a false failover creates two writers.
- **Fencing** is mandatory: the old primary must be prevented from accepting writes after promotion (lease expiry, epoch numbers checked on every commit).

### 3. Active-active

Every region accepts writes for every bucket. This keeps writes local and
available during partitions, and gives up per-key linearizability.

- **Conflict:** two regions commit different versions of the same key during a partition (or just within the lag window).
- **Resolution options:**
  - *Last-writer-wins by hybrid logical clock* (HLC: physical time + logical counter, with the region ID as tiebreaker). Simple; silently discards a concurrent write. This is effectively what versioned object stores do across regions.
  - *Keep both as versions.* Natural for a versioned bucket: both versions exist and the "latest" pointer follows LWW; nothing is lost, and a client can inspect history.
  - *Application-level merge*: not meaningful for opaque blobs.
- **Deletes** need tombstones that replicate and win or lose by the same rule; tombstones must be retained longer than the maximum replication lag, or a delayed write can resurrect a deleted object.

## Recommended design for this system

**Per-bucket home region (active-passive), with optional read replicas, and
versioning required for cross-region buckets.**

Reasons:

1. It preserves the property the rest of the design depends on: one authority per key. Every consistency argument in [consistency.md](consistency.md) continues to hold within the home region.
2. Object storage writes are usually region-affine (the application writing a bucket lives in one region), so remote writes are rare.
3. Versioning makes failover safe to reason about: anything replicated late is just another version.

## What would have to change

| Area | Change |
|---|---|
| Metadata | A global, replicated bucket directory (bucket -> home region, replica regions, sovereignty policy). Per-bucket metadata stays regional. |
| Change stream | Expose each shard's committed records in `Seq` order with a durable consumer cursor per remote region. The WAL already records effects deterministically; it needs retention beyond snapshot compaction. |
| Replication worker | Per (shard, remote region): read changes, copy blobs (verified by SHA-256), then commit the version remotely with the original version ID and Seq. Idempotent by version ID. |
| Remote commit | A "replicated commit" API that inserts a version with a given ID and origin Seq, without making it latest if a newer one exists. |
| Version ordering | Add origin region + HLC to versions so a total order exists across regions (needed for active-active or for reconciling after a failover). |
| Gateways | Route writes to the home region (or reject with a redirect); serve reads locally with an explicit `X-Replication-Lag` or `stale-ok` semantics. |
| Failover | Epoch-fenced home-region change in the directory; runbook and controller. |
| Deletes | Replicate delete markers; GC must not purge a marker until all regions have acknowledged it. |
| Data sovereignty | Placement policy evaluated before replication: a bucket pinned to `eu` never gets a non-EU replica region. |
| Observability | Replication lag per (shard, region) in seconds and bytes; oldest unreplicated commit age = the current RPO. |

## Replication latency, bandwidth and cost

```
lag      = queueing + blob transfer time + remote commit
         ~ RTT (50 ms) + object size / per-stream bandwidth, plus backlog
bandwidth: 10 GB/s ingest replicated to 2 regions = 20 GB/s cross-region
           = ~52 PB/month; at typical cloud inter-region prices this can
           exceed the storage cost, so replication is opt-in per bucket.
```

Large objects dominate lag; a backlog after a partition must drain at a rate
that does not starve live replication (separate queues per priority, or
newest-first with a catch-up worker).

## RPO / RTO summary

| Option | RPO | RTO | Write latency | Consistency |
|---|---|---|---|---|
| 1. Primary + read replicas | lag (seconds) | n/a for writes (read-only elsewhere) | local | linearizable at home, stale reads remote |
| 2. Active-passive | lag (seconds) | minutes (automated) | local in home; remote writers pay RTT | linearizable within an epoch |
| 3. Active-active (LWW) | ~0 for acknowledged writes in surviving regions | ~0 | local everywhere | eventual; concurrent writes lost to LWW (or kept as versions) |
| Synchronous multi-region | 0 | ~0 | + cross-region RTT per write | linearizable |

## What the current implementation supports

- A `region` attribute on nodes, visible in placement and `/admin/cluster`.
- Immutable versions with a per-shard commit sequence: the foundation for an ordered change stream.
- Checksummed blobs and idempotent blob writes: the foundation for verified, retryable cross-region copies.

Nothing else in this document is implemented.
