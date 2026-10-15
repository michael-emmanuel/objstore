# Distributed Consistency, Partitions, Durability and Availability

[consistency.md](consistency.md) defines the guarantees. This document tests
them against concrete failure scenarios: what each party can observe, and
which of consistency, availability or durability the system gives up.

## Vocabulary

- **Durability:** will acknowledged data survive failures? A property of how
  many independent copies exist, and how quickly lost copies are recreated.
- **Availability:** can a request be served _now_?
- **Consistency:** does a successful read return the latest committed version?

They are different properties, and the system trades them against each other
in specific, configurable places.

## CAP, applied plane by plane

CAP says that during a network partition a system must choose, for a given
operation, between answering (availability) and answering correctly
(linearizability). It does not force one choice for the whole system.

| Plane / operation                             | Choice during a partition                                                                                                            | Why                                                                                                                                               |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Metadata reads and writes                     | **Consistency.** If the gateway cannot reach the owning shard, it returns 503.                                                       | The shard is the single authority for "which version is current". Answering from anywhere else could return a superseded version or lose a write. |
| Metadata reads with `-metadata-cache-ttl > 0` | **Availability**, bounded staleness.                                                                                                 | Explicit opt-in: serve cached versions during an outage.                                                                                          |
| Data reads                                    | Effectively both, because blobs are immutable: any reachable replica with a matching checksum is correct. If none is reachable, 503. | No conflict is possible on immutable data.                                                                                                        |
| Data writes                                   | **Durability threshold W.** Succeed iff W replicas are reachable and commit.                                                         | We will not acknowledge below the configured durability.                                                                                          |
| Membership                                    | Availability: gateways use their last snapshot.                                                                                      | Stale membership only affects placement efficiency, never correctness, because metadata records the replicas actually written.                    |

## Scenario 1: replica 1 acks, replica 2 unreachable, replica 3 "stale"

Setup: RF=3, object `k` currently at version v1. A client PUTs v2.
Placement for v2's segment is [n1, n2, n3]. n1 acknowledges, n2 is
unreachable, and n3 is "stale".

First, what does "stale" mean here? n3 cannot hold an old value of v2's blob
because the blob ID is new. "Stale" can only mean n3 has _not received_ the
new blob (it is slow or its write failed), which is the same as "absent".

| W                             | Outcome for the writer                                                                                 | What any reader observes                                                                                                          |
| ----------------------------- | ------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| 2 (default)                   | Only 1 ack. After n3 stalls for `-replica-stall-timeout`, the write fails: 503 `InsufficientReplicas`. | v1, before and after. v2 never existed. n1's copy is an orphan, reclaimed by GC after the pending TTL.                            |
| 2, n3 eventually acks in time | 2 acks: 200 with `X-Replicas-Acked: 2/3`. Metadata records [n1, n3].                                   | Before the commit: v1. After: v2, read from n1 or n3 only. Repair later adds a third replica.                                     |
| 1                             | 200 after n1's ack. Metadata records [n1].                                                             | v2, from n1. If n1's disk dies before repair copies it, v2 is lost: reads return 503 `DataUnavailable` (not v1, not wrong bytes). |
| 3                             | 503.                                                                                                   | v1.                                                                                                                               |

The client never observes a mix of v1 and v2, and never observes v2's bytes
from a replica that does not have them, because readers only consult replicas
listed in metadata, and only replicas that acknowledged are listed.

## Scenario 2: the gateway is partitioned from the metadata shard

The gateway can reach storage nodes but not shard 1.

- PUT to a bucket on shard 1: `RegisterPending` fails before any byte is
  sent; 503. If the partition happens after the data is written but before the
  commit, the commit fails; the blobs are orphans; 503. The client retries
  later. Nothing partial is visible to anyone.
- GET: 503 unless the opt-in metadata cache holds the key.
- Buckets on shard 0 are unaffected. Partitioning by bucket limits the blast
  radius of a shard outage to the buckets it owns.

## Scenario 3: a storage node is partitioned from the coordinator, but not from gateways

n2 stops heartbeating (the partition blocks it from the coordinator), but it is
still serving data to gateways.

1. After `-suspect-after` (3 s locally): n2 is SUSPECT. Gateways stop placing
   new data on it but still read from it.
2. After `-down-after`: DOWN. Readers deprioritise it (last resort only).
3. After `-repair-delay`: the repairer copies n2's segments to other nodes.
   This is a **false positive**: n2 was alive. The cost is repair traffic and
   temporary over-replication.
4. When the partition heals, n2 heartbeats again and becomes ACTIVE. Segments
   now have RF+1 copies. The rebalancer trims the excess; the GC sweep deletes
   copies that metadata no longer lists on n2.

Nothing is inconsistent at any point; the cost of the false positive is
bandwidth. This is why detection uses two thresholds and repair waits an
additional delay: the cost of declaring a live node dead is a repair storm,
and the cost of waiting is a longer window with reduced redundancy. See
[tradeoffs.md](tradeoffs.md#22-repair).

## Scenario 4: two gateways write the same key from different sides of a partition

Both gateways can reach the owning shard (otherwise they fail; scenario 2).
Both write their blobs to whichever storage nodes they can reach (each needs W
acks). Both commits arrive at the single shard and are serialized. The later
`Seq` wins; the earlier becomes noncurrent or garbage. There is no
"split-brain" because there is exactly one authority per key.

The price of that guarantee: a gateway that cannot reach the shard cannot
write at all. An active-active multi-region design that accepts writes on both
sides of a partition must instead resolve conflicts (last-writer-wins by a
hybrid clock, or keep both as versions); see [multi-region.md](multi-region.md).

## Scenario 5: the coordinator is down

- Heartbeats fail; failure detection stops; no node can change state.
- Gateways, the repairer and the rebalancer keep their last membership snapshot.
- Reads and writes continue normally. If a node dies during the coordinator
  outage, writes that target it lose one replica (they still succeed with W),
  reads fail over, and repair does not start until the coordinator returns.
- On restart, the coordinator gives every known node a fresh heartbeat window
  rather than declaring the whole cluster DOWN.

## Scenario 6: a whole availability zone fails

With 3 zones and RF=3, placement puts one replica in each zone when possible.
Losing a zone leaves 2 copies of every segment: all data stays readable and
writes still reach W=2 if the other two zones have writable nodes. Repair
cannot restore 3 zone-distinct copies until the zone returns (only 2 zones
exist); it places the third copy in an already-used zone, preferring a
different rack. When the zone returns, the rebalancer moves copies back so
that each segment spans 3 zones again.

Metadata is not zone-redundant in this implementation: if the zone that hosts
a shard fails, that shard's buckets are unavailable until it returns.

## Durability versus availability, with examples

A system can **refuse service to protect durability**. Examples in this
implementation:

| Mechanism                                                                | Gives up                                                 | Protects                                                   |
| ------------------------------------------------------------------------ | -------------------------------------------------------- | ---------------------------------------------------------- |
| W=3 for a critical bucket's gateway                                      | write availability when any one replica target is down   | every acknowledged write exists on 3 nodes                 |
| Refusing to commit when fewer than W replicas acked                      | write availability                                       | the durability promise attached to a 200                   |
| fsync before ack (storage and metadata)                                  | latency and throughput (3-6x for small writes, measured) | acknowledged data survives power loss                      |
| Shard goes read-only after a WAL append failure                          | write availability of that shard                         | the WAL never contains a valid record behind a corrupt one |
| GET aborts the response on mid-stream corruption with no healthy replica | availability of that object                              | never serving wrong bytes as success                       |
| GC never deletes when metadata does not answer                           | storage efficiency                                       | never deleting live data on a transient error              |

And the reverse, **accepting reduced durability for availability**:

| Mechanism                                            | Gives up                              | Gains                                                                                    |
| ---------------------------------------------------- | ------------------------------------- | ---------------------------------------------------------------------------------------- |
| W=2 of 3                                             | a window where a segment has 2 copies | writes continue during a single-node outage; tail latency set by the 2nd-fastest replica |
| Repair delay after DOWN                              | longer reduced-redundancy window      | no repair storm when a node reboots                                                      |
| Dropping stalled replicas (`-replica-stall-timeout`) | one copy (repaired later)             | one slow disk does not slow every write                                                  |

_durability is about the number of independent copies and the time to recreate lost ones; availability is about whether the copies you need right now are reachable. W, repair delay and repair bandwidth are the knobs that move the system along that line._
