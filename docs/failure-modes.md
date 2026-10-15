# Failure Modes

Each entry uses the same fields. "Test" points to the automated test or
demo that exercises the behaviour; "observed" values come from the local
cluster runs recorded in [demo.md](demo.md).

Defaults referenced below: RF=3, W=2, suspect after 3 s, DOWN after 10 s
(local cluster: 6 s), repair delay 30 s (local: 10 s), GC grace 1 h
(local: 30 s).

---

## 1. Storage node crash

| Field | |
|---|---|
| Detection | Missed heartbeats: SUSPECT after `-suspect-after`, DOWN after `-down-after`. Gateways also see connection errors immediately and fail over per request. |
| Recovery | Reads fail over to other replicas at once. Writes are placed on other ACTIVE nodes (a SUSPECT node gets no new data). After `-repair-delay`, the repairer re-replicates every segment that had a copy on the node. If the node returns, its copies are either re-used (if still listed) or garbage-collected (if repair already replaced them). |
| Data loss risk | None for segments with another healthy copy. Elevated during the reduced-redundancy window (repair delay + copy time): a second and third failure in that window can lose segments whose copies were all on the failed nodes. |
| Availability impact | None for reads (latency blip on failover). Writes unaffected while at least W writable nodes exist in placement. |
| Consistency impact | None. Metadata is unaffected; data is immutable. |
| Repair behavior | Segments with one remaining copy are repaired first and immediately (no delay). Bandwidth-limited (`-bandwidth-bytes-per-sec`). |
| Operator response | Investigate the host. If it will not return, mark it DECOMMISSIONED after repair finishes. Watch `repair_under_replicated_segments` reach 0. |
| Test | `TestNodeFailureReadAvailabilityAndRepair`, `TestRepairDelayAvoidsStorms`; demo 5 (30/30 reads succeeded immediately after SIGKILL; 99 segments / 1.08 GB repaired at the 50 MB/s budget). |

## 2. Disk failure

| Field | |
|---|---|
| Detection | Writes fail with I/O errors (the replica is dropped from that write); reads fail with I/O errors or CRC mismatches (blob quarantined). The node keeps heartbeating, so it is not DOWN. |
| Recovery | Each failed read quarantines the blob and reports it; the repairer removes the bad replica from metadata and re-replicates. The scrubber finds blobs nobody reads. |
| Data loss risk | Same as node crash, but detection of cold data is bounded by the scrub interval. |
| Availability impact | Requests touching the disk fail over; latency increases. |
| Consistency impact | None. |
| Repair behavior | Per-blob as blobs are discovered. Limitation: one node = one data directory here, so a disk failure looks like a sick node. Production nodes manage many disks and would mark one disk failed and repair its contents as a unit. |
| Operator response | Replace the disk; restart the node with an empty data dir (it rejoins empty and the rebalancer fills it). |
| Test | Simulated by corruption and `fail_reads` faults (`TestFaults`, `TestCorruptionDetectionFailoverAndRepair`). |

## 3. Disk full

| Field | |
|---|---|
| Detection | Engine capacity check per chunk (`ErrDiskFull`, HTTP 507) or real `ENOSPC`. Heartbeats report used/capacity; placement skips nodes above 95% used. |
| Recovery | The write drops that replica and continues on the others; if W are still met, it succeeds under-replicated. Temporary files of the failed write are deleted. |
| Data loss risk | None. |
| Availability impact | None while other nodes have space; cluster-wide fullness eventually fails writes with 503. |
| Consistency impact | None. |
| Repair behavior | The missing replica is placed elsewhere by repair. The rebalancer refuses to move data onto nodes above `-target-max-fullness`. |
| Operator response | Add capacity; check for GC backlog (`gc_queue_depth`) and abandoned uploads. |
| Test | `TestDiskFull`; demo 13 (write succeeded with `X-Replicas-Acked: 2/3`). |

## 4. Corrupted object (bit rot, torn write)

| Field | |
|---|---|
| Detection | CRC32C per 64 KiB block is checked **before** a block's bytes leave the node; the full SHA-256 is checked by the scrubber and on verify. Sidecar has its own CRC. Data/sidecar length mismatch is detected on stat. |
| Recovery | Node quarantines the blob (moved out of the serving path, not deleted) and aborts the response. Gateway resumes the read on another replica at the exact byte offset. Repairer drops the bad replica and re-replicates from a good copy, verifying SHA-256 on arrival, then acknowledges (deletes) the quarantined file. |
| Data loss risk | Only if every replica is corrupt. |
| Availability impact | None if another replica is good. |
| Consistency impact | None: corrupt bytes are never served. |
| Repair behavior | Immediate (quarantine reports are processed at the start of each repair pass). |
| Operator response | A rising `storage_node_corruptions_total` on one node indicates failing hardware. |
| Test | `TestCorruptionDetectedBeforeBytesServed`, `TestCorruptionMidStreamAbortsConnection`, `TestCorruptionDetectionFailoverAndRepair`; demo 6 (detected, failed over and repaired within ~160 ms). |

## 5. Replica failure during a write

| Field | |
|---|---|
| Detection | Error or non-matching checksum from the replica's PUT; or the replica's queue stays full for `-replica-stall-timeout`; or it acks more than `-straggler-timeout` after the W-th ack. |
| Recovery | The replica is dropped and its request cancelled (no clean EOF, so it cannot commit a truncated blob). The write continues with the rest. A node dropped for stalling or straggling is penalized by that gateway for `-slow-node-penalty`: new segments avoid it, so a slow node costs one timeout, not one per segment. If avoiding every penalized node would leave fewer than RF candidates, penalized nodes are re-admitted one at a time, fewest strikes first (a node gains a strike each time it is slow again), so one false positive on a healthy node does not let a persistently slow node back in. |
| Data loss risk | None for acknowledged writes (W copies). |
| Availability impact | If fewer than W replicas remain, the write fails with 503 `InsufficientReplicas`. |
| Consistency impact | None. Metadata lists only acknowledged replicas. |
| Repair behavior | Under-replicated segment repaired immediately (no node is DOWN, so no delay applies). |
| Operator response | Look at `gateway_replica_write_failures_total{reason}` and `gateway_penalized_nodes`. A node that keeps being penalized is sick even if it heartbeats: drain it. |
| Test | `TestReplicaFailureDuringPutThenRepair`, `TestWriteQuorumUnreachable`, `TestDurabilityPolicyW3`, `TestBackpressureSlowReplica`, `TestSlowNodePenalizedAcrossSegments`; demo 4 (node killed mid-upload: 6 segments with 3 replicas, 6 with 2, 200 OK, content intact). |

## 6. Network partition

| Field | |
|---|---|
| Detection | Same signals as node failure (heartbeat timeout, connection errors). A partition cannot be distinguished from a crash by the coordinator. |
| Recovery | See [distributed-consistency.md](distributed-consistency.md) scenarios 2-4. A node cut off from the coordinator but reachable by gateways is falsely declared DOWN and repaired; on healing, excess replicas are trimmed and stale copies swept. |
| Data loss risk | None from the partition itself. |
| Availability impact | Gateways cut off from a metadata shard return 503 for its buckets. Writes need W reachable replicas. |
| Consistency impact | None: one authority per key; a gateway that cannot reach it does not answer. |
| Repair behavior | False-positive repair after the delay: bandwidth cost, temporary over-replication. |
| Operator response | Fix the network. If the partition will be short, raise the repair delay temporarily to avoid unnecessary repair traffic. |
| Test | `partitioned` fault (`TestFaults`); `TestNodeReturnsAfterRepair` covers the healing side. |

## 7. Metadata service failure (shard down)

| Field | |
|---|---|
| Detection | RPC errors at gateways (`gateway_metadata_seconds`, 503 `MetadataUnavailable`). |
| Recovery | Restart the shard: it loads the latest snapshot and replays the WAL (deterministic: records contain effects, not requests). A torn final WAL frame is detected by length/CRC and truncated. |
| Data loss risk | With fsync on: none for acknowledged commits. **Loss of the shard's disk loses that shard's metadata**: its objects' blobs survive but nothing references them (they would eventually be garbage-collected). This is the single most important limitation of the project; production replicates each shard (Raft). |
| Availability impact | All operations on buckets owned by the shard fail (unless served from the opt-in gateway metadata cache). Other shards unaffected. |
| Consistency impact | None (fails closed). |
| Repair behavior | Repair and GC fail their scans for that shard; GC never deletes on errors. |
| Operator response | Restore the process; back up WAL + snapshot regularly. |
| Test | `TestMetadataUnavailable`; WAL replay and torn-frame tests in `internal/metadata`; demo (metadata fault: 503, immediate recovery). |

## 8. Stale metadata

| Field | |
|---|---|
| Detection | Replica in metadata is missing on the node (404 / quarantine): failover + repair. Blob on a node not in metadata: GC sweep. |
| Recovery | Repairer and GC converge both directions. |
| Data loss risk | None by construction: ordering guarantees metadata never references a blob that was not durably written, and never stops referencing a replica after deleting it. |
| Availability impact | A read may try a missing replica first (one failover). |
| Consistency impact | None for versions. The gateway's opt-in metadata cache can be up to TTL stale (bounded staleness, documented). |
| Repair behavior | As above. |
| Operator response | None normally. |
| Test | `TestNodeReturnsAfterRepair`, `TestGarbageCollection`. |

## 9. Partial upload

| Field | |
|---|---|
| Detection | Body shorter than Content-Length (`io.ErrUnexpectedEOF`), client idle beyond `-client-idle-timeout`, `X-Checksum-SHA256` mismatch, or size limit exceeded. |
| Recovery | The gateway aborts the replica streams with an error (never a clean EOF), so no node commits a truncated blob; nothing is committed to metadata. Segments that completed before the failure are unreferenced but pending-protected; GC reclaims them after `PendingTTL`. |
| Data loss risk | None (nothing was acknowledged). |
| Availability impact | None. |
| Consistency impact | None: the previous version stays current; no half object is visible. |
| Repair behavior | n/a; GC cleans up. |
| Operator response | None. For large objects, clients should use multipart so only a part is retried. |
| Test | `TestPartialWriteNeverVisible`, `TestClientDisconnectMidUpload`; demo 14 (30 of 50 MiB then disconnect: 404, zero temp files left). |

## 10. Client disconnect

| Field | |
|---|---|
| Detection | Request context cancelled; write errors to the response. |
| Recovery | Upload: as partial upload. Download: the gateway stops reading from the replica (connection closed); no other effect. A slow-but-connected downloader is bounded by a per-write deadline so it cannot pin a slot forever. |
| Data loss risk | None. |
| Availability impact | Frees the tenant's concurrency slot. |
| Consistency impact | None. If the disconnect happens after the data is durable, the metadata commit still completes (detached context) so the outcome is not left ambiguous on the server side. |
| Repair behavior | n/a. |
| Operator response | None. |
| Test | `TestClientDisconnectMidUpload`; `copyTracked` distinguishes client write errors from replica read errors, so a disconnecting client is never blamed on a replica. |

## 11. Checksum mismatch

| Field | |
|---|---|
| Detection | Client-supplied `X-Checksum-SHA256` vs computed (gateway); replica-reported SHA-256 vs gateway's (replica write); metadata SHA-256 vs pulled bytes (repair/rebalance); block CRCs (reads). |
| Recovery | Upload: 400 `BadDigest`, nothing committed. Replica: dropped. Repair: copy rejected, source treated as suspect. Read: failover + quarantine. |
| Data loss risk | None. |
| Availability impact | The failed request only. |
| Consistency impact | None. |
| Repair behavior | As for corruption. |
| Operator response | Repeated mismatches between gateway and one node suggest a bad NIC or memory on that host. |
| Test | `TestPullVerifiesChecksum`, multipart demo (bad part rejected, retried alone). |

## 12. Failed repair

| Field | |
|---|---|
| Detection | `repair_failures_total`; log line with blob and error. |
| Recovery | The job is retried next pass. Copy is idempotent: an identical blob already on the target is accepted. A copy that lands but whose metadata update fails is an orphan for GC. |
| Data loss risk | Prolongs the reduced-redundancy window. |
| Availability impact | None directly. |
| Consistency impact | None: metadata is updated only after a verified copy. |
| Repair behavior | Retried with the same priority ordering (fewest healthy copies first). |
| Operator response | Check failures by error. Real example from this project's own testing: repair failed 100% of the time with `401` because node-to-node pulls did not carry the cluster token. The metric made it obvious; the fix was a configurable peer HTTP client (`TestPullBetweenAuthenticatedNodes`). |
| Test | `TestNodeFailsDuringRepair`. |

## 13. Rebalancing failure

| Field | |
|---|---|
| Detection | Log errors; `rebalance_misplaced_segments` not decreasing. |
| Recovery | Copy-before-delete means every intermediate state has at least RF copies. An interrupted move leaves either an extra unreferenced copy (GC) or a temporarily over-replicated segment (trimmed next pass). |
| Data loss risk | None. |
| Availability impact | None. |
| Consistency impact | None. |
| Repair behavior | Rebalancer skips under-replicated segments; repair goes first. |
| Operator response | None; it resumes next pass. |
| Test | `TestRebalanceOnNodeAddition`, `TestNodeDrain`. |

## 14. Hot object

| Field | |
|---|---|
| Detection | `gateway_requests_total` skew; `gateway_object_cache_hits_total`; node `storage_node_bytes_out_total` concentrated on 3 nodes. |
| Recovery | Small objects (default up to 1 MiB) are served from the gateway's LRU after the first read; concurrent misses are coalesced (single-flight), so a cold hot key causes one backend read, not thousands. Reads of large objects are randomized across replicas. |
| Data loss risk | None. |
| Availability impact | Without the cache, the 3 replica nodes' NICs bound throughput for that object. |
| Consistency impact | None: cache keyed by immutable version ID. |
| Repair behavior | n/a. |
| Operator response | For large hot objects: increase RF for that bucket, put a CDN in front, or raise the cacheable size. |
| Test | `TestHotObjectCacheAndStampede` (100 concurrent cold reads -> 1 backend load); demo 9 (99.6% hit rate; clients received ~3 GB while nodes served ~13 MB). |

## 15. Cache failure

| Field | |
|---|---|
| Detection | n/a (in-process caches cannot fail independently); a gateway restart empties them. |
| Recovery | Cold caches refill on demand. |
| Data loss risk | None: caches hold no authoritative state. |
| Availability impact | A restart of all gateways at once turns all hot-object reads into backend reads: a load spike on the replica nodes of hot objects. Single-flight limits it to one load per object per gateway. |
| Consistency impact | None. |
| Repair behavior | n/a. |
| Operator response | Restart gateways one at a time. |
| Test | Cache unit tests. |

## 16. Coordinator failure

| Field | |
|---|---|
| Detection | Heartbeat errors in node logs; membership refresh warnings in gateway logs. |
| Recovery | Restart: operator states (DRAINING, DECOMMISSIONED) are persisted; every known node gets a fresh heartbeat window so the cluster is not declared dead on restart. |
| Data loss risk | None directly; failure detection is paused, so repair of a node that dies meanwhile is delayed. |
| Availability impact | None for reads and writes (last membership snapshot). No nodes can join or change state. |
| Consistency impact | None. |
| Repair behavior | Paused (membership frozen). |
| Operator response | Restart. Production: replicate the coordinator (it is small; a Raft group or the metadata store itself). |
| Test | Membership unit tests (persistence and restart window). |

## 17. Region failure

| Field | |
|---|---|
| Detection | Everything in the region is unreachable. |
| Recovery | **Not supported.** The implementation is single-region; placement understands a `region` attribute but there is no cross-region replication. |
| Data loss risk | Total for the region's data if the loss is permanent. |
| Availability impact | Total. |
| Consistency impact | n/a. |
| Repair behavior | n/a. |
| Operator response | Restore from backup. |
| Design | [multi-region.md](multi-region.md) describes asynchronous replication of committed versions, RPO/RTO and conflict handling. |

---

## Correlated failures

The failure modes above assume independence. Real failures correlate:

- **Same rack / same power / same top-of-rack switch.** Placement spreads replicas across zones first, then racks.
- **Same disk model / firmware batch.** Placement cannot see this; operators mix batches across zones.
- **Same software bug.** A bad release on every node at once defeats replication. Rolling upgrades one zone at a time, with health gates, are the mitigation.
- **Repair-induced overload.** A large failure triggers repair traffic that slows the surviving nodes, which then time out and are declared DOWN: the failure spreads. The repair delay, bandwidth cap and concurrency limit exist to break this feedback loop.
- **Zone imbalance.** With one replica per zone, a zone with fewer nodes receives proportionally more data per node. Observed in the local cluster: with zones of 1, 2 and 1 nodes, the single-node zones took twice the write bytes of the others (see [performance.md](performance.md)). Capacity must be planned per zone.
