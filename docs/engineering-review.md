# Engineering Review

Contents: A 30-second · B 2-minute · C 10-minute walkthrough ·
D Data plane · E Metadata plane · F Storage · G Replication ·
H Consistency · I Failure modes · J Scaling · K Performance ·
L Tradeoffs · M Staff-level · Staff-Level Design Discussion

---

## A. 30-second explanation

"It is a distributed object store in Go, built from first principles. A
stateless gateway streams objects into immutable blobs replicated across
availability zones, with a write quorum, and commits a version record to a
sharded metadata service: that commit is the only visibility point. Because
blobs never change, replicas can be missing but never stale, so all
consistency lives in a single-writer metadata plane, and background loops
(repair, rebalancing, garbage collection, scrubbing) converge the physical
world toward it. It is honest about its limits: metadata is not replicated
by consensus, and erasure coding and multi-region are designed but not
built."

## B. 2-minute explanation

1. **Problem:** store large amounts of data on unreliable machines, with
   clear durability, availability and consistency guarantees.
2. **Split:** the _metadata plane_ (buckets, versions, which blobs make up
   an object, where replicas are) is small, mutable and needs ordering; the
   _data plane_ (bytes) is huge and parallel. Keeping them separate puts
   coordination only where it is needed.
3. **Write:** the gateway cuts the body into 64 MiB segments, places each by
   consistent hashing on its blob ID across zones, streams it to RF nodes at
   once with bounded memory, waits for W fsynced, checksum-matched acks, then
   commits the version to the bucket's metadata shard.
4. **Read:** look up the version, read each segment from the best replica,
   verified block by block, failing over mid-stream at an exact byte offset.
5. **Healing:** heartbeats drive SUSPECT/DOWN; the repairer restores RF
   fewest-copies-first after a delay; the rebalancer moves data copy-before-
   delete when nodes join or drain; the GC reclaims dereferenced and orphaned
   blobs with grace periods and double checks; scrubbers find silent
   corruption.
6. **Isolation and ops:** per-tenant token buckets for requests and
   bandwidth, concurrency limits, quotas; Prometheus metrics, structured
   logs, trace propagation, fault injection, a load generator.
7. **Limits:** metadata shards are single processes (no Raft); one directory
   per node; replication only; single region.

## C. 10-minute architecture walkthrough

Draw this, then walk the lifecycle.

```
client -> gateway (auth, limits, placement, fan-out, caches)
            |-- metadata shard = hash(bucket) mod S  (versions, segments, replicas, uploads, GC queue, usage; WAL+snapshot)
            |-- coordinator (heartbeats -> ACTIVE/SUSPECT/DOWN; DRAINING/DECOMMISSIONED)
            `-- storage nodes (immutable blobs, CRC per 64 KiB, SHA-256, tmp+fsync+rename)
background: repairer + GC, rebalancer, per-node scrubber
```

1. **Identity (1 min).** Logical `(bucket, key, version)`; physical blob ID
   = hash of (bucket, key, version, segment index). Keys never become paths.
   Blob IDs are unique per write, hence immutable.
2. **PUT (2 min).** Auth -> limits -> per segment: place (ring + zones),
   register as pending (GC protection), stream to RF nodes (chunk queues,
   stall and straggler timeouts), need W acks -> commit version with quota
   ceiling -> respond with `X-Replicas-Acked`. What if replica 3 fails: W
   decides; metadata lists only acked replicas; repair fills the gap.
3. **Node commit protocol (1 min).** tmp file, SHA-256 + CRC per block,
   fsync, sidecar, rename, dir fsync. Crash at any step leaves nothing
   visible or a complete blob.
4. **GET (1 min).** Metadata lookup, replica order (same zone, healthy,
   random), block-verified streaming, mid-stream failover, hot-object cache
   keyed by version.
5. **Consistency (1 min).** Linearizable per key because one shard owns the
   bucket; data cannot be stale; R is verification, not version resolution.
6. **Failure handling (2 min).** Node death -> failover, SUSPECT, DOWN,
   repair after delay, fewest copies first. Corruption -> quarantine ->
   repair. Slow node -> dropped + penalized. Partition -> fail closed on
   metadata. Metadata outage -> its buckets unavailable (the main limitation).
7. **Scale (1 min).** What breaks first: per-shard commit rate (fsync),
   metadata memory, full-scan repair/GC, gateway fan-out egress. What I would
   replace: metadata with a Raft-replicated range-sharded store.

---

## D. Data-plane deep dive

### Q1. Why stream objects instead of buffering them?

- **Testing:** memory as a resource, backpressure, large-object thinking.
- **Strong answer:** Buffering makes memory proportional to object size times concurrency, which is unbounded. Here a write is a pipeline of 256 KiB chunks through per-replica queues of depth 8: memory per upload is about replicas x (depth + 2) x chunk, independent of size. The producer reads from the client only when every live replica has queue space, so the slowest replica sets the pace and TCP flow control pushes back on the client. Measured: gateway RSS stayed under 19 MB while streaming 1 GiB.
- **Follow-up:** "What happens to a slow replica?" Its queue stays full; after the stall timeout it is dropped (request cancelled so it cannot commit a truncated blob); if W remain, the write continues; the gateway penalizes that node for new placements.
- **Weak answer:** "Go's io.Copy streams, so it's fine." It ignores fan-out to N replicas and what happens when one is slow.
- **Tradeoff:** streaming means the gateway cannot retry a failed replica from the beginning (it holds no copy); the copy is repaired later. Spooling to gateway disk would allow retries at the cost of extra I/O.

### Q2. How do you handle a 100 GB object?

- **Testing:** large-object end to end: upload, placement, retries, reads, checksums.
- **Strong answer:** Use multipart: e.g. 1,000 parts of 100 MB uploaded in parallel; each part is an independently placed, replicated, checksummed blob; a failed part is retried alone; completion is one atomic metadata commit. A simple PUT also works (streamed, cut into 64 MiB segments placed independently, so ~1,600 segments spread across the cluster), but a failure means resending from the start. Reads use Range and can fetch segments from different nodes in parallel.
- **Follow-up:** "How is the whole-object checksum computed?" For multipart, a composite ETag (hash of part hashes plus `-N`); each part has its own SHA-256. A full-object SHA-256 would need a second pass or a client-supplied value.
- **Weak answer:** "Increase the timeout."
- **Tradeoff:** more metadata per object (one record per segment/part, ~150 B each) for parallelism and partial retry.

### Q3. How do you handle range reads?

- **Testing:** storage layout for random access, verification of partial data.
- **Strong answer:** The version's segment list has offsets; the gateway maps the byte range to segments and to offsets within them, and requests only those ranges from replicas. On the node, data is stored contiguously, and a CRC32C per 64 KiB block lets the node verify exactly the blocks it reads, so a range read is verified without reading the whole blob. Example: `bytes=1000000-1000099` of a 1 GiB object touches one block of one segment.
- **Follow-up:** "Why not just verify the SHA-256?" It covers the whole segment; verifying it would require reading 64 MiB for a 100-byte range.
- **Weak answer:** "Seek and read." Misses integrity.
- **Tradeoff:** sidecar storage (4 bytes per 64 KiB) and CRC CPU for verified random access.

### Q4. How do you detect corruption?

- **Testing:** layered integrity, where checks happen.
- **Strong answer:** Four layers. (1) Client can send `X-Checksum-SHA256`; mismatch -> 400, nothing committed. (2) Each replica computes SHA-256 of what it received; the gateway accepts only matching acks. (3) On every read, the node verifies each block's CRC32C before sending its bytes; mismatch -> quarantine + abort. (4) A rate-limited scrubber re-verifies every blob's CRCs and SHA-256 to find silent corruption in data nobody reads. Repair and rebalance verify SHA-256 from metadata on arrival.
- **Follow-up:** "What about corruption in the checksum itself?" The sidecar has its own CRC, and the SHA-256 in metadata is independent of the node.
- **Weak answer:** "TCP has checksums." 16-bit, and nothing at rest.
- **Tradeoff:** CPU (SHA-256 ~31% of gateway CPU under write load) for never serving bad bytes.

### Q5. Why use checksums? Aren't they authentication?

- **Testing:** knowing the difference between integrity against accidents and against adversaries.
- **Strong answer:** Checksums (CRC32C, even SHA-256 used unkeyed) detect accidental corruption: bit rot, torn writes, truncated transfers, bugs. They do not authenticate: anyone who can modify the data can recompute them. Authentication needs a keyed MAC or authenticated encryption (AES-GCM), with keys the attacker does not have.
- **Follow-up:** "Then why SHA-256 rather than CRC everywhere?" SHA-256 is collision resistant, so it can serve as content identity and as an end-to-end equality check across replicas; CRC is cheap and block-local.
- **Weak answer:** "SHA-256 is cryptographic, so it's secure."
- **Tradeoff:** collision resistance and identity vs ~14x CPU.

### Q6. What happens during a partial upload?

- **Testing:** atomicity, cleanup, ambiguous states.
- **Strong answer:** Nothing becomes visible: the metadata commit is the only visibility point and it happens only after all segments have W acks and the size/checksum matched. If the client disconnects or sends fewer bytes than Content-Length, the gateway aborts the replica streams _with an error_ (never a clean EOF, which would let a node commit a truncated blob). Temporary files are deleted; completed segments were registered as pending before the first byte, so GC leaves them alone until the pending TTL and then reclaims them. Demo: 30 of 50 MiB then disconnect -> 404, zero tmp files.
- **Follow-up:** "net/http reports a short body as io.ErrUnexpectedEOF; what if you treat that like EOF?" You would commit a truncated object. The writer passes it through as an error.
- **Weak answer:** "We delete the file."
- **Tradeoff:** completed segments of failed uploads occupy space until GC.

### Q7. How do you make PUT atomic?

- **Testing:** commit points across two stores.
- **Strong answer:** Two levels. Per blob on a node: tmp write, fsync, rename (atomic within a filesystem), dir fsync. Per object: the version record (all segments, replica lists) is committed in one WAL record by the owning shard. Readers find objects only through the version pointer, so they see the old version or the new one, never a mix. Ordering: blobs durable on W nodes before the commit; therefore metadata never references bytes that do not exist.
- **Follow-up:** "What if the gateway crashes after the commit but before responding?" The object exists; the client sees an error and may retry, creating another version with the same content: ambiguity is inherent, the outcome is safe.
- **Weak answer:** "Use a transaction." Across which systems?
- **Tradeoff:** orphans on failures (cheap, GC'd) instead of distributed transactions.

### Q8. Why use temporary files?

- **Testing:** crash consistency on POSIX filesystems.
- **Strong answer:** So that a file at its final name is always complete. Writing in place means a crash or concurrent read can observe a prefix. Writing elsewhere and renaming makes visibility a single atomic syscall, and startup recovery is trivial: delete everything in `tmp/`.
- **Follow-up:** "Is rename durable?" Not until the parent directory is fsynced; we do that.
- **Weak answer:** "To avoid conflicts."
- **Tradeoff:** an extra directory operation and fsync per blob.

### Q9. Why fsync?

- **Testing:** what "durable" means.
- **Strong answer:** `write` returns when data is in the page cache; a power loss loses it. An ack from a replica is a durability promise, so data and the sidecar are fsynced, and after the renames the directory is fsynced. Replication alone does not replace fsync because a rack or zone power event hits all page caches at once.
- **Follow-up:** "Can disks lie?" Yes, with volatile write caches; enterprise disks use power-loss protection; it is a hardware qualification issue.
- **Weak answer:** "The OS flushes eventually."
- **Tradeoff:** latency and throughput (next question).

### Q10. What is the cost of fsync?

- **Testing:** quantitative thinking.
- **Strong answer:** Measured here: 1 MiB blobs 3x slower with fsync (213 vs 639 MB/s), 64 KiB blobs 6x slower, metadata commits 21x slower (267 µs vs 13 µs, ~3,700 commits/s per shard). Small writes are dominated by fsync and filesystem metadata, not bandwidth. Mitigations: group commit in the WAL, packing small objects into append-only volumes (one fsync per batch), NVMe/NVRAM journals.
- **Follow-up:** "Why not periodic fsync?" Then an ack does not mean durable; you would need to tell clients.
- **Weak answer:** "fsync is slow, so we skip it."
- **Tradeoff:** latency vs the meaning of an acknowledgement.

## E. Metadata-plane deep dive

### Q11. Why separate metadata from object data?

- **Testing:** the core architectural idea.
- **Strong answer:** They scale and fail differently. Data scales with bytes, is immutable, and needs no coordination; metadata scales with object count and operation rate, is mutable, and needs per-key ordering. Separating them lets the data plane scale by adding nodes without consensus, lets repair and rebalancing move bytes by updating one replica list, and confines coordination to a small service. Costs: two hops per request, the metadata service as the critical dependency, and reconciliation between two stores (made safe by ordering rules).
- **Follow-up:** "Could you store metadata on the storage nodes?" Yes (each node indexes what it holds), but LIST becomes scatter-gather, and "which version is current" needs agreement among nodes.
- **Weak answer:** "Separation of concerns."
- **Tradeoff:** an extra hop and a critical dependency for scalability and simplicity.

### Q12. What happens if metadata is unavailable?

- **Testing:** dependency analysis, fail-closed vs fail-open.
- **Strong answer:** For buckets on the unavailable shard: reads and writes fail with 503; other shards are unaffected (bucket partitioning limits the blast radius). The data plane is intact but unreachable, because without the version record we do not know which blobs form the object. GC and repair skip work rather than act on missing answers. With the opt-in metadata cache, gateways serve cached versions (bounded staleness).
- **Follow-up:** "How would production avoid this?" Replicate each shard (Raft, 3 or 5 replicas across zones); a leader failover takes seconds.
- **Weak answer:** "Reads still work because data is on the storage nodes."
- **Tradeoff:** consistency (fail closed) vs availability (serve stale).

### Q13. Can objects still be read when metadata is down?

- **Testing:** precise reasoning, not hand-waving.
- **Strong answer:** Only if the gateway has the version cached (opt-in metadata cache; hot-object cache keyed by version needs the version first). Blob IDs are derived from (bucket, key, version, index), so without knowing the current version ID you cannot even compute them. A design for degraded reads would cache version records with a TTL or keep a read-only replica of metadata.
- **Follow-up:** "Would you make that the default?" No; it weakens read-after-write across gateways; make it a per-bucket policy.
- **Weak answer:** "Yes, data is replicated."
- **Tradeoff:** availability during outages vs stale reads.

### Q14. How do you scale metadata?

- **Testing:** sharding, replication, storage engines.
- **Strong answer:** Today: `hash(bucket) mod S` shards, each a single process with in-memory state, WAL and snapshots. Production: range partitioning on (bucket, key) with automatic splitting by size and load; each range replicated by Raft; an LSM engine on SSD; a small directory mapping ranges to groups; group commit. Caching reduces read load; only sharding and batching raise commit throughput.
- **Follow-up:** "Why not consistent hashing on (bucket, key)?" It destroys key order, so LIST becomes a merge across every shard.
- **Weak answer:** "Use a bigger database."
- **Tradeoff:** range sharding needs split/merge machinery; hash sharding cannot split a hot bucket.

### Q15. How do you scale LIST operations?

- **Testing:** understanding range scans.
- **Strong answer:** Keep keys ordered so a prefix list is a range scan; paginate with continuation tokens (key positions, stateless); bound each page (1,000 entries, scanning at most 10x that to skip delete markers). In a range-sharded store, a list may cross ranges: iterate ranges in order. For bulk enumeration (billions of keys), publish an inventory: a periodic export of the index as objects.
- **Follow-up:** "What about delimiter listing?" Group by the next delimiter after the prefix and skip ahead to the end of each group (seek past `prefix + group + 0xFF`), otherwise one huge "directory" costs a full scan.
- **Weak answer:** "Cache the list."
- **Tradeoff:** no snapshot across pages in exchange for bounded, stateless pages.

### Q16. Why are LIST operations harder than GET?

- **Testing:** point lookup vs scan.
- **Strong answer:** GET is a point lookup: O(log n), one owner, cacheable by key. LIST costs proportional to entries _examined_ (including delete markers and noncurrent versions), may span shards, cannot be cached usefully (any write in the range invalidates it), and has subtle consistency (pagination is not a snapshot).
- **Follow-up:** "How does S3 behave?" Strongly consistent LIST since 2020; the index is range-partitioned with splitting on hot prefixes.
- **Weak answer:** "LIST returns more data."
- **Tradeoff:** ordered index for LIST makes hot sequential prefixes a hotspot.

### Q17. How would you migrate the metadata schema?

- **Testing:** online migrations.
- **Strong answer:** Versioned records; readers accept old and new formats; deploy readers first, then writers that produce the new format, then a background rewrite, then remove the old reader. The WAL here stores JSON records describing effects, so adding optional fields is compatible; renaming or changing semantics needs the expand/contract sequence.
- **Follow-up:** "How do you roll back?" Only if the new writers are not yet emitting the new format, which is why the steps are separate deployments.
- **Weak answer:** "Stop the cluster, migrate, start."
- **Tradeoff:** longer, multi-step rollouts for zero downtime.

### Q18. Which state must be durable?

- **Testing:** authoritative vs derived state.
- **Strong answer:** Bucket configs, version records (segments, checksums, replica lists), multipart upload records, the garbage queue, tenant usage, operator node states, and the blobs. Everything else is derived.
- **Follow-up:** "Is the garbage queue really required?" Not strictly: the mark-and-sweep can find every orphan, but the queue makes reclamation precise and cheap.
- **Weak answer:** "Everything."
- **Tradeoff:** less durable state means less to replicate and recover.

### Q19. Which state can be reconstructed?

- **Testing:** same, the other way.
- **Strong answer:** Node liveness (heartbeats are soft state; a coordinator restart gives every node a fresh window), membership snapshots, all caches, repair queues, rebalance plans, scrub progress, pending registrations (they expire), usage could be recomputed from versions, and the garbage queue could be rebuilt by a sweep.
- **Follow-up:** "What about replica lists?" In principle by scanning every node for every blob (expensive); that is why they are durable.
- **Weak answer:** "Caches."
- **Tradeoff:** reconstructable state can be cheap and unreplicated, at the cost of recovery work.

### Q20. Where is the largest coordination bottleneck?

- **Testing:** finding the serialization point.
- **Strong answer:** The metadata shard commit path: one exclusive lock held across WAL append and fsync, so per-shard writes are bounded by ~1/fsync latency (~3,700/s measured). Every PUT, DELETE and multipart completion goes through it. Fixes: group commit, more shards, range splitting of hot buckets.
- **Follow-up:** "Why hold the lock during fsync?" It makes "current version" trivially unambiguous; group commit keeps ordering while amortizing fsync.
- **Weak answer:** "The network."
- **Tradeoff:** simplicity of a single serialization point vs throughput.

## F. Storage deep dive

### Q21. How do tombstones work?

- **Testing:** logical vs physical deletion.
- **Strong answer:** DELETE commits a delete marker as the newest version: one atomic metadata operation. GET returns 404. Dereferenced blobs go to the garbage queue with their replica list, and are physically deleted after a grace period. Markers in unversioned buckets are purged after `-tombstone-ttl`.
- **Follow-up:** "When is it safe to purge a tombstone?" When nothing older can reappear: here a single authority per shard makes that immediate; in replicated or multi-region metadata, only after every replica has seen it.
- **Weak answer:** "We mark deleted=true." Without the why.
- **Tradeoff:** space held until purge vs safety.

### Q22. Why not immediately delete object data?

- **Testing:** distributed deletion hazards.
- **Strong answer:** Immediate physical deletion breaks: concurrent readers streaming the old version; replicas on nodes that are down during the delete (they keep the data forever unless tracked); retries that race with re-creation; stale replicas resurrecting data after returning; and any chance of undo. Logical delete first, physical later, with re-checks, avoids all of these.
- **Follow-up:** "What about compliance deletion?" Needs a bounded-time physical path: tracked per-replica deletion with verification.
- **Weak answer:** "Deleting is slow."
- **Tradeoff:** delayed space reclamation.

### Q23. How does garbage collection work?

- **Testing:** safe reclamation.
- **Strong answer:** Two mechanisms. The queue: metadata enqueues every blob it dereferences (overwrite in unversioned buckets, permanent version delete, aborted upload, replaced part) with its replicas; after the grace period the GC re-checks the reference, deletes each copy, acks. The sweep: page through every node's blobs; a blob is an orphan if nothing references it and no write has it pending, or if it is referenced but not on this node; delete only if older than grace _and_ seen as an orphan in two consecutive sweeps. If metadata does not answer, nothing is deleted.
- **Follow-up:** "Why both?" The queue cannot know about failures it never saw (crashed writes, nodes returning after repair); the sweep is the safety net.
- **Weak answer:** "Reference counting."
- **Tradeoff:** delayed reclamation and O(blobs) sweeps for never deleting live data.

### Q24. What happens if garbage collection races with a new reference?

- **Testing:** race analysis.
- **Strong answer:** Three cases. (1) In-flight write: the gateway registers the blob as pending in metadata _before_ sending any byte; the sweep treats pending as referenced. (2) Repair/rebalance copy lands on node X before metadata lists X: the grace period (minimum age) and the two-sighting rule cover that window. (3) A queued blob that became referenced again: the re-check after grace sees it and drops the queue entry, keeping the data. Deletion only happens on positive, repeated evidence of non-reference.
- **Follow-up:** "What if the grace period is shorter than the longest upload?" Then a slow upload's early segments could be swept: pending registration (6 h TTL) is what protects uploads, not the grace period.
- **Weak answer:** "Use a lock."
- **Tradeoff:** longer grace = more garbage held; shorter = more risk.

### Q25. How would you detect silent data corruption?

- **Testing:** proactive integrity.
- **Strong answer:** Background scrubbing: re-read every blob periodically at a bounded rate, verify CRCs and SHA-256, quarantine and report mismatches; repair restores from good copies. Track scrub coverage (last full pass time) as an SLO. Compare replica checksums across nodes for a sample (they must be identical for the same blob ID). End-to-end checks on reads catch the rest.
- **Follow-up:** "How fast should scrubbing be?" Enough to cover all data well within the expected time between failures, without hurting client latency: e.g. full pass every 2-4 weeks.
- **Weak answer:** "Users will notice."
- **Tradeoff:** disk bandwidth spent on reads nobody asked for.

## G. Replication deep dive

### Q26. How do you choose the placement algorithm?

- **Testing:** decision-making from requirements and measurements.
- **Strong answer:** Requirements: deterministic, minimal movement on membership change, failure-domain aware, cheap enough to evaluate for every segment on every reconcile pass. Measured: ring and rendezvous both move ~9% of keys when adding the 11th node (modulo moves 91%). Rendezvous balances better (1.5% vs 6.2% stddev) but costs 169 µs per lookup at 1,000 nodes vs 0.66 µs for the ring. Repair and rebalance evaluate placement per segment, so I chose the ring; for small clusters rendezvous would be fine. Both are behind one interface (`-placement`).
- **Follow-up:** "Does correctness depend on placement?" No: metadata records actual replicas; placement only decides where data should converge.
- **Weak answer:** "Consistent hashing, because it's standard."
- **Tradeoff:** balance and simplicity (rendezvous) vs lookup cost (ring).

### Q27. Why not hash(key) % N?

- **Testing:** the classic.
- **Strong answer:** Changing N changes almost every key's owner: for 10 -> 11 nodes, measured 90.7% of keys move. That turns every expansion or failure into a near-total data reshuffle.
- **Follow-up:** "Is there any case where % N is fine?" Fixed N with a level of indirection (e.g. fixed number of placement groups, then a directory from groups to nodes), which is what Ceph does with PGs.
- **Weak answer:** "It isn't distributed."
- **Tradeoff:** simplicity vs movement.

### Q28. How does consistent hashing help?

- **Testing:** mechanism understanding.
- **Strong answer:** Nodes are hashed to many points (virtual nodes) on a ring; a key's owner is the first node clockwise from its hash; replicas are the next distinct nodes. Adding a node takes over only the ranges adjacent to its points: ~1/(N+1) of keys. Virtual nodes smooth balance (1 vnode: 3.2x max/mean; 256: 1.09x) and let weights reflect capacity.
- **Follow-up:** "How do you get distinct zones?" Walk the preference list and pick nodes from unused zones first, then racks, then any.
- **Weak answer:** "It distributes keys evenly." Not without vnodes.
- **Tradeoff:** vnode memory and imperfect balance.

### Q29. What happens when a node joins?

- **Testing:** membership change end to end.
- **Strong answer:** It heartbeats to the coordinator and becomes ACTIVE; gateways refresh membership and start placing new segments on it; the rebalancer finds segments whose desired placement now includes it, copies the blob there, adds it to metadata (RF+1 copies), removes the displaced replica from metadata, then deletes that copy. Bandwidth-limited. Observed: adding a fifth node moved 99 replicas (1.08 GB) at 20 MB/s.
- **Follow-up:** "What if it joins with old data?" Copies still listed in metadata are used; others are orphans and are swept.
- **Weak answer:** "The ring updates."
- **Tradeoff:** rebalance traffic vs balance.

### Q30. What happens when a node leaves?

- **Testing:** planned vs unplanned.
- **Strong answer:** Unplanned: SUSPECT (no new data, still readable), DOWN (readers deprioritize), after the repair delay its segments are re-replicated. Planned: DRAINING -> rebalancer migrates its replicas while it still serves reads -> decommission allowed when its referenced replica count is 0 (`safe_to_remove`); refused earlier unless forced.
- **Follow-up:** "Why is abrupt removal worse?" Every segment on the node is under-replicated at once, repair must copy everything urgently while redundancy is reduced, and the burst competes with client traffic.
- **Weak answer:** "Repair handles it."
- **Tradeoff:** draining is slower but has no durability window.

### Q31. How do you avoid moving all data?

- **Testing:** movement minimization.
- **Strong answer:** Consistent hashing (only the new node's share moves); recorded replica lists (nothing _has_ to move for correctness, so moves can be throttled); stable placement that ignores fullness (otherwise a node crossing a threshold would make all its data "misplaced" and oscillate); and moves only for fully replicated segments.
- **Follow-up:** "What if you change the vnode count?" That changes ownership broadly: treat it as a placement-algorithm migration.
- **Weak answer:** "Use consistent hashing."
- **Tradeoff:** convergence speed vs disruption.

### Q32. How do you choose replication factor?

- **Testing:** durability math and cost.
- **Strong answer:** From failure domains and failure rates: RF 3 across 3 zones survives a zone loss and keeps W=2 writes available. Durability depends on RF _and_ repair time: the risk is that the remaining copies fail within the repair window. Cheaper classes: RF 2 for reproducible data; erasure coding for large cold data.
- **Follow-up:** "When RF 5?" Very hot small objects (read capacity) or extremely critical metadata.
- **Weak answer:** "3 is standard."
- **Tradeoff:** storage and write bandwidth vs durability and read capacity.

### Q33. Why replicate across failure domains?

- **Testing:** correlated failures.
- **Strong answer:** Failures correlate: rack power, top-of-rack switch, zone outage, bad firmware batch. Three copies in one rack are one failure. Placement prefers distinct zones, then racks. Watch out for imbalance: with one replica per zone, a small zone takes proportionally more data per node (observed 2x).
- **Follow-up:** "What does it cost?" Cross-zone bandwidth on writes (two of three copies cross zones) and repair.
- **Weak answer:** "For safety."
- **Tradeoff:** cross-zone cost and per-zone capacity planning.

### Q34. How do you repair a missing replica?

- **Testing:** the repair pipeline.
- **Strong answer:** Classify every segment's replicas: healthy (ACTIVE/SUSPECT/DRAINING), lost (DOWN past the repair delay, unknown, decommissioned, quarantined), pending-down (DOWN within the delay). Queue segments with healthy < RF unless the deficit is covered by pending-down (but repair immediately when only one healthy copy is left). Order by fewest healthy copies. For each: choose a target via placement excluding current holders and preferring new failure domains; the target pulls from a healthy source and verifies the SHA-256 from metadata; then metadata adds the new replica and drops lost ones. Bandwidth- and concurrency-limited.
- **Follow-up:** "Why node-to-node?" The repairer does not become a bandwidth bottleneck and bytes take one hop.
- **Weak answer:** "Copy from another node."
- **Tradeoff:** scan cost (O(segments)) vs simplicity.

### Q35. What happens during repair if another node fails?

- **Testing:** compound failures.
- **Strong answer:** Jobs whose source or target was that node fail and are retried next pass with other choices. The newly failed node's segments enter the classification; any segment now at one copy jumps to the front of the queue and skips the delay. Data is lost only if all copies were on the failed nodes, which zone spreading makes unlikely for two failures with RF 3.
- **Follow-up:** "How do you bound the risk?" Short repair windows (bandwidth), declustered placement so many nodes participate in rebuilding, and priorities.
- **Weak answer:** "It retries."
- **Tradeoff:** repair aggressiveness vs load on survivors.

### Q36. How do you avoid repair storms?

- **Testing:** feedback loops.
- **Strong answer:** Two detection thresholds (SUSPECT then DOWN); an extra repair delay after DOWN so a rebooting node does not trigger copying of everything; bandwidth and concurrency caps; priority by remaining copies so the cap is spent on what matters; coordinator restart gives nodes a fresh heartbeat window instead of declaring the cluster dead. Without these, repair traffic slows survivors, they miss heartbeats, more nodes are declared down: a cascade.
- **Follow-up:** "What about a zone outage?" Do not try to rebuild a whole zone (10 PB at 10 GB/s is ~12 days); keep running on two copies and repair only single-copy segments. This project does not automate that distinction (documented).
- **Weak answer:** "Rate-limit repair."
- **Tradeoff:** a longer reduced-redundancy window vs stability.

### Q37. How would you change the placement algorithm?

- **Testing:** migrations on live data.
- **Strong answer:** Because actual replicas are recorded per segment, a placement change only changes the _desired_ location. Deploy the new algorithm to all placement consumers together (it must match across gateway, repairer and rebalancer, or they fight), then let the rebalancer converge with a bandwidth cap, monitoring `rebalance_misplaced_segments`. To limit churn, use a version field: new writes use the new algorithm, old segments move over time.
- **Follow-up:** "What if components disagree during rollout?" The rebalancer can oscillate; gate it off during the rollout.
- **Weak answer:** "Recompute and move everything."
- **Tradeoff:** convergence time vs migration traffic.

### Q38. How would you increase the replication factor?

- **Testing:** policy changes on existing data.
- **Strong answer:** Change the bucket's RF; new writes use it immediately; existing segments become "under-replicated" relative to the new RF and the repairer adds copies at its bandwidth budget. Capacity first: +1 copy of the bucket's data.
- **Follow-up:** "Does this project support it?" RF is per bucket and recorded per version; repair compares against the segment's RF, so raising it requires updating stored versions' RF (a metadata rewrite); not exposed as an API.
- **Weak answer:** "Restart with RF 4."
- **Tradeoff:** a temporary repair load.

### Q39. How would you reduce the replication factor?

- **Testing:** safe removal.
- **Strong answer:** Change the policy, then a trimming pass removes excess replicas: unpublish from metadata first, then delete physical copies via GC. Choose which replica to drop to preserve zone diversity. Never trim a segment that is not fully healthy.
- **Follow-up:** "What's the risk?" Lower durability; do it per storage class, not globally.
- **Weak answer:** "Delete a copy."
- **Tradeoff:** capacity vs durability.

## H. Consistency deep dive

### Q40. What is a quorum?

- **Testing:** fundamentals.
- **Strong answer:** A subset of N replicas whose agreement is required: W for writes, R for reads. If R + W > N, every read quorum intersects every write quorum, so some replica in the read set saw the latest acknowledged write.
- **Follow-up:** "What assumptions does that need?" Fixed membership of the N, no sloppy quorums, and a way to tell which response is newest.
- **Weak answer:** "A majority."
- **Tradeoff:** latency and availability of each operation vs overlap.

### Q41. How does quorum improve consistency, and how is it used here?

- **Testing:** applying theory to this design.
- **Strong answer:** In Dynamo-style stores, R + W > N gives read-your-writes for mutable values. Here, values are immutable blobs and version resolution happens in a single-writer metadata shard, so consistency does not come from R + W > N. W is a durability threshold (acknowledged data exists on W disks). R is read verification (`?consistency=quorum` checks R replicas' checksums before serving), useful for detecting missing replicas, not for freshness.
- **Follow-up:** "So why have R at all?" Integrity assurance for critical reads and as a cheap way to surface missing replicas.
- **Weak answer:** "N=3, W=2, R=2 gives strong consistency." Without assumptions.
- **Tradeoff:** we pay for consistency with metadata availability instead of read latency.

### Q42. What happens when quorum cannot be reached?

- **Testing:** failure semantics.
- **Strong answer:** Writes: fewer than W healthy targets -> 503 `InsufficientReplicas` before or after streaming; partial copies are orphans for GC; the previous version stays current. Reads with `consistency=quorum`: 503 if fewer than R confirm. Default reads need only one replica.
- **Follow-up:** "Would you ever accept W-1?" Not silently; a per-bucket policy (sloppy writes with hinted handoff) is a conscious durability downgrade.
- **Weak answer:** "Retry."
- **Tradeoff:** availability vs the meaning of an acknowledgement.

### Q43. How do you handle stale replicas?

- **Testing:** recognizing that immutability changes the problem.
- **Strong answer:** A replica cannot be stale because a blob ID is never rewritten: it either has exactly those bytes (verified) or it does not. The "stale" cases become: a missing replica (read fails over, repair recreates) or a superfluous one (node returned after repair: rebalancer trims, GC sweeps). Version staleness exists only in metadata caches (TTL, opt-in).
- **Follow-up:** "What about a returning node serving old data?" Readers only use replicas listed in the current version record; an old copy of an old version is only served for that old version, which is correct.
- **Weak answer:** "Read repair."
- **Tradeoff:** immutability costs space (new blobs per write) and needs GC.

### Q44. How does caching affect consistency?

- **Testing:** cache semantics.
- **Strong answer:** The hot-object cache is keyed by version ID: immutable, never stale, no invalidation. The metadata cache (key -> latest version) is the only stale-able cache: off by default; with TTL T, a gateway invalidates on its own writes (read-your-writes through that gateway), other gateways may serve the previous version for up to T.
- **Follow-up:** "How would you get caching without staleness?" Leases or invalidation pushed from metadata.
- **Weak answer:** "Invalidate on write." On which gateways?
- **Tradeoff:** metadata load vs cross-gateway freshness.

### Q45. What happens during a network partition?

- **Testing:** CAP applied concretely.
- **Strong answer:** Per plane. A gateway cut off from a shard returns 503 for its buckets (CP for metadata). A node cut off from the coordinator but not from gateways is falsely declared DOWN and repaired around: wasted bandwidth, temporary over-replication, then trimming; no inconsistency. Writes need W reachable replicas. Two gateways on different sides cannot both commit unless both reach the single shard, which serializes them.
- **Follow-up:** "What if the coordinator itself is partitioned from all nodes?" All nodes become DOWN in its view; with the repair delay and the single-copy exception there is time to notice. A production coordinator would refuse to mark more than a fraction of nodes down at once.
- **Weak answer:** "We choose availability."
- **Tradeoff:** metadata unavailability vs split-brain.

### Q46. What consistency guarantees does the system provide?

- **Testing:** precision.
- **Strong answer:** With the default config: per-key linearizable PUT/GET/HEAD/DELETE; atomic visibility; read-after-write across gateways; LIST pages consistent at an instant but not a snapshot across pages; multipart completion atomic. Assumptions: one process per shard, honest fsync, metadata cache disabled. Not provided: multi-key transactions, conditional writes, cross-shard ordering; quota soft across shards; rate limits per gateway.
- **Follow-up:** "What changes with Raft-replicated shards?" Same guarantees if reads go through the leader with a lease or a read index.
- **Weak answer:** "Strong consistency."
- **Tradeoff:** availability of metadata.

## I. Failure-mode questions

### Q47. What happens when an entire availability zone fails?

- **Testing:** correlated failure, durability vs availability.
- **Strong answer:** With RF 3 over 3 zones, every segment keeps 2 copies: reads continue; writes continue at W=2 if the other two zones have writable nodes (placement falls back to already-used zones, preferring new racks). Repair cannot restore three-zone spread until the zone returns; rebuilding a full zone (one replica of everything, e.g. 10 PB) would take days and compete with clients, so the right policy is to repair only segments that dropped to one copy and wait. When the zone returns, the rebalancer restores zone spread and trims extras. Metadata shards located in that zone are unavailable here (no consensus); in production, Raft groups span zones.
- **Follow-up:** "Does this implementation make that policy decision?" No: after the repair delay it re-replicates everything affected; operators should raise the delay. Documented as a gap.
- **Weak answer:** "Repair copies the data."
- **Tradeoff:** waiting (two copies, still tolerates one failure) vs a massive rebuild.

### Q48. How would you diagnose rising repair traffic?

- **Testing:** operational reasoning from signals.
- **Strong answer:** Start from what feeds repair: (1) node states in `/admin/cluster` (flapping nodes, false DOWNs: check heartbeat gaps vs network or GC pauses); (2) `storage_node_corruptions_total` by node (a failing disk); (3) `gateway_under_replicated_writes_total` and `gateway_replica_write_failures_total{reason}` (writes acked below RF because of stalls or full disks); (4) `repair_failures_total` with logs (repairs failing and being retried: in this project's own testing, a 401 from missing cluster auth on node-to-node pulls); (5) recent membership changes. Then check whether repair keeps up: `rate(repair_bytes_total)` vs `repair_queue_depth`.
- **Follow-up:** "Repair traffic is hurting clients. What now?" Lower the bandwidth budget while redundancy allows; prioritize single-copy segments; fix the root cause.
- **Weak answer:** "Add more bandwidth."
- **Tradeoff:** repair speed vs client latency.

## J. Scaling questions

### Q49. How would you support billions of objects?

- **Testing:** metadata at scale.
- **Strong answer:** Metadata dominates: 1 B objects x ~600 B = ~600 GB, ~2 TB replicated with overhead. Move shards to an LSM on SSD (memory holds caches, not everything), replicate with Raft, range-shard by (bucket, key) with load-based splitting, group commit. Replace full scans: repair driven by a per-node blob index; incremental GC sweeps. Small objects: pack into volume files to save inodes and fsyncs. LIST at that scale: inventory exports.
- **Follow-up:** "What is the first thing to break in this code?" Shard memory and commit rate.
- **Weak answer:** "Add storage nodes."
- **Tradeoff:** a far more complex metadata service.

### Q50. What becomes the bottleneck at 10 PB?

- **Testing:** capacity reasoning.
- **Strong answer:** 30 PB raw (~1,900 20 TB disks at 80% fill, ~78 nodes). Disk failures every ~2 weeks, each 16 TB to re-replicate (minutes at 10 GB/s, hours at 1 GB/s). Small-write IOPS (each PUT ~7 random I/Os per replica) before bandwidth. Metadata of 2.5-40 B objects depending on size. Gateway egress at 3x ingress.
- **Follow-up:** "IOPS math?" 10k PUT/s x 3 x 7 = 210k IOPS vs ~280k available.
- **Weak answer:** "Disk space."
- **Tradeoff:** see capacity-planning.md.

### Q51. What becomes the bottleneck at 100 PB?

- **Testing:** economics and operations at scale.
- **Strong answer:** Cost of replication (300 PB raw at RF 3): erasure coding for cold data is required. Continuous failures (several disks a day) make repair permanent background load needing QoS. Metadata tens of TB. Network: repair and rebalance must stay zone-local where possible. Operations: rolling upgrades of thousands of nodes, automatic remediation.
- **Follow-up:** "What changes in placement?" Placement groups instead of per-blob placement, so the system tracks thousands of groups instead of billions of blobs.
- **Weak answer:** "More of everything."
- **Tradeoff:** EC's repair cost vs storage savings.

### Q52. How would you handle 1 million GET/s for one object?

- **Testing:** hot keys.
- **Strong answer:** One object lives on 3 nodes; their NICs cap throughput (~3 x 3 GB/s). Small object: gateway caches (keyed by immutable version, single-flight), many gateways behind a load balancer: 1M GET/s across e.g. 50 gateways is 20k/s each, measured ~3k/s on a 2-vCPU VM. Also: cache the metadata lookup (or the response) because each GET still needs the version pointer; 1M/s on one key would overwhelm one metadata shard (the opt-in TTL cache, or leases). Large object: CDN, or temporarily raise RF for hot objects. Ideally put a CDN in front for public content.
- **Follow-up:** "What about the metadata shard?" It is the real bottleneck for a single hot key; that is the case for short-TTL metadata caching.
- **Weak answer:** "Add replicas."
- **Tradeoff:** staleness (metadata TTL) and memory vs backend load.

### Q53. How would you handle 1 million different objects per second?

- **Testing:** spread load vs hot load.
- **Strong answer:** No single hot spot, so scale out every tier: gateways (stateless), storage nodes (placement spreads by blob ID), and metadata (1M lookups/s across many shards; ~0.5 µs per in-memory lookup, so RPC and network dominate; batch or cache). Caches do not help (no reuse); disk IOPS do: 1M random reads/s needs ~7,000 HDDs at 150 IOPS or SSDs.
- **Follow-up:** "Writes instead of reads?" 1M commits/s: ~270 shards at the measured per-shard rate, or group commit.
- **Weak answer:** "Use caching."
- **Tradeoff:** hardware vs latency.

### Q54. What would you redesign at 100x scale?

- **Testing:** knowing which decisions are scale-dependent.
- **Strong answer:** Metadata (Raft-replicated, range-sharded LSM); full-scan repair/GC (indexed and incremental); per-blob placement (placement groups); one-file-per-blob for small objects (volume files); the single coordinator (replicated); gateway fan-out (chain replication or primary-copy for large writes to cut gateway egress). Keep: plane separation, immutable blobs, metadata commit as the visibility point, copy-before-delete, delayed deletion, bounded queues.
- **Follow-up:** "What would you not change?" The ordering rules that make orphans and missing replicas the only possible divergences.
- **Weak answer:** "Rewrite in a faster language."
- **Tradeoff:** complexity budget.

## K. Performance questions

### Q55. How would you diagnose metadata latency?

- **Testing:** systematic latency analysis.
- **Strong answer:** Split by operation: `gateway_metadata_seconds{op}`. Reads slow but commits fine: network, RPC overhead, lock contention from long commits (reads share a lock with commits), or a shard doing a snapshot/compaction (WAL records count dropping). Commits slow: fsync latency of the shard's disk (rate of `metadata_lsn` flat at ~1/fsync), a hot shard (compare `metadata_keys` and LSN rates across shards), or quota checks paying usage RPCs. Profile with pprof (`-debug-listen`). Fixes: group commit, shard split, cache.
- **Follow-up:** "One bucket is hot. Options?" With bucket sharding, none short of moving the bucket; with range sharding, split the range.
- **Weak answer:** "Add an index."
- **Tradeoff:** each fix trades complexity for throughput.

### Q56. How do you handle hot objects?

- **Testing:** layered mitigation.
- **Strong answer:** Spread reads across all replicas (randomized within the healthy tier); cache small objects at gateways keyed by version (measured 99.6% hit rate, backend traffic ~0.4% of client traffic); single-flight to stop stampedes; for large hot objects raise RF or use a CDN; optionally cache the version pointer with a short TTL. Not everything should be solved with caching: large objects would evict everything else.
- **Follow-up:** "Why not cache everything?" Memory cost and eviction of the working set; the page cache on nodes already helps.
- **Weak answer:** "Add Redis."
- **Tradeoff:** gateway memory vs backend load.

### Q57. How do you prevent cache stampedes?

- **Testing:** thundering herd.
- **Strong answer:** Single-flight per key: the first miss loads, concurrent misses wait for that result (measured: 100 concurrent cold reads -> 1 backend load, 99 coalesced). The load runs on a context detached from any single caller so one impatient client cannot fail all waiters. For TTL caches: jittered expiry, serve-stale-while-revalidate.
- **Follow-up:** "Across gateways?" Each gateway still loads once; acceptable (N loads, not N x clients), or add a shared tier.
- **Weak answer:** "Use a lock."
- **Tradeoff:** waiters share the loader's latency and errors.

### Q58. How do multipart uploads work, performance-wise?

- **Testing:** parallelism and retry economics.
- **Strong answer:** Initiate (metadata record, ID embeds bucket for routing); upload parts in parallel, each a replicated blob with its own checksum on independently chosen nodes, so N parallel parts use N x 3 nodes; retries cost one part; complete validates parts and commits one version. Minimum part size (5 MiB default) bounds metadata per object.
- **Follow-up:** "Why a minimum part size?" Otherwise a 1 TB object of 4 KB parts is 250 M segments of metadata.
- **Weak answer:** "Upload in chunks."
- **Tradeoff:** per-part metadata vs retry granularity.

## L. Tradeoff questions

### Q59. Why use replication instead of erasure coding?

- **Testing:** EC literacy and judgment.
- **Strong answer:** Replication: simple, cheap writes in CPU, single-node reads, degraded reads are just another copy, repair copies one fragment-sized unit. EC (e.g. RS(10,4)): 1.4x storage instead of 3x and tolerates 4 failures, but repair reads k fragments (10x the lost data), degraded reads touch k nodes and decode, needs k+m failure domains, and small objects become many tiny fragments. This project is replication-only because correctness and repair simplicity matter most at this size.
- **Follow-up:** "How would you add EC?" A storage class whose segments hold k+m fragment references; gateway encodes per segment; repair reconstructs from k; placement over k+m domains; background conversion from replicated to EC for cold objects.
- **Weak answer:** "EC is more efficient, so use it."
- **Tradeoff:** storage cost vs repair/read cost and complexity.

### Q60. When would erasure coding make more sense?

- **Testing:** knowing the regime.
- **Strong answer:** Large, cold objects at scale with many failure domains: e.g. 70% cold data in a 10 PB system turns 30 PB raw into ~19 PB. Production systems write replicated, then transition to EC by age/size (lifecycle), and pack small objects before coding.
- **Follow-up:** "Why not EC on write?" Write latency (encode), small-object overhead, and the data is often still hot.
- **Weak answer:** "When storage is expensive."
- **Tradeoff:** storage savings vs degraded-read latency.

### Q61. How do you prevent one tenant from consuming all bandwidth?

- **Testing:** multi-dimensional isolation.
- **Strong answer:** Separate token buckets per tenant for requests/s, upload bytes/s and download bytes/s (bandwidth charged per byte as it flows, so large requests are slowed, not rejected), concurrency semaphores for uploads and downloads, a maximum object size and a storage quota. Request rate alone fails: 10 req/s of 1 GB objects is 10 GB/s. Observed: a 4 MiB upload at a 1 MiB/s limit took 3.8 s; bursts got 429.
- **Follow-up:** "Limits are per gateway; with 20 gateways?" A tenant gets up to 20x; fix with a global limiter or by leasing quota slices to gateways.
- **Weak answer:** "Rate-limit requests."
- **Tradeoff:** hard caps vs work-conserving fair sharing.

## M. Staff-level questions

### Q62. How would you implement multi-region replication?

- **Testing:** async replication design.
- **Strong answer:** Per-bucket home region (active-passive). Tail each shard's ordered commit log (Seq), copy blobs (verified), then commit the version remotely with its original ID; idempotent by version ID; publish remotely only when all blobs arrived. Track lag (RPO). Failover with fencing (epochs). Require versioning for replicated buckets so late arrivals are just versions. Active-active only with HLC-based LWW or keep-both-as-versions, and tombstones retained beyond max lag.
- **Follow-up:** "RPO and RTO?" RPO = lag at failure; RTO = detection + decision + promotion, minutes if automated.
- **Weak answer:** "Replicate synchronously."
- **Tradeoff:** RPO > 0 for local write latency.

### Q63. How would you perform rolling upgrades?

- **Testing:** safe change in a running distributed system.
- **Strong answer:** One failure domain at a time (a zone, or a few nodes), each node drained of traffic gracefully (SIGTERM: stop accepting, grace period for in-flight transfers; the commit protocol makes cut-offs invisible), restarted, health-checked, then wait for `repair_under_replicated_segments` to return to baseline before the next batch. Raise the repair delay above restart time so restarts do not trigger repair. Compatible wire and storage formats across adjacent versions (expand/contract). Canary first; automatic rollback on SLO regression. In this project the storage nodes were restarted one at a time onto a fixed binary while repair was in progress, and repair resumed and completed; read availability during that restart was not measured.
- **Follow-up:** "Metadata shards?" With Raft, restart followers first, then transfer leadership; here, a shard restart is a brief outage for its buckets.
- **Weak answer:** "Restart everything at night."
- **Tradeoff:** upgrade duration vs risk.

### Q64. What would you replace with a mature infrastructure component?

- **Testing:** build vs buy judgment.
- **Strong answer:** Metadata store -> FoundationDB/TiKV/Spanner-class database (consensus, range sharding, transactions are hard to get right). Coordinator -> etcd. Cluster token -> mTLS with SPIFFE identities. Span logging -> OpenTelemetry. Metrics -> client_golang. Tenant file -> IdP + policy engine. I would keep the data plane, placement, replication writer and repair logic, which are the domain-specific parts.
- **Follow-up:** "Why not use Ceph?" Then you are operating Ceph, not building an object store; fine as a product decision, but the point here was to understand what it does.
- **Weak answer:** "Use S3."
- **Tradeoff:** control and learning vs maturity.

### Q65. What is the hardest correctness problem?

- **Testing:** depth: where do bugs lose data?
- **Strong answer:** Deletion: making physical deletion safe against every concurrent actor (in-flight writes, repair copies landing before metadata lists them, rebalancer moves, returning nodes, retried deletes, readers of old versions). The design answers it with ordering rules (write before reference; unpublish before delete), pending registrations, grace periods, re-checks, two-sighting sweeps and "no answer means no delete". The second hardest: the ambiguity window between data durability and metadata commit (solved by making the commit the only visibility point and orphans harmless).
- **Follow-up:** "Prove GC never deletes a live blob." Enumerate how a blob becomes live (commit, repair add, rebalance add) and show each is either preceded by a pending registration or covered by grace + re-check.
- **Weak answer:** "Consistency."
- **Tradeoff:** reclamation delay for safety.

---

# Staff-Level Design Discussion

Short, opinionated answers to open-ended prompts. Each ends with what this
repository does today.

**How would you store 100 PB?** Tiered: replicated hot tier (RF 3) for recent and small objects; RS(10,4) cold tier for large objects older than N days, converted in the background. ~4,000 HDDs for the cold tier vs ~19,000 at RF 3. Placement groups, zone-local repair, separate QoS for client and background traffic. _Today:_ replication only; EC is designed in tradeoffs.md.

**How would you handle 1 billion objects?** ~0.6 TB metadata: LSM-backed, Raft-replicated shards; range sharding for large buckets; group commit; incremental repair/GC. _Today:_ in-memory shards, practical to tens of millions.

**How would you handle 100 billion objects?** ~60 TB metadata (more with indexes and replicas): a dedicated metadata cluster with automatic range splitting and balancing; small-object packing (volumes); inventory exports instead of LIST for bulk use; per-node indexes for repair. _Today:_ not supported.

**What is the metadata bottleneck?** Serialized commits per shard (~1/fsync), then memory, then hot buckets that cannot be split. _Today:_ measured 3,700 commits/s per shard.

**What is the repair bottleneck?** Finding work (O(segments) scans) and bandwidth (one node = hundreds of TB). Fix: per-node blob index, declustered placement so thousands of disks rebuild in parallel, priorities. _Today:_ full scan, fewest-copies-first, bandwidth cap.

**How would you minimize cross-AZ traffic?** Zone-local reads (gateway knows its zone), zone-local gateways, chain replication so only one copy crosses each zone boundary, repair sources chosen in the target's zone, rebalance prefer same-zone moves. Writes still cross zones (that is the point of zone diversity). _Today:_ zone-preferring reads; fan-out writes.

**How would you minimize cross-region traffic?** Replicate only opted-in buckets; replicate committed versions once per region then fan out locally; compress where useful; dedupe re-sends by version ID; avoid replicating short-lived objects (delay + skip if deleted). _Today:_ single region.

**How would you perform placement changes without downtime?** Record actual replicas (done), so placement is only an intention; deploy the new function everywhere at once (or versioned per segment); converge with a capped rebalancer; monitor misplaced segments. _Today:_ supported by design; `-placement` switch.

**How would you migrate from replication to erasure coding?** Per segment: encode from a healthy replica into k+m fragments on k+m domains, verify, commit a metadata swap (replica list -> fragment list) atomically, enqueue old replicas for GC. Throttled, resumable, idempotent; readers handle both forms. _Today:_ not implemented.

**How would you change the replication factor?** Change policy, update stored RF, let repair add or a trimmer remove copies (unpublish before delete). _Today:_ per-bucket RF at creation; repair honours per-segment RF.

**How would you recover from a metadata outage?** Short: restart (snapshot + WAL replay); production: Raft failover in seconds. Lost disk without replication: restore from backup; then the GC must be paused, or it would sweep blobs that the restored (older) metadata does not reference but newer writes did. _Today:_ restart recovery; no replication.

**How would you recover from an entire AZ failure?** Run on two copies; repair only single-copy segments; restore zone spread when it returns; metadata survives if Raft groups span zones. _Today:_ data survives with RF 3 over 3 zones; metadata in the failed zone is unavailable.

**How would you recover from an entire region failure?** Failover to the replica region for replicated buckets (RPO = lag), fenced promotion, then reverse replication when the region returns. _Today:_ not supported.

**How would you detect silent corruption?** Scrubbing with coverage SLOs, block CRCs on every read, end-to-end SHA-256 on write and repair, replica comparison. _Today:_ all four.

**How would you prevent correlated replica failures?** Zone/rack spreading, mixing hardware batches across zones, staggered upgrades, repair throttling to avoid cascades, and not trusting one zone's capacity for more than its share. _Today:_ zone/rack-aware placement.

**How would you perform rolling upgrades?** One failure domain at a time with drain-free graceful restarts, repair delay longer than restart time, health gates on under-replication, version-compatible formats. _Today:_ graceful shutdown with grace periods (SIGTERM); node-by-node restarts were exercised, without a formal health gate.

**How would you migrate metadata?** Expand/contract schema changes; for moving buckets between shards, a routing directory with per-bucket migration (copy, dual-write or brief write block, flip, drain). _Today:_ fixed hash(bucket) mod S.

**How would you change object schemas?** Versioned records, tolerant readers first, writers second, background rewrite, remove old readers. _Today:_ JSON records, additive changes are compatible.

**How would you handle a hot bucket?** Range-split its metadata; ensure keys are spread (avoid sequential prefixes); per-bucket rate limits; cache. _Today:_ a bucket is bound to one shard: the limit is that shard.

**How would you handle a hot prefix?** Range splitting by load (moving the split point for sequential prefixes), or internal hash-prefixing of keys at the cost of ordered LIST. _Today:_ same as hot bucket.

**How would you handle a single hot object?** Gateway cache keyed by version with single-flight, short-TTL metadata caching, CDN, extra replicas for large hot objects. _Today:_ first two (metadata cache opt-in).

**How would you isolate tenants?** Per-tenant limits in every dimension (requests, bandwidth, concurrency, object size, storage), global enforcement for multi-gateway fleets, separate background-work budgets, and for the largest tenants dedicated shards or cells. _Today:_ per-gateway limits in all dimensions; quotas enforced at commit.

**How would you design storage classes?** Each class = (durability scheme, placement policy, media, minimum object size, retrieval latency): STANDARD RF 3 on HDD, REDUCED RF 2, COLD RS(10,4) with packing, ARCHIVE offline media. Lifecycle rules move objects between classes in the background. _Today:_ STANDARD and REDUCED_REDUNDANCY (RF only).

**What would you sacrifice during extreme resource pressure?** In order: rebalancing (pause), scrubbing (pause), GC (pause unless out of space), cache fill for large objects, read verification; then shed best-effort tenants with 429/503; then accept writes at W rather than RF (already default); last, reads of non-critical buckets. Never: acknowledged durability below W, never serving unverified bytes, never deleting on uncertain information. _Today:_ load shedding with explicit 429/503, bandwidth budgets per background loop.
