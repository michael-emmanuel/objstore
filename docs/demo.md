# Demonstrations

Fifteen demonstrations. Each lists **setup**, **expected behavior**,
**observed behavior**, the **system principle** it shows, and the
**tradeoff** involved.

"Observed" values were recorded on 2026-10-14 against the local cluster
(`scripts/local-cluster.sh`, all processes on one 2-vCPU VM) or, where noted,
from the in-process integration tests. Re-running will give different
timings; the behaviour should match.

## Common setup

```bash
scripts/local-cluster.sh up        # 2 metadata shards, 4 nodes (az-a, az-b, az-c, az-a), gateway, repairer, rebalancer
source .local/env                  # generated secrets; sets OBJSTORE_TOKEN (tenant alice) and GW
A="Authorization: Bearer $OBJSTORE_TOKEN"
ADM="Authorization: Bearer $OBJSTORE_TOKEN_ADMIN"
CT="X-Cluster-Token: $CLUSTER_TOKEN"
curl -s -X PUT -H "$A" $GW/v1/buckets/photos -d '{"versioning":true}'
```

Docker Compose works the same way (`scripts/dev-config.sh`, then
`docker compose -f deploy/docker-compose.yml up -d --build`; secrets in
`deploy/.env`; node N is on host port 810N).

---

## Demo 1: Basic PUT/GET

**Setup**
```bash
echo hello-world | curl -s -X PUT -H "$A" -H 'Content-Type: text/plain' --data-binary @- \
  $GW/v1/buckets/photos/objects/user-123/profile.jpg -D -
curl -s -H "$A" $GW/v1/buckets/photos/objects/user-123/profile.jpg -D -
curl -s -H "$ADM" $GW/admin/objects/photos/user-123/profile.jpg     # inspect metadata
```
**Expected:** 200 with ETag (= SHA-256), version ID and `X-Replicas-Acked`; the GET returns the bytes; the inspect output shows one segment on three nodes in three zones.
**Observed:** `X-Replicas-Acked: 3/3`; GET `X-Cache: MISS` then body `hello-world`; segment `5c74a331...` on node-2 (az-b), node-1 (az-a), node-3 (az-c), `zones_spanned: 3`, desired placement `[node-1, node-2, node-3]`.
**Principle:** metadata (version, checksum, replica list) is separate from data (an opaque blob replicated across failure domains).
**Tradeoff:** two hops per request (metadata then data) in exchange for independently scalable planes.

## Demo 2: Large streaming upload

**Setup**
```bash
head -c 1073741824 /dev/urandom > /tmp/big.bin
SUM=$(sha256sum /tmp/big.bin | cut -d' ' -f1)
curl -s -X PUT -H "$A" -H "X-Checksum-SHA256: $SUM" -T /tmp/big.bin $GW/v1/buckets/photos/objects/big/1g.bin
curl -s -H "$A" $GW/v1/buckets/photos/objects/big/1g.bin | sha256sum
curl -s -H "$A" -r 1000000-1000099 $GW/v1/buckets/photos/objects/big/1g.bin -D - -o /dev/null
grep VmRSS /proc/$(cat .local/pids/gateway)/status   # sample during the upload
```
**Expected:** the object is split into segments that stream to replicas without being buffered; gateway memory stays flat; the downloaded hash matches; the range returns 206.
**Observed:** 1 GiB in 64 segments, 3/3 acks, upload 10.7 s, download 2.3 s, hash matched, **gateway peak RSS 18.9 MB**; `206`, `Content-Range: bytes 1000000-1000099/1073741824`, bytes identical.
**Principle:** end-to-end streaming with bounded queues; TCP flow control carries backpressure to the client.
**Tradeoff:** a replica that fails mid-stream cannot be retried from the gateway (it holds no copy); it is repaired afterwards instead.

## Demo 3: Multipart upload

**Setup**
```bash
for i in 1 2 3; do head -c 3145728 /dev/urandom > /tmp/p$i; done
ID=$(curl -s -X POST -H "$A" $GW/v1/buckets/photos/multipart -d '{"key":"video/raw.mp4"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["upload_id"])')
for i in 1 2 3; do curl -s -X POST -H "$A" -H "X-Checksum-SHA256: $(sha256sum /tmp/p$i|cut -d' ' -f1)" \
  --data-binary @/tmp/p$i "$GW/v1/multipart/$ID/parts?part_number=$i"; done
# simulate a corrupted part 2 in transit, then retry only part 2
curl -s -X POST -H "$A" -H "X-Checksum-SHA256: $(sha256sum /tmp/p1|cut -d' ' -f1)" --data-binary @/tmp/p2 "$GW/v1/multipart/$ID/parts?part_number=2"
curl -s -X POST -H "$A" -H "X-Checksum-SHA256: $(sha256sum /tmp/p2|cut -d' ' -f1)" --data-binary @/tmp/p2 "$GW/v1/multipart/$ID/parts?part_number=2"
PARTS=$(for i in 1 2 3; do echo "{\"part_number\":$i,\"etag\":\"$(sha256sum /tmp/p$i|cut -d' ' -f1)\"}"; done | paste -sd,)
curl -s -X POST -H "$A" $GW/v1/multipart/$ID/complete -d "{\"parts\":[$PARTS]}"
curl -s -H "$A" $GW/v1/buckets/photos/objects/video/raw.mp4 | sha256sum; cat /tmp/p1 /tmp/p2 /tmp/p3 | sha256sum
```
**Expected:** the bad part is rejected; only that part is retried; completion makes the object visible atomically with a composite ETag.
**Observed:** upload ID `photos~0eea...`; bad part `400 BadDigest`; retry 200; completion ETag `70e4e186...-3`, size 9,437,184; downloaded and concatenated hashes both `4146900f...`.
**Principle:** parts are independent, replicated, checksummed blobs; completion is one metadata commit.
**Tradeoff:** upload state in metadata and abandoned-upload cleanup; composite ETag instead of a whole-object SHA-256.

## Demo 4: Replica failure (during a PUT)

**Setup**
```bash
head -c 200000000 /dev/urandom > /tmp/big200
curl -s -X PUT -H "$A" -T /tmp/big200 $GW/v1/buckets/lab/objects/during-kill -D - &
sleep 1; scripts/local-cluster.sh kill-node 3; wait
curl -s -H "$ADM" $GW/admin/objects/lab/during-kill | python3 -c 'import sys,json;print([len(s["replicas"]) for s in json.load(sys.stdin)["segments"]])'
```
**Expected:** with W=2 the PUT still succeeds; segments written before the kill have 3 replicas, later ones 2; metadata lists only replicas that acknowledged; repair adds the missing copies.
**Observed:** `200 OK`, `X-Replicas-Acked: 2/3`, `under_replicated: true`, 12 segments with replica counts `[3,3,3,3,3,3,2,2,2,2,2,2]`; downloaded content identical.
**Principle:** W is a durability threshold; partial replication is recorded honestly and repaired asynchronously.
**Tradeoff:** W=2 keeps writes available through a node failure at the cost of a window with 2 copies; W=3 would have failed the PUT.

## Demo 5: Storage-node failure

**Setup**
```bash
for i in $(seq 1 30); do echo "obj-$i" | curl -s -o /dev/null -X PUT -H "$A" --data-binary @- $GW/v1/buckets/photos/objects/small/$i; done
scripts/local-cluster.sh kill-node 2
for i in $(seq 1 30); do curl -s -H "$A" $GW/v1/buckets/photos/objects/small/$i; done
watch -n2 "curl -s -H '$ADM' $GW/admin/cluster | grep -E 'id|state'"
```
**Expected:** every read succeeds immediately; the node becomes SUSPECT, then DOWN; writes continue with 2/3 for segments that targeted it, then avoid it.
**Observed:** 30/30 reads OK at t+0 s; PUT at t+0 s acked 2/3; SUSPECT at t+4 s, DOWN at t+8 s (configured 3 s / 6 s with 500 ms heartbeats and a 2 s observation interval).
**Principle:** two-threshold timeout failure detection; reads fail over per request without waiting for detection.
**Tradeoff:** shorter timeouts detect faster but declare busy nodes dead (false positives trigger repair).

## Demo 6: Checksum corruption

**Setup**
```bash
head -c 2097152 /dev/urandom > /tmp/c.bin
curl -s -X PUT -H "$A" --data-binary @/tmp/c.bin $GW/v1/buckets/photos/objects/corrupt-me
BLOB=$(curl -s -H "$ADM" $GW/admin/objects/photos/corrupt-me | python3 -c 'import sys,json;print(json.load(sys.stdin)["segments"][0]["blob_id"])')
curl -s -X POST -H "$CT" "localhost:8101/admin/corrupt/$BLOB?offset=100000"   # a node that holds it
for i in 1 2 3 4 5 6; do curl -s -H "$A" $GW/v1/buckets/photos/objects/corrupt-me | cmp - /tmp/c.bin && echo ok; done
grep "$BLOB" .local/logs/*.log
```
**Expected:** the corrupt block is caught by its CRC on the node before its bytes are sent; the node quarantines the blob; the gateway resumes on another replica; every read returns correct data; repair replaces the bad copy.
**Observed:** 6/6 reads correct. Logs: corrupted at .802 s; at .833 s the node logged `blob quarantined ... crc mismatch in block 1` and the gateway `replica read failed, failing over`; at .961 s the repairer `corrupt replica removed from metadata; will re-replicate`; a fresh verified copy was in place within the same pass (~160 ms end to end).
**Principle:** block-level checksums verified before serving; immutable blobs allow failover at an exact byte offset.
**Tradeoff:** CRC per block costs a sidecar and CPU (21 GB/s, cheap); SHA-256 for identity costs more (1.5 GB/s) and is kept for end-to-end integrity.

## Demo 7: Replica repair

**Setup:** continue from Demo 5 (node-2 DOWN). Watch the repairer:
```bash
watch -n2 "curl -s localhost:9100/metrics | grep -E '^repair_'"
```
**Expected:** after the repair delay (10 s locally), under-replicated segments are queued fewest-copies-first and copied node-to-node within the bandwidth budget until the count reaches 0.
**Observed:** `repair_under_replicated_segments` 1 at t+4 s (a W=2 write during the failure, repaired without delay since no node needed to be DOWN), 99 at t+10 s once node-2 was DOWN; queue drained at ~3 segments/s (16 MiB segments at the 50 MB/s budget): **99 copies, 1.08 GB, ~21 s, ending at 0**.
Also observed: the very first attempt failed 100% with `401 cluster authentication required` because node-to-node pulls did not carry the cluster token; `repair_failures_total` and the log line made the cause obvious. Fixed (peer HTTP client with the token) and covered by `TestPullBetweenAuthenticatedNodes`.
**Principle:** repair is reconciliation toward RF, prioritized by risk, rate-limited, verified, and orchestrated without carrying bytes through the repairer.
**Tradeoff:** the repair delay lengthens the reduced-redundancy window to avoid repair storms from reboots.

## Demo 8: Node addition and rebalancing

**Setup**
```bash
scripts/local-cluster.sh start-node 2     # old node returns with stale copies
scripts/local-cluster.sh start-node 5     # brand-new node (az-b)
watch -n4 "curl -s localhost:9200/metrics | grep -E '^rebalance_'"
for n in 1 2 3 5; do curl -s -H "$CT" localhost:810$n/stats; done
```
**Expected:** the new node joins ACTIVE; the rebalancer moves the share placement assigns to it with copy-before-delete; the returning node's stale copies are trimmed and swept.
**Observed:** blob counts went from node-1=100 node-2=100 node-3=100 node-4=99 node-5=0 to, 42 s later, 74/26/100/78/22; total **99 moves, 1.08 GB** at the 20 MB/s budget. node-2's 100 stale copies were GC'd (100 -> 12 at t+21 s, after the 30 s demo grace) and then node-2 started receiving moved replicas again because it is a placement owner.
**Principle:** consistent hashing moves only the new node's share; copy-before-delete keeps >= RF copies at every instant.
**Tradeoff:** the returning node's still-valid copies were deleted and later re-copied: wasted bandwidth. Production systems re-adopt copies on rejoining nodes after verifying checksums.

## Demo 9: Hot object

**Setup**
```bash
go run ./cmd/loadgen -workload read-only -size 64KiB -keys 200 -hot-ratio 0.9 -concurrency 32 -duration 15s \
  -scrape http://localhost:8080/metrics,http://localhost:8101/metrics,http://localhost:8102/metrics,http://localhost:8103/metrics,http://localhost:8105/metrics \
  -scrape-match 'storage_node_bytes_out_total|gateway_object_cache'
```
**Expected:** after the first read, the hot object is served from the gateway cache; storage nodes see almost no read traffic.
**Observed:** 47,707 GETs at 3,180/s (p50 9.5 ms, p99 25 ms); cache **HIT 99.6%**, MISS 200 (one per key), COALESCED 12; storage nodes sent 13 MB in total while clients received ~3 GB.
**Principle:** immutable versions make caching trivially correct (keyed by version ID, no invalidation).
**Tradeoff:** only small objects (<= 1 MiB by default) are cached; each gateway caches independently.

## Demo 10: Cache behavior (stampede, consistency)

**Setup:** `go test ./tests -run TestHotObjectCacheAndStampede -v` (100 concurrent cold reads of one key), then overwrite a cached key and read it back:
```bash
echo v1 | curl -s -X PUT -H "$A" --data-binary @- $GW/v1/buckets/photos/objects/cached
curl -s -H "$A" $GW/v1/buckets/photos/objects/cached -D - | grep -i x-cache   # MISS
curl -s -H "$A" $GW/v1/buckets/photos/objects/cached -D - | grep -i x-cache   # HIT
echo v2 | curl -s -X PUT -H "$A" --data-binary @- $GW/v1/buckets/photos/objects/cached
curl -s -H "$A" $GW/v1/buckets/photos/objects/cached                            # v2
```
**Expected:** one backend load for 100 concurrent misses; an overwrite is visible immediately because the cache key is the version ID and the version pointer is read from metadata.
**Observed:** integration test `cache sources for 100 concurrent cold reads: map[COALESCED:99 MISS:1]`. Local cluster: `X-Cache: MISS`, then `HIT`; after the overwrite the next GET returned `v2` with `X-Cache: MISS` (a new version ID is a new cache key; the metadata cache is off by default).
**Principle:** cache immutable things; look up mutable pointers authoritatively (or with an explicit TTL).
**Tradeoff:** every GET still costs a metadata lookup unless the opt-in metadata cache (bounded staleness) is enabled.

## Demo 11: Backpressure

**Setup**
```bash
curl -s -X POST -H "$CT" localhost:8101/admin/faults -d '{"chunk_delay_ms":300}'   # slow disk on node-1
curl -s -X PUT -H "$A" -T /tmp/big200 $GW/v1/buckets/lab/objects/slow -D - | grep -i acked
curl -s localhost:8080/metrics | grep -E 'replica_write_failures|penalized'
curl -s -X POST -H "$CT" localhost:8101/admin/faults -d '{}'
```
**Expected:** node-1's bounded queue fills; after the stall timeout it is dropped; the write continues at the healthy replicas' speed; gateway memory does not grow; later segments avoid node-1.
**Observed:** before the slow-node penalty existed: **41.6 s** for 200 MB (each of 8 segments placed on node-1 paid the 5 s stall timeout), gateway RSS 18 MB. After adding the penalty: **6.9 s**, `stalled` = 1, `gateway_penalized_nodes 1`, RSS 15 MB, acked 2/3.
**Principle:** bounded queues turn a slow consumer into either backpressure or an explicit drop, never memory growth; a circuit breaker stops paying for the same slow node repeatedly.
**Tradeoff:** the stall timeout trades durability-at-ack (one fewer copy, repaired later) for latency; too short a timeout drops healthy-but-busy nodes.

## Demo 12: Tenant quota and isolation

**Setup** (tenant bob: 10 MiB max object, 20 req/s, 1 MiB/s upload, 100 MiB storage)
```bash
B="Authorization: Bearer $OBJSTORE_TOKEN_BOB"
curl -s -X PUT -H "$B" $GW/v1/buckets/bob-data
curl -s -H "$A" $GW/v1/buckets/bob-data/objects/x                               # alice -> 403
head -c 11534336 /dev/zero | curl -s -X PUT -H "$B" --data-binary @- $GW/v1/buckets/bob-data/objects/too-big
for i in $(seq 1 40); do curl -s -o /dev/null -w '%{http_code} ' -H "$B" $GW/v1/buckets/bob-data/objects/nope; done
time (head -c 4194304 /dev/urandom | curl -s -o /dev/null -X PUT -H "$B" --data-binary @- $GW/v1/buckets/bob-data/objects/4m)
for i in $(seq 1 12); do head -c 9437184 /dev/zero | curl -s -o /dev/null -w '%{http_code} ' -X PUT -H "$B" --data-binary @- $GW/v1/buckets/bob-data/objects/fill-$i; done
```
**Expected:** cross-tenant access denied; oversize object rejected; request bursts rate-limited; uploads throttled to bandwidth; storage quota enforced.
**Observed:** alice -> `403 AccessDenied`; 11 MiB -> `413 EntityTooLarge`; 40 rapid requests -> 27 x 404, 13 x `429`; 4 MiB upload took **3.8 s** at the 1 MiB/s limit; ten 9 MiB writes succeeded and the 11th returned `403 QuotaExceeded`.
**Principle:** request rate and bandwidth are separate dimensions; quotas are enforced atomically at the metadata commit.
**Tradeoff:** limits are per gateway; cross-shard quota is soft (bounded overshoot).

## Demo 13: Disk-full simulation

**Setup**
```bash
USED=$(curl -s -H "$CT" localhost:8101/stats | python3 -c 'import sys,json;print(json.load(sys.stdin)["used_bytes"])')
curl -s -X POST -H "$CT" localhost:8101/admin/faults -d "{\"capacity_bytes\": $((USED+1000))}"
head -c 2097152 /dev/urandom | curl -s -X PUT -H "$A" --data-binary @- $GW/v1/buckets/photos/objects/while-disk-full -D - | grep -i acked
curl -s -X POST -H "$CT" localhost:8101/admin/faults -d '{}'
```
**Expected:** node-1 rejects the write with insufficient storage; the PUT succeeds with the remaining replicas; no partial file remains on node-1.
**Observed:** `200 OK`, `X-Replicas-Acked: 2/3`.
**Principle:** a full disk is a per-replica failure, absorbed by W < RF; placement avoids nodes over 95%.
**Tradeoff:** a cluster-wide full condition would fail writes (503) rather than accept them under-replicated below W.

## Demo 14: Partial upload failure

**Setup:** a client declares 50 MiB, sends 30 MiB, and disconnects.
```bash
python3 - <<'EOF'
import socket,os
s=socket.create_connection(("localhost",8080))
s.sendall(("PUT /v1/buckets/photos/objects/partial HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\nContent-Length: %d\r\n\r\n" % (os.environ["OBJSTORE_TOKEN"], 50<<20)).encode())
for _ in range(30): s.sendall(os.urandom(1<<20))
s.close()
EOF
curl -s -o /dev/null -w '%{http_code}\n' -H "$A" $GW/v1/buckets/photos/objects/partial
ls .local/data/node-*/tmp | wc -l
```
**Expected:** nothing becomes visible; the in-flight replica requests are aborted (never cleanly ended, so no node commits a truncated blob); temporary files are removed; completed segments are unreferenced and reclaimed by GC after the pending TTL.
**Observed:** `404`; zero temporary files on all nodes before and after.
**Principle:** the metadata commit is the only visibility point; aborting a stream must look like an error to the receiver, never like EOF.
**Tradeoff:** completed segments of a failed upload consume space until GC (pending TTL, default 6 h).

## Demo 15: Load testing

**Setup**
```bash
S=http://localhost:8101/metrics,http://localhost:8102/metrics,http://localhost:8103/metrics,http://localhost:8105/metrics
go run ./cmd/loadgen -workload write-only  -size 4KiB  -keys 5000 -concurrency 32 -duration 12s -scrape $S -scrape-match 'storage_node_bytes_in_total'
go run ./cmd/loadgen -workload write-heavy -size 1MiB  -keys 500  -concurrency 16 -duration 12s -scrape $S -scrape-match 'storage_node_bytes_(in|out)_total'
go run ./cmd/loadgen -workload mixed       -size small -keys 2000 -concurrency 16 -duration 12s
go run ./cmd/loadgen -workload read-heavy  -size 4MiB  -keys 100  -concurrency 16 -duration 12s
```
**Expected:** throughput and p50/p95/p99 per operation; server counter deltas show write amplification and per-node balance.
**Observed** (single 2-vCPU VM, see [performance.md](performance.md)):

| Workload | PUT ops/s (p50/p99 ms) | GET ops/s (p50/p99 ms) |
|---|---|---|
| write-only 4 KiB | 285 (110/192) | - |
| write-heavy 1 MiB | 75 (200/347) | 20 (39/134) |
| mixed 1-64 KiB | 207 (65/122) | 209 (10/36) |
| read-heavy 4 MiB | 3.7 (196/415) | 101 (145/334), 406 MiB/s |

Nodes received exactly 3.0x the logical write bytes; with zones of 1, 2 and 1 nodes the single-node zones took twice the bytes of the others.
**Principle:** measure; let counters, not intuition, say where the bytes go.
**Tradeoff:** every process shares one disk and two CPUs here, so absolute numbers are not representative; relative effects (fsync cost, cache effect, write amplification, zone imbalance) are.
