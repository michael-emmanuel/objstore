# Fault Injection

Faults are injected into **real processes** through HTTP endpoints that exist
only when a process is started with `-enable-fault-injection` (the local
cluster and Docker Compose enable it; production must not). Process-level
faults use signals. Every experiment below lists the commands, the expected
behaviour, and the automated test that asserts the same behaviour in-process.

## Setup

```bash
scripts/local-cluster.sh up
source .local/env
A="Authorization: Bearer $OBJSTORE_TOKEN"            # tenant alice
ADM="Authorization: Bearer $OBJSTORE_TOKEN_ADMIN"     # admin
CT="X-Cluster-Token: $CLUSTER_TOKEN"                  # internal endpoints
node() { echo "http://localhost:$((8100 + $1))"; }    # node N -> URL
curl -s -X PUT -H "$A" $GW/v1/buckets/lab -d '{"versioning":true}'
```

## Fault controls

**Storage node** `POST /admin/faults` (JSON; omitted fields reset to zero):

| Field | Effect |
|---|---|
| `fail_writes` | every PUT/pull returns 503 |
| `fail_reads` | every GET/HEAD returns 503 |
| `write_delay_ms` / `read_delay_ms` | delay before accepting a write / serving a read |
| `chunk_delay_ms` | delay per 64 KiB read from a PUT body (a slow disk or replica) |
| `partitioned` | close data-plane connections without a response **and** stop heartbeating |
| `capacity_bytes` | change the node's capacity (simulate a full disk) |

`POST /admin/corrupt/{blob_id}?offset=N` flips one byte of a stored blob
without updating its checksums.

**Metadata shard** `POST /admin/faults`: `fail_reads`, `fail_writes`, `delay_ms`.

**Processes:** `scripts/local-cluster.sh kill-node N` (SIGKILL),
`start-node N` (restart with the same disk, or add a new node for N > 4).

Find where an object's blobs live:

```bash
curl -s -H "$ADM" $GW/admin/objects/lab/my-key | python3 -m json.tool
```

---

## 1. Kill a storage node

```bash
scripts/local-cluster.sh kill-node 2
```

**Expected:** reads succeed immediately (failover). The node is SUSPECT after
3 s and DOWN after 6 s (`/admin/cluster`). After the 10 s repair delay,
`repair_under_replicated_segments` rises, then falls to 0 as copies are made.
**Test:** `TestNodeFailureReadAvailabilityAndRepair`.

## 2. Kill a replica during PUT

```bash
head -c 200000000 /dev/urandom > /tmp/big
curl -s -X PUT -H "$A" -T /tmp/big $GW/v1/buckets/lab/objects/during-kill -D - &
sleep 1; scripts/local-cluster.sh kill-node 3
wait
```

**Expected:** the stream to node 3 fails; the write continues with the other
two. Response 200 with `X-Replicas-Acked: 2/3` (W=2). Segments that had
already completed before the kill keep 3 replicas. Repair restores RF.
With `-write-quorum=3` the same experiment returns 503 and nothing is visible.
**Tests:** `TestReplicaFailureDuringPutThenRepair`, `TestDurabilityPolicyW3`.

## 3. Delay a replica

```bash
curl -s -X POST -H "$CT" $(node 1)/admin/faults -d '{"chunk_delay_ms":200}'
curl -s -X PUT -H "$A" -T /tmp/big $GW/v1/buckets/lab/objects/slow -D - | grep -i acked
curl -s -X POST -H "$CT" $(node 1)/admin/faults -d '{}'
```

**Expected:** node 1's queue fills; after `-replica-stall-timeout` (5 s) it is
dropped, and the upload continues at the speed of the healthy replicas.
`gateway_replica_write_failures_total{reason="stalled"}` increments once and
`gateway_penalized_nodes` becomes 1: later segments avoid node 1 for
`-slow-node-penalty`. Repair adds the missing copies. Memory stays bounded
throughout. Observed: 200 MB in 6.9 s with one stall (41.6 s before the
penalty existed).
**Tests:** `TestBackpressureSlowReplica`, `TestSlowNodePenalizedAcrossSegments`.

## 4. Corrupt a stored object

```bash
echo hello | curl -s -X PUT -H "$A" --data-binary @- $GW/v1/buckets/lab/objects/c
BLOB=$(curl -s -H "$ADM" $GW/admin/objects/lab/c | python3 -c 'import sys,json;print(json.load(sys.stdin)["segments"][0]["blob_id"])')
curl -s -X POST -H "$CT" "$(node 1)/admin/corrupt/$BLOB?offset=2"   # use a node that holds it
for i in 1 2 3 4 5; do curl -s -H "$A" $GW/v1/buckets/lab/objects/c; done
```

**Expected:** whichever read reaches the corrupt copy fails its block CRC on
the node; the node quarantines the blob and aborts; the gateway fails over at
the same offset; every client read returns correct bytes. Within one repair
pass the bad replica is replaced. Cold data nobody reads is found by the
scrubber (`-scrub-interval`) or `POST /blobs/{id}/verify`.
**Tests:** `TestCorruptionDetectionFailoverAndRepair`, `TestCorruptionDetectedBeforeBytesServed`.

## 5. Fill a disk

```bash
USED=$(curl -s -H "$CT" $(node 1)/stats | python3 -c 'import sys,json;print(json.load(sys.stdin)["used_bytes"])')
curl -s -X POST -H "$CT" $(node 1)/admin/faults -d "{\"capacity_bytes\": $((USED+1000))}"
head -c 2000000 /dev/urandom | curl -s -X PUT -H "$A" --data-binary @- $GW/v1/buckets/lab/objects/full -D - | grep -i acked
```

**Expected:** node 1 rejects the write (507 internally); the gateway reports
`disk_full`; the PUT succeeds with 2/3. No temporary file is left on node 1.
Placement stops choosing a node once it reports >95% used.
**Test:** `TestDiskFull`.

## 6. Disconnect a node (partition)

```bash
curl -s -X POST -H "$CT" $(node 2)/admin/faults -d '{"partitioned":true}'
sleep 15
curl -s -X POST -H "$CT" $(node 2)/admin/faults -d '{}'
```

**Expected:** connections to node 2 are dropped mid-request and it stops
heartbeating: from the cluster's point of view it is indistinguishable from a
crash. After healing, it heartbeats again and becomes ACTIVE. If repair
already ran, its old copies are surplus: the rebalancer trims extra replicas
and the GC sweep removes copies metadata no longer lists on it.
**Test:** `TestNodeReturnsAfterRepair`, `TestFaults`.

## 7. Slow a client

```bash
curl -s -H "$A" --limit-rate 10k $GW/v1/buckets/lab/objects/slow -o /dev/null &
python3 - <<'EOF'   # an uploader that stops sending
import socket,os,time
s=socket.create_connection(("localhost",8080))
s.sendall(b"PUT /v1/buckets/lab/objects/stuck HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer "+os.environ["OBJSTORE_TOKEN"].encode()+b"\r\nContent-Length: 1000000\r\n\r\n")
s.sendall(os.urandom(1000)); time.sleep(90)
EOF
```

**Expected:** the slow downloader is served at its own pace; TCP flow control
stops the gateway reading from the replica, so memory does not grow. The
stuck uploader is aborted after `-client-idle-timeout` (60 s), releasing its
upload slot; nothing is committed.
**Tests:** `TestClientDisconnectMidUpload`; `stallReader`/`deadlineWriter` in `internal/api`.

## 8. Fail metadata lookup

```bash
for p in 7000 7001; do curl -s -X POST -H "$CT" localhost:$p/admin/faults -d '{"fail_reads":true,"fail_writes":true}'; done
curl -s -H "$A" $GW/v1/buckets/lab/objects/c -w ' %{http_code}\n'
for p in 7000 7001; do curl -s -X POST -H "$CT" localhost:$p/admin/faults -d '{}'; done
```

**Expected:** 503 `MetadataUnavailable` (fail closed). With the gateway
started with `-metadata-cache-ttl=30s`, a key read shortly before the fault is
still served from cache (bounded staleness). GC and repair skip work rather
than act on missing answers.
**Test:** `TestMetadataUnavailable`.

## 9. Interrupt a multipart upload

```bash
UP=$(curl -s -X POST -H "$A" $GW/v1/buckets/lab/multipart -d '{"key":"mp"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["upload_id"])')
head -c 2000000 /dev/urandom > /tmp/p1
curl -s -X POST -H "$A" --data-binary @/tmp/p1 "$GW/v1/multipart/$UP/parts?part_number=1"
# interrupt part 2 half-way
head -c 2000000 /dev/urandom | curl -s -X POST -H "$A" -H 'Content-Length: 4000000' --data-binary @- "$GW/v1/multipart/$UP/parts?part_number=2" --max-time 2
curl -s -H "$A" "$GW/v1/multipart/$UP"         # part 1 present, part 2 absent
# retry only part 2, then complete
```

**Expected:** only the interrupted part needs re-sending. An abandoned upload
is aborted after `-upload-ttl`, and its parts are reclaimed by GC.
**Tests:** `TestMultipartUploadWithPartRetry`, `TestAbortAndExpireUploads`.

## 10. Kill a node during repair

```bash
scripts/local-cluster.sh kill-node 2      # wait for repair to start (repair_queue_depth > 0)
scripts/local-cluster.sh kill-node 3      # a second failure mid-repair
```

**Expected:** repair jobs whose source or target was node 3 fail and are
retried next pass with a different source/target. Segments that are now down
to one copy are prioritized and repaired without waiting for the repair delay.
No segment becomes unavailable unless all of its copies were on nodes 2 and 3
(impossible with RF 3 across 3 zones while a third zone is healthy).
**Test:** `TestNodeFailsDuringRepair`.

## 11. Add a new node

```bash
scripts/local-cluster.sh start-node 5
```

**Expected:** node 5 joins as ACTIVE; new writes start using it; the
rebalancer moves roughly 1/5 of replicas to it using copy-before-delete, at
its bandwidth budget. `rebalance_moves_total` and node 5's `storage_node_blobs`
rise; nothing becomes under-replicated at any point.
**Test:** `TestRebalanceOnNodeAddition`.

## 12. Drain a node

```bash
curl -s -X POST -H "$ADM" $GW/admin/nodes/node-4/state -d '{"state":"DRAINING"}'
watch -n2 "curl -s -H '$ADM' $GW/admin/nodes/node-4/drain"
curl -s -X POST -H "$ADM" $GW/admin/nodes/node-4/state -d '{"state":"DECOMMISSIONED"}'
scripts/local-cluster.sh kill-node 4
```

**Expected:** node 4 receives no new data but keeps serving reads; its
referenced replica count falls to 0; then `safe_to_remove: true`.
Decommissioning before that returns 409 `NodeNotDrained` (override with
`?force=true`). Killing the node after decommissioning causes no repair
traffic, because nothing references it.
**Test:** `TestNodeDrain`.

---

## Scripted fault experiments in the test suite

`go test ./tests -v` runs all of the above in-process with deterministic
timing (25 tests, ~20 s). The fault model (`storagenode.Faults`) is the same
one exposed over HTTP.
