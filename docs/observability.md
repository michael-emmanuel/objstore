# Observability

Every component exposes Prometheus metrics at `GET /metrics` (no cluster
token required), writes JSON structured logs with `log/slog`, and propagates
trace context. This page lists what exists and how to use it to answer
operational questions.

The metrics registry is hand-written (`internal/observability`, ~300 lines,
text exposition format) to avoid dependencies; it is compatible with
Prometheus scraping. A production build would use `prometheus/client_golang`
and OpenTelemetry.

Local endpoints: gateway `:8080`, metadata `:7000`/`:7001`, nodes
`:8101-8105`, repairer `:9100`, rebalancer `:9200`. With Docker Compose,
`--profile observability` starts Prometheus on `:9090` with
`deploy/prometheus.yml`.

## Metrics

### Requests (gateway)

| Metric | Type | Use |
|---|---|---|
| `gateway_requests_total{op,code}` | counter | GET/s, PUT/s, DELETE/s, LIST/s: `rate(...[1m])` by `op`; error rate by `code` |
| `gateway_request_seconds{op}` | histogram | p50/p95/p99 via `histogram_quantile` |
| `gateway_inflight_requests{op}` | gauge | concurrency |
| `gateway_inflight_uploads` | gauge | upload slots in use (load shedding at `-max-inflight-uploads`) |
| `gateway_rate_limited_total{tenant,kind}` | counter | noisy-neighbour enforcement (`requests`, `concurrent_uploads`, ...) |
| `gateway_object_size_bytes` | histogram | workload shape |

### Network (gateway and nodes)

| Metric | Use |
|---|---|
| `gateway_ingress_bytes_total`, `gateway_egress_bytes_total{source}` | client traffic (egress split by source: cache or replicas) |
| `gateway_replication_bytes_total` | fan-out traffic to nodes (= ingress x replicas acked) |
| `storage_node_bytes_in_total{kind}` | writes by kind: `client` vs `repair` vs `rebalance` |
| `storage_node_bytes_out_total{kind}` | reads by kind |

### Storage (nodes)

| Metric | Use |
|---|---|
| `storage_node_used_bytes`, `storage_node_capacity_bytes` | disk utilization and free capacity |
| `storage_node_inflight_bytes` | bytes reserved by writes in progress |
| `storage_node_blobs` | object (blob) count per node |
| `storage_node_op_seconds{op}` | disk/engine latency per operation |
| `storage_node_ops_total{op,result}` | errors by operation |
| `storage_node_corruptions_total{source}` | corruption found by `read` or `scrubber` |
| `storage_node_quarantined_blobs` | corrupt blobs awaiting repair acknowledgement |
| `storage_node_scrubbed_bytes_total`, `storage_node_scrub_last_pass_unixtime` | scrub progress (silent-corruption coverage) |

### Replication and repair

| Metric | Use |
|---|---|
| `gateway_under_replicated_writes_total` | writes acknowledged below RF (the visible form of "replica lag") |
| `gateway_replica_write_failures_total{reason}` | `error`, `stalled`, `straggler`, `disk_full` |
| `gateway_read_failovers_total` | reads that switched replica |
| `gateway_penalized_nodes` | nodes this gateway currently avoids after they stalled or straggled (slow-node circuit breaker) |
| `repair_under_replicated_segments` | segments below RF at the last scan |
| `repair_unavailable_segments` | segments with **no** healthy replica: page someone |
| `repair_queue_depth` | repair jobs left in the current pass |
| `repair_copies_total`, `repair_bytes_total` | repair throughput |
| `repair_failures_total` | failed repair attempts (check logs for the error) |
| `repair_quarantine_processed_total` | corrupt replicas replaced |
| `repair_last_pass_seconds` | scan cost |
| `rebalance_misplaced_segments`, `rebalance_moves_total`, `rebalance_bytes_total`, `rebalance_trimmed_replicas_total` | rebalancing progress |
| `gc_queue_depth`, `gc_deleted_blobs_total{source}`, `gc_reclaimed_bytes_total`, `gc_expired_uploads_total`, `gc_purged_tombstones_total` | reclamation |

### Cache

| Metric | Use |
|---|---|
| `gateway_object_cache_hits_total`, `..._misses_total`, `..._evictions_total` | hit rate and eviction rate |
| `gateway_object_cache_cost` | memory used (bytes) |
| `gateway_object_cache_coalesced_total` | stampede protection at work |
| `gateway_metadata_cache_*` | the same for the metadata cache (when enabled; cost counts entries) |

### Metadata

| Metric | Use |
|---|---|
| `gateway_metadata_seconds{op}` | metadata lookup / commit latency as seen by the gateway |
| `metadata_keys`, `metadata_versions`, `metadata_segments`, `metadata_buckets` | shard utilization (compare across shards for skew) |
| `metadata_lsn` | committed mutations; `rate()` = commit throughput per shard |
| `metadata_wal_records` | WAL length since snapshot (recovery time) |
| `metadata_garbage_queue`, `metadata_pending_blobs`, `metadata_multipart_uploads` | background backlog |
| `gateway_membership_epoch` | membership changes seen |

## Logs

JSON, one record per line, always with `service`; request-scoped records have
`request_id` and `trace_id`. Every request emits one access record (`method`,
`path`, `status`, `bytes`, `duration`). Mutations additionally emit an
`audit` record (tenant, operation, bucket, key, version). Failures in the
background loops log the blob ID and node, which is how the cluster-token bug
in repair was diagnosed in minutes:

```json
{"level":"WARN","msg":"repair failed","service":"repairer","blob":"e5208bea...",
 "error":"storage node http 500: pull source: storage node http 401: cluster authentication required"}
```

## Tracing

`internal/observability/trace.go` implements W3C `traceparent` propagation
and spans that are emitted as structured log records (`trace_id`, `span_id`,
`parent_id`, name, duration, attributes). This is deliberately minimal: there
is no exporter, and spans are reassembled from logs by `trace_id`.

What is traced today:

```
client -> gateway            server span per request (middleware), X-Request-Id
            -> write_blob    child span per segment write: blob, rf, membership epoch,
                             replicas acked/failed
            -> metadata RPC  traceparent injected; the metadata server continues the
                             trace (server span per RPC, debug level)
            -> storage node  traceparent injected; the node continues the trace
                             (server span per blob request, debug level)
```

Placement is recorded as span attributes (membership epoch, chosen nodes in
the failure log line) rather than as its own span: it is an in-memory
computation of under a microsecond.

Server spans on internal services log at debug level (`-log-level=debug`),
because heartbeats and replica traffic would otherwise dominate the logs; the
gateway logs every request at info.

**Asynchronous work.** Repair, rebalance and GC are not caused by a client
request and do not start traces. Their log records carry the **blob ID**, which
is the join key: grep for a blob ID to see the original write (gateway
`write_blob` span), the corruption or failure (node), and the repair
(repairer). A production system would give each repair job its own trace and
record the originating write's trace as a span *link* (OpenTelemetry links),
not as a parent: the repair is caused by the failure, not by the write.

## Example questions and queries

| Question | Query |
|---|---|
| Are we losing durability? | `repair_under_replicated_segments > 0` for long; `repair_unavailable_segments > 0` (urgent) |
| Is repair keeping up? | `rate(repair_bytes_total[5m])` vs growth of `repair_queue_depth` |
| Why is repair traffic rising? | node states in `/admin/cluster`; `storage_node_corruptions_total` by node; `repair_failures_total` |
| Which node is slow? | `storage_node_op_seconds` p99 by instance; `gateway_replica_write_failures_total{reason="stalled"}` |
| Is a tenant noisy? | `gateway_rate_limited_total` by tenant; request rate by tenant from access logs |
| Is metadata the bottleneck? | `gateway_metadata_seconds` p99 rising with `rate(metadata_lsn)` flat at ~fsync rate |
| Is the cache working? | hits / (hits + misses); evictions rate |
| Is a node safe to remove? | `GET /admin/nodes/{id}/drain` -> `safe_to_remove` |

## Alerting suggestions

| Alert | Condition |
|---|---|
| Data unavailable | `repair_unavailable_segments > 0` |
| Durability degraded | `repair_under_replicated_segments > 0` for 30 min |
| Repair failing | `rate(repair_failures_total[10m]) > 0` and queue not shrinking |
| Disk nearly full | `storage_node_used_bytes / storage_node_capacity_bytes > 0.85` |
| Corruption on a node | `increase(storage_node_corruptions_total[1h]) > N` on one instance |
| Metadata latency | p99 `gateway_metadata_seconds` above SLO |
| Scrub stale | `time() - storage_node_scrub_last_pass_unixtime > 2 x interval` |
