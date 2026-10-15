#!/usr/bin/env bash
# Runs a complete cluster as local processes (no Docker required):
#
#   2 metadata shards  (:7000 also runs the coordinator, :7001)
#   4 storage nodes    (:8101-:8104 in az-a, az-b, az-c, az-a)
#   1 gateway          (:8080)
#   1 repairer + GC    (:9100 metrics)
#   1 rebalancer       (:9200 metrics)
#
# pprof (localhost only): gateway :6060, node N :600N.
#
# Timers are shortened so failure handling is observable in seconds:
# SUSPECT after 3s, DOWN after 6s, repair 10s after DOWN.
#
# Secrets are generated on first start into .local/env (git-ignored) and are
# never written anywhere else. Source it to use the API:  source .local/env
#
# Usage:
#   scripts/local-cluster.sh up            build and start everything
#   scripts/local-cluster.sh down          stop everything (data is kept)
#   scripts/local-cluster.sh clean         stop and delete all data
#   scripts/local-cluster.sh status        show processes and cluster view
#   scripts/local-cluster.sh kill-node N   SIGKILL storage node N (1-based)
#   scripts/local-cluster.sh start-node N  (re)start storage node N; N>4 adds a node
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STATE="$ROOT/.local"
BIN="$STATE/bin"
LOGS="$STATE/logs"
PIDS="$STATE/pids"
ZONES=(az-a az-b az-c az-a az-b az-c az-a az-b)

mkdir -p "$BIN" "$LOGS" "$PIDS"

gen_secret() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

ensure_env() {
  if [[ ! -f "$STATE/env" ]]; then
    umask 077
    cat >"$STATE/env" <<EOF
export CLUSTER_TOKEN=$(gen_secret)
export OBJSTORE_TOKEN_ALICE=$(gen_secret)
export OBJSTORE_TOKEN_BOB=$(gen_secret)
export OBJSTORE_TOKEN_ADMIN=$(gen_secret)
# Convenience for curl/loadgen: the default identity is tenant "alice".
export OBJSTORE_TOKEN=\$OBJSTORE_TOKEN_ALICE
export GW=http://localhost:8080
EOF
    echo "generated secrets in $STATE/env"
  fi
  # shellcheck disable=SC1091
  source "$STATE/env"
  # The tenants file references environment variable NAMES, never tokens.
  cat >"$STATE/tenants.json" <<'EOF'
[
  {"id": "alice", "token_env": "OBJSTORE_TOKEN_ALICE",
   "limits": {"max_storage_bytes": 10737418240, "max_object_bytes": 5368709120,
              "upload_bytes_per_sec": 209715200, "download_bytes_per_sec": 419430400,
              "requests_per_sec": 5000, "request_burst": 10000,
              "max_concurrent_uploads": 64, "max_concurrent_downloads": 256}},
  {"id": "bob", "token_env": "OBJSTORE_TOKEN_BOB",
   "limits": {"max_storage_bytes": 104857600, "max_object_bytes": 10485760,
              "upload_bytes_per_sec": 1048576, "download_bytes_per_sec": 2097152,
              "requests_per_sec": 20, "request_burst": 20,
              "max_concurrent_uploads": 2, "max_concurrent_downloads": 4}},
  {"id": "admin", "token_env": "OBJSTORE_TOKEN_ADMIN", "admin": true, "limits": {}}
]
EOF
}

build() {
  echo "building binaries..."
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/...)
}

start() { # name, args...
  local name=$1; shift
  if [[ -f "$PIDS/$name" ]] && kill -0 "$(cat "$PIDS/$name")" 2>/dev/null; then
    echo "$name already running"; return
  fi
  "$@" >>"$LOGS/$name.log" 2>&1 &
  echo $! >"$PIDS/$name"
  echo "started $name (pid $!)"
}

stop() { # name [signal]
  local name=$1 sig=${2:-TERM}
  if [[ -f "$PIDS/$name" ]]; then
    kill "-$sig" "$(cat "$PIDS/$name")" 2>/dev/null || true
    rm -f "$PIDS/$name"
    echo "stopped $name ($sig)"
  fi
}

wait_http() { # url
  for _ in $(seq 1 100); do
    curl -fsS "$1" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "timeout waiting for $1" >&2; return 1
}

start_node() {
  local n=$1 port=$((8100 + $1)) zone=${ZONES[$(( ($1 - 1) % ${#ZONES[@]} ))]}
  start "node-$n" "$BIN/storage-node" -id "node-$n" -listen ":$port" -advertise "http://localhost:$port" \
    -zone "$zone" -rack "rack-$n" -data-dir "$STATE/data/node-$n" -coordinator http://localhost:7000 \
    -heartbeat 500ms -scrub-interval 1m -enable-fault-injection -debug-listen "localhost:$((6000 + $1))"
  wait_http "http://localhost:$port/healthz"
}

common_cluster_flags=(-metadata http://localhost:7000,http://localhost:7001 -coordinator http://localhost:7000)

up() {
  ensure_env
  build
  start meta-0 "$BIN/metadata" -listen :7000 -shard-index 0 -data-dir "$STATE/data/meta-0" \
    -coordinator -suspect-after 3s -down-after 6s -min-part-size 1048576 -enable-fault-injection
  start meta-1 "$BIN/metadata" -listen :7001 -shard-index 1 -data-dir "$STATE/data/meta-1" \
    -min-part-size 1048576 -enable-fault-injection
  wait_http http://localhost:7000/healthz
  wait_http http://localhost:7001/healthz
  for n in 1 2 3 4; do start_node "$n"; done
  start gateway "$BIN/gateway" -listen :8080 "${common_cluster_flags[@]}" -tenants "$STATE/tenants.json" \
    -min-part-size 1048576 -segment-size 16777216 -debug-listen localhost:6060
  start repairer "$BIN/repairer" -listen :9100 "${common_cluster_flags[@]}" \
    -interval 3s -repair-delay 10s -gc-interval 15s -gc-grace 30s -upload-ttl 10m
  start rebalancer "$BIN/rebalancer" -listen :9200 "${common_cluster_flags[@]}" -interval 5s
  wait_http http://localhost:8080/healthz
  echo
  echo "cluster is up. Next:  source $STATE/env   then see docs/demo.md"
}

down() {
  for f in "$PIDS"/*; do [[ -e "$f" ]] && stop "$(basename "$f")"; done
  return 0
}

status() {
  ensure_env >/dev/null
  for f in "$PIDS"/*; do
    [[ -e "$f" ]] || continue
    local name pid; name=$(basename "$f"); pid=$(cat "$f")
    if kill -0 "$pid" 2>/dev/null; then echo "running  $name ($pid)"; else echo "dead     $name"; fi
  done
  echo
  curl -fsS -H "Authorization: Bearer $OBJSTORE_TOKEN_ADMIN" http://localhost:8080/admin/cluster || true
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  clean) down; rm -rf "$STATE/data" "$LOGS" ;;
  status) status ;;
  kill-node) ensure_env >/dev/null; stop "node-$2" KILL ;;
  start-node) ensure_env >/dev/null; [[ -x "$BIN/storage-node" ]] || build; start_node "$2" ;;
  *) sed -n '2,25p' "$0"; exit 2 ;;
esac
