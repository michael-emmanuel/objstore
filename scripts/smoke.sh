#!/usr/bin/env bash
# End-to-end smoke test against a running cluster (local-cluster.sh or
# docker compose). Exercises the public API and fails loudly on any mismatch.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [[ -z "${OBJSTORE_TOKEN:-}" && -f "$ROOT/.local/env" ]]; then source "$ROOT/.local/env"; fi
: "${OBJSTORE_TOKEN:?set OBJSTORE_TOKEN}"
GW=${GW:-http://localhost:8080}
AUTH="Authorization: Bearer $OBJSTORE_TOKEN"
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
B="smoke-$RANDOM$RANDOM"
fail() { echo "FAIL: $*" >&2; exit 1; }
code() { curl -s -o "$TMP/out" -w '%{http_code}' -H "$AUTH" "$@"; }

[[ $(code -X PUT "$GW/v1/buckets/$B" -d '{"versioning":true}') == 200 ]] || fail "create bucket"

head -c 5000000 /dev/urandom >"$TMP/obj"
SUM=$(sha256sum "$TMP/obj" | cut -d' ' -f1)
[[ $(code -X PUT -H "X-Checksum-SHA256: $SUM" --data-binary @"$TMP/obj" "$GW/v1/buckets/$B/objects/dir/a.bin") == 200 ]] || fail "put"
[[ $(code -X PUT -H "X-Checksum-SHA256: $SUM" --data-binary @/dev/null "$GW/v1/buckets/$B/objects/bad") == 400 ]] || fail "bad digest accepted"
[[ $(code "$GW/v1/buckets/$B/objects/dir/a.bin") == 200 ]] || fail "get"
[[ $(sha256sum <"$TMP/out" | cut -d' ' -f1) == "$SUM" ]] || fail "content mismatch"
[[ $(code -r 100-199 "$GW/v1/buckets/$B/objects/dir/a.bin") == 206 ]] || fail "range"
cmp -s "$TMP/out" <(tail -c +101 "$TMP/obj" | head -c 100) || fail "range content"
[[ $(code -I "$GW/v1/buckets/$B/objects/dir/a.bin") == 200 ]] || fail "head"

for i in 1 2 3; do echo "$i" | code -X PUT --data-binary @- "$GW/v1/buckets/$B/objects/dir/n$i" >/dev/null; done
[[ $(code "$GW/v1/buckets/$B/objects?prefix=dir/&max_keys=2") == 200 ]] || fail "list"
grep -q next_continuation_token "$TMP/out" || fail "pagination token missing"

[[ $(code -X DELETE "$GW/v1/buckets/$B/objects/dir/a.bin") == 204 ]] || fail "delete"
[[ $(code "$GW/v1/buckets/$B/objects/dir/a.bin") == 404 ]] || fail "deleted object still readable"

head -c 1100000 /dev/urandom >"$TMP/p1"; head -c 1000 /dev/urandom >"$TMP/p2"
code -X POST "$GW/v1/buckets/$B/multipart" -d '{"key":"mp"}' >/dev/null
ID=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["upload_id"])' "$TMP/out")
for i in 1 2; do [[ $(code -X POST --data-binary @"$TMP/p$i" "$GW/v1/multipart/$ID/parts?part_number=$i") == 200 ]] || fail "part $i"; done
E1=$(sha256sum "$TMP/p1" | cut -d' ' -f1); E2=$(sha256sum "$TMP/p2" | cut -d' ' -f1)
[[ $(code -X POST "$GW/v1/multipart/$ID/complete" -d "{\"parts\":[{\"part_number\":1,\"etag\":\"$E1\"},{\"part_number\":2,\"etag\":\"$E2\"}]}") == 200 ]] || fail "complete"
code "$GW/v1/buckets/$B/objects/mp" >/dev/null
cmp -s "$TMP/out" <(cat "$TMP/p1" "$TMP/p2") || fail "multipart content"

[[ $(curl -s -o /dev/null -w '%{http_code}' "$GW/v1/buckets/$B/objects/mp") == 401 ]] || fail "anonymous read allowed"
echo "smoke test passed"
