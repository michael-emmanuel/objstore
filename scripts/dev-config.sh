#!/usr/bin/env bash
# Generates deploy/.env with random secrets for the Docker Compose cluster.
# The file is git-ignored and readable only by you. Re-running keeps the
# existing file unless --force is given.
set -euo pipefail
cd "$(dirname "$0")/../deploy"
if [[ -f .env && "${1:-}" != "--force" ]]; then
  echo "deploy/.env exists (use --force to regenerate)"; exit 0
fi
gen() { head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
umask 077
cat > .env <<ENV
CLUSTER_TOKEN=$(gen)
OBJSTORE_TOKEN_ALICE=$(gen)
OBJSTORE_TOKEN_BOB=$(gen)
OBJSTORE_TOKEN_ADMIN=$(gen)
ENV
echo "wrote deploy/.env. To use the API from your shell:"
echo "  set -a; source deploy/.env; set +a; export OBJSTORE_TOKEN=\$OBJSTORE_TOKEN_ALICE GW=http://localhost:8080"
