#!/usr/bin/env bash
# Launch HelixMCP stdio gateway with correct working directory.
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
cd "$ROOT"
export GATEWAY_POLICY_PATH="${GATEWAY_POLICY_PATH:-$ROOT/policies/default.policy.yaml}"
export OBJECT_STORE_DIR="${OBJECT_STORE_DIR:-$ROOT/var/objects}"
export RUNS_DIR="${RUNS_DIR:-$ROOT/var/runs}"
export AUTO_SCHEMA="${AUTO_SCHEMA:-true}"
mkdir -p "$OBJECT_STORE_DIR" "$RUNS_DIR"
exec node "$ROOT/dist/index.js" "$@"
