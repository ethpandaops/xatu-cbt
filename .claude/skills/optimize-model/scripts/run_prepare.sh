#!/usr/bin/env bash
# Compatible with Bash 3.2+ (macOS default bash).
set -euo pipefail

MODEL="${1:-}"
if [ -z "$MODEL" ]; then
  echo "Usage: run_prepare.sh <model_path_or_name>" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-.}"
SESSION_ID="${SESSION_ID:-$(date +%s)-$$}"
PERIOD_MODE="${PERIOD_MODE:-sane}"
PREP_OUTPUT="${PREP_OUTPUT:-/tmp/optimize-model.${SESSION_ID}.prepare.json}"

EXTERNAL_ENDPOINT="${EXTERNAL_ENDPOINT:-http://chendpoint-xatu-clickhouse.analytics.production.ethpandaops:8123}"
EXTERNAL_DB="${EXTERNAL_DB:-default}"
EXTERNAL_USER="${EXTERNAL_USER:-}"
EXTERNAL_PASS="${EXTERNAL_PASS:-}"

TRANSFORM_ENDPOINT="${TRANSFORM_ENDPOINT:-http://chendpoint-xatu-cbt-clickhouse.analytics.production.ethpandaops:8123}"
TRANSFORM_DB="${TRANSFORM_DB:-mainnet}"
TRANSFORM_USER="${TRANSFORM_USER:-}"
TRANSFORM_PASS="${TRANSFORM_PASS:-}"

ACCESS_TO_EXTERNAL_CLUSTER="${ACCESS_TO_EXTERNAL_CLUSTER:-cluster('{remote_cluster}', database.table_name)}"
NETWORK="${NETWORK:-mainnet}"
TIMEOUT="${TIMEOUT:-30}"

cmd=(
  python3 "$SCRIPT_DIR/prepare_model_analysis.py"
  --model "$MODEL"
  --repo-root "$REPO_ROOT"
  --external-endpoint "$EXTERNAL_ENDPOINT"
  --external-database "$EXTERNAL_DB"
  --external-username "$EXTERNAL_USER"
  --external-password "$EXTERNAL_PASS"
  --transformation-endpoint "$TRANSFORM_ENDPOINT"
  --transformation-database "$TRANSFORM_DB"
  --transformation-username "$TRANSFORM_USER"
  --transformation-password "$TRANSFORM_PASS"
  --access-to-external-cluster "$ACCESS_TO_EXTERNAL_CLUSTER"
  --network "$NETWORK"
  --session-id "$SESSION_ID"
  --period-mode "$PERIOD_MODE"
  --timeout "$TIMEOUT"
  --output "$PREP_OUTPUT"
)

if [ -n "${BOUNDS_START:-}" ] && [ -n "${BOUNDS_END:-}" ]; then
  cmd+=(--bounds-start "$BOUNDS_START" --bounds-end "$BOUNDS_END")
fi
if [ -n "${TASK_START:-}" ]; then
  cmd+=(--task-start "$TASK_START")
fi

"${cmd[@]}"

echo "PREP_OUTPUT=$PREP_OUTPUT"
