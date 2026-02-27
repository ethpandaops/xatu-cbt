#!/usr/bin/env bash
# Compatible with Bash 3.2+ (macOS default bash).
set -euo pipefail

PREP_OUTPUT="${1:-}"
CANDIDATE_SQL="${2:-}"

if [ -z "$PREP_OUTPUT" ] || [ -z "$CANDIDATE_SQL" ]; then
  echo "Usage: run_candidate_benchmark.sh <prepare_output_json_path> <candidate_sql_file>" >&2
  exit 2
fi
if [ ! -f "$PREP_OUTPUT" ]; then
  echo "Prepare output not found: $PREP_OUTPUT" >&2
  exit 1
fi
if [ ! -f "$CANDIDATE_SQL" ]; then
  echo "Candidate SQL file not found: $CANDIDATE_SQL" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SESSION_ID="${SESSION_ID:-$(python3 - <<'PY' "$PREP_OUTPUT"
import json,sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    data = json.load(f)
print(data.get('session_id', ''))
PY
)}"
if [ -z "$SESSION_ID" ]; then
  SESSION_ID="$(date +%s)-$$"
fi

TRANSFORM_ENDPOINT="${TRANSFORM_ENDPOINT:-http://chendpoint-xatu-cbt-clickhouse.analytics.production.ethpandaops:8123}"
TRANSFORM_DB="${TRANSFORM_DB:-mainnet}"
TRANSFORM_USER="${TRANSFORM_USER:-}"
TRANSFORM_PASS="${TRANSFORM_PASS:-}"

WARMUP="${WARMUP:-1}"
RUNS="${RUNS:-3}"
BENCH_TIMEOUT="${BENCH_TIMEOUT:-300}"

WINDOW_LABEL_RAW="${WINDOW_LABEL:-window}"
CANDIDATE_LABEL_RAW="${CANDIDATE_LABEL:-$(basename "$CANDIDATE_SQL" .sql)}"
WINDOW_LABEL="$(printf '%s' "$WINDOW_LABEL_RAW" | tr -cs 'A-Za-z0-9._-' '_')"
CANDIDATE_LABEL="$(printf '%s' "$CANDIDATE_LABEL_RAW" | tr -cs 'A-Za-z0-9._-' '_')"

BENCH_OUTPUT="${BENCH_OUTPUT:-/tmp/optimize-model.${SESSION_ID}.bench.${WINDOW_LABEL}.${CANDIDATE_LABEL}.json}"

python3 "$SCRIPT_DIR/benchmark_query.py" \
  --query-file "$CANDIDATE_SQL" \
  --endpoint "$TRANSFORM_ENDPOINT" \
  --database "$TRANSFORM_DB" \
  --username "$TRANSFORM_USER" \
  --password "$TRANSFORM_PASS" \
  --warmup "$WARMUP" \
  --runs "$RUNS" \
  --timeout "$BENCH_TIMEOUT" \
  --output "$BENCH_OUTPUT"

echo "BENCH_OUTPUT=$BENCH_OUTPUT"
