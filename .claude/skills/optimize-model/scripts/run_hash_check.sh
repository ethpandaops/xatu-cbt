#!/usr/bin/env bash
# Compatible with Bash 3.2+ (macOS default bash).
set -euo pipefail

PREP_OUTPUT="${1:-}"
CANDIDATE_SQL="${2:-}"

if [ -z "$PREP_OUTPUT" ] || [ -z "$CANDIDATE_SQL" ]; then
  echo "Usage: run_hash_check.sh <prepare_output_json_path> <candidate_sql_file>" >&2
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

BASELINE_SQL="${BASELINE_SQL:-$(python3 - <<'PY' "$PREP_OUTPUT"
import json,sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    data = json.load(f)
print(data['paths']['rendered_sql'])
PY
)}"

TRANSFORM_ENDPOINT="${TRANSFORM_ENDPOINT:-http://chendpoint-xatu-cbt-clickhouse.analytics.production.ethpandaops:8123}"
TRANSFORM_DB="${TRANSFORM_DB:-mainnet}"
TRANSFORM_USER="${TRANSFORM_USER:-}"
TRANSFORM_PASS="${TRANSFORM_PASS:-}"
HASH_TIMEOUT="${HASH_TIMEOUT:-300}"
HASH_OUTPUT="${HASH_OUTPUT:-/tmp/optimize-model.${SESSION_ID}.hash.json}"
SERVER_VERSION="${SERVER_VERSION:-$(python3 - <<'PY' "$PREP_OUTPUT"
import json,sys
with open(sys.argv[1], 'r', encoding='utf-8') as f:
    data = json.load(f)
print(((data.get('clickhouse_versions') or {}).get('transformation') or {}).get('version', ''))
PY
)}"

cmd=(
  python3 "$SCRIPT_DIR/compare_query_hash.py"
  --baseline-query-file "$BASELINE_SQL"
  --candidate-query-file "$CANDIDATE_SQL"
  --endpoint "$TRANSFORM_ENDPOINT"
  --database "$TRANSFORM_DB"
  --username "$TRANSFORM_USER"
  --password "$TRANSFORM_PASS"
  --timeout "$HASH_TIMEOUT"
  --output "$HASH_OUTPUT"
)

if [ -n "$SERVER_VERSION" ]; then
  cmd+=(--server-version "$SERVER_VERSION")
fi

"${cmd[@]}"

echo "HASH_OUTPUT=$HASH_OUTPUT"
