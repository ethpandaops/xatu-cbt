#!/bin/bash
#
# Export test data from a Xatu ClickHouse instance to parquet files
# This script auto-discovers tables from test data YAML files and detects
# whether each table uses slot or block_number partitioning.
#
# Usage:
#   ./scripts/export_test_data.sh \
#     --network sepolia \
#     --spec fusaka \
#     --slot-start 333500 \
#     --slot-end 333520 \
#     --block-start 123456 \
#     --block-end 123476 \
#     --output ./output/sepolia/fusaka \
#     --clickhouse-host "http://user:pass@production.clickhouse:8123"
#
# Example:
#   ./scripts/export_test_data.sh \
#     --network mainnet \
#     --spec pectra \
#     --slot-start 12145370 \
#     --slot-end 12145390 \
#     --block-start 22923418 \
#     --block-end 22923437 \
#     --output ./output/mainnet/pectra \
#     --clickhouse-host "http://default:password@localhost:8123"

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CLICKHOUSE_HOST="http://localhost:8123"
TESTS_DIR="tests"
MODELS_DIR="models/external"

# Function to print colored output
print_info() {
  echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
  echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
  echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
  echo -e "${RED}✗${NC} $1"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --network)
      NETWORK="$2"
      shift 2
      ;;
    --spec)
      SPEC="$2"
      shift 2
      ;;
    --slot-start)
      SLOT_START="$2"
      shift 2
      ;;
    --slot-end)
      SLOT_END="$2"
      shift 2
      ;;
    --block-start)
      BLOCK_START="$2"
      shift 2
      ;;
    --block-end)
      BLOCK_END="$2"
      shift 2
      ;;
    --output)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --clickhouse-host)
      CLICKHOUSE_HOST="$2"
      shift 2
      ;;
    --tests-dir)
      TESTS_DIR="$2"
      shift 2
      ;;
    --models-dir)
      MODELS_DIR="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# //; s/^#//'
      exit 0
      ;;
    *)
      print_error "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Validate required parameters
if [ -z "$NETWORK" ] || [ -z "$SPEC" ] || [ -z "$SLOT_START" ] || [ -z "$SLOT_END" ] || [ -z "$BLOCK_START" ] || [ -z "$BLOCK_END" ] || [ -z "$OUTPUT_DIR" ]; then
  print_error "Missing required parameters"
  echo ""
  echo "Required:"
  echo "  --network <network>       Network name (e.g., mainnet, sepolia)"
  echo "  --spec <spec>             Spec name (e.g., pectra, fusaka)"
  echo "  --slot-start <slot>       Starting slot number"
  echo "  --slot-end <slot>         Ending slot number"
  echo "  --block-start <block>     Starting block number"
  echo "  --block-end <block>       Ending block number"
  echo "  --output <dir>            Output directory for parquet files"
  echo ""
  echo "Optional:"
  echo "  --clickhouse-host <url>   ClickHouse HTTP URL (default: http://localhost:8123)"
  echo "  --tests-dir <dir>         Tests directory (default: tests)"
  echo "  --models-dir <dir>        Models directory (default: models/external)"
  echo ""
  echo "Use --help for full usage information"
  exit 1
fi

# Check if test data directory exists
DATA_DIR="${TESTS_DIR}/${NETWORK}/${SPEC}/data"
if [ ! -d "$DATA_DIR" ]; then
  print_error "Test data directory does not exist: ${DATA_DIR}"
  print_info "Expected structure: tests/<network>/<spec>/data/"
  exit 1
fi

# Check if models directory exists
if [ ! -d "$MODELS_DIR" ]; then
  print_error "Models directory does not exist: ${MODELS_DIR}"
  exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

print_info "Configuration:"
echo "  Network:        ${NETWORK}"
echo "  Spec:           ${SPEC}"
echo "  Slot range:     ${SLOT_START} - ${SLOT_END}"
echo "  Block range:    ${BLOCK_START} - ${BLOCK_END}"
echo "  Output:         ${OUTPUT_DIR}"
echo "  ClickHouse:     ${CLICKHOUSE_HOST}"
echo "  Data dir:       ${DATA_DIR}"
echo ""

# Function to extract interval type from external model
get_interval_type() {
  local table_name=$1
  local model_file="${MODELS_DIR}/${table_name}.sql"

  if [ ! -f "$model_file" ]; then
    echo "slot"  # Default to slot silently
    return
  fi

  # Extract interval.type from YAML frontmatter
  local interval_type=$(sed -n '/^---$/,/^---$/p' "$model_file" | grep "type:" | awk '{print $2}' | head -1)

  if [ -z "$interval_type" ]; then
    echo "slot"  # Default to slot silently
  else
    echo "$interval_type"
  fi
}

# Function to get partition column name from interval type
get_partition_column() {
  local interval_type=$1

  case "$interval_type" in
    slot)
      echo "slot"
      ;;
    block)
      echo "block_number"
      ;;
    *)
      echo "slot"  # Default silently
      ;;
  esac
}

# Discover tables from data YAML files
print_info "Discovering tables from ${DATA_DIR}..."
TABLES=()
while IFS= read -r yaml_file; do
  table_name=$(basename "$yaml_file" .yaml)
  TABLES+=("$table_name")
done < <(find "$DATA_DIR" -type f -name "*.yaml" | sort)

if [ ${#TABLES[@]} -eq 0 ]; then
  print_error "No YAML files found in ${DATA_DIR}"
  exit 1
fi

print_success "Found ${#TABLES[@]} tables to export"
echo ""

# Export each table
export_count=0
failed_tables=()

for table in "${TABLES[@]}"; do
  export_count=$((export_count + 1))
  print_info "[${export_count}/${#TABLES[@]}] Processing ${table}..."

  # Detect interval type
  interval_type=$(get_interval_type "$table")
  partition_col=$(get_partition_column "$interval_type")

  # Determine range based on interval type
  if [ "$interval_type" == "block" ]; then
    range_start=$BLOCK_START
    range_end=$BLOCK_END
  else
    range_start=$SLOT_START
    range_end=$SLOT_END
  fi

  echo "  Interval type:  ${interval_type}"
  echo "  Partition col:  ${partition_col}"
  echo "  Range:          ${range_start} - ${range_end}"

  # Build output file path
  output_file="${OUTPUT_DIR}/${table}.parquet"

  # Build SQL query
  query="SELECT * FROM ${table} WHERE ${partition_col} BETWEEN ${range_start} AND ${range_end} AND meta_network_name = '${NETWORK}' FORMAT Parquet"

  # Execute export
  if curl -sS "${CLICKHOUSE_HOST}" --data-binary "$query" -o "$output_file" 2>/dev/null; then
    file_size=$(du -h "$output_file" | cut -f1)
    print_success "Exported ${table} (${file_size})"
  else
    print_error "Failed to export ${table}"
    failed_tables+=("$table")
    rm -f "$output_file"
  fi

  echo ""
done

# Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ ${#failed_tables[@]} -eq 0 ]; then
  print_success "Export complete! All ${#TABLES[@]} tables exported successfully"
  echo ""
  print_info "Output directory: ${OUTPUT_DIR}"
  print_info "Total files: $(find "$OUTPUT_DIR" -name "*.parquet" | wc -l)"
  print_info "Total size: $(du -sh "$OUTPUT_DIR" | cut -f1)"
  echo ""
  print_info "Next steps:"
  echo "  1. Review the parquet files in: ${OUTPUT_DIR}"
  echo "  2. Upload to R2: aws s3 cp ${OUTPUT_DIR} s3://xatu-cbt/${NETWORK}/${SPEC}/ --recursive"
  echo "  3. Update test data YAMLs to point to new URLs"
else
  print_warning "Export completed with ${#failed_tables[@]} failures"
  echo ""
  print_error "Failed tables:"
  for table in "${failed_tables[@]}"; do
    echo "  - ${table}"
  done
  exit 1
fi
