#!/bin/bash

# Quick and dirty parquet exporter for ClickHouse tables
# Usage: ./export_parquet.sh -t "table1,table2" -n network -k primary_key -s start -e end -o output_dir [-h host:port]
#
# Example:
# ./scripts/export_parquet.sh -t "beacon_api_eth_v1_events_attestation,libp2p_gossipsub_beacon_attestation" -n 'fusaka-devnet-3' -k slot -s 333500 -e 334500 -o /tmp/fusaka -h 'http://default:password@localhost:8123'

set -e

# Default values
CLICKHOUSE_HOST="http://localhost:8123"
OUTPUT_DIR="."

# Parse arguments
while getopts "t:n:k:s:e:o:h:" opt; do
  case $opt in
    t) TABLES="$OPTARG" ;;
    n) NETWORK="$OPTARG" ;;
    k) PRIMARY_KEY="$OPTARG" ;;
    s) RANGE_START="$OPTARG" ;;
    e) RANGE_END="$OPTARG" ;;
    o) OUTPUT_DIR="$OPTARG" ;;
    h) CLICKHOUSE_HOST="$OPTARG" ;;
    \?) echo "Invalid option -$OPTARG" >&2; exit 1 ;;
  esac
done

# Check required parameters
if [ -z "$TABLES" ] || [ -z "$NETWORK" ] || [ -z "$PRIMARY_KEY" ] || [ -z "$RANGE_START" ] || [ -z "$RANGE_END" ]; then
  echo "Usage: $0 -t \"table1,table2\" -n network -k primary_key -s start -e end [-o output_dir] [-h host:port]"
  echo "  -t: Comma-separated list of tables"
  echo "  -n: Network name"
  echo "  -k: Primary key column name"
  echo "  -s: Range start value"
  echo "  -e: Range end value"
  echo "  -o: Output directory (default: current dir)"
  echo "  -h: ClickHouse host (default: http://localhost:8123)"
  echo ""
  echo "Example:"
  echo "  ./scripts/export_parquet.sh -t \"beacon_api_eth_v1_events_attestation,libp2p_gossipsub_beacon_attestation\" \\"
  echo "    -n 'fusaka-devnet-3' -k slot -s 333500 -e 334500 -o /tmp/fusaka \\"
  echo "    -h 'http://default:password@localhost:8123'"
  exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Convert comma-separated list to array
IFS=',' read -ra TABLE_ARRAY <<< "$TABLES"

echo "Exporting ${#TABLE_ARRAY[@]} tables from network '$NETWORK'"
echo "Range: $PRIMARY_KEY BETWEEN $RANGE_START AND $RANGE_END"
echo "Output directory: $OUTPUT_DIR"
echo "ClickHouse host: $CLICKHOUSE_HOST"
echo ""

# Export each table
for TABLE in "${TABLE_ARRAY[@]}"; do
  # Trim whitespace
  TABLE=$(echo "$TABLE" | xargs)
  OUTPUT_FILE="$OUTPUT_DIR/${TABLE}.parquet"
  
  echo "Exporting $TABLE to $OUTPUT_FILE..."
  
  QUERY="SELECT * FROM $TABLE WHERE $PRIMARY_KEY BETWEEN $RANGE_START AND $RANGE_END AND meta_network_name = '$NETWORK' FORMAT Parquet"
  
  echo "  Query: $QUERY"
  
  curl -sS "$CLICKHOUSE_HOST" \
    --data-binary "$QUERY" \
    -o "$OUTPUT_FILE"
  
  if [ $? -eq 0 ]; then
    FILE_SIZE=$(ls -lh "$OUTPUT_FILE" | awk '{print $5}')
    echo "  ✓ Exported $TABLE ($FILE_SIZE)"
  else
    echo "  ✗ Failed to export $TABLE"
  fi
done

echo ""
echo "Export complete!"