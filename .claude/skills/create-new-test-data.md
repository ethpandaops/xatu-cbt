---
name: create-new-test-data
description: Guides you through the process of creating new test data for a specific network and spec (fork).
---


## Prerequisites

- Access to ethpandaops production Clickhouse via MCP tools
- Understanding of the target network (mainnet, sepolia, holesky, etc.)
- Understanding of the target spec/fork (deneb, electra, fusaka, etc.)
- Credentials for Clickhouse (if using direct queries)

## Step 1: Understand Requirements

Ask yourself or the user:

1. **What network?** (e.g., sepolia, holesky, mainnet)
2. **What spec/fork?** (e.g., fusaka, electra, deneb)
3. **What external tables are required?** Check `tests/{network}/{spec}/data/` directory
4. **Time range preference?** (recent data preferred, typically last 20-24 hours)

## Step 2: Identify Fork-Specific Features

Different forks have different data characteristics:

- **Deneb**: Has blob sidecars (`beacon_api_eth_v1_events_blob_sidecar`, `canonical_beacon_blob_sidecar`)
- **Fusaka**: Has data column sidecars (`beacon_api_eth_v1_events_data_column_sidecar`), NO blob sidecars
- **Electra**: Check fork specification for unique features

**Action**: Query Clickhouse to find when the fork activated:

```sql
-- For Fusaka (data column sidecars), find first appearance
SELECT
    min(slot) as first_slot,
    max(slot) as last_slot
FROM default.beacon_api_eth_v1_events_data_column_sidecar
WHERE meta_network_name = '{network}'
```

## Step 3: Check External Table List

List all required external tables:

```bash
ls tests/{network}/{spec}/data/*.yaml | xargs -I {} basename {} .yaml
```

Common critical tables:
- All `beacon_api_*` tables
- All `canonical_beacon_*` tables
- All `canonical_execution_*` tables (especially traces, transactions, storage_diffs, storage_reads)
- `libp2p_gossipsub_*` tables
- `mev_relay_*` tables
- `ethseer_validator_entity`

## Step 4: Identify Data Availability Issues

**CRITICAL**: Some tables may lag behind or stop collecting data. Always check:

### Check `canonical_execution_balance_diffs`

```sql
SELECT
    max(block_number) as max_block,
    count(*) as total_rows
FROM default.canonical_execution_balance_diffs
WHERE meta_network_name = '{network}'
```

If max_block is significantly behind recent blocks, the data is catching up. Calculate catch-up rate:

1. Record current max_block
2. Wait 2-5 minutes
3. Query again
4. Calculate: `(new_max - old_max) / minutes_waited = blocks_per_minute`
5. Estimate time to reach target: `(target_block - current_max) / blocks_per_minute`

### Check Other Critical Execution Tables

```sql
-- Check recent data availability
SELECT
    'transactions' as table_name,
    max(block_number) as max_block,
    count(*) as rows
FROM default.canonical_execution_transaction
WHERE meta_network_name = '{network}'
  AND block_number >= {recent_block}

UNION ALL

SELECT
    'traces' as table_name,
    max(block_number) as max_block,
    count(*) as rows
FROM default.canonical_execution_traces
WHERE meta_network_name = '{network}'
  AND block_number >= {recent_block}

UNION ALL

SELECT
    'storage_diffs' as table_name,
    max(block_number) as max_block,
    count(*) as rows
FROM default.canonical_execution_storage_diffs
WHERE meta_network_name = '{network}'
  AND block_number >= {recent_block}

UNION ALL

SELECT
    'storage_reads' as table_name,
    max(block_number) as max_block,
    count(*) as rows
FROM default.canonical_execution_storage_reads
WHERE meta_network_name = '{network}'
  AND block_number >= {recent_block}
```

## Step 5: Find Suitable Slot/Block Range

### Strategy 1: Recent Data (Preferred)

Find a recent 10-block window with good coverage:

```sql
-- Get recent slot-to-block mapping
SELECT
    slot,
    execution_payload_block_number as block_number,
    slot_start_date_time
FROM default.canonical_beacon_block FINAL
WHERE meta_network_name = '{network}'
  AND slot_start_date_time >= NOW() - INTERVAL 20 HOUR
  AND slot_start_date_time <= NOW() - INTERVAL 30 MINUTE
ORDER BY slot DESC
LIMIT 20
```

Pick a contiguous 10-slot range (e.g., slots X to X+9).

### Strategy 2: Fork Activation Window

For testing fork-specific features, use data near fork activation:

```sql
-- Find slot/block at fork activation (use fork-specific query)
-- Then add a buffer (e.g., +1000 slots) to ensure stability
```

## Step 6: Verify All Tables Have Data in Range

**CRITICAL STEP**: Before exporting, verify EVERY external table has data:

```sql
-- Template for checking each table
SELECT count(*) as row_count
FROM default.{table_name}
WHERE meta_network_name = '{network}'
  AND {partition_column} >= {range_start}
  AND {partition_column} <= {range_end}
```

For slot-based tables, use slot range. For block-based tables, use block range.

**Create a checklist** and mark each table:
- ✅ Has data (count > 0)
- ⚠️ No data (count = 0) - check if expected (e.g., blob_sidecar in Fusaka)
- ❌ Missing unexpectedly - **BLOCKER**

## Step 7: Export Data

### Option A: Using export_test_data.sh (if available)

```bash
./scripts/export_test_data.sh \
  --network {network} \
  --spec {spec} \
  --slot-start {slot_start} \
  --slot-end {slot_end} \
  --block-start {block_start} \
  --block-end {block_end} \
  --output output/{network}/{spec} \
  --clickhouse-host "https://user:password@clickhouse.xatu.ethpandaops.io"
```

### Option B: Using export_parquet.sh

For each table in `tests/{network}/{spec}/data/`:

```bash
# Read YAML to get interval_type and primary_key
# Then export accordingly

# For slot-based tables:
./scripts/export_parquet.sh \
  -t "{table_name}" \
  -n '{network}' \
  -k slot \
  -s {slot_start} \
  -e {slot_end} \
  -o output/{network}/{spec} \
  -h 'https://user:password@clickhouse.xatu.ethpandaops.io'

# For block-based tables:
./scripts/export_parquet.sh \
  -t "{table_name}" \
  -n '{network}' \
  -k block_number \
  -s {block_start} \
  -e {block_end} \
  -o output/{network}/{spec} \
  -h 'https://user:password@clickhouse.xatu.ethpandaops.io'
```

## Step 8: Validate Exported Data

Verify each parquet file has expected data:

```python
import pandas as pd
import os

for f in sorted(os.listdir('output/{network}/{spec}')):
    if f.endswith('.parquet'):
        df = pd.read_parquet(f'output/{network}/{spec}/{f}')
        print(f"{f:60s} {len(df):8d} rows")
```

**Check for**:
- All tables present (32 files for typical Fusaka setup)
- No 0-row files (unless expected, like blob_sidecar in Fusaka)
- Reasonable row counts (transactions should have many rows, etc.)

## Step 9: Upload to R2
This is a manual step done by the user. You can give them an `open $OUTPUT_DIR` command to open the output directory in the file explorer, and they can upload the files to R2 manually.

## Step 10: Update Test Data YAMLs

For each table in `tests/{network}/{spec}/data/`, update the `url` field:

```yaml
url: https://data.ethpandaops.io/xatu-cbt/{network}/{spec}/{table_name}.parquet
```

## Step 11: Run Tests and Update Assertions

```bash
# Run tests to see which assertions fail
make test NETWORK={network} SPEC={spec}
```

Then update `tests/{network}/{spec}/assertions/*.yaml` with actual values.

## Common Pitfalls

1. **Not checking balance_diffs catch-up status** - This table lags frequently
2. **Not validating ALL external tables** - Missing even one table causes test failures
3. **Using data before fork activation** - Won't have fork-specific features
4. **Not accounting for blob_sidecar removal in Fusaka** - These SHOULD be 0
5. **Forgetting the 30-minute buffer** - Always use `NOW() - INTERVAL 30 MINUTE` to avoid incomplete data
6. **Wrong partition column** - Some tables use `slot`, others use `block_number`, some use `slot_start_date_time`

## Quick Reference: Table Partition Columns

- **Slot-based**: `slot` (most beacon tables)
- **Block-based**: `block_number` (execution tables)
- **Time-based**: `slot_start_date_time`, `block_timestamp`, `event_date_time`
- **Network-only**: `ethseer_validator_entity` (no range filter)

## Success Criteria

✅ All external tables have data in the chosen range
✅ Fork-specific features present (e.g., data_column_sidecars for Fusaka)
✅ Fork-deprecated features absent (e.g., blob_sidecars in Fusaka)
✅ Data is recent (< 24 hours old preferred)
✅ All parquet files exported successfully
✅ Files uploaded to R2
✅ Tests pass after assertion updates
