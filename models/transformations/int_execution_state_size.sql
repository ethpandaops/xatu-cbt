---
table: int_execution_state_size
type: incremental
interval:
  type: block
  max: 10000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5m" 
tags:
  - execution
  - storage
dependencies:
  - "{{external}}.execution_state_size_delta"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get the maximum block number before this chunk
max_prev_block AS (
    SELECT max(block_number) AS block_number
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    HAVING block_number < {{ .bounds.start }}
),
-- Get the last known cumulative state before this chunk
-- Returns all records with the max block number (may have multiple state_roots due to reorgs)
prev_state AS (
    SELECT
        block_number,
        state_root,
        accounts,
        account_bytes,
        account_trienodes,
        account_trienode_bytes,
        contract_codes,
        contract_code_bytes,
        storages,
        storage_bytes,
        storage_trienodes,
        storage_trienode_bytes
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE block_number = (SELECT block_number FROM max_prev_block)
),
-- Get all delta records for this block range
all_deltas AS (
    SELECT
        block_number,
        state_root,
        parent_state_root,
        account_delta,
        account_bytes_delta,
        account_trienode_delta,
        account_trienode_bytes_delta,
        contract_code_delta,
        contract_code_bytes_delta,
        storage_delta,
        storage_bytes_delta,
        storage_trienode_delta,
        storage_trienode_bytes_delta
    FROM {{ index .dep "{{external}}" "execution_state_size_delta" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Build valid state roots: start with prev_state roots (or genesis), then collect all reachable state_roots
valid_state_roots AS (
    -- Previous state roots (or genesis empty state root if no prev_state)
    SELECT state_root FROM prev_state
    UNION ALL
    SELECT '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421' AS state_root
    UNION ALL
    -- All state_roots from deltas in this range (these are valid parents for subsequent blocks)
    SELECT state_root FROM all_deltas
),
-- Filter to canonical chain: each block's parent_state_root must exist in valid_state_roots
canonical_deltas AS (
    SELECT d.*
    FROM all_deltas d
    WHERE d.parent_state_root IN (SELECT state_root FROM valid_state_roots)
),
-- Get the first canonical delta's parent_state_root to find the matching prev_state
first_parent_state_root AS (
    SELECT parent_state_root
    FROM canonical_deltas
    WHERE block_number = {{ .bounds.start }}
    LIMIT 1
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    d.block_number,
    d.state_root,
    d.parent_state_root,
    -- Account metrics: previous state + running sum of deltas
    toUInt64(COALESCE((SELECT accounts FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.account_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS accounts,
    toUInt64(COALESCE((SELECT account_bytes FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.account_bytes_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS account_bytes,
    toUInt64(COALESCE((SELECT account_trienodes FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.account_trienode_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS account_trienodes,
    toUInt64(COALESCE((SELECT account_trienode_bytes FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.account_trienode_bytes_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS account_trienode_bytes,
    -- Contract code metrics
    toUInt64(COALESCE((SELECT contract_codes FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.contract_code_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS contract_codes,
    toUInt64(COALESCE((SELECT contract_code_bytes FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.contract_code_bytes_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS contract_code_bytes,
    -- Storage metrics
    toUInt64(COALESCE((SELECT storages FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.storage_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS storages,
    toUInt64(COALESCE((SELECT storage_bytes FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.storage_bytes_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS storage_bytes,
    toUInt64(COALESCE((SELECT storage_trienodes FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.storage_trienode_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS storage_trienodes,
    toUInt64(COALESCE((SELECT storage_trienode_bytes FROM prev_state WHERE state_root = (SELECT parent_state_root FROM first_parent_state_root)), 0)
        + SUM(d.storage_trienode_bytes_delta) OVER (ORDER BY d.block_number ROWS UNBOUNDED PRECEDING)) AS storage_trienode_bytes
FROM canonical_deltas d
ORDER BY d.block_number
SETTINGS max_threads = 4;
