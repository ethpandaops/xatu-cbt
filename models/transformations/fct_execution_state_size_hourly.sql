---
table: fct_execution_state_size_hourly
type: incremental
interval:
  type: block
  max: 10000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - hourly
  - execution
  - state_size
dependencies:
  - "{{external}}.execution_state_size"
  - "{{external}}.canonical_execution_block"
---
-- This query expands the block range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. For example, if we process
-- blocks 2001-3000 spanning 11:46-12:30, we expand to include ALL blocks from 11:00-12:59
-- so that hour 11:00 (which was partial in the previous run) gets re-aggregated with
-- complete data. The ReplacingMergeTree will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries for the current block range
    hour_bounds AS (
        SELECT
            toStartOfHour(min(block_date_time)) AS min_hour,
            toStartOfHour(max(block_date_time)) AS max_hour
        FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),
    -- Find ALL blocks that fall within those hour boundaries
    blocks_in_hours AS (
        SELECT
            block_number,
            block_date_time
        FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds)
          AND block_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND meta_network_name = '{{ .env.NETWORK }}'
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    accounts,
    account_bytes,
    account_trienodes,
    account_trienode_bytes,
    contract_codes,
    contract_code_bytes,
    storages,
    storage_bytes,
    storage_trienodes,
    storage_trienode_bytes,
    account_trienode_bytes + contract_code_bytes + storage_trienode_bytes AS total_bytes
FROM (
    SELECT
        toStartOfHour(block_date_time) AS hour_start_date_time,

        argMax(accounts, block_number) AS accounts,
        argMax(account_bytes, block_number) AS account_bytes,
        argMax(account_trienodes, block_number) AS account_trienodes,
        argMax(account_trienode_bytes, block_number) AS account_trienode_bytes,

        argMax(contract_codes, block_number) AS contract_codes,
        argMax(contract_code_bytes, block_number) AS contract_code_bytes,

        argMax(storages, block_number) AS storages,
        argMax(storage_bytes, block_number) AS storage_bytes,
        argMax(storage_trienodes, block_number) AS storage_trienodes,
        argMax(storage_trienode_bytes, block_number) AS storage_trienode_bytes
    FROM (
        SELECT
            s.block_number,
            s.accounts,
            s.account_bytes,
            s.account_trienodes,
            s.account_trienode_bytes,
            s.contract_codes,
            s.contract_code_bytes,
            s.storages,
            s.storage_bytes,
            s.storage_trienodes,
            s.storage_trienode_bytes,
            b.block_date_time
        FROM {{ index .dep "{{external}}" "execution_state_size" "helpers" "from" }} AS s FINAL
        GLOBAL INNER JOIN blocks_in_hours AS b ON s.block_number = b.block_number
    )
    GROUP BY hour_start_date_time
)
