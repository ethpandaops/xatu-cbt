---
table: fct_contract_storage_state_with_expiry_by_block_hourly
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1m"
tags:
  - hourly
  - execution
  - storage
  - contract
dependencies:
  - "{{transformation}}.int_contract_storage_state_with_expiry_by_block"
  - "{{transformation}}.int_execution_block_by_date"
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
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    ),
    -- Find ALL blocks that fall within those hour boundaries
    blocks_in_hours AS (
        SELECT
            block_number,
            block_date_time
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds)
          AND block_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    expiry_policy,
    active_slots,
    effective_bytes,
    active_contracts
FROM (
    SELECT
        toStartOfHour(block_date_time) AS hour_start_date_time,
        expiry_policy,
        argMax(active_slots, block_number) AS active_slots,
        argMax(effective_bytes, block_number) AS effective_bytes,
        argMax(active_contracts, block_number) AS active_contracts
    FROM (
        SELECT
            s.block_number,
            s.expiry_policy,
            s.active_slots,
            s.effective_bytes,
            s.active_contracts,
            b.block_date_time
        FROM {{ index .dep "{{transformation}}" "int_contract_storage_state_with_expiry_by_block" "helpers" "from" }} AS s FINAL
        GLOBAL INNER JOIN blocks_in_hours AS b ON s.block_number = b.block_number
    )
    GROUP BY expiry_policy, hour_start_date_time
)
