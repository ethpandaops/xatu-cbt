---
table: fct_storage_slot_state_daily
type: incremental
interval:
  type: block
  max: 50000
schedules:
  forwardfill: "@every 2m"
  backfill: "@every 1m"
tags:
  - daily
  - execution
  - storage
dependencies:
  - "{{transformation}}.fct_storage_slot_state"
  - "{{transformation}}.fct_storage_slot_state_with_expiry_by_6m"
  - "{{transformation}}.int_execution_block_by_date"
---
-- This query expands the block range to complete day boundaries to handle partial
-- day aggregations at the head of incremental processing. For example, if we process
-- blocks spanning 11:46-12:30 on day N, we expand to include ALL blocks from day N
-- so that the day gets re-aggregated with complete data as more blocks arrive.
-- The ReplacingMergeTree will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the day boundaries for the current block range
    day_bounds AS (
        SELECT
            toDate(min(block_date_time)) AS min_day,
            toDate(max(block_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    ),
    -- Find ALL blocks that fall within those day boundaries
    blocks_in_days AS (
        SELECT
            block_number,
            block_date_time
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(block_date_time) <= (SELECT max_day FROM day_bounds)
    )
-- TODO: Add columns for 1-year expiry policy once fct_storage_slot_state_with_expiry_by_1y is available:
--   active_slots_with_1y_expiry, effective_bytes_with_1y_expiry
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    active_slots,
    effective_bytes,
    active_slots_with_six_months_expiry,
    effective_bytes_with_six_months_expiry
FROM (
    SELECT
        toDate(b.block_date_time) AS day_start_date,
        argMax(s.active_slots, s.block_number) AS active_slots,
        argMax(s.effective_bytes, s.block_number) AS effective_bytes,
        argMax(e.active_slots, e.block_number) AS active_slots_with_six_months_expiry,
        argMax(e.effective_bytes, e.block_number) AS effective_bytes_with_six_months_expiry
    FROM blocks_in_days AS b
    GLOBAL INNER JOIN {{ index .dep "{{transformation}}" "fct_storage_slot_state" "helpers" "from" }} AS s FINAL
        ON b.block_number = s.block_number
    GLOBAL INNER JOIN {{ index .dep "{{transformation}}" "fct_storage_slot_state_with_expiry_by_6m" "helpers" "from" }} AS e FINAL
        ON b.block_number = e.block_number
    GROUP BY day_start_date
)
