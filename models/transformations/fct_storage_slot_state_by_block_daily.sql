---
table: fct_storage_slot_state_by_block_daily
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
  - daily
  - execution
  - storage
dependencies:
  - "{{transformation}}.int_storage_slot_state_by_block"
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
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    active_slots,
    effective_bytes
FROM (
    SELECT
        toDate(block_date_time) AS day_start_date,
        argMax(active_slots, block_number) AS active_slots,
        argMax(effective_bytes, block_number) AS effective_bytes
    FROM (
        SELECT
            s.block_number,
            s.active_slots,
            s.effective_bytes,
            b.block_date_time
        FROM {{ index .dep "{{transformation}}" "int_storage_slot_state_by_block" "helpers" "from" }} AS s FINAL
        GLOBAL INNER JOIN blocks_in_days AS b ON s.block_number = b.block_number
    )
    GROUP BY day_start_date
)
