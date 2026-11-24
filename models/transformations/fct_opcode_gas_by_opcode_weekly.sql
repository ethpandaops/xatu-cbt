---
table: fct_opcode_gas_by_opcode_weekly
type: incremental
interval:
  type: block
  max: 50400
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5m"
tags:
  - weekly
  - execution
  - opcode
  - gas
dependencies:
  - "{{transformation}}.fct_opcode_gas_by_opcode_daily"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Weekly (Monday to Sunday, UTC) aggregation of gas consumption per EVM opcode, summed from the daily table.
--
-- Expands the block range to complete week boundaries, so partial weeks at either edge of an interval are
-- recomputed in full from the daily rows whenever an interval touches them. The ReplacingMergeTree keeps the
-- latest row. Blocks are counted per day, and a block belongs to one day, so the daily block counts add up.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    week_bounds AS (
        SELECT
            toMonday(min(block_date_time)) AS min_week,
            toMonday(max(block_date_time)) AS max_week
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    ),
    per_day AS (
        SELECT
            day_start_date,
            opcode,
            block_count AS d_block_count,
            total_count AS d_total_count,
            total_gas AS d_total_gas,
            total_error_count AS d_total_error_count
        FROM {{ index .dep "{{transformation}}" "fct_opcode_gas_by_opcode_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_week FROM week_bounds)
          AND day_start_date < (SELECT max_week FROM week_bounds) + INTERVAL 7 DAY
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toMonday(day_start_date) AS week_start_date,
    opcode,
    toUInt32(sum(d_block_count)) AS block_count,
    toUInt64(sum(d_total_count)) AS total_count,
    toUInt64(sum(d_total_gas)) AS total_gas,
    toUInt64(sum(d_total_error_count)) AS total_error_count,
    round(if(sum(d_block_count) > 0, sum(d_total_count) / sum(d_block_count), 0), 4) AS avg_count_per_block,
    round(if(sum(d_block_count) > 0, sum(d_total_gas) / sum(d_block_count), 0), 4) AS avg_gas_per_block,
    round(if(sum(d_total_count) > 0, sum(d_total_gas) / sum(d_total_count), 0), 4) AS avg_gas_per_execution
FROM per_day
GROUP BY week_start_date, opcode
-- Merge sums across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0
