---
table: fct_opcode_gas_by_opcode_hourly
type: incremental
interval:
  type: block
  max: 100000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1m"
tags:
  - hourly
  - execution
  - opcode
  - gas
dependencies:
  - "{{transformation}}.int_block_opcode_gas"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Hourly aggregation of gas consumption per EVM opcode.
-- Powers the "Top Opcodes by Gas" chart showing which opcodes consume the most gas.
--
-- This query expands the block range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. The ReplacingMergeTree
-- will merge duplicates keeping the latest row.
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
    toStartOfHour(b.block_date_time) AS hour_start_date_time,
    o.opcode,
    count(DISTINCT o.block_number) AS block_count,
    sum(o.count) AS total_count,
    sum(o.gas) AS total_gas,
    sum(o.error_count) AS total_error_count,
    round(avg(o.count), 4) AS avg_count_per_block,
    round(avg(o.gas), 4) AS avg_gas_per_block,
    round(if(sum(o.count) > 0, sum(o.gas) / sum(o.count), 0), 4) AS avg_gas_per_execution
FROM (SELECT * FROM {{ index .dep "{{transformation}}" "int_block_opcode_gas" "helpers" "from" }} FINAL) AS o
INNER JOIN blocks_in_hours AS b ON o.block_number = b.block_number
GROUP BY
    toStartOfHour(b.block_date_time),
    o.opcode
