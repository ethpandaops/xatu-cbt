---
table: fct_opcode_gas_by_opcode_daily
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1h"
tags:
  - daily
  - execution
  - opcode
  - gas
dependencies:
  - "{{transformation}}.int_block_opcode_gas"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Daily aggregation of gas consumption per EVM opcode.
-- Powers the "Top Opcodes by Gas" chart showing which opcodes consume the most gas.
--
-- This query expands the block range to complete day boundaries to handle partial
-- day aggregations at the head of incremental processing. The ReplacingMergeTree
-- will merge duplicates keeping the latest row.
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
    toDate(b.block_date_time) AS day_start_date,
    o.opcode,
    count(DISTINCT o.block_number) AS block_count,
    sum(o.count) AS total_count,
    sum(o.gas) AS total_gas,
    sum(o.error_count) AS total_error_count,
    round(avg(o.count), 4) AS avg_count_per_block,
    round(avg(o.gas), 4) AS avg_gas_per_block,
    round(if(sum(o.count) > 0, sum(o.gas) / sum(o.count), 0), 4) AS avg_gas_per_execution
FROM (SELECT * FROM {{ index .dep "{{transformation}}" "int_block_opcode_gas" "helpers" "from" }} FINAL) AS o
INNER JOIN blocks_in_days AS b ON o.block_number = b.block_number
GROUP BY
    toDate(b.block_date_time),
    o.opcode
