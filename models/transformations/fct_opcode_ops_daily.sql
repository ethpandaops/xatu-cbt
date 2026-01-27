---
table: fct_opcode_ops_daily
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
  - daily
  - execution
  - opcode
  - gas
dependencies:
  - "{{transformation}}.int_block_opcode_gas"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Daily aggregation of opcode execution rate (ops/sec).
-- Uses actual block time gaps instead of assumed 12-second slots for accurate ops/sec calculation.
-- Computes percentiles, Bollinger bands, and 7-day moving averages across all blocks in each day.
--
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
            block_date_time,
            toUnixTimestamp(block_date_time) AS block_timestamp
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(block_date_time) <= (SELECT max_day FROM day_bounds)
    ),
    -- Calculate time since previous block for each block
    -- Note: lagInFrame with self-reference default makes first row return 0 (filtered out later)
    blocks_with_time AS (
        SELECT
            block_number,
            block_date_time,
            block_timestamp,
            dateDiff('second',
                lagInFrame(block_date_time, 1, block_date_time) OVER (ORDER BY block_number),
                block_date_time
            ) AS block_time_seconds
        FROM blocks_in_days
    ),
    -- Get opcode counts per block from int_block_opcode_gas
    opcode_per_block AS (
        SELECT
            block_number,
            sum(count) AS opcode_count,
            sum(gas) AS total_gas
        FROM {{ index .dep "{{transformation}}" "int_block_opcode_gas" "helpers" "from" }} FINAL
        WHERE block_number GLOBAL IN (SELECT block_number FROM blocks_in_days)
        GROUP BY block_number
    ),
    -- Join blocks with opcode counts and calculate per-block ops/sec
    blocks_with_ops AS (
        SELECT
            b.block_number,
            b.block_date_time,
            b.block_timestamp,
            b.block_time_seconds,
            COALESCE(o.opcode_count, 0) AS opcode_count,
            COALESCE(o.total_gas, 0) AS total_gas,
            -- ops/sec = opcode_count / actual_time_seconds (avoid div by zero)
            if(b.block_time_seconds > 0,
               toFloat32(COALESCE(o.opcode_count, 0)) / toFloat32(b.block_time_seconds),
               0) AS ops
        FROM blocks_with_time b
        LEFT JOIN opcode_per_block o ON b.block_number = o.block_number
        WHERE b.block_time_seconds IS NOT NULL  -- Skip first block (no previous to calculate gap)
          AND b.block_time_seconds > 0          -- Skip zero-time blocks
    ),
    -- Calculate 7-day moving average for each block
    blocks_with_ma AS (
        SELECT
            block_number,
            block_date_time,
            block_time_seconds,
            opcode_count,
            total_gas,
            ops,
            -- 7-day (604800 seconds) moving average of ops/sec
            avg(ops) OVER (ORDER BY block_timestamp RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS ma_ops
        FROM blocks_with_ops
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(block_date_time) AS day_start_date,
    count() AS block_count,
    sum(opcode_count) AS total_opcode_count,
    sum(total_gas) AS total_gas,
    sum(block_time_seconds) AS total_seconds,
    -- Ops/sec stats using actual block time gaps
    round(avg(ops), 4) AS avg_ops,
    round(min(ops), 4) AS min_ops,
    round(max(ops), 4) AS max_ops,
    round(quantile(0.05)(ops), 4) AS p05_ops,
    round(quantile(0.50)(ops), 4) AS p50_ops,
    round(quantile(0.95)(ops), 4) AS p95_ops,
    round(stddevPop(ops), 4) AS stddev_ops,
    -- Bollinger bands (avg +/- 2 standard deviations)
    round(avg(ops) + 2 * stddevPop(ops), 4) AS upper_band_ops,
    round(avg(ops) - 2 * stddevPop(ops), 4) AS lower_band_ops,
    -- 7-day moving average
    round(avg(ma_ops), 4) AS moving_avg_ops
FROM blocks_with_ma
GROUP BY toDate(block_date_time)
