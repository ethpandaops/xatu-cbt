---
table: fct_execution_gas_used_daily
type: incremental
interval:
  type: block
  max: 10000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5s"
tags:
  - daily
  - execution
  - gas
dependencies:
  - "{{external}}.canonical_execution_block"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Daily aggregation of execution layer gas used.
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
    -- Get the actual block number range for the expanded day boundaries
    block_range AS (
        SELECT
            min(block_number) AS min_block,
            max(block_number) AS max_block
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(block_date_time) <= (SELECT max_day FROM day_bounds)
    ),
    -- Get all blocks within the expanded day boundaries
    expanded_blocks AS (
        SELECT
            block_number,
            block_date_time,
            toUnixTimestamp(block_date_time) AS block_timestamp
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(block_date_time) <= (SELECT max_day FROM day_bounds)
    ),
    -- Join with external table using computed block range for efficient filtering
    blocks_in_days AS (
        SELECT
            b.block_number,
            b.block_date_time,
            b.block_timestamp,
            COALESCE(eb.gas_used, 0) AS gas_used
        FROM expanded_blocks b
        GLOBAL INNER JOIN (
            SELECT block_number, gas_used
            FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
            WHERE block_number >= (SELECT min_block FROM block_range)
              AND block_number <= (SELECT max_block FROM block_range)
        ) eb ON b.block_number = eb.block_number
    ),
    -- Calculate 7-day moving average for each block
    blocks_with_ma AS (
        SELECT
            block_number,
            block_date_time,
            gas_used,
            -- 7-day (604800 seconds) moving average of gas_used
            avg(gas_used) OVER (ORDER BY block_timestamp RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS ma_gas_used
        FROM blocks_in_days
    ),
    -- Aggregate stats per day
    daily_stats AS (
        SELECT
            toDate(block_date_time) AS day,
            count() AS block_count,
            sum(gas_used) AS total_gas_used,
            toUInt64(round(avg(gas_used))) AS avg_gas_used,
            min(gas_used) AS min_gas_used,
            max(gas_used) AS max_gas_used,
            toUInt64(round(quantile(0.05)(gas_used))) AS p05_gas_used,
            toUInt64(round(quantile(0.50)(gas_used))) AS p50_gas_used,
            toUInt64(round(quantile(0.95)(gas_used))) AS p95_gas_used,
            toUInt64(round(stddevPop(gas_used))) AS stddev_gas_used,
            toUInt64(round(avg(gas_used) + 2 * stddevPop(gas_used))) AS upper_band_gas_used,
            toInt64(round(avg(gas_used) - 2 * stddevPop(gas_used))) AS lower_band_gas_used,
            toUInt64(round(avg(ma_gas_used))) AS moving_avg_gas_used
        FROM blocks_with_ma
        GROUP BY day
    ),
    -- Get previous cumulative total
    prev_cumulative AS (
        SELECT COALESCE(max(cumulative_gas_used), 0) AS prev_total
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE day_start_date < (SELECT min(day) FROM daily_stats)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day AS day_start_date,
    block_count,
    total_gas_used,
    (SELECT prev_total FROM prev_cumulative)
        + sum(total_gas_used) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING) AS cumulative_gas_used,
    avg_gas_used,
    min_gas_used,
    max_gas_used,
    p05_gas_used,
    p50_gas_used,
    p95_gas_used,
    stddev_gas_used,
    upper_band_gas_used,
    lower_band_gas_used,
    moving_avg_gas_used
FROM daily_stats
ORDER BY day
