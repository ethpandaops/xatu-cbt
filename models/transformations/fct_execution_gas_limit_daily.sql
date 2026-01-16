---
table: fct_execution_gas_limit_daily
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
-- Daily aggregation of execution layer gas limit.
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
            COALESCE(eb.gas_limit, 0) AS gas_limit
        FROM expanded_blocks b
        GLOBAL INNER JOIN (
            SELECT block_number, gas_limit
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
            gas_limit,
            -- 7-day (604800 seconds) moving average of gas_limit
            avg(gas_limit) OVER (ORDER BY block_timestamp RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS ma_gas_limit
        FROM blocks_in_days
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(block_date_time) AS day_start_date,
    count() AS block_count,
    sum(gas_limit) AS total_gas_limit,
    -- Gas limit stats (rounded to integers)
    toUInt64(round(avg(gas_limit))) AS avg_gas_limit,
    min(gas_limit) AS min_gas_limit,
    max(gas_limit) AS max_gas_limit,
    toUInt64(round(quantile(0.05)(gas_limit))) AS p05_gas_limit,
    toUInt64(round(quantile(0.50)(gas_limit))) AS p50_gas_limit,
    toUInt64(round(quantile(0.95)(gas_limit))) AS p95_gas_limit,
    toUInt64(round(stddevPop(gas_limit))) AS stddev_gas_limit,
    -- Bollinger bands (avg +/- 2 standard deviations)
    toUInt64(round(avg(gas_limit) + 2 * stddevPop(gas_limit))) AS upper_band_gas_limit,
    toInt64(round(avg(gas_limit) - 2 * stddevPop(gas_limit))) AS lower_band_gas_limit,
    -- 7-day moving average
    toUInt64(round(avg(ma_gas_limit))) AS moving_avg_gas_limit
FROM blocks_with_ma
GROUP BY toDate(block_date_time)
