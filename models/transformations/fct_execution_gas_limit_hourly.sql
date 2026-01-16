---
table: fct_execution_gas_limit_hourly
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
  - hourly
  - execution
  - gas
dependencies:
  - "{{external}}.canonical_execution_block"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Hourly aggregation of execution layer gas limit.
-- Computes percentiles, Bollinger bands, and 6-hour moving averages across all blocks in each hour.
--
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
    -- Get the actual block number range for the expanded hour boundaries
    block_range AS (
        SELECT
            min(block_number) AS min_block,
            max(block_number) AS max_block
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds)
          AND block_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    -- Get all blocks within the expanded hour boundaries
    expanded_blocks AS (
        SELECT
            block_number,
            block_date_time,
            toUnixTimestamp(block_date_time) AS block_timestamp
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds)
          AND block_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    -- Join with external table using computed block range for efficient filtering
    blocks_in_hours AS (
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
    -- Calculate 6-hour moving average for each block
    blocks_with_ma AS (
        SELECT
            block_number,
            block_date_time,
            gas_limit,
            -- 6-hour (21600 seconds) moving average of gas_limit
            avg(gas_limit) OVER (ORDER BY block_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_gas_limit
        FROM blocks_in_hours
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(block_date_time) AS hour_start_date_time,
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
    -- 6-hour moving average
    toUInt64(round(avg(ma_gas_limit))) AS moving_avg_gas_limit
FROM blocks_with_ma
GROUP BY toStartOfHour(block_date_time)
