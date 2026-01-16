---
table: fct_execution_gas_used_hourly
type: incremental
interval:
  type: block
  max: 100000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5m"
tags:
  - hourly
  - execution
  - gas
dependencies:
  - "{{external}}.canonical_execution_block"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Hourly aggregation of execution layer gas used.
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
            COALESCE(eb.gas_used, 0) AS gas_used
        FROM expanded_blocks b
        GLOBAL INNER JOIN (
            SELECT block_number, gas_used
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
            gas_used,
            -- 6-hour (21600 seconds) moving average of gas_used
            avg(gas_used) OVER (ORDER BY block_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_gas_used
        FROM blocks_in_hours
    ),
    -- Aggregate stats per hour
    hourly_stats AS (
        SELECT
            toStartOfHour(block_date_time) AS hour,
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
        GROUP BY hour
    ),
    -- Get previous cumulative total
    prev_cumulative AS (
        SELECT COALESCE(max(cumulative_gas_used), 0) AS prev_total
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE hour_start_date_time < (SELECT min(hour) FROM hourly_stats)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour AS hour_start_date_time,
    block_count,
    total_gas_used,
    (SELECT prev_total FROM prev_cumulative)
        + sum(total_gas_used) OVER (ORDER BY hour ROWS UNBOUNDED PRECEDING) AS cumulative_gas_used,
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
FROM hourly_stats
ORDER BY hour
