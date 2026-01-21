---
table: fct_execution_tps_hourly
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
  - tps
  - transactions
dependencies:
  - "{{external}}.canonical_execution_transaction"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Hourly aggregation of execution layer TPS (transactions per second).
-- Uses actual block time gaps instead of assumed 12-second slots for accurate TPS calculation.
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
    -- Find ALL blocks that fall within those hour boundaries
    blocks_in_hours AS (
        SELECT
            block_number,
            block_date_time,
            toUnixTimestamp(block_date_time) AS block_timestamp
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds)
          AND block_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
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
        FROM blocks_in_hours
    ),
    -- Count transactions per block from canonical_execution_transaction
    tx_per_block AS (
        SELECT
            block_number,
            count() AS tx_count
        FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }} FINAL
        WHERE block_number GLOBAL IN (SELECT block_number FROM blocks_in_hours)
        GROUP BY block_number
    ),
    -- Join blocks with transactions and calculate per-block TPS
    blocks_with_tps AS (
        SELECT
            b.block_number,
            b.block_date_time,
            b.block_timestamp,
            b.block_time_seconds,
            COALESCE(t.tx_count, 0) AS tx_count,
            -- TPS = tx_count / actual_time_seconds (avoid div by zero)
            if(b.block_time_seconds > 0,
               toFloat32(COALESCE(t.tx_count, 0)) / toFloat32(b.block_time_seconds),
               0) AS tps
        FROM blocks_with_time b
        LEFT JOIN tx_per_block t ON b.block_number = t.block_number
        WHERE b.block_time_seconds IS NOT NULL  -- Skip first block (no previous to calculate gap)
          AND b.block_time_seconds > 0          -- Skip zero-time blocks
    ),
    -- Calculate 5-minute moving average for each block
    blocks_with_ma AS (
        SELECT
            block_number,
            block_date_time,
            block_time_seconds,
            tx_count,
            tps,
            -- 6-hour (21600 seconds) moving average of TPS
            avg(tps) OVER (ORDER BY block_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_tps
        FROM blocks_with_tps
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(block_date_time) AS hour_start_date_time,
    count() AS block_count,
    sum(tx_count) AS total_transactions,
    sum(block_time_seconds) AS total_seconds,
    -- TPS stats using actual block time gaps
    round(avg(tps), 4) AS avg_tps,
    round(min(tps), 4) AS min_tps,
    round(max(tps), 4) AS max_tps,
    round(quantile(0.05)(tps), 4) AS p05_tps,
    round(quantile(0.50)(tps), 4) AS p50_tps,
    round(quantile(0.95)(tps), 4) AS p95_tps,
    round(stddevPop(tps), 4) AS stddev_tps,
    -- Bollinger bands (avg +/- 2 standard deviations)
    round(avg(tps) + 2 * stddevPop(tps), 4) AS upper_band_tps,
    round(avg(tps) - 2 * stddevPop(tps), 4) AS lower_band_tps,
    -- 6-hour moving average
    round(avg(ma_tps), 4) AS moving_avg_tps
FROM blocks_with_ma
GROUP BY toStartOfHour(block_date_time)
