---
table: fct_execution_receipt_size_daily
type: incremental
interval:
  type: block
  max: 100000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  backfill: "@every 1h"
  forwardfill: "@every 1h"
tags:
  - daily
  - execution
  - receipt
  - size
dependencies:
  - "{{transformation}}.int_block_receipt_size"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Daily aggregation of receipt size statistics.
-- Computes per-block distributions, Bollinger bands, and a 7-day moving average.
-- Per-transaction percentile fields are approximations derived from block-level
-- percentile summaries (not recomputed from raw transactions here).
--
-- This query expands block bounds to full day boundaries and excludes the partial
-- current day to keep daily cumulative series stable. ReplacingMergeTree keeps
-- the latest row.
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
    -- Get the actual block number range for the expanded day boundaries (excluding partial current day)
    block_range AS (
        SELECT
            min(block_number) AS min_block,
            max(block_number) AS max_block
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(block_date_time) < (SELECT max_day FROM day_bounds)
    ),
    -- Get all blocks within the expanded day boundaries (excluding partial current day)
    expanded_blocks AS (
        SELECT
            block_number,
            block_date_time,
            toUnixTimestamp(block_date_time) AS block_timestamp
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(block_date_time) < (SELECT max_day FROM day_bounds)
    ),
    -- Join blocks with receipt stats from int_block_receipt_size
    blocks_in_days AS (
        SELECT
            b.block_number,
            b.block_date_time,
            b.block_timestamp,
            COALESCE(r.receipt_bytes, 0) AS block_receipt_bytes,
            COALESCE(r.transaction_count, 0) AS block_tx_count,
            COALESCE(r.log_count, 0) AS block_log_count,
            r.avg_receipt_bytes_per_transaction,
            r.p50_receipt_bytes_per_transaction,
            r.p95_receipt_bytes_per_transaction
        FROM expanded_blocks b
        LEFT JOIN (
            SELECT *
            FROM {{ index .dep "{{transformation}}" "int_block_receipt_size" "helpers" "from" }} FINAL
            WHERE block_number >= (SELECT min_block FROM block_range)
              AND block_number <= (SELECT max_block FROM block_range)
        ) r ON b.block_number = r.block_number
    ),
    -- Calculate 7-day moving average for each block
    blocks_with_ma AS (
        SELECT
            block_number,
            block_date_time,
            block_receipt_bytes,
            block_tx_count,
            block_log_count,
            avg_receipt_bytes_per_transaction,
            p50_receipt_bytes_per_transaction,
            p95_receipt_bytes_per_transaction,
            -- 7-day (604800 seconds) moving average
            avg(block_receipt_bytes) OVER (
                ORDER BY block_timestamp
                RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
            ) AS ma_receipt_bytes
        FROM blocks_in_days
    ),
    -- Aggregate stats per day
    daily_stats AS (
        SELECT
            toDate(block_date_time) AS day,
            toUInt32(count()) AS block_count,
            toUInt64(sum(block_tx_count)) AS transaction_count,
            sum(block_receipt_bytes) AS total_receipt_bytes,
            round(avg(block_receipt_bytes), 4) AS avg_receipt_bytes_per_block,
            min(block_receipt_bytes) AS min_receipt_bytes_per_block,
            max(block_receipt_bytes) AS max_receipt_bytes_per_block,
            toUInt64(round(quantile(0.05)(block_receipt_bytes))) AS p05_receipt_bytes_per_block,
            toUInt64(round(quantile(0.50)(block_receipt_bytes))) AS p50_receipt_bytes_per_block,
            toUInt64(round(quantile(0.95)(block_receipt_bytes))) AS p95_receipt_bytes_per_block,
            round(stddevPop(block_receipt_bytes), 4) AS stddev_receipt_bytes_per_block,
            round(avg(block_receipt_bytes) + 2 * stddevPop(block_receipt_bytes), 4) AS upper_band_receipt_bytes_per_block,
            round(avg(block_receipt_bytes) - 2 * stddevPop(block_receipt_bytes), 4) AS lower_band_receipt_bytes_per_block,
            round(avg(ma_receipt_bytes), 4) AS moving_avg_receipt_bytes_per_block,
            -- Per-transaction stats: weighted average across blocks
            round(if(sum(block_tx_count) > 0,
                sum(avg_receipt_bytes_per_transaction * block_tx_count) / sum(block_tx_count),
                0), 4) AS avg_receipt_bytes_per_transaction,
            toUInt64(round(quantile(0.50)(p50_receipt_bytes_per_transaction))) AS p50_receipt_bytes_per_transaction,
            toUInt64(round(quantile(0.95)(p95_receipt_bytes_per_transaction))) AS p95_receipt_bytes_per_transaction,
            toUInt64(sum(block_log_count)) AS total_log_count,
            round(if(sum(block_tx_count) > 0, sum(block_log_count) / sum(block_tx_count), 0), 4) AS avg_log_count_per_transaction
        FROM blocks_with_ma
        GROUP BY day
    ),
    -- Get previous cumulative total
    prev_cumulative AS (
        SELECT COALESCE(max(cumulative_receipt_bytes), 0) AS prev_total
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE day_start_date < (SELECT min(day) FROM daily_stats)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day AS day_start_date,
    block_count,
    transaction_count,
    total_receipt_bytes,
    avg_receipt_bytes_per_block,
    min_receipt_bytes_per_block,
    max_receipt_bytes_per_block,
    p05_receipt_bytes_per_block,
    p50_receipt_bytes_per_block,
    p95_receipt_bytes_per_block,
    stddev_receipt_bytes_per_block,
    upper_band_receipt_bytes_per_block,
    lower_band_receipt_bytes_per_block,
    moving_avg_receipt_bytes_per_block,
    avg_receipt_bytes_per_transaction,
    p50_receipt_bytes_per_transaction,
    p95_receipt_bytes_per_transaction,
    total_log_count,
    avg_log_count_per_transaction,
    (SELECT prev_total FROM prev_cumulative)
        + sum(total_receipt_bytes) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING) AS cumulative_receipt_bytes
FROM daily_stats
ORDER BY day
