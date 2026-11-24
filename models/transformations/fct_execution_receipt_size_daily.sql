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
  - "{{external}}.canonical_execution_block"
  - "{{transformation}}.int_block_receipt_size"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Daily aggregation of receipt size statistics.
-- Computes per-block distributions, Bollinger bands, and a 7-day moving average.
-- The scan reaches back one window before the first day so the moving average covers the full 7 days.
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
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds) - INTERVAL 7 DAY
          AND toDate(block_date_time) < (SELECT max_day FROM day_bounds)
    ),
    -- Get all blocks within the expanded day boundaries (excluding partial current day)
    expanded_blocks AS (
        SELECT
            block_number,
            block_date_time,
            toUnixTimestamp(block_date_time) AS block_timestamp
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds) - INTERVAL 7 DAY
          AND toDate(block_date_time) < (SELECT max_day FROM day_bounds)
    ),
    -- First and last block with receipt data in each day
    day_coverage AS (
        SELECT
            toDate(b.block_date_time) AS day,
            min(b.block_number) AS first_block,
            max(b.block_number) AS last_block
        FROM expanded_blocks b
        GLOBAL INNER JOIN (
            SELECT block_number
            FROM {{ index .dep "{{transformation}}" "int_block_receipt_size" "helpers" "from" }} FINAL
            WHERE block_number >= (SELECT min_block FROM block_range)
              AND block_number <= (SELECT max_block FROM block_range)
        ) r ON b.block_number = r.block_number
        GROUP BY day
    ),
    -- Empty blocks (gas_used = 0 means no transactions, so no receipts) between the
    -- first and last block with receipt data in their day
    empty_blocks AS (
        SELECT b.block_number
        FROM expanded_blocks b
        GLOBAL INNER JOIN day_coverage c ON toDate(b.block_date_time) = c.day
        WHERE b.block_number BETWEEN c.first_block AND c.last_block
          AND b.block_number GLOBAL IN (
                SELECT block_number
                FROM {{ index .dep "{{external}}" "canonical_execution_block" "helpers" "from" }} FINAL
                WHERE block_number >= (SELECT min_block FROM block_range)
                  AND block_number <= (SELECT max_block FROM block_range)
                  AND meta_network_name = '{{ .env.NETWORK }}'
                  AND gas_used = 0
          )
    ),
    -- Join blocks with receipt stats from int_block_receipt_size. A block missing
    -- from it is a source gap, not a zero, so it is skipped unless it is an empty
    -- block within its day's receipt data. Days without receipt data produce no row.
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
        GLOBAL LEFT JOIN (
            SELECT *
            FROM {{ index .dep "{{transformation}}" "int_block_receipt_size" "helpers" "from" }} FINAL
            WHERE block_number >= (SELECT min_block FROM block_range)
              AND block_number <= (SELECT max_block FROM block_range)
        ) r ON b.block_number = r.block_number
        WHERE r.transaction_count > 0
           OR b.block_number GLOBAL IN (SELECT block_number FROM empty_blocks)
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
        WHERE toDate(block_date_time) >= (SELECT min_day FROM day_bounds)
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
