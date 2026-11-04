---
table: fct_execution_receipt_size_hourly
type: incremental
interval:
  type: block
  max: 100000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  backfill: "@every 5m"
  forwardfill: "@every 5m"
tags:
  - hourly
  - execution
  - receipt
  - size
dependencies:
  - "{{external}}.canonical_execution_block"
  - "{{transformation}}.int_block_receipt_size"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Hourly aggregation of receipt size statistics.
-- Computes per-block distributions, Bollinger bands, and a 6-hour moving average.
-- The scan reaches back one window before the first hour so the moving average covers the full 6 hours.
-- Per-transaction percentile fields are approximations derived from block-level
-- percentile summaries (not recomputed from raw transactions here).
--
-- This query expands block bounds to full hour boundaries so head updates can
-- recompute complete hour buckets. ReplacingMergeTree keeps the latest row.
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
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds) - INTERVAL 6 HOUR
          AND block_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    -- Get all blocks within the expanded hour boundaries
    expanded_blocks AS (
        SELECT
            block_number,
            block_date_time,
            toUnixTimestamp(block_date_time) AS block_timestamp
        FROM {{ index .dep "{{transformation}}" "int_execution_block_by_date" "helpers" "from" }} FINAL
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds) - INTERVAL 6 HOUR
          AND block_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    -- First and last block with receipt data in each hour
    hour_coverage AS (
        SELECT
            toStartOfHour(b.block_date_time) AS hour,
            min(b.block_number) AS first_block,
            max(b.block_number) AS last_block
        FROM expanded_blocks b
        GLOBAL INNER JOIN (
            SELECT block_number
            FROM {{ index .dep "{{transformation}}" "int_block_receipt_size" "helpers" "from" }} FINAL
            WHERE block_number >= (SELECT min_block FROM block_range)
              AND block_number <= (SELECT max_block FROM block_range)
        ) r ON b.block_number = r.block_number
        GROUP BY hour
    ),
    -- Empty blocks (gas_used = 0 means no transactions, so no receipts) between the
    -- first and last block with receipt data in their hour
    empty_blocks AS (
        SELECT b.block_number
        FROM expanded_blocks b
        GLOBAL INNER JOIN hour_coverage c ON toStartOfHour(b.block_date_time) = c.hour
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
    -- block within its hour's receipt data. Hours without receipt data produce no row.
    blocks_in_hours AS (
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
    -- Calculate 6-hour moving average for each block
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
            -- 6-hour (21600 seconds) moving average
            avg(block_receipt_bytes) OVER (
                ORDER BY block_timestamp
                RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW
            ) AS ma_receipt_bytes
        FROM blocks_in_hours
    ),
    -- Aggregate stats per hour
    hourly_stats AS (
        SELECT
            toStartOfHour(block_date_time) AS hour,
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
        WHERE block_date_time >= (SELECT min_hour FROM hour_bounds)
        GROUP BY hour
    ),
    -- Get previous cumulative total
    prev_cumulative AS (
        SELECT COALESCE(max(cumulative_receipt_bytes), 0) AS prev_total
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE hour_start_date_time < (SELECT min(hour) FROM hourly_stats)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour AS hour_start_date_time,
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
        + sum(total_receipt_bytes) OVER (ORDER BY hour ROWS UNBOUNDED PRECEDING) AS cumulative_receipt_bytes
FROM hourly_stats
ORDER BY hour
