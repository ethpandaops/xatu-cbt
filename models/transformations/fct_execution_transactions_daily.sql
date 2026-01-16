---
table: fct_execution_transactions_daily
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
  - transactions
  - cumulative
dependencies:
  - "{{external}}.canonical_execution_transaction"
  - "{{transformation}}.int_execution_block_by_date"
---
-- Daily aggregation of execution layer transaction counts with cumulative totals.
-- Tracks per-period stats (avg/min/max/percentiles per block) and running total since genesis.
--
-- This query expands the block range to complete day boundaries and EXCLUDES the partial
-- current day to ensure cumulative accuracy. The ReplacingMergeTree will merge duplicates
-- keeping the latest row.
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
    -- Count transactions per block using block range for efficient filtering
    tx_per_block AS (
        SELECT
            block_number,
            toUInt32(count()) AS txn_count
        FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }} FINAL
        WHERE block_number >= (SELECT min_block FROM block_range)
          AND block_number <= (SELECT max_block FROM block_range)
        GROUP BY block_number
    ),
    -- Join blocks with transaction counts
    blocks_in_days AS (
        SELECT
            b.block_number,
            b.block_date_time,
            b.block_timestamp,
            COALESCE(t.txn_count, 0) AS txn_count
        FROM expanded_blocks b
        LEFT JOIN tx_per_block t ON b.block_number = t.block_number
    ),
    -- Calculate 7-day moving average for each block
    blocks_with_ma AS (
        SELECT
            block_number,
            block_date_time,
            txn_count,
            -- 7-day (604800 seconds) moving average
            avg(txn_count) OVER (
                ORDER BY block_timestamp
                RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
            ) AS ma_txn
        FROM blocks_in_days
    ),
    -- Aggregate stats per day
    daily_stats AS (
        SELECT
            toDate(block_date_time) AS day,
            toUInt32(count()) AS block_count,
            toUInt64(sum(txn_count)) AS total_transactions,
            round(avg(txn_count), 4) AS avg_txn_per_block,
            min(txn_count) AS min_txn_per_block,
            max(txn_count) AS max_txn_per_block,
            toUInt32(round(quantile(0.50)(txn_count))) AS p50_txn_per_block,
            toUInt32(round(quantile(0.95)(txn_count))) AS p95_txn_per_block,
            toUInt32(round(quantile(0.05)(txn_count))) AS p05_txn_per_block,
            round(stddevPop(txn_count), 4) AS stddev_txn_per_block,
            round(avg(txn_count) + 2 * stddevPop(txn_count), 4) AS upper_band_txn_per_block,
            round(avg(txn_count) - 2 * stddevPop(txn_count), 4) AS lower_band_txn_per_block,
            round(avg(ma_txn), 4) AS moving_avg_txn_per_block
        FROM blocks_with_ma
        GROUP BY day
    ),
    -- Get previous cumulative total
    prev_cumulative AS (
        SELECT COALESCE(max(cumulative_transactions), 0) AS prev_total
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE day_start_date < (SELECT min(day) FROM daily_stats)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day AS day_start_date,
    block_count,
    total_transactions,
    (SELECT prev_total FROM prev_cumulative)
        + sum(total_transactions) OVER (ORDER BY day ROWS UNBOUNDED PRECEDING) AS cumulative_transactions,
    avg_txn_per_block,
    min_txn_per_block,
    max_txn_per_block,
    p50_txn_per_block,
    p95_txn_per_block,
    p05_txn_per_block,
    stddev_txn_per_block,
    upper_band_txn_per_block,
    lower_band_txn_per_block,
    moving_avg_txn_per_block
FROM daily_stats
ORDER BY day
