---
table: fct_blob_count_by_daily
type: incremental
interval:
  type: slot
  max: 604800
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - consensus
  - blob
dependencies:
  - "{{transformation}}.fct_block_blob_count"
---
-- Daily aggregation of blob counts per slot.
-- Computes percentiles, Bollinger bands, and 7-day moving averages across all canonical slots in each day.
--
-- This query expands the slot range to complete day boundaries to handle partial
-- day aggregations at the head of incremental processing. For example, if we process
-- slots spanning 11:46-12:30 on day N, we expand to include ALL slots from day N
-- so that the day gets re-aggregated with complete data as more slots arrive.
-- The ReplacingMergeTree will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the day boundaries for the current slot range
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_block_blob_count" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND status = 'canonical'
    ),
    -- Find ALL canonical slots that fall within those day boundaries
    slots_in_days AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            blob_count
        FROM {{ index .dep "{{transformation}}" "fct_block_blob_count" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
          AND status = 'canonical'
    ),
    -- Calculate 7-day (604800 seconds) moving average of blob count for each slot
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            blob_count,
            avg(blob_count) OVER (ORDER BY slot_timestamp RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS ma_blob_count
        FROM slots_in_days
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
    count() AS block_count,
    sum(blob_count) AS total_blobs,
    -- Blob count stats
    round(avg(blob_count), 4) AS avg_blob_count,
    min(blob_count) AS min_blob_count,
    max(blob_count) AS max_blob_count,
    round(quantile(0.05)(blob_count), 4) AS p05_blob_count,
    round(quantile(0.50)(blob_count), 4) AS p50_blob_count,
    round(quantile(0.95)(blob_count), 4) AS p95_blob_count,
    round(stddevPop(blob_count), 4) AS stddev_blob_count,
    -- Bollinger bands (avg +/- 2 standard deviations)
    round(avg(blob_count) + 2 * stddevPop(blob_count), 4) AS upper_band_blob_count,
    round(avg(blob_count) - 2 * stddevPop(blob_count), 4) AS lower_band_blob_count,
    -- 7-day moving average
    round(avg(ma_blob_count), 4) AS moving_avg_blob_count
FROM slots_with_ma
GROUP BY toDate(slot_start_date_time)
