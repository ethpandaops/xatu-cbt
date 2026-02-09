---
table: fct_blob_count_by_hourly
type: incremental
interval:
  type: slot
  max: 25200
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - consensus
  - blob
dependencies:
  - "{{transformation}}.fct_block_blob_count"
---
-- Hourly aggregation of blob counts per slot.
-- Computes percentiles, Bollinger bands, and 6-hour moving averages across all canonical slots in each hour.
--
-- This query expands the slot range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. For example, if we process
-- slots spanning 11:46-12:30, we expand to include ALL slots from 11:00-12:59
-- so that hour 11:00 (which was partial in the previous run) gets re-aggregated with
-- complete data. The ReplacingMergeTree will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries for the current slot range
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_block_blob_count" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND status = 'canonical'
    ),
    -- Find ALL canonical slots that fall within those hour boundaries
    slots_in_hours AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            blob_count
        FROM {{ index .dep "{{transformation}}" "fct_block_blob_count" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND status = 'canonical'
    ),
    -- Calculate 6-hour (21600 seconds) moving average of blob count for each slot
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            blob_count,
            avg(blob_count) OVER (ORDER BY slot_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_blob_count
        FROM slots_in_hours
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
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
    -- 6-hour moving average
    round(avg(ma_blob_count), 4) AS moving_avg_blob_count
FROM slots_with_ma
GROUP BY toStartOfHour(slot_start_date_time)
