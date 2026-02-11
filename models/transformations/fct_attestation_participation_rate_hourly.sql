---
table: fct_attestation_participation_rate_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - consensus
  - attestation
dependencies:
  - "{{transformation}}.fct_attestation_correctness_canonical"
---
-- Hourly aggregation of attestation participation rate per slot.
-- Computes percentiles, Bollinger bands, and 6-hour moving averages across all canonical slots in each hour.
-- Participation rate = (votes_head + votes_other) / votes_max * 100.
--
-- This query expands the slot range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries for the current slot range
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND votes_max > 0
    ),
    -- Find ALL slots that fall within those hour boundaries, compute participation rate
    slots_in_hours AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            (COALESCE(votes_head, 0) + COALESCE(votes_other, 0)) / votes_max * 100 AS participation_rate
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND votes_max > 0
    ),
    -- Calculate 6-hour (21600 seconds) moving average of participation rate for each slot
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            participation_rate,
            avg(participation_rate) OVER (ORDER BY slot_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_participation_rate
        FROM slots_in_hours
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    count() AS slot_count,
    -- Participation rate stats
    round(avg(participation_rate), 4) AS avg_participation_rate,
    round(min(participation_rate), 4) AS min_participation_rate,
    round(max(participation_rate), 4) AS max_participation_rate,
    round(quantile(0.05)(participation_rate), 4) AS p05_participation_rate,
    round(quantile(0.50)(participation_rate), 4) AS p50_participation_rate,
    round(quantile(0.95)(participation_rate), 4) AS p95_participation_rate,
    round(stddevPop(participation_rate), 4) AS stddev_participation_rate,
    -- Bollinger bands (avg +/- 2 standard deviations)
    round(avg(participation_rate) + 2 * stddevPop(participation_rate), 4) AS upper_band_participation_rate,
    round(avg(participation_rate) - 2 * stddevPop(participation_rate), 4) AS lower_band_participation_rate,
    -- 6-hour moving average
    round(avg(ma_participation_rate), 4) AS moving_avg_participation_rate
FROM slots_with_ma
GROUP BY toStartOfHour(slot_start_date_time)
SETTINGS join_use_nulls = 1
