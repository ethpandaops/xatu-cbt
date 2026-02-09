---
table: fct_attestation_participation_rate_daily
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
  - attestation
dependencies:
  - "{{transformation}}.fct_attestation_correctness_canonical"
---
-- Daily aggregation of attestation participation rate per slot.
-- Computes percentiles, Bollinger bands, and 7-day moving averages across all canonical slots in each day.
-- Participation rate = (votes_head + votes_other) / votes_max * 100.
--
-- This query expands the slot range to complete day boundaries to handle partial
-- day aggregations at the head of incremental processing.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the day boundaries for the current slot range
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND votes_max > 0
    ),
    -- Find ALL slots that fall within those day boundaries, compute participation rate
    slots_in_days AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            (COALESCE(votes_head, 0) + COALESCE(votes_other, 0)) / votes_max * 100 AS participation_rate
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_canonical" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
          AND votes_max > 0
    ),
    -- Calculate 7-day (604800 seconds) moving average of participation rate for each slot
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            participation_rate,
            avg(participation_rate) OVER (ORDER BY slot_timestamp RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS ma_participation_rate
        FROM slots_in_days
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
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
    -- 7-day moving average
    round(avg(ma_participation_rate), 4) AS moving_avg_participation_rate
FROM slots_with_ma
GROUP BY toDate(slot_start_date_time)
SETTINGS join_use_nulls = 1
