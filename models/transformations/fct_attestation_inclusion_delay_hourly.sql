---
table: fct_attestation_inclusion_delay_hourly
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
  - attestation
dependencies:
  - "{{transformation}}.fct_attestation_correctness_by_validator_canonical"
---
-- Hourly aggregation of attestation inclusion delay.
-- First aggregates per-validator data to per-slot averages, then computes hourly
-- stats across slots. This prevents individual validator outliers from dominating.
-- Computes percentiles, Bollinger bands, and 6-hour moving averages.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_by_validator_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND status = 'canonical'
          AND inclusion_distance IS NOT NULL
    ),
    -- Aggregate per-validator data to per-slot averages first
    slot_averages AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            avg(inclusion_distance) AS avg_inclusion_delay
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_by_validator_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND status = 'canonical'
          AND inclusion_distance IS NOT NULL
        GROUP BY slot, slot_start_date_time
    ),
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            avg_inclusion_delay,
            avg(avg_inclusion_delay) OVER (ORDER BY slot_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_inclusion_delay
        FROM slot_averages
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    count() AS slot_count,
    round(avg(avg_inclusion_delay), 4) AS avg_inclusion_delay,
    round(min(avg_inclusion_delay), 4) AS min_inclusion_delay,
    round(max(avg_inclusion_delay), 4) AS max_inclusion_delay,
    round(quantile(0.05)(avg_inclusion_delay), 4) AS p05_inclusion_delay,
    round(quantile(0.50)(avg_inclusion_delay), 4) AS p50_inclusion_delay,
    round(quantile(0.95)(avg_inclusion_delay), 4) AS p95_inclusion_delay,
    round(stddevPop(avg_inclusion_delay), 4) AS stddev_inclusion_delay,
    round(avg(avg_inclusion_delay) + 2 * stddevPop(avg_inclusion_delay), 4) AS upper_band_inclusion_delay,
    round(avg(avg_inclusion_delay) - 2 * stddevPop(avg_inclusion_delay), 4) AS lower_band_inclusion_delay,
    round(avg(ma_inclusion_delay), 4) AS moving_avg_inclusion_delay
FROM slots_with_ma
GROUP BY toStartOfHour(slot_start_date_time)
SETTINGS join_use_nulls = 1
