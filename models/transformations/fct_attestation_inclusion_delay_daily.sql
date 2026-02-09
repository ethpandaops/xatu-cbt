---
table: fct_attestation_inclusion_delay_daily
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
  - "{{transformation}}.fct_attestation_correctness_by_validator_canonical"
---
-- Daily aggregation of attestation inclusion delay.
-- First aggregates per-validator data to per-slot averages, then computes daily
-- stats across slots. This prevents individual validator outliers from dominating.
-- Computes percentiles, Bollinger bands, and 7-day moving averages.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_by_validator_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND status = 'canonical'
          AND inclusion_distance IS NOT NULL
    ),
    slot_averages AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            avg(inclusion_distance) AS avg_inclusion_delay
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_by_validator_canonical" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
          AND status = 'canonical'
          AND inclusion_distance IS NOT NULL
        GROUP BY slot, slot_start_date_time
    ),
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            avg_inclusion_delay,
            avg(avg_inclusion_delay) OVER (ORDER BY slot_timestamp RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS ma_inclusion_delay
        FROM slot_averages
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
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
GROUP BY toDate(slot_start_date_time)
SETTINGS join_use_nulls = 1
