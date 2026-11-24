---
table: fct_attestation_inclusion_delay_hourly
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
  - "{{transformation}}.int_attestation_attested_canonical"
  - "{{transformation}}.int_block_proposer_canonical"
---
-- Hourly aggregation of attestation inclusion delay.
-- First aggregates per-validator data to per-slot averages, then computes hourly
-- stats across slots. This prevents individual validator outliers from dominating.
-- Computes percentiles, Bollinger bands, and 6-hour moving averages.
-- The scan reaches back one window before the first hour so the moving average covers the full 6 hours.
--
-- Reads the canonical attestations, which reach genesis. A vote counts when the block it is for is canonical and at
-- most 64 slots older than the vote, the votes fct_attestation_correctness_by_validator_canonical marks canonical.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "int_attestation_attested_canonical" "helpers" "from" }}
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    -- Canonical blocks from 64 slots before the scan below to its end
    canonical_blocks AS (
        SELECT
            slot AS block_slot,
            block_root
        FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds) - INTERVAL 6 HOUR - INTERVAL 768 SECOND
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND block_root IS NOT NULL
    ),
    -- Aggregate per-validator data to per-slot averages first
    slot_averages AS (
        SELECT
            a.slot AS slot,
            a.slot_start_date_time AS slot_start_date_time,
            toUnixTimestamp(a.slot_start_date_time) AS slot_timestamp,
            avg(a.inclusion_distance) AS slot_avg_delay
        FROM {{ index .dep "{{transformation}}" "int_attestation_attested_canonical" "helpers" "from" }} AS a FINAL
        GLOBAL INNER JOIN canonical_blocks AS b ON a.block_root = b.block_root
        WHERE a.slot_start_date_time >= (SELECT min_hour FROM hour_bounds) - INTERVAL 6 HOUR
          AND a.slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND a.slot >= b.block_slot
          AND a.slot - b.block_slot <= 64
        GROUP BY slot, slot_start_date_time
    ),
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            slot_avg_delay,
            avg(slot_avg_delay) OVER (ORDER BY slot_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_inclusion_delay
        FROM slot_averages
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    count() AS slot_count,
    round(avg(slot_avg_delay), 4) AS avg_inclusion_delay,
    round(min(slot_avg_delay), 4) AS min_inclusion_delay,
    round(max(slot_avg_delay), 4) AS max_inclusion_delay,
    round(quantile(0.05)(slot_avg_delay), 4) AS p05_inclusion_delay,
    round(quantile(0.50)(slot_avg_delay), 4) AS p50_inclusion_delay,
    round(quantile(0.95)(slot_avg_delay), 4) AS p95_inclusion_delay,
    round(stddevPop(slot_avg_delay), 4) AS stddev_inclusion_delay,
    round(avg(slot_avg_delay) + 2 * stddevPop(slot_avg_delay), 4) AS upper_band_inclusion_delay,
    round(avg(slot_avg_delay) - 2 * stddevPop(slot_avg_delay), 4) AS lower_band_inclusion_delay,
    round(avg(ma_inclusion_delay), 4) AS moving_avg_inclusion_delay
FROM slots_with_ma
WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
GROUP BY toStartOfHour(slot_start_date_time)
SETTINGS join_use_nulls = 1
