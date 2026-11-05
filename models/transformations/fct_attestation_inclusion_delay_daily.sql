---
table: fct_attestation_inclusion_delay_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - consensus
  - attestation
dependencies:
  - "{{transformation}}.int_attestation_attested_canonical"
  - "{{transformation}}.int_block_proposer_canonical"
---
-- Daily aggregation of attestation inclusion delay.
-- First aggregates per-validator data to per-slot averages, then computes daily
-- stats across slots. This prevents individual validator outliers from dominating.
-- Computes percentiles, Bollinger bands, and 7-day moving averages.
-- The scan reaches back one window before the first day so the moving average covers the full 7 days.
--
-- Reads the canonical attestations, which reach genesis. A vote counts when the block it is for is canonical and at
-- most 64 slots older than the vote, the votes fct_attestation_correctness_by_validator_canonical marks canonical.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "int_attestation_attested_canonical" "helpers" "from" }}
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    -- Canonical blocks from 64 slots before the scan below to its end
    canonical_blocks AS (
        SELECT
            slot AS block_slot,
            block_root
        FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= toDateTime((SELECT min_day FROM day_bounds) - INTERVAL 7 DAY) - INTERVAL 768 SECOND
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
          AND block_root IS NOT NULL
    ),
    slot_averages AS (
        SELECT
            a.slot AS slot,
            a.slot_start_date_time AS slot_start_date_time,
            toUnixTimestamp(a.slot_start_date_time) AS slot_timestamp,
            avg(a.inclusion_distance) AS slot_avg_delay
        FROM {{ index .dep "{{transformation}}" "int_attestation_attested_canonical" "helpers" "from" }} AS a FINAL
        GLOBAL INNER JOIN canonical_blocks AS b ON a.block_root = b.block_root
        WHERE toDate(a.slot_start_date_time) >= (SELECT min_day FROM day_bounds) - INTERVAL 7 DAY
          AND toDate(a.slot_start_date_time) <= (SELECT max_day FROM day_bounds)
          AND a.slot >= b.block_slot
          AND a.slot - b.block_slot <= 64
        GROUP BY slot, slot_start_date_time
    ),
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            slot_avg_delay,
            avg(slot_avg_delay) OVER (ORDER BY slot_timestamp RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS ma_inclusion_delay
        FROM slot_averages
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
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
WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
GROUP BY toDate(slot_start_date_time)
SETTINGS join_use_nulls = 1
