---
table: fct_head_vote_correctness_rate_hourly
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
  - "{{transformation}}.fct_attestation_correctness_canonical"
---
-- Hourly aggregation of head vote correctness rate per slot.
-- Head vote correctness = votes_head / votes_max * 100 (percentage of validators
-- that voted for the block that became canonical).
-- Computes percentiles, Bollinger bands, and 6-hour moving averages.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND votes_max > 0
    ),
    slots_in_hours AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            COALESCE(votes_head, 0) / votes_max * 100 AS head_vote_rate
        FROM {{ index .dep "{{transformation}}" "fct_attestation_correctness_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND votes_max > 0
    ),
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            head_vote_rate,
            avg(head_vote_rate) OVER (ORDER BY slot_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS ma_head_vote_rate
        FROM slots_in_hours
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    count() AS slot_count,
    round(avg(head_vote_rate), 4) AS avg_head_vote_rate,
    round(min(head_vote_rate), 4) AS min_head_vote_rate,
    round(max(head_vote_rate), 4) AS max_head_vote_rate,
    round(quantile(0.05)(head_vote_rate), 4) AS p05_head_vote_rate,
    round(quantile(0.50)(head_vote_rate), 4) AS p50_head_vote_rate,
    round(quantile(0.95)(head_vote_rate), 4) AS p95_head_vote_rate,
    round(stddevPop(head_vote_rate), 4) AS stddev_head_vote_rate,
    round(avg(head_vote_rate) + 2 * stddevPop(head_vote_rate), 4) AS upper_band_head_vote_rate,
    round(avg(head_vote_rate) - 2 * stddevPop(head_vote_rate), 4) AS lower_band_head_vote_rate,
    round(avg(ma_head_vote_rate), 4) AS moving_avg_head_vote_rate
FROM slots_with_ma
GROUP BY toStartOfHour(slot_start_date_time)
SETTINGS join_use_nulls = 1
