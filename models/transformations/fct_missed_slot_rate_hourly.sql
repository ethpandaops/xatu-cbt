---
table: fct_missed_slot_rate_hourly
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
  - missed
dependencies:
  - "{{transformation}}.fct_block_proposer"
---
-- Hourly aggregation of missed slot rate.
-- Computes the percentage of missed slots per hour with a 6-hour moving average.
-- Source: fct_block_proposer (status = 'missed' indicates no block was produced).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    slots_in_hours AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            if(status = 'missed', 1, 0) AS is_missed
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            is_missed,
            avg(is_missed) OVER (ORDER BY slot_timestamp RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) * 100 AS ma_missed_rate
        FROM slots_in_hours
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    toUInt32(count()) AS slot_count,
    toUInt32(sum(is_missed)) AS missed_count,
    round(sum(is_missed) / count() * 100, 4) AS missed_rate,
    round(avg(ma_missed_rate), 4) AS moving_avg_missed_rate
FROM slots_with_ma
GROUP BY toStartOfHour(slot_start_date_time)
