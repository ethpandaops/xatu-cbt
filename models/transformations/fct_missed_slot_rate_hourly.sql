---
table: fct_missed_slot_rate_hourly
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
  - missed
dependencies:
  - "{{transformation}}.int_block_proposer_canonical"
---
-- Hourly aggregation of missed slot rate.
-- Computes the percentage of missed slots per hour with a 6-hour moving average.
-- A missed slot is any slot with no block in the canonical chain (block_root
-- IS NULL). Sourced from int_block_proposer_canonical rather than the head path
-- so the metric reaches genesis. The trade-off is that orphaned slots (a block
-- was proposed but reorged out, leaving no canonical block) count as missed,
-- because distinguishing orphaned from missed requires head-time observation.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    slots_in_hours AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            if(block_root IS NULL, 1, 0) AS is_missed
        FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
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
