---
table: fct_missed_slot_rate_daily
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
  - missed
dependencies:
  - "{{transformation}}.fct_block_proposer"
---
-- Daily aggregation of missed slot rate.
-- Computes the percentage of missed slots per day with a 7-day moving average.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    slots_in_days AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            if(status = 'missed', 1, 0) AS is_missed
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
    ),
    slots_with_ma AS (
        SELECT
            slot,
            slot_start_date_time,
            is_missed,
            avg(is_missed) OVER (ORDER BY slot_timestamp RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) * 100 AS ma_missed_rate
        FROM slots_in_days
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
    toUInt32(count()) AS slot_count,
    toUInt32(sum(is_missed)) AS missed_count,
    round(sum(is_missed) / count() * 100, 4) AS missed_rate,
    round(avg(ma_missed_rate), 4) AS moving_avg_missed_rate
FROM slots_with_ma
GROUP BY toDate(slot_start_date_time)
