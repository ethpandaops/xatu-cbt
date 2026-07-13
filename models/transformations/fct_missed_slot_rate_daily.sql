---
table: fct_missed_slot_rate_daily
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
  - missed
dependencies:
  - "{{transformation}}.int_block_proposer_canonical"
---
-- Daily aggregation of missed slot rate.
-- Computes the percentage of missed slots per day with a 7-day moving average.
-- A missed slot is any slot with no block in the canonical chain (block_root
-- IS NULL). Sourced from int_block_proposer_canonical rather than the head path
-- so the metric reaches genesis. The trade-off is that orphaned slots (a block
-- was proposed but reorged out, leaving no canonical block) count as missed,
-- because distinguishing orphaned from missed requires head-time observation.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    slots_in_days AS (
        SELECT
            slot,
            slot_start_date_time,
            toUnixTimestamp(slot_start_date_time) AS slot_timestamp,
            if(block_root IS NULL, 1, 0) AS is_missed
        FROM {{ index .dep "{{transformation}}" "int_block_proposer_canonical" "helpers" "from" }} FINAL
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
