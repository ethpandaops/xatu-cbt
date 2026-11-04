---
table: fct_block_payload_status_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - payload
  - epbs
dependencies:
  - "{{transformation}}.fct_block_payload_ptc_vote"
---
-- Gloas (ePBS): daily payload delivery outcomes judged by the PTC, the daily twin of
-- fct_block_payload_status_hourly. A block counts as delivered when a majority of observed PTC votes said
-- the payload was present, absent otherwise. Rewrites every day the bounds touch so partial days are recounted
-- in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    target_days AS (
        SELECT DISTINCT
            toDate(slot_start_date_time) AS day_start_date
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    status_dim AS (
        SELECT
            arrayJoin(['delivered', 'absent']) AS status
    ),
    status_counts AS (
        SELECT
            toDate(slot_start_date_time) AS day_start_date,
            if(payload_present_votes * 2 >= ptc_validators, 'delivered', 'absent') AS status,
            toUInt32(count()) AS slot_count
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND ptc_validators > 0
        GROUP BY day_start_date, status
    ),
    candidate_rows AS (
        SELECT
            d.day_start_date,
            s.status
        FROM target_days d
        CROSS JOIN status_dim s
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.day_start_date,
    c.status,
    COALESCE(sc.slot_count, 0) AS slot_count
FROM candidate_rows c
GLOBAL LEFT JOIN status_counts sc
    ON c.day_start_date = sc.day_start_date
    AND c.status = sc.status
SETTINGS join_use_nulls = 1
