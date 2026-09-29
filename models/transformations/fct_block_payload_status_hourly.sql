---
table: fct_block_payload_status_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - payload
  - epbs
dependencies:
  - "{{transformation}}.fct_block_payload_ptc_vote"
---
-- Gloas (ePBS): hourly payload delivery outcomes judged by the PTC. A block
-- counts as delivered when a majority of observed PTC votes said the payload
-- was present, absent otherwise. Grouped for stacked area charts, mirroring
-- fct_block_proposal_status_hourly.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    target_hours AS (
        SELECT DISTINCT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    status_dim AS (
        SELECT
            arrayJoin(['delivered', 'absent']) AS status
    ),
    status_counts AS (
        SELECT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time,
            if(payload_present_votes * 2 >= ptc_validators, 'delivered', 'absent') AS status,
            toUInt32(count()) AS slot_count
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND ptc_validators > 0
        GROUP BY hour_start_date_time, status
    ),
    candidate_rows AS (
        SELECT
            h.hour_start_date_time,
            s.status
        FROM target_hours h
        CROSS JOIN status_dim s
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.hour_start_date_time,
    c.status,
    COALESCE(sc.slot_count, 0) AS slot_count
FROM candidate_rows c
GLOBAL LEFT JOIN status_counts sc
    ON c.hour_start_date_time = sc.hour_start_date_time
    AND c.status = sc.status
SETTINGS join_use_nulls = 1
