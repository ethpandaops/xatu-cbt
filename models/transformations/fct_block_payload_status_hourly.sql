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
-- Gloas (ePBS): hourly payload delivery outcomes of canonical blocks judged by
-- the PTC, using the fork-choice quorum: a payload is timely when more than
-- PAYLOAD_TIMELY_THRESHOLD = PTC_SIZE // 2 = 256 of the 512 PTC members voted
-- it present (strictly greater, counted against the full committee rather
-- than the votes that happened to be included). Canonical blocks whose PTC
-- votes never reached the chain (the next slot was missed) are counted as
-- no_votes rather than dropped. Orphaned blocks are excluded. Grouped for
-- stacked area charts, mirroring fct_block_proposal_status_hourly.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND `status` = 'canonical'
    ),
    target_hours AS (
        SELECT DISTINCT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND `status` = 'canonical'
    ),
    status_dim AS (
        SELECT
            arrayJoin(['delivered', 'absent', 'no_votes']) AS status
    ),
    status_counts AS (
        SELECT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time,
            multiIf(
                ptc_validators = 0, 'no_votes',
                payload_present_votes > 256, 'delivered',
                'absent'
            ) AS payload_outcome,
            toUInt32(count()) AS slot_count
        FROM {{ index .dep "{{transformation}}" "fct_block_payload_ptc_vote" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND `status` = 'canonical'
        GROUP BY hour_start_date_time, payload_outcome
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
    AND c.status = sc.payload_outcome
SETTINGS join_use_nulls = 1
