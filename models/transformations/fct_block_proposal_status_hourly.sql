---
table: fct_block_proposal_status_hourly
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
  - block
dependencies:
  - "{{transformation}}.fct_block_proposer"
---
-- Hourly aggregation of block proposal status counts.
-- Groups slots by hour and status (canonical, orphaned, missed) for stacked area charts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    target_hours AS (
        SELECT DISTINCT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    status_dim AS (
        SELECT
            arrayJoin(['canonical', 'orphaned', 'missed']) AS status
    ),
    status_counts AS (
        SELECT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time,
            status,
            toUInt32(count()) AS slot_count
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND status IN ('canonical', 'orphaned', 'missed')
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
    toUInt32(coalesce(sc.slot_count, toUInt32(0))) AS slot_count
FROM candidate_rows c
LEFT JOIN status_counts sc
    ON c.hour_start_date_time = sc.hour_start_date_time
    AND c.status = sc.status
