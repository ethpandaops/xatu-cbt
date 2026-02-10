---
table: fct_block_proposal_status_daily
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
  - block
dependencies:
  - "{{transformation}}.fct_block_proposer"
---
-- Daily aggregation of block proposal status counts.
-- Groups slots by day and status (canonical, orphaned, missed) for stacked area charts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    target_days AS (
        SELECT DISTINCT
            toDate(slot_start_date_time) AS day_start_date
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
    ),
    status_dim AS (
        SELECT
            arrayJoin(['canonical', 'orphaned', 'missed']) AS status
    ),
    status_counts AS (
        SELECT
            toDate(slot_start_date_time) AS day_start_date,
            status,
            toUInt32(count()) AS slot_count
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
          AND status IN ('canonical', 'orphaned', 'missed')
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
    toUInt32(coalesce(sc.slot_count, toUInt32(0))) AS slot_count
FROM candidate_rows c
LEFT JOIN status_counts sc
    ON c.day_start_date = sc.day_start_date
    AND c.status = sc.status
