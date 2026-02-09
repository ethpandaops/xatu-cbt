---
table: fct_reorg_by_daily
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
  - reorg
dependencies:
  - "{{transformation}}.fct_block_proposer"
---
-- Daily aggregation of reorg events detected by consecutive orphaned slots.
-- Uses gaps-and-islands to identify runs of consecutive orphaned slots as reorg events.
-- Each reorg event has a depth (number of consecutive orphaned slots).
-- Output is grouped by day and depth.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    orphaned_slots AS (
        SELECT
            slot,
            slot_start_date_time
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
          AND status = 'orphaned'
        ORDER BY slot
    ),
    grouped AS (
        SELECT
            slot,
            slot_start_date_time,
            slot - row_number() OVER (ORDER BY slot) AS grp
        FROM orphaned_slots
    ),
    reorg_events AS (
        SELECT
            grp,
            count() AS depth,
            min(slot_start_date_time) AS reorg_start_date_time
        FROM grouped
        GROUP BY grp
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(reorg_start_date_time) AS day_start_date,
    toUInt16(depth) AS depth,
    count() AS reorg_count
FROM reorg_events
GROUP BY toDate(reorg_start_date_time), depth
