---
table: fct_reorg_by_hourly
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
  - reorg
dependencies:
  - "{{transformation}}.fct_block_proposer"
---
-- Hourly aggregation of reorg events detected by consecutive orphaned slots.
-- Uses gaps-and-islands to identify runs of consecutive orphaned slots as reorg events.
-- Each reorg event has a depth (number of consecutive orphaned slots).
-- Output is grouped by hour and depth.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    -- Get all orphaned slots within expanded hour boundaries
    orphaned_slots AS (
        SELECT
            slot,
            slot_start_date_time
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND status = 'orphaned'
        ORDER BY slot
    ),
    -- Gaps-and-islands: consecutive orphaned slots get the same group ID
    grouped AS (
        SELECT
            slot,
            slot_start_date_time,
            slot - row_number() OVER (ORDER BY slot) AS grp
        FROM orphaned_slots
    ),
    -- Each group is a reorg event, depth = count of consecutive orphaned slots
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
    toStartOfHour(reorg_start_date_time) AS hour_start_date_time,
    toUInt16(depth) AS depth,
    count() AS reorg_count
FROM reorg_events
GROUP BY toStartOfHour(reorg_start_date_time), depth
