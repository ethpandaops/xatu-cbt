---
table: fct_reorg_daily
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
    -- Expanded day bounds converted to slot bounds for deterministic slot-based padding.
    expanded_slot_bounds AS (
        SELECT
            min(slot) AS min_slot,
            max(slot) AS max_slot
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
    ),
    -- Target days that will be rewritten in this run.
    target_days AS (
        SELECT DISTINCT
            toDate(slot_start_date_time) AS day_start_date
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
    ),
    -- Get orphaned slots with +/- 64 slot padding so edge events keep their true depth.
    orphaned_slots AS (
        SELECT
            slot,
            slot_start_date_time
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot >= (SELECT min_slot FROM expanded_slot_bounds) - 64
          AND slot <= (SELECT max_slot FROM expanded_slot_bounds) + 64
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
    ),
    -- Keep only events that start in the target days.
    reorg_counts AS (
        SELECT
            toDate(reorg_start_date_time) AS day_start_date,
            toUInt16(depth) AS depth,
            toUInt32(count()) AS reorg_count
        FROM reorg_events
        WHERE toDate(reorg_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(reorg_start_date_time) <= (SELECT max_day FROM day_bounds)
        GROUP BY day_start_date, depth
    ),
    -- Include existing depths so stale (day, depth) rows are overwritten to zero when needed.
    existing_depths AS (
        SELECT DISTINCT
            e.depth AS depth
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL AS e
        INNER JOIN target_days d
            ON e.day_start_date = d.day_start_date
    ),
    depth_dim AS (
        SELECT depth FROM existing_depths
        UNION DISTINCT
        SELECT depth FROM reorg_counts
    ),
    candidate_rows AS (
        SELECT
            d.day_start_date,
            dd.depth
        FROM target_days d
        CROSS JOIN depth_dim dd
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.day_start_date,
    c.depth,
    toUInt32(coalesce(r.reorg_count, toUInt32(0))) AS reorg_count
FROM candidate_rows c
LEFT JOIN reorg_counts r
    ON c.day_start_date = r.day_start_date
    AND c.depth = r.depth
