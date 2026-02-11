---
table: fct_reorg_hourly
type: incremental
interval:
  type: slot
  max: 25200
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
    -- Expanded hour bounds converted to slot bounds for deterministic slot-based padding.
    expanded_slot_bounds AS (
        SELECT
            min(slot) AS min_slot,
            max(slot) AS max_slot
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    -- Target hours that will be rewritten in this run.
    target_hours AS (
        SELECT DISTINCT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
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
    ),
    -- Keep only events that start in the target hours.
    reorg_counts AS (
        SELECT
            toStartOfHour(reorg_start_date_time) AS hour_start_date_time,
            toUInt16(depth) AS depth,
            toUInt32(count()) AS reorg_count
        FROM reorg_events
        WHERE reorg_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND reorg_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
        GROUP BY hour_start_date_time, depth
    ),
    -- Include existing depths so stale (hour, depth) rows are overwritten to zero when needed.
    existing_depths AS (
        SELECT DISTINCT
            e.depth AS depth
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL AS e
        INNER JOIN target_hours h
            ON e.hour_start_date_time = h.hour_start_date_time
    ),
    depth_dim AS (
        SELECT depth FROM existing_depths
        UNION DISTINCT
        SELECT depth FROM reorg_counts
    ),
    candidate_rows AS (
        SELECT
            h.hour_start_date_time,
            d.depth
        FROM target_hours h
        CROSS JOIN depth_dim d
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.hour_start_date_time,
    c.depth,
    toUInt32(coalesce(r.reorg_count, toUInt32(0))) AS reorg_count
FROM candidate_rows c
LEFT JOIN reorg_counts r
    ON c.hour_start_date_time = r.hour_start_date_time
    AND c.depth = r.depth
