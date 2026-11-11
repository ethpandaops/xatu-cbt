---
table: fct_block_first_seen_by_network_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - block
  - node
dependencies:
  - "{{transformation}}.fct_block_first_seen_by_node_daily"
  - "{{transformation}}.fct_block_first_seen_by_node"
---
-- Block arrival across every Xatu node, one row per day: the p10, median and p90 of the per-node medians in
-- fct_block_first_seen_by_node_daily, every node weighing the same, with linear interpolation between ranks.
-- Rewrites every day the bounds touch so partial days are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toStartOfDay(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY AS max_day
),
slots AS (
    SELECT
        toDate(slot_start_date_time) AS day_start_date,
        toUInt32(uniqExact(slot_start_date_time)) AS slot_count
    FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node" "helpers" "from" }}
    WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
        AND slot_start_date_time < (SELECT max_day FROM day_bounds)
        AND node_id != ''
    GROUP BY day_start_date
),
nodes AS (
    SELECT
        day_start_date,
        toUInt32(uniqExact(node_id)) AS node_count,
        quantilesExactInclusive(0.1, 0.5, 0.9)(p50_seen_slot_start_diff) AS spread
    FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= toDate((SELECT min_day FROM day_bounds))
        AND day_start_date < toDate((SELECT max_day FROM day_bounds))
    GROUP BY day_start_date
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    n.day_start_date AS day_start_date,
    n.node_count AS node_count,
    s.slot_count AS slot_count,
    n.spread[1] AS p10_median_seen_slot_start_diff,
    n.spread[2] AS p50_median_seen_slot_start_diff,
    n.spread[3] AS p90_median_seen_slot_start_diff
FROM nodes AS n
GLOBAL LEFT JOIN slots AS s ON n.day_start_date = s.day_start_date
-- Merge across shards before inserting instead of each shard inserting its partial spread.
SETTINGS parallel_distributed_insert_select = 0
