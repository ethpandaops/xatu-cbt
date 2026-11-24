---
table: fct_block_first_seen_by_network_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - block
  - node
dependencies:
  - "{{transformation}}.fct_block_first_seen_by_node_weekly"
  - "{{transformation}}.fct_block_first_seen_by_node"
---
-- Block arrival across every Xatu node, one row per week (Monday to Sunday, UTC): the p10, median and p90 of the
-- per-node medians in fct_block_first_seen_by_node_weekly, every node weighing the same, with linear interpolation
-- between ranks.
-- Rewrites every week the bounds touch so partial weeks are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
week_bounds AS (
    SELECT
        toDateTime(toMonday(fromUnixTimestamp({{ .bounds.start }}))) AS min_week,
        toDateTime(toMonday(fromUnixTimestamp({{ .bounds.end }}))) + INTERVAL 7 DAY AS max_week
),
slots AS (
    SELECT
        toMonday(slot_start_date_time) AS week_start_date,
        toUInt32(uniqExact(slot_start_date_time)) AS slot_count
    FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node" "helpers" "from" }}
    WHERE slot_start_date_time >= (SELECT min_week FROM week_bounds)
        AND slot_start_date_time < (SELECT max_week FROM week_bounds)
        AND node_id != ''
    GROUP BY week_start_date
),
nodes AS (
    SELECT
        week_start_date,
        toUInt32(uniqExact(node_id)) AS node_count,
        quantilesExactInclusive(0.1, 0.5, 0.9)(p50_seen_slot_start_diff) AS spread
    FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node_weekly" "helpers" "from" }} FINAL
    WHERE week_start_date >= toDate((SELECT min_week FROM week_bounds))
        AND week_start_date < toDate((SELECT max_week FROM week_bounds))
    GROUP BY week_start_date
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    n.week_start_date AS week_start_date,
    n.node_count AS node_count,
    s.slot_count AS slot_count,
    n.spread[1] AS p10_median_seen_slot_start_diff,
    n.spread[2] AS p50_median_seen_slot_start_diff,
    n.spread[3] AS p90_median_seen_slot_start_diff
FROM nodes AS n
GLOBAL LEFT JOIN slots AS s ON n.week_start_date = s.week_start_date
-- Merge across shards before inserting instead of each shard inserting its partial spread.
SETTINGS parallel_distributed_insert_select = 0
