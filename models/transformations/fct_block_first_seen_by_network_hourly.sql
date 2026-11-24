---
table: fct_block_first_seen_by_network_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - block
  - node
dependencies:
  - "{{transformation}}.fct_block_first_seen_by_node_hourly"
  - "{{transformation}}.fct_block_first_seen_by_node"
---
-- Block arrival across every Xatu node, one row per hour: the p10, median and p90 of the per-node medians in
-- fct_block_first_seen_by_node_hourly, every node weighing the same, with linear interpolation between ranks.
-- Rewrites every hour the bounds touch so partial hours are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
hour_bounds AS (
    SELECT
        toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_hour,
        toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 HOUR AS max_hour
),
slots AS (
    SELECT
        toStartOfHour(slot_start_date_time) AS hour_start_date_time,
        toUInt32(uniqExact(slot_start_date_time)) AS slot_count
    FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node" "helpers" "from" }}
    WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
        AND slot_start_date_time < (SELECT max_hour FROM hour_bounds)
        AND node_id != ''
    GROUP BY hour_start_date_time
),
nodes AS (
    SELECT
        hour_start_date_time,
        toUInt32(uniqExact(node_id)) AS node_count,
        quantilesExactInclusive(0.1, 0.5, 0.9)(p50_seen_slot_start_diff) AS spread
    FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node_hourly" "helpers" "from" }} FINAL
    WHERE hour_start_date_time >= (SELECT min_hour FROM hour_bounds)
        AND hour_start_date_time < (SELECT max_hour FROM hour_bounds)
    GROUP BY hour_start_date_time
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    n.hour_start_date_time AS hour_start_date_time,
    n.node_count AS node_count,
    s.slot_count AS slot_count,
    n.spread[1] AS p10_median_seen_slot_start_diff,
    n.spread[2] AS p50_median_seen_slot_start_diff,
    n.spread[3] AS p90_median_seen_slot_start_diff
FROM nodes AS n
GLOBAL LEFT JOIN slots AS s ON n.hour_start_date_time = s.hour_start_date_time
-- Merge across shards before inserting instead of each shard inserting its partial spread.
SETTINGS parallel_distributed_insert_select = 0
