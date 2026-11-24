---
table: fct_node_count_by_cl_client_version_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - head
  - node
  - client
dependencies:
  - "{{transformation}}.fct_head_first_seen_by_node_daily"
  - "{{transformation}}.dim_xatu_node"
---
-- Xatu nodes per day, consensus client and the consensus version they reported that day.
-- fct_head_first_seen_by_node_daily holds one row per node name per day with its last version that day. Names are
-- counted as the Xatu node dim_xatu_node chains them into.
-- Rewrites every day the bounds touch so partial days are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
),
-- dim_xatu_node chains the rotating hashed names of one machine into one Xatu node. Names newer than its last
-- hourly run count as themselves.
nodes AS (
    SELECT meta_client_name, stable_node_id
    FROM {{ index .dep "{{transformation}}" "dim_xatu_node" "helpers" "from" }} FINAL
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    meta_consensus_implementation,
    meta_consensus_version,
    toUInt32(uniqExact(coalesce(nullIf(n.stable_node_id, ''), t.meta_client_name))) AS node_count
FROM {{ index .dep "{{transformation}}" "fct_head_first_seen_by_node_daily" "helpers" "from" }} AS t FINAL
GLOBAL LEFT JOIN nodes AS n ON t.meta_client_name = n.meta_client_name
WHERE t.day_start_date >= (SELECT min_day FROM day_bounds)
    AND t.day_start_date <= (SELECT max_day FROM day_bounds)
    AND t.meta_consensus_implementation != ''
GROUP BY day_start_date, meta_consensus_implementation, meta_consensus_version
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
