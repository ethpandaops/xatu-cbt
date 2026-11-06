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
---
-- Xatu nodes per day, consensus client and the consensus version they reported that day.
-- fct_head_first_seen_by_node_daily holds one row per node per day with the node's last version that day.
-- Rewrites every day the bounds touch so partial days are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    meta_consensus_implementation,
    meta_consensus_version,
    toUInt32(uniqExact(node_id)) AS node_count
FROM {{ index .dep "{{transformation}}" "fct_head_first_seen_by_node_daily" "helpers" "from" }} FINAL
WHERE day_start_date >= (SELECT min_day FROM day_bounds)
    AND day_start_date <= (SELECT max_day FROM day_bounds)
    AND meta_consensus_implementation != ''
GROUP BY day_start_date, meta_consensus_implementation, meta_consensus_version
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
