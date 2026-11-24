---
table: fct_prepared_block_by_node_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - prepared
  - block
  - locally-built
  - node
dependencies:
  - "{{transformation}}.fct_prepared_block"
---
-- Locally built blocks per node and consensus client version, one row per day.
-- Rewrites every day the bounds touch so partial days are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toStartOfDay(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY AS max_day
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
    meta_client_name,
    meta_consensus_implementation,
    meta_consensus_version,
    toUInt32(count()) AS block_count
FROM {{ index .dep "{{transformation}}" "fct_prepared_block" "helpers" "from" }} FINAL
WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
    AND slot_start_date_time < (SELECT max_day FROM day_bounds)
GROUP BY day_start_date, meta_client_name, meta_consensus_implementation, meta_consensus_version
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
