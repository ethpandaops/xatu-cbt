---
table: fct_block_graffiti_client_by_entity_daily
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
  - graffiti
  - client
  - entity
dependencies:
  - "{{transformation}}.fct_block_graffiti"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days
-- that the recount no longer produces (a reprocess moved their blocks to another key) are rewritten as zero.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    entity,
    consensus_client,
    execution_client,
    toUInt32(sum(blocks)) AS block_count
FROM (
    SELECT
        toDate(slot_start_date_time) AS day_start_date,
        entity,
        consensus_client,
        execution_client,
        count() AS blocks
    FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
    WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
      AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
    GROUP BY day_start_date, entity, consensus_client, execution_client

    UNION ALL

    SELECT
        day_start_date,
        entity,
        consensus_client,
        execution_client,
        toUInt64(0) AS blocks
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
      AND block_count > 0
)
GROUP BY day_start_date, entity, consensus_client, execution_client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
