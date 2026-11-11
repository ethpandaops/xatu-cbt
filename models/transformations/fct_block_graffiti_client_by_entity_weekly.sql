---
table: fct_block_graffiti_client_by_entity_weekly
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
  - graffiti
  - client
  - entity
dependencies:
  - "{{transformation}}.fct_block_graffiti"
---
-- Weeks run Monday to Sunday (UTC). Rewrites every week the bounds touch so partial weeks are recounted in full.
-- Keys stored for those weeks that the recount no longer produces (a reprocess moved their blocks to another key)
-- are rewritten as zero.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    week_bounds AS (
        SELECT
            toMonday(min(slot_start_date_time)) AS min_week,
            toMonday(max(slot_start_date_time)) AS max_week
        FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    week_start_date,
    entity,
    consensus_client,
    execution_client,
    toUInt32(sum(blocks)) AS block_count
FROM (
    SELECT
        toMonday(slot_start_date_time) AS week_start_date,
        entity,
        consensus_client,
        execution_client,
        count() AS blocks
    FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
    WHERE toMonday(slot_start_date_time) >= (SELECT min_week FROM week_bounds)
      AND toMonday(slot_start_date_time) <= (SELECT max_week FROM week_bounds)
    GROUP BY week_start_date, entity, consensus_client, execution_client

    UNION ALL

    SELECT
        week_start_date,
        entity,
        consensus_client,
        execution_client,
        toUInt64(0) AS blocks
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE week_start_date >= (SELECT min_week FROM week_bounds)
      AND week_start_date <= (SELECT max_week FROM week_bounds)
      AND block_count > 0
)
GROUP BY week_start_date, entity, consensus_client, execution_client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
