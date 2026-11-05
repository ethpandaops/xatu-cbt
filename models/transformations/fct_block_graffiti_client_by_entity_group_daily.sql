---
table: fct_block_graffiti_client_by_entity_group_daily
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
  - "{{transformation}}.fct_block_graffiti_client_by_entity_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts.
-- An entity belongs to the lido group when its name contains lido.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    members AS (
        SELECT
            day_start_date,
            if(position(entity, 'lido') > 0, 'lido', '') AS entity_group,
            entity,
            consensus_client,
            execution_client,
            block_count
        FROM {{ index .dep "{{transformation}}" "fct_block_graffiti_client_by_entity_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND block_count > 0
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    entity_group,
    consensus_client,
    execution_client,
    toUInt32(sum(entity_count)) AS entity_count,
    toUInt32(sum(block_count)) AS block_count
FROM (
    SELECT
        day_start_date,
        entity_group,
        consensus_client,
        execution_client,
        uniqExact(entity) AS entity_count,
        sum(block_count) AS block_count
    FROM members
    WHERE entity_group != ''
    GROUP BY day_start_date, entity_group, consensus_client, execution_client

    UNION ALL

    SELECT
        day_start_date,
        entity_group,
        consensus_client,
        execution_client,
        0 AS entity_count,
        0 AS block_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, entity_group, consensus_client, execution_client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
