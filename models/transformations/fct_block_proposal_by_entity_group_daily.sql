---
table: fct_block_proposal_by_entity_group_daily
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
  - proposer
  - mev
  - entity
dependencies:
  - "{{transformation}}.fct_block_proposal_by_entity_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and a NULL average.
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
            block_count,
            mev_block_count,
            ifNull(avg_mev_value_eth * mev_block_count, 0) AS mev_value_sum,
            if(avg_mev_value_eth IS NULL, 0, mev_block_count) AS mev_value_count,
            orphaned_block_count,
            missed_slot_count
        FROM {{ index .dep "{{transformation}}" "fct_block_proposal_by_entity_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND block_count + orphaned_block_count + missed_slot_count > 0
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    entity_group,
    toUInt32(sum(entity_count)) AS entity_count,
    toUInt32(sum(block_count)) AS block_count,
    toUInt32(sum(mev_block_count)) AS mev_block_count,
    max(avg_mev_value_eth) AS avg_mev_value_eth,
    toUInt32(sum(orphaned_block_count)) AS orphaned_block_count,
    toUInt32(sum(missed_slot_count)) AS missed_slot_count
FROM (
    SELECT
        day_start_date,
        entity_group,
        uniqExact(entity) AS entity_count,
        sum(block_count) AS block_count,
        sum(mev_block_count) AS mev_block_count,
        sum(mev_value_sum) / nullIf(sum(mev_value_count), 0) AS avg_mev_value_eth,
        sum(orphaned_block_count) AS orphaned_block_count,
        sum(missed_slot_count) AS missed_slot_count
    FROM members
    WHERE entity_group != ''
    GROUP BY day_start_date, entity_group

    UNION ALL

    SELECT
        day_start_date,
        entity_group,
        0 AS entity_count,
        0 AS block_count,
        0 AS mev_block_count,
        NULL AS avg_mev_value_eth,
        0 AS orphaned_block_count,
        0 AS missed_slot_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, entity_group
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
