---
table: fct_block_proposal_by_entity_daily
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
  - "{{transformation}}.fct_block_proposer_by_entity"
  - "{{transformation}}.int_block_mev_canonical"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and a NULL average.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    proposals AS (
        SELECT
            slot_start_date_time,
            entity,
            block_root,
            `status`
        FROM {{ index .dep "{{transformation}}" "fct_block_proposer_by_entity" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    mev AS (
        SELECT block_root, value
        FROM {{ index .dep "{{transformation}}" "int_block_mev_canonical" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    joined AS (
        SELECT
            toDate(p.slot_start_date_time) AS day_start_date,
            p.entity AS entity,
            p.`status` AS proposal_status,
            p.`status` = 'canonical' AND m.block_root IS NOT NULL AS is_mev,
            m.value AS mev_value
        FROM proposals AS p
        GLOBAL LEFT JOIN mev AS m ON p.block_root = m.block_root
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    entity,
    toUInt32(sum(block_count)) AS block_count,
    toUInt32(sum(mev_block_count)) AS mev_block_count,
    max(avg_mev_value_eth) AS avg_mev_value_eth,
    toUInt32(sum(orphaned_block_count)) AS orphaned_block_count,
    toUInt32(sum(missed_slot_count)) AS missed_slot_count
FROM (
    SELECT
        day_start_date,
        entity,
        countIf(proposal_status = 'canonical') AS block_count,
        countIf(is_mev) AS mev_block_count,
        if(isNaN(avgIf(toFloat64(mev_value) / 1e18, is_mev AND mev_value IS NOT NULL)), NULL, avgIf(toFloat64(mev_value) / 1e18, is_mev AND mev_value IS NOT NULL)) AS avg_mev_value_eth,
        countIf(proposal_status = 'orphaned') AS orphaned_block_count,
        countIf(proposal_status = 'missed') AS missed_slot_count
    FROM joined
    GROUP BY day_start_date, entity

    UNION ALL

    SELECT
        day_start_date,
        entity,
        0 AS block_count,
        0 AS mev_block_count,
        NULL AS avg_mev_value_eth,
        0 AS orphaned_block_count,
        0 AS missed_slot_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, entity
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
