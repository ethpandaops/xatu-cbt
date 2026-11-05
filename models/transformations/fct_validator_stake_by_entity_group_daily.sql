---
table: fct_validator_stake_by_entity_group_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - validator
  - entity
  - credential
dependencies:
  - "{{transformation}}.fct_validator_stake_by_entity_daily"
---
-- Rewrites every day the bounds touch. Keys stored for those days that the rewrite no longer produces are
-- written as zero. An entity belongs to the lido group when its name contains lido.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    entity_group,
    credential_type,
    toUInt32(sum(entity_count)) AS entity_count,
    toUInt32(sum(active_validator_count)) AS active_validator_count,
    toUInt64(sum(active_effective_balance)) AS active_effective_balance,
    toUInt64(sum(active_balance)) AS active_balance
FROM (
    SELECT
        day_start_date,
        if(position(entity, 'lido') > 0, 'lido', '') AS entity_group,
        credential_type,
        uniqExactIf(s.entity, s.active_validator_count > 0) AS entity_count,
        sum(s.active_validator_count) AS active_validator_count,
        sum(s.active_effective_balance) AS active_effective_balance,
        sum(s.active_balance) AS active_balance
    FROM {{ index .dep "{{transformation}}" "fct_validator_stake_by_entity_daily" "helpers" "from" }} AS s FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
      AND position(entity, 'lido') > 0
    GROUP BY day_start_date, entity_group, credential_type

    UNION ALL

    SELECT
        day_start_date,
        entity_group,
        credential_type,
        0 AS entity_count,
        0 AS active_validator_count,
        0 AS active_effective_balance,
        0 AS active_balance
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, entity_group, credential_type
-- Merge sums across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0
