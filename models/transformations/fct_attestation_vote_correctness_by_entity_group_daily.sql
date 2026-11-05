---
table: fct_attestation_vote_correctness_by_entity_group_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - attestation
  - vote_correctness
  - entity
dependencies:
  - "{{transformation}}.fct_attestation_vote_correctness_by_entity_daily"
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
            total_duties,
            attested_count,
            missed_count,
            head_correct_count,
            target_correct_count,
            source_correct_count,
            ifNull(avg_inclusion_distance * attested_count, 0) AS inclusion_distance_sum,
            if(avg_inclusion_distance IS NULL, 0, attested_count) AS inclusion_attested_count
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_entity_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND total_duties > 0
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    entity_group,
    toUInt32(sum(entity_count)) AS entity_count,
    toUInt64(sum(total_duties)) AS total_duties,
    toUInt64(sum(attested_count)) AS attested_count,
    toUInt64(sum(missed_count)) AS missed_count,
    toUInt64(sum(head_correct_count)) AS head_correct_count,
    toUInt64(sum(target_correct_count)) AS target_correct_count,
    toUInt64(sum(source_correct_count)) AS source_correct_count,
    max(avg_inclusion_distance) AS avg_inclusion_distance
FROM (
    SELECT
        day_start_date,
        entity_group,
        uniqExact(entity) AS entity_count,
        sum(total_duties) AS total_duties,
        sum(attested_count) AS attested_count,
        sum(missed_count) AS missed_count,
        sum(head_correct_count) AS head_correct_count,
        sum(target_correct_count) AS target_correct_count,
        sum(source_correct_count) AS source_correct_count,
        sum(inclusion_distance_sum) / nullIf(sum(inclusion_attested_count), 0) AS avg_inclusion_distance
    FROM members
    WHERE entity_group != ''
    GROUP BY day_start_date, entity_group

    UNION ALL

    SELECT
        day_start_date,
        entity_group,
        0 AS entity_count,
        0 AS total_duties,
        0 AS attested_count,
        0 AS missed_count,
        0 AS head_correct_count,
        0 AS target_correct_count,
        0 AS source_correct_count,
        NULL AS avg_inclusion_distance
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, entity_group
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
