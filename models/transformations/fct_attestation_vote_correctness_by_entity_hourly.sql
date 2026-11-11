---
table: fct_attestation_vote_correctness_by_entity_hourly
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - attestation
  - vote_correctness
  - entity
dependencies:
  - "{{transformation}}.fct_attestation_vote_correctness_by_validator_hourly"
  - "{{transformation}}.dim_node"
---
-- Rewrites every hour the bounds touch so partial hours are recounted in full. Keys stored for those hours that
-- the recount no longer produces are rewritten with zero counts and a NULL average.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_hour,
            toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) AS max_hour
    ),
    -- Deduplicated with argMax rather than FINAL so the hour-ordered projection serves the hour filter.
    validator_hours AS (
        SELECT
            hour_start_date_time,
            validator_index,
            argMax(total_duties, updated_date_time) AS total_duties,
            argMax(attested_count, updated_date_time) AS attested_count,
            argMax(missed_count, updated_date_time) AS missed_count,
            argMax(head_correct_count, updated_date_time) AS head_correct_count,
            argMax(target_correct_count, updated_date_time) AS target_correct_count,
            argMax(source_correct_count, updated_date_time) AS source_correct_count,
            -- Inclusion distance summed over the validator's included attestations, so the entity average is per attestation.
            ifNull(argMax(avg_inclusion_distance, updated_date_time) * attested_count, 0) AS inclusion_distance_sum,
            if(argMax(avg_inclusion_distance, updated_date_time) IS NULL, 0, attested_count) AS inclusion_attested_count
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_validator_hourly" "helpers" "from" }}
        WHERE hour_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND hour_start_date_time <= (SELECT max_hour FROM hour_bounds)
        GROUP BY hour_start_date_time, validator_index
    ),
    mapped AS (
        SELECT
            v.*,
            if(dn.source IS NULL OR dn.source = '', 'unknown', dn.source) AS entity
        FROM validator_hours AS v
        GLOBAL LEFT JOIN {{ index .dep "{{transformation}}" "dim_node" "helpers" "from" }} AS dn FINAL
            ON v.validator_index = dn.validator_index
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    entity,
    toUInt32(sum(validator_count)) AS validator_count,
    toUInt64(sum(total_duties)) AS total_duties,
    toUInt64(sum(attested_count)) AS attested_count,
    toUInt64(sum(missed_count)) AS missed_count,
    toUInt64(sum(head_correct_count)) AS head_correct_count,
    toUInt64(sum(target_correct_count)) AS target_correct_count,
    toUInt64(sum(source_correct_count)) AS source_correct_count,
    max(avg_inclusion_distance) AS avg_inclusion_distance
FROM (
    SELECT
        hour_start_date_time,
        entity,
        uniqExact(validator_index) AS validator_count,
        sum(total_duties) AS total_duties,
        sum(attested_count) AS attested_count,
        sum(missed_count) AS missed_count,
        sum(head_correct_count) AS head_correct_count,
        sum(target_correct_count) AS target_correct_count,
        sum(source_correct_count) AS source_correct_count,
        sum(inclusion_distance_sum) / nullIf(sum(inclusion_attested_count), 0) AS avg_inclusion_distance
    FROM mapped
    GROUP BY hour_start_date_time, entity

    UNION ALL

    SELECT
        hour_start_date_time,
        entity,
        0 AS validator_count,
        0 AS total_duties,
        0 AS attested_count,
        0 AS missed_count,
        0 AS head_correct_count,
        0 AS target_correct_count,
        0 AS source_correct_count,
        NULL AS avg_inclusion_distance
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE hour_start_date_time >= (SELECT min_hour FROM hour_bounds)
      AND hour_start_date_time <= (SELECT max_hour FROM hour_bounds)
)
GROUP BY hour_start_date_time, entity
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
