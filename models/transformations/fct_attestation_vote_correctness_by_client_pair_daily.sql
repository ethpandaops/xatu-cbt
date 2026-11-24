---
table: fct_attestation_vote_correctness_by_client_pair_daily
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
  - graffiti
  - client
dependencies:
  - "{{transformation}}.fct_attestation_vote_correctness_by_validator_hourly"
  - "{{transformation}}.fct_block_graffiti"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and a NULL average.
-- One row per consensus and execution client pair. A validator takes the pair of its most recent graffiti naming
-- either client from the 365 days before the end of the day, a side that graffiti does not name being unknown.
-- Validators without one count as unknown/unknown, so that row shows how much of the validator set is uncovered.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    -- Hourly rows rather than the per-validator daily table, whose day bounds need an hour start inside the
    -- interval, so short intervals would come out empty.
    validator_days AS (
        SELECT
            toDate(hour_start_date_time) AS day_start_date,
            toDateTime(toDate(hour_start_date_time)) + INTERVAL 1 DAY AS day_end_date_time,
            validator_index,
            total_duties,
            attested_count,
            missed_count,
            head_correct_count,
            target_correct_count,
            source_correct_count,
            -- Inclusion distance summed over the validator's included attestations, so the client average is per attestation.
            ifNull(avg_inclusion_distance * attested_count, 0) AS inclusion_distance_sum,
            if(avg_inclusion_distance IS NULL, 0, attested_count) AS inclusion_attested_count
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_validator_hourly" "helpers" "from" }} FINAL
        WHERE hour_start_date_time >= (SELECT min_day FROM day_bounds)
          AND hour_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    identified AS (
        SELECT
            proposer_index,
            slot_start_date_time,
            consensus_client,
            execution_client
        FROM {{ index .dep "{{transformation}}" "fct_block_graffiti" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds) - INTERVAL 365 DAY
          AND slot_start_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND (consensus_client != 'unknown' OR execution_client != 'unknown')
    ),
    mapped AS (
        SELECT
            v.*,
            if(i.slot_start_date_time >= v.day_end_date_time - INTERVAL 365 DAY, i.consensus_client, 'unknown') AS consensus_client,
            if(i.slot_start_date_time >= v.day_end_date_time - INTERVAL 365 DAY, i.execution_client, 'unknown') AS execution_client
        FROM validator_days AS v
        ASOF LEFT JOIN identified AS i
            ON v.validator_index = i.proposer_index AND v.day_end_date_time > i.slot_start_date_time
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    consensus_client,
    execution_client,
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
        day_start_date,
        ifNull(consensus_client, 'unknown') AS consensus_client,
        ifNull(execution_client, 'unknown') AS execution_client,
        uniqExact(validator_index) AS validator_count,
        sum(total_duties) AS total_duties,
        sum(attested_count) AS attested_count,
        sum(missed_count) AS missed_count,
        sum(head_correct_count) AS head_correct_count,
        sum(target_correct_count) AS target_correct_count,
        sum(source_correct_count) AS source_correct_count,
        sum(inclusion_distance_sum) / nullIf(sum(inclusion_attested_count), 0) AS avg_inclusion_distance
    FROM mapped
    GROUP BY day_start_date, consensus_client, execution_client

    UNION ALL

    SELECT
        day_start_date,
        consensus_client,
        execution_client,
        0 AS validator_count,
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
GROUP BY day_start_date, consensus_client, execution_client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
