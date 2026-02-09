---
table: fct_attestation_vote_correctness_by_validator_hourly
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - hourly
  - attestation
  - canonical
  - vote_correctness
  - validator_performance
dependencies:
  - "{{transformation}}.fct_attestation_vote_correctness_by_validator"
---
-- This query expands the slot range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. For example, if we process
-- slots spanning 11:46-12:30, we expand to include ALL slots from 11:00-12:59
-- so that hour 11:00 (which was partial in the previous run) gets re-aggregated with
-- complete data. The ReplacingMergeTree will merge duplicates keeping the latest row.
--
-- Sources from the per-slot table rather than re-deriving from raw data.
-- Uses argMax dedup instead of FINAL so the p_by_slot_start_date_time projection is used.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries for the current slot range
    -- No FINAL needed: duplicates have identical slot_start_date_time so min/max is unaffected
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_validator" "helpers" "from" }}
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),

    -- Deduplicated per-slot data within the expanded hour boundaries
    -- Uses argMax instead of FINAL so the projection on slot_start_date_time is used
    per_slot AS (
        SELECT
            validator_index,
            slot_start_date_time,
            argMax(attested, updated_date_time) AS attested,
            argMax(head_correct, updated_date_time) AS head_correct,
            argMax(target_correct, updated_date_time) AS target_correct,
            argMax(source_correct, updated_date_time) AS source_correct,
            argMax(inclusion_distance, updated_date_time) AS inclusion_distance
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_validator" "helpers" "from" }}
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
        GROUP BY validator_index, slot_start_date_time
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    validator_index,
    -- Aggregated metrics per hour
    toUInt32(count()) AS total_duties,
    toUInt32(countIf(attested)) AS attested_count,
    toUInt32(countIf(NOT attested)) AS missed_count,
    toUInt32(countIf(head_correct = true)) AS head_correct_count,
    toUInt32(countIf(target_correct = true)) AS target_correct_count,
    toUInt32(countIf(source_correct = true)) AS source_correct_count,
    if(countIf(inclusion_distance IS NOT NULL) > 0,
       toFloat32(avgIf(inclusion_distance, inclusion_distance IS NOT NULL)),
       NULL) AS avg_inclusion_distance
FROM per_slot
GROUP BY hour_start_date_time, validator_index
