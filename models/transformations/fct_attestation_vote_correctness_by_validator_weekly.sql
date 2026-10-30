---
table: fct_attestation_vote_correctness_by_validator_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - attestation
  - canonical
  - vote_correctness
  - validator_performance
dependencies:
  - "{{transformation}}.fct_attestation_vote_correctness_by_validator_daily"
---
-- Sources from the daily table and expands the day range to complete week
-- boundaries (Monday-Sunday, UTC), so partial weeks at either edge of an interval
-- are recomputed in full whenever an interval touches them. The ReplacingMergeTree
-- keeps the latest row.
-- Uses argMax dedup on the daily table, then sums the pre-aggregated counts to
-- weekly granularity.
-- Weighted average for avg_inclusion_distance: sum(avg * count) / sum(count).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the week boundaries for the current range
    week_bounds AS (
        SELECT
            toMonday(min(day_start_date)) AS min_week,
            toMonday(max(day_start_date)) AS max_week
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_validator_daily" "helpers" "from" }}
        WHERE day_start_date BETWEEN toDate(fromUnixTimestamp({{ .bounds.start }})) AND toDate(fromUnixTimestamp({{ .bounds.end }}))
    ),

    -- Deduplicated daily data within the expanded week boundaries
    per_day AS (
        SELECT
            validator_index,
            day_start_date,
            argMax(total_duties, updated_date_time) AS d_total_duties,
            argMax(attested_count, updated_date_time) AS d_attested_count,
            argMax(missed_count, updated_date_time) AS d_missed_count,
            argMax(head_correct_count, updated_date_time) AS d_head_correct_count,
            argMax(target_correct_count, updated_date_time) AS d_target_correct_count,
            argMax(source_correct_count, updated_date_time) AS d_source_correct_count,
            argMax(avg_inclusion_distance, updated_date_time) AS d_avg_inclusion_distance
        FROM {{ index .dep "{{transformation}}" "fct_attestation_vote_correctness_by_validator_daily" "helpers" "from" }}
        WHERE day_start_date >= (SELECT min_week FROM week_bounds)
          AND day_start_date < (SELECT max_week FROM week_bounds) + INTERVAL 7 DAY
        GROUP BY validator_index, day_start_date
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toMonday(day_start_date) AS week_start_date,
    validator_index,
    -- Aggregated metrics per week (sum of daily pre-aggregated counts)
    toUInt32(sum(d_total_duties)) AS total_duties,
    toUInt32(sum(d_attested_count)) AS attested_count,
    toUInt32(sum(d_missed_count)) AS missed_count,
    toUInt32(sum(d_head_correct_count)) AS head_correct_count,
    toUInt32(sum(d_target_correct_count)) AS target_correct_count,
    toUInt32(sum(d_source_correct_count)) AS source_correct_count,
    -- Weighted average: sum(avg * count) / sum(count)
    if(sum(d_attested_count) > 0,
       toFloat32(sum(d_avg_inclusion_distance * d_attested_count) / sum(d_attested_count)),
       NULL) AS avg_inclusion_distance
FROM per_day
GROUP BY week_start_date, validator_index
