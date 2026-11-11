---
table: fct_sync_committee_participation_by_validator_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - sync_committee
  - canonical
  - validator_performance
dependencies:
  - "{{transformation}}.fct_sync_committee_participation_by_validator_daily"
---
-- Sources from the daily table and expands the day range to complete week
-- boundaries (Monday-Sunday, UTC), so partial weeks at either edge of an interval
-- are recomputed in full whenever an interval touches them. The ReplacingMergeTree
-- keeps the latest row.
-- Uses argMax dedup on the daily table, then sums the pre-aggregated counts to
-- weekly granularity.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the week boundaries for the current range
    week_bounds AS (
        SELECT
            toMonday(min(day_start_date)) AS min_week,
            toMonday(max(day_start_date)) AS max_week
        FROM {{ index .dep "{{transformation}}" "fct_sync_committee_participation_by_validator_daily" "helpers" "from" }}
        WHERE day_start_date BETWEEN toDate(fromUnixTimestamp({{ .bounds.start }})) AND toDate(fromUnixTimestamp({{ .bounds.end }}))
    ),

    -- Deduplicated daily data within the expanded week boundaries
    per_day AS (
        SELECT
            validator_index,
            day_start_date,
            argMax(total_slots, updated_date_time) AS d_total_slots,
            argMax(participated_count, updated_date_time) AS d_participated_count,
            argMax(missed_count, updated_date_time) AS d_missed_count
        FROM {{ index .dep "{{transformation}}" "fct_sync_committee_participation_by_validator_daily" "helpers" "from" }}
        WHERE day_start_date >= (SELECT min_week FROM week_bounds)
          AND day_start_date < (SELECT max_week FROM week_bounds) + INTERVAL 7 DAY
        GROUP BY validator_index, day_start_date
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toMonday(day_start_date) AS week_start_date,
    validator_index,
    -- Aggregated metrics per week (sum of daily pre-aggregated counts)
    toUInt32(sum(d_total_slots)) AS total_slots,
    toUInt32(sum(d_participated_count)) AS participated_count,
    toUInt32(sum(d_missed_count)) AS missed_count
FROM per_day
GROUP BY week_start_date, validator_index
