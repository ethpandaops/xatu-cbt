---
table: fct_validator_balance_weekly
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - weekly
  - validator
  - balance
  - validator_performance
dependencies:
  - "{{transformation}}.fct_validator_balance_daily"
---
-- Sources from the daily table and expands the day range to complete week
-- boundaries (Monday-Sunday, UTC), so partial weeks at either edge of an interval
-- are recomputed in full whenever an interval touches them. The ReplacingMergeTree
-- keeps the latest row.
-- All aggregations (min/max/argMin/argMax) are composable from daily rows.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the week boundaries for the current range
    week_bounds AS (
        SELECT
            toMonday(min(day_start_date)) AS min_week,
            toMonday(max(day_start_date)) AS max_week
        FROM {{ index .dep "{{transformation}}" "fct_validator_balance_daily" "helpers" "from" }}
        WHERE day_start_date BETWEEN toDate(fromUnixTimestamp({{ .bounds.start }})) AND toDate(fromUnixTimestamp({{ .bounds.end }}))
    ),

    -- Deduplicated daily data within the expanded week boundaries
    per_day AS (
        SELECT
            validator_index,
            day_start_date,
            argMax(start_epoch, updated_date_time) AS d_start_epoch,
            argMax(end_epoch, updated_date_time) AS d_end_epoch,
            argMax(start_balance, updated_date_time) AS d_start_balance,
            argMax(end_balance, updated_date_time) AS d_end_balance,
            argMax(min_balance, updated_date_time) AS d_min_balance,
            argMax(max_balance, updated_date_time) AS d_max_balance,
            argMax(effective_balance, updated_date_time) AS d_effective_balance,
            argMax(status, updated_date_time) AS d_status,
            argMax(slashed, updated_date_time) AS d_slashed
        FROM {{ index .dep "{{transformation}}" "fct_validator_balance_daily" "helpers" "from" }}
        WHERE day_start_date >= (SELECT min_week FROM week_bounds)
          AND day_start_date < (SELECT max_week FROM week_bounds) + INTERVAL 7 DAY
        GROUP BY validator_index, day_start_date
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toMonday(day_start_date) AS week_start_date,
    validator_index,
    -- Epoch range for the week
    min(d_start_epoch) AS start_epoch,
    max(d_end_epoch) AS end_epoch,
    -- Balance at start of week (from earliest day)
    argMin(d_start_balance, d_start_epoch) AS start_balance,
    -- Balance at end of week (from latest day)
    argMax(d_end_balance, d_end_epoch) AS end_balance,
    -- Min/max balance during the week
    min(d_min_balance) AS min_balance,
    max(d_max_balance) AS max_balance,
    -- End of week state (from latest day)
    argMax(d_effective_balance, d_end_epoch) AS effective_balance,
    argMax(d_status, d_end_epoch) AS status,
    argMax(d_slashed, d_end_epoch) AS slashed
FROM per_day
GROUP BY week_start_date, validator_index
SETTINGS max_bytes_before_external_group_by = 10000000000
