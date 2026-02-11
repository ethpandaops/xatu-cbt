---
table: fct_validator_balance_daily
type: incremental
interval:
  type: slot
  max: 100000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - daily
  - validator
  - balance
  - validator_performance
dependencies:
  - "{{transformation}}.fct_validator_balance_hourly"
---
-- This query expands the hour range to complete day boundaries to handle partial
-- day aggregations at the head of incremental processing. The ReplacingMergeTree
-- will merge duplicates keeping the latest row.
-- Sources from hourly aggregates rather than raw epoch data since all aggregations
-- (min/max/argMin/argMax) are composable.
-- Uses argMax dedup instead of FINAL so the p_by_hour_start_date_time projection is used.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the day boundaries for the current hour range
    day_bounds AS (
        SELECT
            toDate(min(hour_start_date_time)) AS min_day,
            toDate(max(hour_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_validator_balance_hourly" "helpers" "from" }}
        WHERE hour_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),

    -- Deduplicated hourly data within the expanded day boundaries
    -- Uses argMax instead of FINAL so the projection on hour_start_date_time is used
    per_hour AS (
        SELECT
            validator_index,
            hour_start_date_time,
            argMax(start_epoch, updated_date_time) AS h_start_epoch,
            argMax(end_epoch, updated_date_time) AS h_end_epoch,
            argMax(start_balance, updated_date_time) AS h_start_balance,
            argMax(end_balance, updated_date_time) AS h_end_balance,
            argMax(min_balance, updated_date_time) AS h_min_balance,
            argMax(max_balance, updated_date_time) AS h_max_balance,
            argMax(effective_balance, updated_date_time) AS h_effective_balance,
            argMax(status, updated_date_time) AS h_status,
            argMax(slashed, updated_date_time) AS h_slashed
        FROM {{ index .dep "{{transformation}}" "fct_validator_balance_hourly" "helpers" "from" }}
        WHERE hour_start_date_time >= toDateTime((SELECT min_day FROM day_bounds))
          AND hour_start_date_time < toDateTime((SELECT max_day FROM day_bounds)) + INTERVAL 1 DAY
        GROUP BY validator_index, hour_start_date_time
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(hour_start_date_time) AS day_start_date,
    validator_index,
    -- Epoch range for the day
    min(h_start_epoch) AS start_epoch,
    max(h_end_epoch) AS end_epoch,
    -- Balance at start of day (from earliest hour)
    argMin(h_start_balance, h_start_epoch) AS start_balance,
    -- Balance at end of day (from latest hour)
    argMax(h_end_balance, h_end_epoch) AS end_balance,
    -- Min/max balance during the day
    min(h_min_balance) AS min_balance,
    max(h_max_balance) AS max_balance,
    -- End of day state (from latest hour)
    argMax(h_effective_balance, h_end_epoch) AS effective_balance,
    argMax(h_status, h_end_epoch) AS status,
    argMax(h_slashed, h_end_epoch) AS slashed
FROM per_hour
GROUP BY day_start_date, validator_index
SETTINGS max_bytes_before_external_group_by = 10000000000
