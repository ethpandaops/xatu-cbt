---
table: fct_sync_committee_participation_by_validator_daily
type: incremental
interval:
  type: slot
  max: 100000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - daily
  - sync_committee
  - canonical
  - validator_performance
dependencies:
  - "{{transformation}}.fct_sync_committee_participation_by_validator_hourly"
---
-- Sources from the hourly table instead of the per-slot external table to avoid OOM.
-- Uses argMax dedup on the hourly table (leveraging the p_by_hour_start_date_time
-- projection), then sums the pre-aggregated counts to daily granularity.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the day boundaries for the current slot range
    day_bounds AS (
        SELECT
            toDate(min(hour_start_date_time)) AS min_day,
            toDate(max(hour_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_sync_committee_participation_by_validator_hourly" "helpers" "from" }}
        WHERE hour_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),

    -- Deduplicated hourly data within the expanded day boundaries
    -- Uses argMax instead of FINAL so the projection on hour_start_date_time is used
    per_hour AS (
        SELECT
            validator_index,
            hour_start_date_time,
            argMax(total_slots, updated_date_time) AS h_total_slots,
            argMax(participated_count, updated_date_time) AS h_participated_count,
            argMax(missed_count, updated_date_time) AS h_missed_count
        FROM {{ index .dep "{{transformation}}" "fct_sync_committee_participation_by_validator_hourly" "helpers" "from" }}
        WHERE hour_start_date_time >= toDateTime((SELECT min_day FROM day_bounds))
          AND hour_start_date_time < toDateTime((SELECT max_day FROM day_bounds)) + INTERVAL 1 DAY
        GROUP BY validator_index, hour_start_date_time
    )

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(hour_start_date_time) AS day_start_date,
    validator_index,
    -- Aggregated metrics per day (sum of hourly pre-aggregated counts)
    toUInt32(sum(h_total_slots)) AS total_slots,
    toUInt32(sum(h_participated_count)) AS participated_count,
    toUInt32(sum(h_missed_count)) AS missed_count
FROM per_hour
GROUP BY day_start_date, validator_index
