---
table: fct_data_column_availability_daily
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - data_column
  - peerdas
  - custody
dependencies:
  - "{{transformation}}.fct_data_column_availability_by_epoch"
---
-- Rewrites every day the bounds touch so partial days are recounted in full.
-- Rolls the epochs up to hours exactly as fct_data_column_availability_hourly does, then the hours up to the day,
-- so hour_count and the averages of hourly values match a rollup of the hourly table.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toStartOfDay(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY AS max_day
),
hourly AS (
    SELECT
        toStartOfHour(epoch_start_date_time) AS hour_start_date_time,
        column_index,
        sum(total_probe_count) AS total_probe_count,
        sum(total_success_count) AS total_success_count,
        sum(total_failure_count) AS total_failure_count,
        sum(total_missing_count) AS total_missing_count,
        round(min(min_availability_pct), 2) AS min_availability_pct,
        round(max(max_availability_pct), 2) AS max_availability_pct,
        round(min(min_response_time_ms)) AS min_response_time_ms,
        round(avg(avg_p50_response_time_ms)) AS avg_p50_response_time_ms,
        round(avg(avg_p95_response_time_ms)) AS avg_p95_response_time_ms,
        round(avg(avg_p99_response_time_ms)) AS avg_p99_response_time_ms,
        round(max(max_response_time_ms)) AS max_response_time_ms,
        max(max_blob_count) AS max_blob_count
    FROM {{ index .dep "{{transformation}}" "fct_data_column_availability_by_epoch" "helpers" "from" }} FINAL
    WHERE epoch_start_date_time >= (SELECT min_day FROM day_bounds)
        AND epoch_start_date_time < (SELECT max_day FROM day_bounds)
    GROUP BY
        toStartOfHour(epoch_start_date_time),
        column_index
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    date,
    column_index,
    hour_count,
    round((total_success_count * 100.0 / total_probe_count), 2) AS avg_availability_pct,
    min_availability_pct,
    max_availability_pct,
    total_probe_count,
    total_success_count,
    total_failure_count,
    total_missing_count,
    min_response_time_ms,
    avg_p50_response_time_ms,
    avg_p95_response_time_ms,
    avg_p99_response_time_ms,
    max_response_time_ms,
    max_blob_count
FROM (
    SELECT
        toDate(hour_start_date_time) AS date,
        column_index,
        count(*) AS hour_count,
        sum(total_probe_count) AS total_probe_count,
        sum(total_success_count) AS total_success_count,
        sum(total_failure_count) AS total_failure_count,
        sum(total_missing_count) AS total_missing_count,
        round(min(min_availability_pct), 2) AS min_availability_pct,
        round(max(max_availability_pct), 2) AS max_availability_pct,
        round(min(min_response_time_ms)) AS min_response_time_ms,
        round(avg(avg_p50_response_time_ms)) AS avg_p50_response_time_ms,
        round(avg(avg_p95_response_time_ms)) AS avg_p95_response_time_ms,
        round(avg(avg_p99_response_time_ms)) AS avg_p99_response_time_ms,
        round(max(max_response_time_ms)) AS max_response_time_ms,
        max(max_blob_count) AS max_blob_count
    FROM hourly
    GROUP BY
        toDate(hour_start_date_time),
        column_index
)
-- Merge across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0
