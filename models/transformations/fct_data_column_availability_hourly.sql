---
table: fct_data_column_availability_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - data_column
  - peerdas
  - custody
dependencies:
  - "{{transformation}}.fct_data_column_availability_by_epoch"
---
-- Rewrites every hour the bounds touch so partial hours are recounted in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
hour_bounds AS (
    SELECT
        toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_hour,
        toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 HOUR AS max_hour
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    hour_start_date_time,
    column_index,
    epoch_count,
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
        toStartOfHour(epoch_start_date_time) AS hour_start_date_time,
        column_index,
        count(*) AS epoch_count,
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
    WHERE epoch_start_date_time >= (SELECT min_hour FROM hour_bounds)
        AND epoch_start_date_time < (SELECT max_hour FROM hour_bounds)
    GROUP BY
        toStartOfHour(epoch_start_date_time),
        column_index
)
-- Merge across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0
