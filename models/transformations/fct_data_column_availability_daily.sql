---
table: fct_data_column_availability_daily
type: scheduled
schedule: "@every 1h"
tags:
  - daily
  - data_column
  - peerdas
  - custody
dependencies:
  - "{{transformation}}.fct_data_column_availability_by_epoch"
---
-- Custody probes keep arriving for about 18 days after a slot, so every run recounts the last
-- DATA_COLUMN_AVAILABILITY_LOOKBACK_DAYS days (default 19) and today from the source. Older rows keep their last
-- count, which no longer changes.
-- Rolls the epochs up to hours exactly as fct_data_column_availability_hourly does, then the hours up to the day,
-- so hour_count and the averages of hourly values match a rollup of the hourly table.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
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
    WHERE epoch_start_date_time >= toStartOfDay(fromUnixTimestamp({{ .task.start }})) - INTERVAL {{ default "19" .env.DATA_COLUMN_AVAILABILITY_LOOKBACK_DAYS }} DAY
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
