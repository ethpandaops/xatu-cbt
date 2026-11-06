---
table: fct_data_column_availability_by_epoch
type: scheduled
schedule: "@every 1h"
tags:
  - epoch
  - data_column
  - peerdas
  - custody
dependencies:
  - "{{transformation}}.fct_data_column_availability_by_slot"
---
-- Custody probes keep arriving for about 18 days after a slot, so every run recounts the last
-- DATA_COLUMN_AVAILABILITY_LOOKBACK_DAYS days (default 19) and today from the source. Older rows keep their last
-- count, which no longer changes.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    epoch,
    epoch_start_date_time,
    min_slot_start_date_time as slot_start_date_time,
    column_index,
    slot_count,
    total_probe_count,
    total_success_count,
    total_failure_count,
    total_missing_count,
    avg_availability_pct,
    min_availability_pct,
    max_availability_pct,
    min_response_time_ms,
    avg_p50_response_time_ms,
    avg_p95_response_time_ms,
    avg_p99_response_time_ms,
    max_response_time_ms,
    max_blob_count
FROM (
    SELECT
        epoch,
        epoch_start_date_time,
        -- This is min(slot_start_date_time) across all slots in the epoch
        -- Using different alias to avoid ClickHouse ILLEGAL_AGGREGATION error
        min(slot_start_date_time) AS min_slot_start_date_time,
        column_index,
        count(*) AS slot_count,
        sum(probe_count) AS total_probe_count,
        sum(success_count) AS total_success_count,
        sum(failure_count) AS total_failure_count,
        sum(missing_count) AS total_missing_count,
        round((sum(success_count) * 100.0 / sum(probe_count)), 2) AS avg_availability_pct,
        round(min(availability_pct), 2) AS min_availability_pct,
        round(max(availability_pct), 2) AS max_availability_pct,
        round(min(min_response_time_ms)) AS min_response_time_ms,
        round(avg(p50_response_time_ms)) AS avg_p50_response_time_ms,
        round(avg(p95_response_time_ms)) AS avg_p95_response_time_ms,
        round(avg(p99_response_time_ms)) AS avg_p99_response_time_ms,
        round(max(max_response_time_ms)) AS max_response_time_ms,
        max(blob_count) AS max_blob_count
    FROM {{ index .dep "{{transformation}}" "fct_data_column_availability_by_slot" "helpers" "from" }} FINAL
    WHERE epoch_start_date_time >= toStartOfDay(fromUnixTimestamp({{ .task.start }})) - INTERVAL {{ default "19" .env.DATA_COLUMN_AVAILABILITY_LOOKBACK_DAYS }} DAY
    GROUP BY
        epoch,
        epoch_start_date_time,
        column_index
)
