---
table: fct_engine_get_blobs_status_hourly
type: scheduled
schedule: "@every 5m"
tags:
  - hourly
  - engine_api
  - get_blobs
dependencies:
  - "{{transformation}}.fct_engine_get_blobs_by_slot"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    hour_start_date_time,
    slot_count,
    observation_count,
    -- Status distribution
    success_count,
    partial_count,
    empty_count,
    unsupported_count,
    error_count,
    round((success_count * 100.0 / observation_count), 2) AS success_pct,
    -- Duration statistics
    avg_duration_ms,
    avg_p50_duration_ms,
    avg_p95_duration_ms,
    max_duration_ms
FROM (
    SELECT
        toStartOfHour(slot_start_date_time) AS hour_start_date_time,
        count(*) AS slot_count,
        sum(observation_count) AS observation_count,
        sum(success_count) AS success_count,
        sum(partial_count) AS partial_count,
        sum(empty_count) AS empty_count,
        sum(unsupported_count) AS unsupported_count,
        sum(error_count) AS error_count,
        round(avg(avg_duration_ms)) AS avg_duration_ms,
        round(avg(median_duration_ms)) AS avg_p50_duration_ms,
        round(avg(p95_duration_ms)) AS avg_p95_duration_ms,
        max(max_duration_ms) AS max_duration_ms
    FROM {{ index .dep "{{transformation}}" "fct_engine_get_blobs_by_slot" "helpers" "from" }} FINAL
    WHERE toStartOfHour(slot_start_date_time) >= now() - INTERVAL {{ default "19" .env.ENGINE_API_LOOKBACK_DAYS }} DAY
    GROUP BY toStartOfHour(slot_start_date_time)
);

DELETE FROM `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }})
