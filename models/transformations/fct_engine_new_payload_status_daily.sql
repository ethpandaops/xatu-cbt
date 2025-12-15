---
table: fct_engine_new_payload_status_daily
type: scheduled
schedule: "@every 5m"
tags:
  - daily
  - engine_api
  - new_payload
dependencies:
  - "{{transformation}}.fct_engine_new_payload_by_slot"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    day_start_date,
    node_class,
    slot_count,
    observation_count,
    -- Status distribution
    valid_count,
    invalid_count,
    syncing_count,
    accepted_count,
    invalid_block_hash_count,
    round((valid_count * 100.0 / observation_count), 2) AS valid_pct,
    -- Duration statistics
    avg_duration_ms,
    avg_p50_duration_ms,
    avg_p95_duration_ms,
    max_duration_ms
FROM (
    SELECT
        toDate(slot_start_date_time) AS day_start_date,
        node_class,
        count(*) AS slot_count,
        sum(observation_count) AS observation_count,
        sum(valid_count) AS valid_count,
        sum(invalid_count) AS invalid_count,
        sum(syncing_count) AS syncing_count,
        sum(accepted_count) AS accepted_count,
        sum(invalid_block_hash_count) AS invalid_block_hash_count,
        round(avg(avg_duration_ms)) AS avg_duration_ms,
        round(avg(median_duration_ms)) AS avg_p50_duration_ms,
        round(avg(p95_duration_ms)) AS avg_p95_duration_ms,
        max(max_duration_ms) AS max_duration_ms
    FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_by_slot" "helpers" "from" }} FINAL
    WHERE toDate(slot_start_date_time) >= now() - INTERVAL {{ default "90" .env.ENGINE_API_DAILY_LOOKBACK_DAYS }} DAY
    GROUP BY toDate(slot_start_date_time), node_class
);

DELETE FROM `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }})
