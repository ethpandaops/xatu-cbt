---
table: fct_engine_new_payload_by_node_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - engine_api
  - new_payload
  - node
dependencies:
  - "{{transformation}}.int_engine_new_payload"
---
-- engine_newPayload calls from each node to its execution client, one row per node per hour.
-- Rewrites every hour the bounds touch so partial hours are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
hour_bounds AS (
    SELECT
        toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_hour,
        toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 HOUR AS max_hour
),
calls AS (
    SELECT
        *,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN splitByChar('/', meta_client_name)[3]
        WHEN startsWith(meta_client_name, 'corp-') THEN splitByChar('/', meta_client_name)[3]
        ELSE splitByChar('/', meta_client_name)[-1]
    END AS node_id,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN splitByChar('/', meta_client_name)[2]
        WHEN startsWith(meta_client_name, 'corp-') THEN splitByChar('/', meta_client_name)[2]
        ELSE 'ethpandaops'
    END AS username,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN 'individual'
        WHEN startsWith(meta_client_name, 'corp-') THEN 'corporate'
        WHEN startsWith(meta_client_name, 'ethpandaops') THEN 'internal'
        ELSE 'unclassified'
    END AS classification
    FROM {{ index .dep "{{transformation}}" "int_engine_new_payload" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
        AND slot_start_date_time < (SELECT max_hour FROM hour_bounds)
        AND meta_client_name != ''
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    node_id,
    argMax(username, slot_start_date_time) AS username,
    argMax(classification, slot_start_date_time) AS classification,
    argMax(meta_client_name, slot_start_date_time) AS meta_client_name,
    argMax(meta_client_implementation, slot_start_date_time) AS meta_client_implementation,
    argMax(meta_client_version, slot_start_date_time) AS meta_client_version,
    argMax(meta_execution_implementation, slot_start_date_time) AS meta_execution_implementation,
    argMax(meta_execution_version, slot_start_date_time) AS meta_execution_version,
    toUInt32(uniqExact(slot)) AS slot_count,
    toUInt32(count()) AS call_count,
    toUInt32(countIf(status = 'VALID')) AS valid_count,
    toUInt32(countIf(status = 'INVALID')) AS invalid_count,
    toUInt32(countIf(status = 'SYNCING')) AS syncing_count,
    toUInt32(countIf(status = 'ACCEPTED')) AS accepted_count,
    toUInt32(ifNotFinite(round(quantilesIf(0.5, 0.9, 0.95)(duration_ms, status = 'VALID')[1]), 0)) AS p50_duration_ms,
    toUInt32(ifNotFinite(round(quantilesIf(0.5, 0.9, 0.95)(duration_ms, status = 'VALID')[2]), 0)) AS p90_duration_ms,
    toUInt32(ifNotFinite(round(quantilesIf(0.5, 0.9, 0.95)(duration_ms, status = 'VALID')[3]), 0)) AS p95_duration_ms,
    toUInt32(maxIf(duration_ms, status = 'VALID')) AS max_duration_ms
FROM calls
GROUP BY hour_start_date_time, node_id
