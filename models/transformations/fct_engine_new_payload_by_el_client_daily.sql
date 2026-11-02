---
table: fct_engine_new_payload_by_el_client_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - engine_api
  - new_payload
  - el_client
dependencies:
  - "{{transformation}}.int_engine_new_payload"
---
-- Daily aggregation of engine_newPayload by execution client, the daily twin of
-- fct_engine_new_payload_by_el_client_hourly. Percentiles are computed exactly over
-- every observation in the day, not combined from hourly rows.
-- Rewrites every day the bounds touch so partial days are recomputed in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toStartOfDay(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY AS max_day
),
payloads AS (
    SELECT
        slot_start_date_time,
        block_hash,
        duration_ms,
        status,
        gas_used,
        gas_limit,
        tx_count,
        blob_count,
        node_class,
        meta_execution_implementation,
        meta_execution_version,
        meta_client_name
    FROM {{ index .dep "{{transformation}}" "int_engine_new_payload" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_day FROM day_bounds)
        AND slot_start_date_time < (SELECT max_day FROM day_bounds)
        AND meta_execution_implementation != ''
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
    meta_execution_implementation,
    meta_execution_version,
    node_class,
    toUInt32(uniqExact(slot_start_date_time, block_hash)) AS slot_count,
    toUInt32(count()) AS observation_count,
    toUInt32(uniqExact(meta_client_name)) AS unique_node_count,
    countIf(status = 'VALID') AS valid_count,
    countIf(status = 'INVALID') AS invalid_count,
    countIf(status = 'SYNCING') AS syncing_count,
    countIf(status = 'ACCEPTED') AS accepted_count,
    -- Duration statistics (VALID status only)
    toUInt64(ifNotFinite(round(avgIf(duration_ms, status = 'VALID')), 0)) AS avg_duration_ms,
    toUInt64(ifNotFinite(round(quantilesExactIf(0.5, 0.95)(duration_ms, status = 'VALID')[1]), 0)) AS p50_duration_ms,
    toUInt64(ifNotFinite(round(quantilesExactIf(0.5, 0.95)(duration_ms, status = 'VALID')[2]), 0)) AS p95_duration_ms,
    minIf(duration_ms, status = 'VALID') AS min_duration_ms,
    maxIf(duration_ms, status = 'VALID') AS max_duration_ms,
    -- Block complexity metrics (VALID status only)
    toUInt64(ifNotFinite(round(avgIf(gas_used, status = 'VALID')), 0)) AS avg_gas_used,
    toUInt64(ifNotFinite(round(avgIf(gas_limit, status = 'VALID')), 0)) AS avg_gas_limit,
    toFloat32(ifNotFinite(round(avgIf(tx_count, status = 'VALID'), 1), 0)) AS avg_tx_count,
    toFloat32(ifNotFinite(round(avgIf(blob_count, status = 'VALID'), 1), 0)) AS avg_blob_count
FROM payloads
GROUP BY day_start_date, meta_execution_implementation, meta_execution_version, node_class
