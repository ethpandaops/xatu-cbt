---
table: fct_engine_get_blobs_by_el_client_daily
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
  - get_blobs
  - el_client
dependencies:
  - "{{transformation}}.int_engine_get_blobs"
---
-- Daily aggregation of engine_getBlobs by execution client, the daily twin of
-- fct_engine_get_blobs_by_el_client_hourly. Percentiles are computed exactly over
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
calls AS (
    SELECT
        slot_start_date_time,
        block_root,
        duration_ms,
        status,
        returned_count,
        node_class,
        meta_execution_implementation,
        meta_execution_version,
        meta_client_name
    FROM {{ index .dep "{{transformation}}" "int_engine_get_blobs" "helpers" "from" }} FINAL
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
    toUInt32(uniqExact(slot_start_date_time, block_root)) AS slot_count,
    toUInt32(count()) AS observation_count,
    toUInt32(uniqExact(meta_client_name)) AS unique_node_count,
    countIf(status = 'SUCCESS') AS success_count,
    countIf(status = 'PARTIAL') AS partial_count,
    countIf(status = 'EMPTY') AS empty_count,
    countIf(status = 'UNSUPPORTED') AS unsupported_count,
    countIf(status = 'ERROR') AS error_count,
    -- Blob count (SUCCESS status only)
    ifNotFinite(round(avgIf(returned_count, status = 'SUCCESS'), 2), 0) AS avg_returned_count,
    -- Duration statistics (SUCCESS status only)
    toUInt64(ifNotFinite(round(avgIf(duration_ms, status = 'SUCCESS')), 0)) AS avg_duration_ms,
    toUInt64(ifNotFinite(round(quantilesExactIf(0.5, 0.95)(duration_ms, status = 'SUCCESS')[1]), 0)) AS p50_duration_ms,
    toUInt64(ifNotFinite(round(quantilesExactIf(0.5, 0.95)(duration_ms, status = 'SUCCESS')[2]), 0)) AS p95_duration_ms,
    toUInt64(minIf(duration_ms, status = 'SUCCESS')) AS min_duration_ms,
    toUInt64(maxIf(duration_ms, status = 'SUCCESS')) AS max_duration_ms
FROM calls
GROUP BY day_start_date, meta_execution_implementation, meta_execution_version, node_class
