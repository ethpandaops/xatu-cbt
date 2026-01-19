---
table: fct_engine_get_blobs_by_el_client_hourly
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - hourly
  - engine_api
  - get_blobs
  - el_client
dependencies:
  - "{{external}}.consensus_engine_api_get_blobs"
---
-- Hourly aggregation of engine_getBlobs by execution client.
-- Computes TRUE percentiles (p50, p95) across all observations in each hour per client,
-- enabling accurate percentile comparisons across execution clients.
--
-- This query expands the slot range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing. For example, if we process
-- slots spanning 11:46-12:30, we expand to include ALL slots from 11:00-12:59
-- so that hour 11:00 (which was partial in the previous run) gets re-aggregated with
-- complete data. The ReplacingMergeTree will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries from the data within the current bounds
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{external}}" "consensus_engine_api_get_blobs" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),
    -- Find ALL events that fall within those hour boundaries (expanding to complete hours)
    events_in_hours AS (
        SELECT
            slot_start_date_time,
            block_root,
            duration_ms,
            status,
            returned_count,
            meta_execution_implementation,
            meta_execution_version,
            meta_client_name,
            CASE
                WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870-block-builder'
                ELSE ''
            END AS node_class
        FROM {{ index .dep "{{external}}" "consensus_engine_api_get_blobs" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND meta_execution_implementation != ''
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(slot_start_date_time) AS hour_start_date_time,
    meta_execution_implementation,
    meta_execution_version,
    node_class,
    count(DISTINCT slot_start_date_time, block_root) AS slot_count,
    count(*) AS observation_count,
    count(DISTINCT meta_client_name) AS unique_node_count,
    -- Status counts
    countIf(status = 'SUCCESS') AS success_count,
    countIf(status = 'PARTIAL') AS partial_count,
    countIf(status = 'EMPTY') AS empty_count,
    countIf(status = 'UNSUPPORTED') AS unsupported_count,
    countIf(status = 'ERROR') AS error_count,
    -- Blob count (SUCCESS status only)
    ifNotFinite(round(avgIf(returned_count, status = 'SUCCESS'), 2), 0) AS avg_returned_count,
    -- Duration statistics (SUCCESS status only) - TRUE percentiles
    ifNotFinite(round(avgIf(duration_ms, status = 'SUCCESS')), 0) AS avg_duration_ms,
    ifNotFinite(round(quantileIf(0.5)(duration_ms, status = 'SUCCESS')), 0) AS p50_duration_ms,
    ifNotFinite(round(quantileIf(0.95)(duration_ms, status = 'SUCCESS')), 0) AS p95_duration_ms,
    ifNotFinite(minIf(duration_ms, status = 'SUCCESS'), 0) AS min_duration_ms,
    ifNotFinite(maxIf(duration_ms, status = 'SUCCESS'), 0) AS max_duration_ms
FROM events_in_hours
GROUP BY
    toStartOfHour(slot_start_date_time),
    meta_execution_implementation,
    meta_execution_version,
    node_class
