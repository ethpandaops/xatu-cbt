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
  - "{{external}}.execution_engine_get_blobs"
  - "{{external}}.beacon_api_eth_v1_beacon_blob"
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
blob_context AS (
    SELECT
        versioned_hash,
        any(slot) AS slot,
        any(slot_start_date_time) AS slot_start_date_time,
        any(block_root) AS block_root
    FROM (
        SELECT *
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_blob" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
            AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
                AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
    )
    GROUP BY versioned_hash
),
engine_get_blobs AS (
    SELECT
        event_date_time,
        -- Use computed duration from timestamps as the snooper's duration_ms field
        -- underreports getBlobs timing (measures only EL compute, not data transfer)
        toUInt32(toUnixTimestamp64Milli(event_date_time) - toUnixTimestamp64Milli(requested_date_time)) AS duration_ms,
        versioned_hashes,
        returned_count,
        status,
        meta_client_name,
        meta_execution_implementation,
        meta_execution_version,
        arrayJoin(versioned_hashes) AS vh
    FROM {{ index .dep "{{external}}" "execution_engine_get_blobs" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
        AND length(versioned_hashes) > 0
),
enriched AS (
    SELECT
        eg.event_date_time,
        eg.duration_ms,
        eg.returned_count,
        eg.status,
        eg.meta_client_name,
        eg.meta_execution_implementation,
        eg.meta_execution_version,
        COALESCE(any(bc.slot_start_date_time), toDateTime(0)) AS slot_start_date_time,
        COALESCE(any(bc.block_root), '') AS block_root
    FROM engine_get_blobs eg
    LEFT JOIN blob_context bc ON eg.vh = bc.versioned_hash
    GROUP BY
        eg.event_date_time,
        eg.duration_ms,
        eg.returned_count,
        eg.status,
        eg.meta_client_name,
        eg.meta_execution_implementation,
        eg.meta_execution_version,
        eg.versioned_hashes
),
-- Find the hour boundaries from the enriched data within the current bounds
hour_bounds AS (
    SELECT
        toStartOfHour(min(slot_start_date_time)) AS min_hour,
        toStartOfHour(max(slot_start_date_time)) AS max_hour
    FROM enriched
    WHERE slot_start_date_time != toDateTime(0)
        AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
),
-- Filter to complete hour boundaries
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
    FROM enriched
    WHERE slot_start_date_time != toDateTime(0)
        AND slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
        AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
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
