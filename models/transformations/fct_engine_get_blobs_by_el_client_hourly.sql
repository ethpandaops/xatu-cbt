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
  - "{{external}}.beacon_api_eth_v1_events_blob_sidecar"
  - "{{external}}.canonical_beacon_blob_sidecar"
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
-- Get versioned_hash->slot context from BOTH blob sidecar sources
versioned_hash_context AS (
    SELECT versioned_hash, slot_start_date_time, block_root
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    UNION ALL
    SELECT versioned_hash, slot_start_date_time, block_root
    FROM {{ index .dep "{{external}}" "canonical_beacon_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Deduplicate versioned_hash context
unique_vh_context AS (
    SELECT versioned_hash,
           argMax(slot_start_date_time, slot_start_date_time) AS slot_start_date_time,
           argMax(block_root, slot_start_date_time) AS block_root
    FROM versioned_hash_context GROUP BY versioned_hash
),
-- Expand get_blobs versioned_hashes array
expanded AS (
    SELECT gb.*, vh AS exp_vh
    FROM {{ index .dep "{{external}}" "execution_engine_get_blobs" "helpers" "from" }} FINAL AS gb
    ARRAY JOIN gb.versioned_hashes AS vh
    WHERE gb.meta_network_name = '{{ .env.NETWORK }}'
        AND gb.meta_execution_implementation != ''
        AND gb.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
),
-- Join with slot context
enriched AS (
    SELECT
        COALESCE(vc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(vc.block_root, '') AS block_root,
        e.duration_ms,
        e.status,
        e.returned_count,
        e.meta_execution_implementation,
        e.meta_execution_version,
        e.meta_client_name,
        CASE
            WHEN positionCaseInsensitive(e.meta_client_name, '7870') > 0 THEN 'eip7870-block-builder'
            ELSE ''
        END AS node_class
    FROM expanded e
    LEFT JOIN unique_vh_context vc ON e.exp_vh = vc.versioned_hash
),
-- Find the hour boundaries from the enriched data
hour_bounds AS (
    SELECT
        toStartOfHour(min(slot_start_date_time)) AS min_hour,
        toStartOfHour(max(slot_start_date_time)) AS max_hour
    FROM enriched
    WHERE slot_start_date_time != toDateTime(0)
),
-- Filter to complete hour boundaries
events_in_hours AS (
    SELECT *
    FROM enriched
    WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
      AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
      AND slot_start_date_time != toDateTime(0)
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
