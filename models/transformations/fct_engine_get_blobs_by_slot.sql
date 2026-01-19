---
table: fct_engine_get_blobs_by_slot
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - engine_api
  - get_blobs
dependencies:
  - "{{external}}.execution_engine_get_blobs"
  - "{{external}}.beacon_api_eth_v1_events_blob_sidecar"
  - "{{external}}.canonical_beacon_blob_sidecar"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Combine versioned_hash->slot mappings from both sources
versioned_hash_context AS (
    -- Real-time source
    SELECT
        versioned_hash,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    UNION ALL
    -- Historical source
    SELECT
        versioned_hash,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root
    FROM {{ index .dep "{{external}}" "canonical_beacon_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),
-- Deduplicate to get unique versioned_hash->slot mapping
unique_versioned_hash_context AS (
    SELECT
        versioned_hash,
        argMax(slot, slot_start_date_time) AS slot,
        argMax(slot_start_date_time, slot_start_date_time) AS slot_start_date_time,
        argMax(epoch, slot_start_date_time) AS epoch,
        argMax(epoch_start_date_time, slot_start_date_time) AS epoch_start_date_time,
        argMax(block_root, slot_start_date_time) AS block_root
    FROM versioned_hash_context
    GROUP BY versioned_hash
),
-- Expand execution_engine_get_blobs versioned_hashes array and join with context
expanded_get_blobs AS (
    SELECT
        gb.updated_date_time,
        gb.event_date_time,
        gb.requested_date_time,
        gb.duration_ms,
        gb.requested_count,
        gb.returned_count,
        gb.status,
        gb.error_message,
        gb.method_version,
        gb.meta_client_name,
        gb.meta_client_implementation,
        gb.meta_execution_implementation,
        vh AS expanded_versioned_hash
    FROM {{ index .dep "{{external}}" "execution_engine_get_blobs" "helpers" "from" }} FINAL AS gb
    ARRAY JOIN gb.versioned_hashes AS vh
    WHERE gb.meta_network_name = '{{ .env.NETWORK }}'
        AND gb.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
),
-- Join with slot context (only need one match per get_blobs call)
enriched AS (
    SELECT
        eg.updated_date_time,
        eg.event_date_time,
        eg.requested_date_time,
        eg.duration_ms,
        eg.requested_count,
        eg.returned_count,
        eg.status,
        eg.error_message,
        eg.method_version,
        eg.meta_client_name,
        eg.meta_client_implementation,
        eg.meta_execution_implementation,
        COALESCE(vc.slot, 0) AS slot,
        COALESCE(vc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(vc.epoch, 0) AS epoch,
        COALESCE(vc.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
        COALESCE(vc.block_root, '') AS block_root
    FROM expanded_get_blobs eg
    LEFT JOIN unique_versioned_hash_context vc ON eg.expanded_versioned_hash = vc.versioned_hash
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, duration_ms) AS slot,
    slot_start_date_time,
    argMin(epoch, duration_ms) AS epoch,
    argMin(epoch_start_date_time, duration_ms) AS epoch_start_date_time,
    block_root,
    status,
    CASE WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class,
    -- Observation counts
    COUNT(*) AS observation_count,
    COUNT(DISTINCT meta_client_name) AS unique_node_count,
    -- Request/Response
    MAX(requested_count) AS max_requested_count,
    round(AVG(returned_count), 2) AS avg_returned_count,
    round(countIf(returned_count = requested_count AND requested_count > 0) * 100.0 / nullIf(countIf(requested_count > 0), 0), 2) AS full_return_pct,
    -- Duration statistics
    round(AVG(duration_ms)) AS avg_duration_ms,
    round(quantile(0.5)(duration_ms)) AS median_duration_ms,
    MIN(duration_ms) AS min_duration_ms,
    MAX(duration_ms) AS max_duration_ms,
    round(quantile(0.95)(duration_ms)) AS p95_duration_ms,
    -- Client diversity
    COUNT(DISTINCT meta_client_implementation) AS unique_cl_implementation_count,
    COUNT(DISTINCT meta_execution_implementation) AS unique_el_implementation_count
FROM enriched
WHERE slot_start_date_time != toDateTime(0)
GROUP BY slot_start_date_time, block_root, status, node_class
