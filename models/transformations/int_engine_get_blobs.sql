---
table: int_engine_get_blobs
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - engine
dependencies:
  - "{{external}}.execution_engine_get_blobs"
  - "{{external}}.beacon_api_eth_v1_beacon_blob"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Deduplicate beacon blob sidecar data to get versioned_hash -> slot context mapping
-- GROUP BY versioned_hash, take any() for slot context fields
blob_context AS (
    SELECT
        versioned_hash,
        any(slot) AS slot,
        any(slot_start_date_time) AS slot_start_date_time,
        any(epoch) AS epoch,
        any(epoch_start_date_time) AS epoch_start_date_time,
        any(block_root) AS block_root,
        any(block_parent_root) AS block_parent_root,
        any(proposer_index) AS proposer_index
    FROM (
        SELECT *
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_blob" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
            -- Use wider window to ensure we catch all blobs that might match engine events
            AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 5 MINUTE
                AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
    )
    GROUP BY versioned_hash
),

-- Get raw engine_getBlobs observations from the execution layer snooper
-- Use arrayJoin to expand versioned_hashes so we can join each hash to blob context
engine_get_blobs AS (
    SELECT
        updated_date_time AS source_updated_date_time,
        event_date_time,
        requested_date_time,
        duration_ms,
        versioned_hashes,
        returned_blob_indexes,
        length(versioned_hashes) AS requested_count,
        returned_count,
        status,
        error_message,
        method_version,
        source,
        meta_execution_version,
        meta_execution_implementation,
        meta_client_name,
        meta_client_implementation,
        meta_client_version,
        meta_client_geo_city,
        meta_client_geo_country,
        meta_client_geo_country_code,
        meta_client_geo_continent_code,
        meta_client_geo_latitude,
        meta_client_geo_longitude,
        meta_client_geo_autonomous_system_number,
        meta_client_geo_autonomous_system_organization,
        -- Expand versioned_hashes array for joining
        arrayJoin(versioned_hashes) AS vh
    FROM {{ index .dep "{{external}}" "execution_engine_get_blobs" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
        -- Filter execution events by the slot time window (with some buffer for timing differences)
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
        -- Skip events with empty versioned_hashes (arrayJoin on empty array produces 0 rows naturally,
        -- but this makes intent explicit)
        AND length(versioned_hashes) > 0
),

-- Join engine events with blob context to enrich with slot/block info
-- LEFT JOIN preserves all snooper observations even if blob context not yet available
-- GROUP BY collapses the arrayJoin expansion back to one row per original event
enriched AS (
    SELECT
        eg.source_updated_date_time,
        eg.event_date_time,
        eg.requested_date_time,
        eg.duration_ms,
        COALESCE(any(bc.slot), 0) AS slot,
        COALESCE(any(bc.slot_start_date_time), toDateTime(0)) AS slot_start_date_time,
        COALESCE(any(bc.epoch), 0) AS epoch,
        COALESCE(any(bc.epoch_start_date_time), toDateTime(0)) AS epoch_start_date_time,
        COALESCE(any(bc.block_root), '') AS block_root,
        COALESCE(any(bc.block_parent_root), '') AS block_parent_root,
        COALESCE(any(bc.proposer_index), 0) AS proposer_index,
        eg.requested_count,
        eg.versioned_hashes,
        eg.returned_count,
        eg.returned_blob_indexes,
        eg.status,
        eg.error_message,
        eg.method_version,
        eg.source,
        eg.meta_execution_version,
        eg.meta_execution_implementation,
        eg.meta_client_name,
        eg.meta_client_implementation,
        eg.meta_client_version,
        eg.meta_client_geo_city,
        eg.meta_client_geo_country,
        eg.meta_client_geo_country_code,
        eg.meta_client_geo_continent_code,
        eg.meta_client_geo_latitude,
        eg.meta_client_geo_longitude,
        eg.meta_client_geo_autonomous_system_number,
        eg.meta_client_geo_autonomous_system_organization
    FROM engine_get_blobs eg
    LEFT JOIN blob_context bc ON eg.vh = bc.versioned_hash
    GROUP BY
        eg.source_updated_date_time,
        eg.event_date_time,
        eg.requested_date_time,
        eg.duration_ms,
        eg.requested_count,
        eg.versioned_hashes,
        eg.returned_count,
        eg.returned_blob_indexes,
        eg.status,
        eg.error_message,
        eg.method_version,
        eg.source,
        eg.meta_execution_version,
        eg.meta_execution_implementation,
        eg.meta_client_name,
        eg.meta_client_implementation,
        eg.meta_client_version,
        eg.meta_client_geo_city,
        eg.meta_client_geo_country,
        eg.meta_client_geo_country_code,
        eg.meta_client_geo_continent_code,
        eg.meta_client_geo_latitude,
        eg.meta_client_geo_longitude,
        eg.meta_client_geo_autonomous_system_number,
        eg.meta_client_geo_autonomous_system_organization
)

-- Aggregate using argMax to deduplicate by ORDER BY key
-- GROUP BY columns (slot_start_date_time, block_root, meta_client_name, event_date_time) are selected directly
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    event_date_time,
    argMax(requested_date_time, source_updated_date_time) AS requested_date_time,
    argMax(duration_ms, source_updated_date_time) AS duration_ms,
    argMax(slot, source_updated_date_time) AS slot,
    slot_start_date_time,
    argMax(epoch, source_updated_date_time) AS epoch,
    argMax(epoch_start_date_time, source_updated_date_time) AS epoch_start_date_time,
    block_root,
    argMax(block_parent_root, source_updated_date_time) AS block_parent_root,
    argMax(proposer_index, source_updated_date_time) AS proposer_index,
    argMax(requested_count, source_updated_date_time) AS requested_count,
    argMax(versioned_hashes, source_updated_date_time) AS versioned_hashes,
    argMax(returned_count, source_updated_date_time) AS returned_count,
    argMax(returned_blob_indexes, source_updated_date_time) AS returned_blob_indexes,
    argMax(status, source_updated_date_time) AS status,
    argMax(error_message, source_updated_date_time) AS error_message,
    argMax(method_version, source_updated_date_time) AS method_version,
    argMax(source, source_updated_date_time) AS source,
    CASE WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class,
    argMax(meta_execution_version, source_updated_date_time) AS meta_execution_version,
    argMax(meta_execution_implementation, source_updated_date_time) AS meta_execution_implementation,
    meta_client_name,
    argMax(meta_client_implementation, source_updated_date_time) AS meta_client_implementation,
    argMax(meta_client_version, source_updated_date_time) AS meta_client_version,
    argMax(meta_client_geo_city, source_updated_date_time) AS meta_client_geo_city,
    argMax(meta_client_geo_country, source_updated_date_time) AS meta_client_geo_country,
    argMax(meta_client_geo_country_code, source_updated_date_time) AS meta_client_geo_country_code,
    argMax(meta_client_geo_continent_code, source_updated_date_time) AS meta_client_geo_continent_code,
    argMax(meta_client_geo_latitude, source_updated_date_time) AS meta_client_geo_latitude,
    argMax(meta_client_geo_longitude, source_updated_date_time) AS meta_client_geo_longitude,
    argMax(meta_client_geo_autonomous_system_number, source_updated_date_time) AS meta_client_geo_autonomous_system_number,
    argMax(meta_client_geo_autonomous_system_organization, source_updated_date_time) AS meta_client_geo_autonomous_system_organization
FROM enriched
-- Filter out records without slot context (orphaned or timing issues)
-- These will be picked up in subsequent backfill runs when the blob context becomes available
WHERE slot_start_date_time != toDateTime(0)
GROUP BY slot_start_date_time, block_root, meta_client_name, event_date_time
