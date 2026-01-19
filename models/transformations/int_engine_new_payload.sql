---
table: int_engine_new_payload
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
  - "{{external}}.execution_engine_new_payload"
  - "{{transformation}}.fct_block_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get slot context and block metadata from fct_block_head
-- This provides CL context (slot, epoch, block_root, proposer_index) that execution_engine lacks
-- Join on execution_payload_block_hash to correlate EL block hash with CL slot
block_context AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        parent_root,
        proposer_index,
        execution_payload_block_hash,
        block_total_bytes,
        block_total_bytes_compressed,
        block_version
    FROM {{ index .dep "{{transformation}}" "fct_block_head" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND execution_payload_block_hash IS NOT NULL
        AND execution_payload_block_hash != ''
),

-- Get raw engine_newPayload observations from the execution layer snooper
-- LEFT JOIN to block_context - preserve all snooper observations even if CL context not yet available
enriched AS (
    SELECT
        ep.updated_date_time AS source_updated_date_time,
        ep.event_date_time,
        ep.requested_date_time,
        ep.duration_ms,
        COALESCE(bc.slot, 0) AS slot,
        COALESCE(bc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(bc.epoch, 0) AS epoch,
        COALESCE(bc.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
        COALESCE(bc.block_root, '') AS block_root,
        ep.block_hash,
        ep.block_number,
        COALESCE(bc.parent_root, '') AS parent_block_root,
        ep.parent_hash,
        COALESCE(bc.proposer_index, 0) AS proposer_index,
        ep.gas_used,
        ep.gas_limit,
        ep.tx_count,
        ep.blob_count,
        ep.status,
        ep.validation_error,
        ep.latest_valid_hash,
        ep.method_version,
        COALESCE(bc.block_total_bytes, 0) AS block_total_bytes,
        COALESCE(bc.block_total_bytes_compressed, 0) AS block_total_bytes_compressed,
        COALESCE(bc.block_version, '') AS block_version,
        ep.meta_execution_version,
        ep.meta_execution_implementation,
        ep.meta_client_name,
        ep.meta_client_implementation,
        ep.meta_client_version,
        ep.meta_client_geo_city,
        ep.meta_client_geo_country,
        ep.meta_client_geo_country_code,
        ep.meta_client_geo_continent_code,
        ep.meta_client_geo_latitude,
        ep.meta_client_geo_longitude,
        ep.meta_client_geo_autonomous_system_number,
        ep.meta_client_geo_autonomous_system_organization
    FROM {{ index .dep "{{external}}" "execution_engine_new_payload" "helpers" "from" }} FINAL AS ep
    LEFT JOIN block_context bc ON ep.block_hash = bc.execution_payload_block_hash
    WHERE ep.meta_network_name = '{{ .env.NETWORK }}'
        -- Filter execution events by the slot time window (with some buffer for timing differences)
        AND ep.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
)

-- Aggregate using argMax to deduplicate by ORDER BY key
-- GROUP BY columns (slot_start_date_time, block_hash, meta_client_name, event_date_time) are selected directly
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    event_date_time,
    argMax(requested_date_time, source_updated_date_time) AS requested_date_time,
    argMax(duration_ms, source_updated_date_time) AS duration_ms,
    argMax(slot, source_updated_date_time) AS slot,
    slot_start_date_time,
    argMax(epoch, source_updated_date_time) AS epoch,
    argMax(epoch_start_date_time, source_updated_date_time) AS epoch_start_date_time,
    argMax(block_root, source_updated_date_time) AS block_root,
    block_hash,
    argMax(block_number, source_updated_date_time) AS block_number,
    argMax(parent_block_root, source_updated_date_time) AS parent_block_root,
    argMax(parent_hash, source_updated_date_time) AS parent_hash,
    argMax(proposer_index, source_updated_date_time) AS proposer_index,
    argMax(gas_used, source_updated_date_time) AS gas_used,
    argMax(gas_limit, source_updated_date_time) AS gas_limit,
    argMax(tx_count, source_updated_date_time) AS tx_count,
    argMax(blob_count, source_updated_date_time) AS blob_count,
    argMax(status, source_updated_date_time) AS status,
    argMax(validation_error, source_updated_date_time) AS validation_error,
    argMax(latest_valid_hash, source_updated_date_time) AS latest_valid_hash,
    argMax(method_version, source_updated_date_time) AS method_version,
    argMax(block_total_bytes, source_updated_date_time) AS block_total_bytes,
    argMax(block_total_bytes_compressed, source_updated_date_time) AS block_total_bytes_compressed,
    argMax(block_version, source_updated_date_time) AS block_version,
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
-- Filter out records without slot context (orphaned blocks or timing issues)
-- These will be picked up in subsequent backfill runs when the CL context becomes available
WHERE slot_start_date_time != toDateTime(0)
GROUP BY slot_start_date_time, block_hash, meta_client_name, event_date_time
