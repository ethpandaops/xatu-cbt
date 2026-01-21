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
  - "{{external}}.consensus_engine_api_new_payload"
  - "{{transformation}}.fct_block_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Step 1: Get raw engine_newPayload observations with source updated_date_time
raw_payloads AS (
    SELECT
        updated_date_time AS source_updated_date_time,
        event_date_time,
        requested_date_time,
        duration_ms,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        block_hash,
        block_number,
        parent_block_root,
        parent_hash,
        proposer_index,
        gas_used,
        gas_limit,
        tx_count,
        blob_count,
        status,
        validation_error,
        latest_valid_hash,
        method_version,
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
        meta_client_geo_autonomous_system_organization
    FROM {{ index .dep "{{external}}" "consensus_engine_api_new_payload" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

-- Step 2: Get block metadata (size, version) from fct_block_head
block_metadata AS (
    SELECT
        slot_start_date_time,
        block_root,
        argMax(block_total_bytes, updated_date_time) AS block_total_bytes,
        argMax(block_total_bytes_compressed, updated_date_time) AS block_total_bytes_compressed,
        argMax(block_version, updated_date_time) AS block_version
    FROM {{ index .dep "{{transformation}}" "fct_block_head" "helpers" "from" }}
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY slot_start_date_time, block_root
),

-- Step 3: Join raw payloads with block metadata
enriched AS (
    SELECT
        rp.source_updated_date_time,
        rp.event_date_time,
        rp.requested_date_time,
        rp.duration_ms,
        rp.slot,
        rp.slot_start_date_time,
        rp.epoch,
        rp.epoch_start_date_time,
        rp.block_root,
        rp.block_hash,
        rp.block_number,
        rp.parent_block_root,
        rp.parent_hash,
        rp.proposer_index,
        rp.gas_used,
        rp.gas_limit,
        rp.tx_count,
        rp.blob_count,
        rp.status,
        rp.validation_error,
        rp.latest_valid_hash,
        rp.method_version,
        bm.block_total_bytes,
        bm.block_total_bytes_compressed,
        COALESCE(bm.block_version, '') AS block_version,
        rp.meta_execution_version,
        rp.meta_execution_implementation,
        rp.meta_client_name,
        rp.meta_client_implementation,
        rp.meta_client_version,
        rp.meta_client_geo_city,
        rp.meta_client_geo_country,
        rp.meta_client_geo_country_code,
        rp.meta_client_geo_continent_code,
        rp.meta_client_geo_latitude,
        rp.meta_client_geo_longitude,
        rp.meta_client_geo_autonomous_system_number,
        rp.meta_client_geo_autonomous_system_organization
    FROM raw_payloads rp
    LEFT JOIN block_metadata bm
        ON rp.slot_start_date_time = bm.slot_start_date_time
        AND rp.block_root = bm.block_root
)

-- Step 4: Aggregate using argMax to deduplicate by ORDER BY key
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
-- Optional filters (uncomment as needed):
-- Only store observations where engine_newPayload took longer than 500ms:
-- WHERE duration_ms > 500
-- Only store VALID responses (successfully validated blocks):
-- WHERE status = 'VALID'
-- Combine both filters (slow + valid only):
-- WHERE duration_ms > 500 AND status = 'VALID'
GROUP BY slot_start_date_time, block_hash, meta_client_name, event_date_time
