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
  - "{{external}}.beacon_api_eth_v2_beacon_block"
  - "{{external}}.canonical_beacon_block"
  - "{{transformation}}.fct_block_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Step 1: Get slot context from BOTH beacon_api (real-time) AND canonical (complete historical)
-- This provides the CL context (slot, epoch, block_root, proposer_index) that execution_engine lacks
slot_context AS (
    -- Real-time source: beacon_api_eth_v2_beacon_block (captures blocks as seen, no finality wait)
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        parent_root AS parent_block_root,
        proposer_index,
        execution_payload_block_hash AS block_hash
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v2_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND execution_payload_block_hash IS NOT NULL
        AND execution_payload_block_hash != ''
    UNION ALL
    -- Historical source: canonical_beacon_block (finalized, complete coverage)
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        parent_root AS parent_block_root,
        proposer_index,
        execution_payload_block_hash AS block_hash
    FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND execution_payload_block_hash IS NOT NULL
        AND execution_payload_block_hash != ''
),

-- Step 2: Get raw engine_newPayload observations from the execution layer snooper
-- LEFT JOIN to slot_context - preserve all snooper observations even if CL context not yet available
raw_payloads AS (
    SELECT
        ep.updated_date_time AS source_updated_date_time,
        ep.event_date_time,
        ep.requested_date_time,
        ep.duration_ms,
        COALESCE(sc.slot, 0) AS slot,
        COALESCE(sc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(sc.epoch, 0) AS epoch,
        COALESCE(sc.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
        COALESCE(sc.block_root, '') AS block_root,
        ep.block_hash,
        ep.block_number,
        COALESCE(sc.parent_block_root, '') AS parent_block_root,
        ep.parent_hash,
        COALESCE(sc.proposer_index, 0) AS proposer_index,
        ep.gas_used,
        ep.gas_limit,
        ep.tx_count,
        ep.blob_count,
        ep.status,
        ep.validation_error,
        ep.latest_valid_hash,
        ep.method_version,
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
    LEFT JOIN slot_context sc ON ep.block_hash = sc.block_hash
    WHERE ep.meta_network_name = '{{ .env.NETWORK }}'
        -- Filter execution events by the slot time window (with some buffer for timing differences)
        AND ep.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
),

-- Step 3: Get block metadata (size, version) from fct_block_head
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

-- Step 4: Join raw payloads with block metadata
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

-- Step 5: Aggregate using argMax to deduplicate by ORDER BY key
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
