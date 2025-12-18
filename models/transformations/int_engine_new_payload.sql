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
  - "{{transformation}}.fct_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Step 1: Get raw engine_newPayload observations
raw_payloads AS (
    SELECT
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

-- Step 2: Get block metadata (size, version, status) from fct_block
block_metadata AS (
    SELECT
        slot_start_date_time,
        block_root,
        block_total_bytes,
        block_total_bytes_compressed,
        block_version,
        status AS block_status
    FROM {{ index .dep "{{transformation}}" "fct_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
)

-- Step 3: Join and output enriched observations
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    rp.event_date_time AS event_date_time,
    rp.requested_date_time AS requested_date_time,
    rp.duration_ms AS duration_ms,
    rp.slot AS slot,
    rp.slot_start_date_time AS slot_start_date_time,
    rp.epoch AS epoch,
    rp.epoch_start_date_time AS epoch_start_date_time,
    rp.block_root AS block_root,
    rp.block_hash AS block_hash,
    rp.block_number AS block_number,
    rp.parent_block_root AS parent_block_root,
    rp.parent_hash AS parent_hash,
    rp.proposer_index AS proposer_index,
    rp.gas_used AS gas_used,
    rp.gas_limit AS gas_limit,
    rp.tx_count AS tx_count,
    rp.blob_count AS blob_count,
    rp.status AS status,
    rp.validation_error AS validation_error,
    rp.latest_valid_hash AS latest_valid_hash,
    rp.method_version AS method_version,
    bm.block_total_bytes AS block_total_bytes,
    bm.block_total_bytes_compressed AS block_total_bytes_compressed,
    COALESCE(bm.block_version, '') AS block_version,
    COALESCE(bm.block_status, 'unknown') AS block_status,
    CASE WHEN positionCaseInsensitive(rp.meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class,
    rp.meta_execution_version AS meta_execution_version,
    rp.meta_execution_implementation AS meta_execution_implementation,
    rp.meta_client_name AS meta_client_name,
    rp.meta_client_implementation AS meta_client_implementation,
    rp.meta_client_version AS meta_client_version,
    rp.meta_client_geo_city AS meta_client_geo_city,
    rp.meta_client_geo_country AS meta_client_geo_country,
    rp.meta_client_geo_country_code AS meta_client_geo_country_code,
    rp.meta_client_geo_continent_code AS meta_client_geo_continent_code,
    rp.meta_client_geo_latitude AS meta_client_geo_latitude,
    rp.meta_client_geo_longitude AS meta_client_geo_longitude,
    rp.meta_client_geo_autonomous_system_number AS meta_client_geo_autonomous_system_number,
    rp.meta_client_geo_autonomous_system_organization AS meta_client_geo_autonomous_system_organization
FROM raw_payloads rp
LEFT JOIN block_metadata bm
    ON rp.slot_start_date_time = bm.slot_start_date_time
    AND rp.block_root = bm.block_root
-- Optional filters (uncomment as needed):
-- Only store observations where engine_newPayload took longer than 500ms:
-- WHERE rp.duration_ms > 500
-- Only store VALID responses (successfully validated blocks):
-- WHERE rp.status = 'VALID'
-- Combine both filters (slow + valid only):
-- WHERE rp.duration_ms > 500 AND rp.status = 'VALID'
