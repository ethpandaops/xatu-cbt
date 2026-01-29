---
table: fct_engine_get_blobs_by_el_client
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
  - el_client
dependencies:
  - "{{external}}.execution_engine_get_blobs"
  - "{{external}}.beacon_api_eth_v1_beacon_blob"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
blob_context AS (
    SELECT
        versioned_hash,
        any(slot) AS slot,
        any(slot_start_date_time) AS slot_start_date_time,
        any(epoch) AS epoch,
        any(epoch_start_date_time) AS epoch_start_date_time,
        any(block_root) AS block_root
    FROM (
        SELECT *
        FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_beacon_blob" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
            AND slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 5 MINUTE
                AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
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
        length(versioned_hashes) AS requested_count,
        returned_count,
        status,
        meta_client_name,
        meta_execution_implementation,
        meta_execution_version,
        arrayJoin(versioned_hashes) AS vh
    FROM {{ index .dep "{{external}}" "execution_engine_get_blobs" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
        AND length(versioned_hashes) > 0
),
enriched AS (
    SELECT
        eg.event_date_time,
        eg.duration_ms,
        eg.requested_count,
        eg.returned_count,
        eg.status,
        eg.meta_client_name,
        eg.meta_execution_implementation,
        eg.meta_execution_version,
        COALESCE(any(bc.slot), 0) AS slot,
        COALESCE(any(bc.slot_start_date_time), toDateTime(0)) AS slot_start_date_time,
        COALESCE(any(bc.epoch), 0) AS epoch,
        COALESCE(any(bc.epoch_start_date_time), toDateTime(0)) AS epoch_start_date_time,
        COALESCE(any(bc.block_root), '') AS block_root
    FROM engine_get_blobs eg
    LEFT JOIN blob_context bc ON eg.vh = bc.versioned_hash
    GROUP BY
        eg.event_date_time,
        eg.duration_ms,
        eg.requested_count,
        eg.returned_count,
        eg.status,
        eg.meta_client_name,
        eg.meta_execution_implementation,
        eg.meta_execution_version,
        eg.versioned_hashes
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, duration_ms) AS slot,
    slot_start_date_time,
    argMin(epoch, duration_ms) AS epoch,
    argMin(epoch_start_date_time, duration_ms) AS epoch_start_date_time,
    block_root,
    meta_execution_implementation,
    meta_execution_version,
    status,
    CASE WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class,
    -- Observation counts
    COUNT(*) AS observation_count,
    COUNT(DISTINCT meta_client_name) AS unique_node_count,
    -- Request/Response
    MAX(requested_count) AS max_requested_count,
    round(AVG(returned_count), 2) AS avg_returned_count,
    -- Duration statistics
    round(AVG(duration_ms)) AS avg_duration_ms,
    round(quantile(0.5)(duration_ms)) AS median_duration_ms,
    MIN(duration_ms) AS min_duration_ms,
    MAX(duration_ms) AS max_duration_ms,
    round(quantile(0.95)(duration_ms)) AS p95_duration_ms
FROM enriched
WHERE slot_start_date_time != toDateTime(0)
    AND meta_execution_implementation != ''
GROUP BY slot_start_date_time, block_root, meta_execution_implementation, meta_execution_version, status, node_class
