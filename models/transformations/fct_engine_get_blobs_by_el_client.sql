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
  - "{{external}}.beacon_api_eth_v1_events_blob_sidecar"
  - "{{external}}.canonical_beacon_blob_sidecar"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
versioned_hash_context AS (
    SELECT versioned_hash, slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
    UNION ALL
    SELECT versioned_hash, slot, slot_start_date_time, epoch, epoch_start_date_time, block_root
    FROM {{ index .dep "{{external}}" "canonical_beacon_blob_sidecar" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),
unique_vh_context AS (
    SELECT versioned_hash,
           argMax(slot, slot_start_date_time) AS slot,
           argMax(slot_start_date_time, slot_start_date_time) AS slot_start_date_time,
           argMax(epoch, slot_start_date_time) AS epoch,
           argMax(epoch_start_date_time, slot_start_date_time) AS epoch_start_date_time,
           argMax(block_root, slot_start_date_time) AS block_root
    FROM versioned_hash_context GROUP BY versioned_hash
),
expanded AS (
    SELECT gb.*, vh AS exp_vh
    FROM {{ index .dep "{{external}}" "execution_engine_get_blobs" "helpers" "from" }} FINAL AS gb
    ARRAY JOIN gb.versioned_hashes AS vh
    WHERE gb.meta_network_name = '{{ .env.NETWORK }}'
        AND gb.meta_execution_implementation != ''
        AND gb.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
),
enriched AS (
    SELECT
        COALESCE(vc.slot, 0) AS slot,
        COALESCE(vc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(vc.epoch, 0) AS epoch,
        COALESCE(vc.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
        COALESCE(vc.block_root, '') AS block_root,
        e.meta_execution_implementation,
        e.meta_execution_version,
        e.status,
        e.meta_client_name,
        e.requested_count,
        e.returned_count,
        e.duration_ms
    FROM expanded e
    LEFT JOIN unique_vh_context vc ON e.exp_vh = vc.versioned_hash
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
GROUP BY slot_start_date_time, block_root, meta_execution_implementation, meta_execution_version, status, node_class
