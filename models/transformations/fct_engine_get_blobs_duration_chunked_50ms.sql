---
table: fct_engine_get_blobs_duration_chunked_50ms
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - engine_api
  - get_blobs
  - chunked
dependencies:
  - "{{external}}.execution_engine_get_blobs"
  - "{{external}}.beacon_api_eth_v1_events_blob_sidecar"
  - "{{external}}.canonical_beacon_blob_sidecar"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get versioned_hash->slot context from BOTH blob sidecar sources
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
-- Deduplicate versioned_hash context
unique_vh_context AS (
    SELECT versioned_hash,
           argMax(slot, slot_start_date_time) AS slot,
           argMax(slot_start_date_time, slot_start_date_time) AS slot_start_date_time,
           argMax(epoch, slot_start_date_time) AS epoch,
           argMax(epoch_start_date_time, slot_start_date_time) AS epoch_start_date_time,
           argMax(block_root, slot_start_date_time) AS block_root
    FROM versioned_hash_context GROUP BY versioned_hash
),
-- Expand get_blobs versioned_hashes array and join with slot context
expanded AS (
    SELECT gb.*, vh AS exp_vh
    FROM {{ index .dep "{{external}}" "execution_engine_get_blobs" "helpers" "from" }} FINAL AS gb
    ARRAY JOIN gb.versioned_hashes AS vh
    WHERE gb.meta_network_name = '{{ .env.NETWORK }}'
        AND gb.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
),
-- Get getBlobs timing data joined with slot context
blobs AS (
    SELECT
        COALESCE(vc.slot, 0) AS slot,
        COALESCE(vc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(vc.epoch, 0) AS epoch,
        COALESCE(vc.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
        COALESCE(vc.block_root, '') AS block_root,
        e.duration_ms,
        e.status,
        CASE WHEN positionCaseInsensitive(e.meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class
    FROM expanded e
    LEFT JOIN unique_vh_context vc ON e.exp_vh = vc.versioned_hash
),

-- Group blobs into 50ms chunks
blobs_chunked AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        node_class,
        floor(duration_ms / 50) * 50 AS chunk_duration_ms,
        COUNT(*) as observation_count,
        countIf(status = 'SUCCESS') AS success_count,
        countIf(status = 'PARTIAL') AS partial_count,
        countIf(status = 'EMPTY') AS empty_count,
        countIf(status = 'ERROR' OR status = 'UNSUPPORTED') AS error_count
    FROM blobs
    WHERE slot_start_date_time != toDateTime(0)
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, node_class, chunk_duration_ms
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    node_class,
    toInt64(chunk_duration_ms) AS chunk_duration_ms,
    observation_count,
    success_count,
    partial_count,
    empty_count,
    error_count
FROM blobs_chunked
WHERE observation_count > 0
