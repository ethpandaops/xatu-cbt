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
        duration_ms,
        versioned_hashes,
        status,
        meta_client_name,
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
        eg.status,
        eg.meta_client_name,
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
        eg.status,
        eg.meta_client_name,
        eg.versioned_hashes
),
-- Get getBlobs timing data
blobs AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        duration_ms,
        status,
        CASE WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class
    FROM enriched
    WHERE slot_start_date_time != toDateTime(0)
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
