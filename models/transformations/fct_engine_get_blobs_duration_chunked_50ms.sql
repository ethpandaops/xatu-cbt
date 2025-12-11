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
  - "{{external}}.consensus_engine_api_get_blobs"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Get getBlobs timing data
WITH blobs AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        duration_ms,
        status,
        positionCaseInsensitive(meta_client_name, '7870') > 0 AS is_reference_node
    FROM {{ index .dep "{{external}}" "consensus_engine_api_get_blobs" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

-- Group blobs into 50ms chunks
blobs_chunked AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        is_reference_node,
        floor(duration_ms / 50) * 50 AS chunk_duration_ms,
        COUNT(*) as observation_count,
        countIf(status = 'SUCCESS') AS success_count,
        countIf(status = 'PARTIAL') AS partial_count,
        countIf(status = 'EMPTY') AS empty_count,
        countIf(status = 'ERROR' OR status = 'UNSUPPORTED') AS error_count
    FROM blobs
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, is_reference_node, chunk_duration_ms
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    is_reference_node,
    toInt64(chunk_duration_ms) AS chunk_duration_ms,
    observation_count,
    success_count,
    partial_count,
    empty_count,
    error_count
FROM blobs_chunked
WHERE observation_count > 0
