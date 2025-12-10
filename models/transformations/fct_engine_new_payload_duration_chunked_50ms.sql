---
table: fct_engine_new_payload_duration_chunked_50ms
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
  - new_payload
  - chunked
dependencies:
  - "{{external}}.consensus_engine_api_new_payload"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Get newPayload timing data
WITH payloads AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_hash,
        duration_ms,
        status,
        positionCaseInsensitive(meta_client_name, '7870') > 0 AS is_reference_node
    FROM {{ index .dep "{{external}}" "consensus_engine_api_new_payload" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

-- Group payloads into 50ms chunks
payloads_chunked AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_hash,
        is_reference_node,
        floor(duration_ms / 50) * 50 AS chunk_duration_ms,
        COUNT(*) as observation_count,
        countIf(status = 'VALID') AS valid_count,
        countIf(status = 'INVALID' OR status = 'INVALID_BLOCK_HASH') AS invalid_count
    FROM payloads
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_hash, is_reference_node, chunk_duration_ms
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_hash,
    is_reference_node,
    toInt64(chunk_duration_ms) AS chunk_duration_ms,
    observation_count,
    valid_count,
    invalid_count
FROM payloads_chunked
WHERE observation_count > 0
