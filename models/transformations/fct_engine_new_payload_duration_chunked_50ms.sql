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
  - "{{external}}.execution_engine_new_payload"
  - "{{external}}.beacon_api_eth_v2_beacon_block"
  - "{{external}}.canonical_beacon_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get slot context from BOTH beacon_api (real-time) AND canonical (complete historical)
slot_context AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        execution_payload_block_hash AS block_hash
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v2_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND execution_payload_block_hash IS NOT NULL AND execution_payload_block_hash != ''
    UNION ALL
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        execution_payload_block_hash AS block_hash
    FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND execution_payload_block_hash IS NOT NULL AND execution_payload_block_hash != ''
),
-- Deduplicate slot context
unique_slot_context AS (
    SELECT
        block_hash,
        argMax(slot, slot_start_date_time) AS slot,
        argMax(slot_start_date_time, slot_start_date_time) AS slot_start_date_time,
        argMax(epoch, slot_start_date_time) AS epoch,
        argMax(epoch_start_date_time, slot_start_date_time) AS epoch_start_date_time
    FROM slot_context
    GROUP BY block_hash
),
-- Get newPayload timing data joined with slot context
payloads AS (
    SELECT
        COALESCE(sc.slot, 0) AS slot,
        COALESCE(sc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(sc.epoch, 0) AS epoch,
        COALESCE(sc.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
        ep.block_hash,
        ep.duration_ms,
        ep.status,
        CASE WHEN positionCaseInsensitive(ep.meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class
    FROM {{ index .dep "{{external}}" "execution_engine_new_payload" "helpers" "from" }} FINAL AS ep
    LEFT JOIN unique_slot_context sc ON ep.block_hash = sc.block_hash
    WHERE ep.meta_network_name = '{{ .env.NETWORK }}'
        AND ep.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
),

-- Group payloads into 50ms chunks
payloads_chunked AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_hash,
        node_class,
        floor(duration_ms / 50) * 50 AS chunk_duration_ms,
        COUNT(*) as observation_count,
        countIf(status = 'VALID') AS valid_count,
        countIf(status = 'INVALID' OR status = 'INVALID_BLOCK_HASH') AS invalid_count
    FROM payloads
    WHERE slot_start_date_time != toDateTime(0)
    GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_hash, node_class, chunk_duration_ms
)

SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_hash,
    node_class,
    toInt64(chunk_duration_ms) AS chunk_duration_ms,
    observation_count,
    valid_count,
    invalid_count
FROM payloads_chunked
WHERE observation_count > 0
