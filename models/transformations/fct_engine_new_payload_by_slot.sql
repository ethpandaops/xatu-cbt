---
table: fct_engine_new_payload_by_slot
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
  - new_payload
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
        block_root,
        proposer_index,
        execution_payload_block_hash AS block_hash
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v2_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND execution_payload_block_hash IS NOT NULL
        AND execution_payload_block_hash != ''
    UNION ALL
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        proposer_index,
        execution_payload_block_hash AS block_hash
    FROM {{ index .dep "{{external}}" "canonical_beacon_block" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND execution_payload_block_hash IS NOT NULL
        AND execution_payload_block_hash != ''
),
-- Join execution engine data with slot context
enriched AS (
    SELECT
        COALESCE(sc.slot, 0) AS slot,
        COALESCE(sc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(sc.epoch, 0) AS epoch,
        COALESCE(sc.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
        COALESCE(sc.block_root, '') AS block_root,
        ep.block_hash,
        ep.block_number,
        COALESCE(sc.proposer_index, 0) AS proposer_index,
        ep.gas_used,
        ep.gas_limit,
        ep.tx_count,
        ep.blob_count,
        ep.duration_ms,
        ep.status,
        ep.meta_client_name,
        ep.meta_client_implementation,
        ep.meta_execution_implementation
    FROM {{ index .dep "{{external}}" "execution_engine_new_payload" "helpers" "from" }} FINAL AS ep
    LEFT JOIN slot_context sc ON ep.block_hash = sc.block_hash
    WHERE ep.meta_network_name = '{{ .env.NETWORK }}'
        AND ep.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, duration_ms) AS slot,
    slot_start_date_time,
    argMin(epoch, duration_ms) AS epoch,
    argMin(epoch_start_date_time, duration_ms) AS epoch_start_date_time,
    argMin(block_root, duration_ms) AS block_root,
    block_hash,
    argMin(block_number, duration_ms) AS block_number,
    argMin(proposer_index, duration_ms) AS proposer_index,
    argMin(gas_used, duration_ms) AS gas_used,
    argMin(gas_limit, duration_ms) AS gas_limit,
    argMin(tx_count, duration_ms) AS tx_count,
    argMin(blob_count, duration_ms) AS blob_count,
    status,
    CASE WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class,
    -- Observation counts
    COUNT(*) AS observation_count,
    COUNT(DISTINCT meta_client_name) AS unique_node_count,
    -- Duration statistics
    round(AVG(duration_ms)) AS avg_duration_ms,
    round(quantile(0.5)(duration_ms)) AS median_duration_ms,
    MIN(duration_ms) AS min_duration_ms,
    MAX(duration_ms) AS max_duration_ms,
    round(quantile(0.95)(duration_ms)) AS p95_duration_ms,
    round(quantile(0.99)(duration_ms)) AS p99_duration_ms,
    -- Client diversity
    COUNT(DISTINCT meta_client_implementation) AS unique_cl_implementation_count,
    COUNT(DISTINCT meta_execution_implementation) AS unique_el_implementation_count
FROM enriched
WHERE slot_start_date_time != toDateTime(0)
GROUP BY slot_start_date_time, block_hash, status, node_class
