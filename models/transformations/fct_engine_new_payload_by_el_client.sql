---
table: fct_engine_new_payload_by_el_client
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
  - el_client
dependencies:
  - "{{external}}.execution_engine_new_payload"
  - "{{transformation}}.fct_block_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
block_context AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_root,
        execution_payload_block_hash
    FROM {{ index .dep "{{transformation}}" "fct_block_head" "helpers" "from" }} FINAL
    -- Use wider window to ensure we catch all blocks that might match engine events
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 5 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
        AND execution_payload_block_hash IS NOT NULL AND execution_payload_block_hash != ''
),
-- Fetch external data separately to avoid cross-cluster join pushdown issues
engine_payloads AS (
    SELECT
        block_hash,
        meta_execution_implementation,
        meta_execution_version,
        status,
        meta_client_name,
        gas_used,
        gas_limit,
        tx_count,
        blob_count,
        duration_ms
    FROM {{ index .dep "{{external}}" "execution_engine_new_payload" "helpers" "from" }} FINAL
    WHERE meta_network_name = '{{ .env.NETWORK }}'
        AND meta_execution_implementation != ''
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 1 MINUTE
),
-- Join execution engine data with slot context locally
enriched AS (
    SELECT
        COALESCE(bc.slot, 0) AS slot,
        COALESCE(bc.slot_start_date_time, toDateTime(0)) AS slot_start_date_time,
        COALESCE(bc.epoch, 0) AS epoch,
        COALESCE(bc.epoch_start_date_time, toDateTime(0)) AS epoch_start_date_time,
        COALESCE(bc.block_root, '') AS block_root,
        ep.block_hash,
        ep.meta_execution_implementation,
        ep.meta_execution_version,
        ep.status,
        ep.meta_client_name,
        ep.gas_used,
        ep.gas_limit,
        ep.tx_count,
        ep.blob_count,
        ep.duration_ms
    FROM engine_payloads ep
    LEFT JOIN block_context bc ON ep.block_hash = bc.execution_payload_block_hash
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, duration_ms) AS slot,
    slot_start_date_time,
    argMin(epoch, duration_ms) AS epoch,
    argMin(epoch_start_date_time, duration_ms) AS epoch_start_date_time,
    argMin(block_root, duration_ms) AS block_root,
    block_hash,
    meta_execution_implementation,
    meta_execution_version,
    status,
    CASE WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870-block-builder' ELSE '' END AS node_class,
    -- Block complexity metrics
    argMin(gas_used, duration_ms) AS gas_used,
    argMin(gas_limit, duration_ms) AS gas_limit,
    argMin(tx_count, duration_ms) AS tx_count,
    argMin(blob_count, duration_ms) AS blob_count,
    -- Observation counts
    COUNT(*) AS observation_count,
    COUNT(DISTINCT meta_client_name) AS unique_node_count,
    -- Duration statistics
    round(AVG(duration_ms)) AS avg_duration_ms,
    round(quantile(0.5)(duration_ms)) AS median_duration_ms,
    MIN(duration_ms) AS min_duration_ms,
    MAX(duration_ms) AS max_duration_ms,
    round(quantile(0.95)(duration_ms)) AS p95_duration_ms
FROM enriched
WHERE slot_start_date_time != toDateTime(0)
GROUP BY slot_start_date_time, block_hash, meta_execution_implementation, meta_execution_version, status, node_class
