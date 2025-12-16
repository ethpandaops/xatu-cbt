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
  - "{{external}}.consensus_engine_api_new_payload"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, duration_ms) AS slot,
    slot_start_date_time,
    argMin(epoch, duration_ms) AS epoch,
    argMin(epoch_start_date_time, duration_ms) AS epoch_start_date_time,
    argMin(block_root, duration_ms) AS block_root,
    block_hash,
    meta_execution_implementation,
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
FROM {{ index .dep "{{external}}" "consensus_engine_api_new_payload" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, block_hash, meta_execution_implementation, status, node_class
