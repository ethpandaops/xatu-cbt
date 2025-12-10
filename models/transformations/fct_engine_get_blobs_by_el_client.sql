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
  - "{{external}}.consensus_engine_api_get_blobs"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, duration_ms) AS slot,
    slot_start_date_time,
    argMin(epoch, duration_ms) AS epoch,
    argMin(epoch_start_date_time, duration_ms) AS epoch_start_date_time,
    block_root,
    meta_client_implementation,
    meta_execution_implementation,
    positionCaseInsensitive(meta_client_name, '7870') > 0 AS is_reference_node,
    -- Observation counts
    COUNT(*) AS observation_count,
    COUNT(DISTINCT meta_client_name) AS unique_node_count,
    -- Request/Response
    MAX(requested_count) AS max_requested_count,
    round(AVG(returned_count), 2) AS avg_returned_count,
    -- Status distribution
    countIf(status = 'SUCCESS') AS success_count,
    countIf(status = 'PARTIAL') AS partial_count,
    countIf(status = 'EMPTY') AS empty_count,
    countIf(status = 'UNSUPPORTED') AS unsupported_count,
    countIf(status = 'ERROR') AS error_count,
    round(countIf(status = 'SUCCESS') * 100.0 / COUNT(*), 2) AS success_pct,
    -- Duration statistics
    round(AVG(duration_ms)) AS avg_duration_ms,
    round(quantile(0.5)(duration_ms)) AS median_duration_ms,
    MIN(duration_ms) AS min_duration_ms,
    MAX(duration_ms) AS max_duration_ms,
    round(quantile(0.95)(duration_ms)) AS p95_duration_ms
FROM {{ index .dep "{{external}}" "consensus_engine_api_get_blobs" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, block_root, meta_client_implementation, meta_execution_implementation, is_reference_node
