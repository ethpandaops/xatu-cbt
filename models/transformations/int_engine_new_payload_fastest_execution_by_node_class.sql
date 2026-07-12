---
table: int_engine_new_payload_fastest_execution_by_node_class
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - engine
dependencies:
  - "{{transformation}}.int_engine_new_payload"
---
-- Fastest valid engine_newPayload observation per slot per node_class.
-- Ties produce one row per implementation matching the slot minimum, with a
-- deterministic representative node per implementation.
-- Filters for VALID status and known execution implementation (data quality).
-- Does NOT filter by node_class — all classes are carried through as a dimension.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
ranked AS (
    SELECT
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        block_hash,
        duration_ms,
        node_class,
        meta_execution_implementation,
        meta_execution_version,
        meta_client_name,
        MIN(duration_ms) OVER (PARTITION BY slot, node_class) AS min_duration_ms,
        ROW_NUMBER() OVER (
            PARTITION BY slot, node_class, meta_execution_implementation
            ORDER BY duration_ms ASC, meta_client_name ASC
        ) AS rn
    FROM {{ index .dep "{{transformation}}" "int_engine_new_payload" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND status = 'VALID'
        AND meta_execution_implementation != ''
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_hash,
    duration_ms,
    node_class,
    meta_execution_implementation,
    meta_execution_version,
    meta_client_name
FROM ranked
WHERE rn = 1 AND duration_ms = min_duration_ms
