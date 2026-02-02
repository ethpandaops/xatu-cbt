---
table: int_transaction_call_frame_opcode_gas
type: incremental
interval:
  type: block
  max: 500
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 10s"
  backfill: "@every 30s"
tags:
  - execution
  - opcode
  - gas
  - call_frame
dependencies:
  - "{{external}}.canonical_execution_transaction_structlog_agg"
---
-- Aggregates opcode execution data per call frame within transactions.
-- This enables per-frame opcode breakdown, answering "which opcodes did frame N execute?"
--
-- =============================================================================
-- DATA SOURCE: canonical_execution_transaction_structlog_agg
-- =============================================================================
--
-- This transformation uses the pre-aggregated structlog_agg table which already
-- contains per-opcode aggregations at the call frame level. The per-opcode rows
-- have operation != '' and contain:
--
--   - opcode_count: Number of times this opcode was executed in this frame
--   - gas: SUM(gas_self) - excludes child frame gas for CALL/CREATE opcodes
--   - gas_cumulative: SUM(gas_used) - includes child frame gas for CALL/CREATE
--   - error_count: Number of errors for this opcode in this frame
--
-- This is a direct SELECT (no aggregation needed) since structlog_agg already
-- contains the per-(frame, opcode) aggregated data.
--
-- =============================================================================
-- GAS METRICS
-- =============================================================================
--
-- Two gas metrics are provided:
--   gas:            Primary metric. sum(gas) = frame's executed gas (no double counting).
--                   Uses gas_self which excludes child frame gas for CALL/CREATE opcodes.
--   gas_cumulative: For CALL opcodes: includes descendant gas. For others: same as gas.
--
-- Use cases:
--   - Dive into a specific call frame to see its opcode composition
--   - Compare opcode usage across different frames in a transaction
--   - Identify which frame is responsible for expensive operations
--
-- Example query:
--   SELECT opcode, count, gas
--   FROM int_transaction_call_frame_opcode_gas
--   WHERE transaction_hash = '0x...' AND call_frame_id = 15
--   ORDER BY gas DESC
--
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    transaction_hash,
    transaction_index,
    call_frame_id,
    operation AS opcode,
    opcode_count AS count,
    gas,
    gas_cumulative,
    error_count,
    meta_network_name
FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog_agg" "helpers" "from" }}
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND operation != ''  -- Per-opcode rows only (exclude frame summary rows)
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;
