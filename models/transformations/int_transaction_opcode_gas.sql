---
table: int_transaction_opcode_gas
type: incremental
interval:
  type: block
  max: 1000
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
dependencies:
  - "{{external}}.canonical_execution_transaction_structlog_agg"
---
-- Aggregates opcode execution data per transaction.
--
-- =============================================================================
-- DATA SOURCE: canonical_execution_transaction_structlog_agg
-- =============================================================================
--
-- This transformation uses the pre-aggregated structlog_agg table which contains
-- per-opcode aggregations at the call frame level. We aggregate across all frames
-- in a transaction to get transaction-level opcode metrics.
--
-- The per-opcode rows have operation != '' and contain:
--   - opcode_count: Number of times this opcode was executed in a frame
--   - gas: SUM(gas_self) - excludes child frame gas for CALL/CREATE opcodes
--   - gas_cumulative: SUM(gas_used) - includes child frame gas for CALL/CREATE
--   - min_depth/max_depth: Depth range where opcode appeared in the frame
--
-- We SUM across frames to get transaction totals, and take MIN/MAX of depth.
--
-- =============================================================================
-- GAS METRICS
-- =============================================================================
--
-- Two gas metrics are provided:
--   gas:            Primary metric. sum(gas) = transaction executed gas (no double counting).
--                   Uses gas_self which excludes child frame gas for CALL/CREATE opcodes.
--
--   gas_cumulative: For CALL opcodes: includes all descendant frame gas.
--                   Useful for "what triggered the most work?" analysis.
--                   For other opcodes: same as gas.
--
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    transaction_hash,
    transaction_index,
    operation AS opcode,
    -- Aggregate across all frames in the transaction
    sum(opcode_count) AS count,
    sum(gas) AS gas,
    sum(gas_cumulative) AS gas_cumulative,
    min(min_depth) AS min_depth,
    max(max_depth) AS max_depth,
    sum(error_count) AS error_count,
    meta_network_name
FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog_agg" "helpers" "from" }}
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND operation != ''  -- Per-opcode rows only (exclude frame summary rows)
GROUP BY
    block_number,
    transaction_hash,
    transaction_index,
    operation,
    meta_network_name
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;
