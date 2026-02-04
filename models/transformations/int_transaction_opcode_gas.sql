---
table: int_transaction_opcode_gas
type: incremental
interval:
  type: block
  max: 2000
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
  - "{{transformation}}.int_transaction_call_frame_opcode_gas"
---
-- Aggregates opcode execution data per transaction.
--
-- =============================================================================
-- DATA SOURCE: int_transaction_call_frame_opcode_gas
-- =============================================================================
--
-- This transformation reads from the per-frame opcode table (which already
-- contains filtered, per-opcode data) and aggregates across all call frames
-- to produce transaction-level opcode metrics.
--
-- This avoids an expensive distributed read from structlog_agg by reading
-- from a local CBT table instead.
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
    opcode,
    -- Aggregate across all frames in the transaction
    sum(count) AS count,
    sum(gas) AS gas,
    sum(gas_cumulative) AS gas_cumulative,
    sum(error_count) AS error_count,
    meta_network_name
FROM {{ index .dep "{{transformation}}" "int_transaction_call_frame_opcode_gas" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY
    block_number,
    transaction_hash,
    transaction_index,
    opcode,
    meta_network_name
SETTINGS
    max_threads = 8;
