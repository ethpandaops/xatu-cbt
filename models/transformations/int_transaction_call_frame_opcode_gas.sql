---
table: int_transaction_call_frame_opcode_gas
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 2s"
  backfill: "@every 2s"
tags:
  - execution
  - opcode
  - gas
  - call_frame
dependencies:
  - "{{external}}.canonical_execution_transaction_structlog"
---
-- Aggregates opcode execution data per call frame within transactions.
-- This enables per-frame opcode breakdown, answering "which opcodes did frame N execute?"
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
WITH opcode_aggregates AS (
    SELECT
        block_number,
        transaction_hash,
        transaction_index,
        call_frame_id,
        operation AS opcode,
        count(*) AS count,
        -- Primary: gas_self excludes child frame gas for CALL/CREATE opcodes.
        -- sum(gas) across all opcodes in frame = frame's execution gas.
        sum(gas_self) AS gas,
        -- Cumulative: gas_used includes child frame gas for CALL/CREATE opcodes.
        -- Useful for flame graph "cumulative" views.
        sum(gas_used) AS gas_cumulative,
        countIf(error IS NOT NULL AND error != '') AS error_count,
        meta_network_name
    FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND operation != ''  -- Exclude synthetic EOA rows (no opcodes executed)
    GROUP BY
        block_number,
        transaction_hash,
        transaction_index,
        call_frame_id,
        operation,
        meta_network_name
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    transaction_hash,
    transaction_index,
    call_frame_id,
    opcode,
    count,
    gas,
    gas_cumulative,
    error_count,
    meta_network_name
FROM opcode_aggregates
SETTINGS
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;
