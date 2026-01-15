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
  forwardfill: "@every 2s"
  backfill: "@every 2s"
tags:
  - execution
  - opcode
  - gas
dependencies:
  - "{{external}}.canonical_execution_transaction_structlog"
---
-- Aggregates opcode execution data per transaction.
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
WITH opcode_aggregates AS (
    SELECT
        block_number,
        transaction_hash,
        transaction_index,
        operation AS opcode,
        count(*) AS count,
        -- Primary: gas_self excludes child frame gas for CALL/CREATE opcodes.
        -- sum(gas) across all opcodes = total transaction execution gas.
        sum(gas_self) AS gas,
        -- Cumulative: gas_used includes child frame gas for CALL/CREATE opcodes.
        -- Useful for flame graph "cumulative" views showing total work triggered by a CALL.
        sum(gas_used) AS gas_cumulative,
        min(depth) AS min_depth,
        max(depth) AS max_depth,
        countIf(error IS NOT NULL) AS error_count,
        meta_network_name
    FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY
        block_number,
        transaction_hash,
        transaction_index,
        operation,
        meta_network_name
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    transaction_hash,
    transaction_index,
    opcode,
    count,
    gas,
    gas_cumulative,
    min_depth,
    max_depth,
    error_count,
    meta_network_name
FROM opcode_aggregates
