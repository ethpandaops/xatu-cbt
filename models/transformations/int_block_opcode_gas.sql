---
table: int_block_opcode_gas
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - execution
  - opcode
  - gas
  - block
dependencies:
  - "{{transformation}}.int_transaction_opcode_gas"
---
-- Aggregates opcode gas data at the block level.
--
-- Derived from int_transaction_opcode_gas by grouping all transactions in a block.
-- Useful for block-level EVM pattern analysis (e.g., "this block was storage-heavy").
--
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    opcode,
    sum(count) AS count,
    sum(gas) AS gas,
    sum(error_count) AS error_count,
    meta_network_name
FROM {{ index .dep "{{transformation}}" "int_transaction_opcode_gas" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND opcode != ''
GROUP BY
    block_number,
    opcode,
    meta_network_name
