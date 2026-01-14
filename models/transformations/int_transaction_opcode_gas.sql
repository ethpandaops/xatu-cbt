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
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - execution
  - opcode
  - gas
dependencies:
  - "{{external}}.canonical_execution_transaction_structlog"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH opcode_aggregates AS (
    SELECT
        block_number,
        transaction_hash,
        transaction_index,
        operation AS opcode,
        count(*) AS count,
        sum(gas_used) AS total_gas,
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
    total_gas,
    min_depth,
    max_depth,
    error_count,
    meta_network_name
FROM opcode_aggregates
