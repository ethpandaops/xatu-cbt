---
table: int_contract_creation
type: incremental
interval:
  type: block
  max: 10000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5s"
tags:
  - execution
  - contract
dependencies:
  - "{{external}}.canonical_execution_contracts"
  - "{{external}}.canonical_execution_traces"
---
-- Contract creation events with projection for efficient address lookups.
-- Used by int_contract_selfdestruct for EIP-6780 same-transaction-creation checks.
-- Joins with canonical_execution_traces to get transaction_index for same-block ordering.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    c.block_number,
    c.transaction_hash,
    t.transaction_index,
    c.internal_index,
    c.contract_address,
    c.deployer,
    c.factory,
    c.init_code_hash
FROM (
    SELECT DISTINCT block_number, transaction_hash, transaction_index
    FROM {{ index .dep "{{external}}" "canonical_execution_traces" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
) t
GLOBAL INNER JOIN {{ index .dep "{{external}}" "canonical_execution_contracts" "helpers" "from" }} AS c
    ON t.block_number = c.block_number AND t.transaction_hash = c.transaction_hash
WHERE c.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND c.meta_network_name = '{{ .env.NETWORK }}'
SETTINGS
    distributed_aggregation_memory_efficient = 1