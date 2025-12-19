---
table: int_contract_creation
type: incremental
interval:
  type: block
  max: 100000
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
---
-- Contract creation events with projection for efficient address lookups.
-- Used by int_contract_selfdestruct for EIP-6780 same-transaction-creation checks.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    transaction_hash,
    internal_index,
    contract_address,
    deployer,
    factory,
    init_code_hash
FROM {{ index .dep "{{external}}" "canonical_execution_contracts" "helpers" "from" }}
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
ORDER BY block_number, contract_address, transaction_hash
SETTINGS
    distributed_aggregation_memory_efficient = 1;
