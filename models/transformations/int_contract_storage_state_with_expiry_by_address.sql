---
table: int_contract_storage_state_with_expiry_by_address
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1m"
tags:
  - execution
  - storage
  - contract
  - cumulative
dependencies:
  - "{{transformation}}.int_contract_storage_state_with_expiry"
---
-- Contract-level expiry state ordered by address for efficient address lookups.
-- Derives from int_contract_storage_state_with_expiry with different sort order.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    address,
    expiry_policy,
    active_slots,
    effective_bytes
FROM {{ index .dep "{{transformation}}" "int_contract_storage_state_with_expiry" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
ORDER BY address, expiry_policy, block_number;
