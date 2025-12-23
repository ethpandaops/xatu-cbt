---
table: int_storage_slot_state_with_expiry_by_address
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
  - cumulative
dependencies:
  - "{{transformation}}.int_storage_slot_state_with_expiry"
---
-- Storage slot state with expiry policies, ordered by address.
-- Derives from int_storage_slot_state_with_expiry with different sort order.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    address,
    expiry_policy,
    net_slots_delta,
    net_bytes_delta,
    cumulative_net_slots,
    cumulative_net_bytes,
    active_slots,
    effective_bytes
FROM {{ index .dep "{{transformation}}" "int_storage_slot_state_with_expiry" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
ORDER BY address, expiry_policy, block_number;
