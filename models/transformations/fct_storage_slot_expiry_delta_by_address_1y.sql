---
table: fct_storage_slot_expiry_delta_by_address_1y
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
  - storage
dependencies:
  - "{{transformation}}.int_storage_slot_expiry_by_1y"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_date_time,
    block_number,
    lower(address) as address,
    -toInt32(count()) as expiry_slots_delta,
    -SUM(toInt64(effective_bytes)) as expiry_bytes_delta
FROM {{ index .dep "{{transformation}}" "int_storage_slot_expiry_by_1y" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY block_number, address
