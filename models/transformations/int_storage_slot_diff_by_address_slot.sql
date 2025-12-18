---
table: int_storage_slot_diff_by_address_slot
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
dependencies:
  - "{{transformation}}.int_storage_slot_diff"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT * FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
