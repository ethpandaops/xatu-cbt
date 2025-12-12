---
table: int_storage_slot_read_by_address_slot
type: incremental
interval:
  type: block
  max: 100000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1s"
tags:
  - execution
  - storage
dependencies:
  - "{{transformation}}.int_storage_slot_read"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT * FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
