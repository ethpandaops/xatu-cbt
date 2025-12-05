---
table: int_storage_slot_read_by_slot
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
  - storage
dependencies:
  - "{{transformation}}.int_storage_slot_read"
---
-- Re-sorted copy of int_storage_slot_read with ORDER BY (address, slot_key, block_number)
-- Enables efficient per-slot range queries using primary key index scans
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_date_time,
    block_number,
    address,
    slot_key,
    read_count,
    effective_bytes
FROM {{ index .dep "{{transformation}}" "int_storage_slot_read" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
