---
table: int_storage_slot_diff_by_slot
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
  - "{{transformation}}.int_storage_slot_diff"
---
-- Re-sorted copy of int_storage_slot_diff with ORDER BY (address, slot_key, block_number)
-- Enables efficient per-slot range queries using primary key index scans
-- Used by int_storage_slot_expiry_by_6m for fast "was slot touched between X and Y" checks
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_date_time,
    block_number,
    address,
    slot_key,
    effective_bytes_from,
    effective_bytes_to
FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
