---
table: fct_storage_slot_delta_by_address
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
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_date_time,
    block_number,
    lower(address) as address,
    -- Slots activated: from=0, to>0 (+1)
    -- Slots deactivated: from>0, to=0 (-1)
    -- Slots modified: from>0, to>0 (no change)
    toInt32(countIf(effective_bytes_from = 0 AND effective_bytes_to > 0))
      - toInt32(countIf(effective_bytes_from > 0 AND effective_bytes_to = 0)) as slots_delta,
    -- Net byte change: sum of (to - from) for all changes
    SUM(toInt64(effective_bytes_to) - toInt64(effective_bytes_from)) as bytes_delta
FROM {{ index .dep "{{transformation}}" "int_storage_slot_diff" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY block_number, address
