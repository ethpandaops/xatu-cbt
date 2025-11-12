---
table: int_block_slots_stat
type: incremental
interval:
  type: block
  max: 10000
schedules:
  forwardfill: "@every 1m"
  backfill: "@every 1m"
tags:
  - storage
  - slots
  - block
dependencies:
  - "{{transformation}}.int_address_slots_stat_per_block"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    block_number,
    sum(slots_cleared) AS slots_cleared,
    sum(slots_set) AS slots_set,
    NULL AS net_slots,
    NULL AS net_slots_bytes
FROM {{ index .dep "{{transformation}}" "int_address_slots_stat_per_block" "helpers" "from" }}
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY block_number;
