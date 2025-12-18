---
table: int_storage_slot_read
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
  - "{{external}}.canonical_execution_storage_reads"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT DISTINCT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    block_number,
    contract_address as address,
    slot as slot_key
FROM {{ index .dep "{{external}}" "canonical_execution_storage_reads" "helpers" "from" }}
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
ORDER BY block_number, address, slot_key
SETTINGS
    max_threads = 4,
    optimize_distinct_in_order = 1,
    distributed_aggregation_memory_efficient = 1;
