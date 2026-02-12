---
table: fct_node_memory_usage_by_process
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - observoor
  - memory
dependencies:
  - "observoor.memory_usage"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    window_start,
    wallclock_slot,
    wallclock_slot_start_date_time,
    meta_client_name,
    meta_network_name,
    pid,
    client_type,
    vm_rss_bytes,
    rss_anon_bytes,
    rss_file_bytes,
    vm_swap_bytes,
    CASE
        WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870'
        ELSE ''
    END AS node_class
FROM {{ index .dep "observoor" "memory_usage" "helpers" "from" }} FINAL
WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
