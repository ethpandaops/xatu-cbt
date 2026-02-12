---
table: fct_node_network_io_by_process
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
  - network
dependencies:
  - "observoor.net_io"
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
    port_label,
    direction,
    sum as io_bytes,
    count as io_count,
    CASE
        WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870'
        ELSE ''
    END AS node_class
FROM {{ index .dep "observoor" "net_io" "helpers" "from" }} FINAL
WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
