---
table: fct_node_disk_io_by_process
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
  - disk
dependencies:
  - "observoor.disk_bytes"
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
    rw,
    sum(sum) as io_bytes,
    sum(count) as io_ops,
    CASE
        WHEN positionCaseInsensitive(meta_client_name, '7870') > 0 THEN 'eip7870'
        ELSE ''
    END AS node_class
FROM {{ index .dep "observoor" "disk_bytes" "helpers" "from" }} FINAL
WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    -- The source is ordered/partitioned by window_start, not wallclock_slot_start_date_time, so
    -- also bound window_start or every run scans the whole network. observoor stamps window_start
    -- at or just after the wallclock slot start (observed lag < 40s); the padding keeps every row
    -- matched by the wallclock filter above.
    AND window_start BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY
    window_start,
    wallclock_slot,
    wallclock_slot_start_date_time,
    meta_client_name,
    meta_network_name,
    pid,
    client_type,
    rw
