---
table: fct_node_disk_io_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - observoor
  - disk
dependencies:
  - "{{transformation}}.fct_node_disk_io_by_process"
---
-- Hourly aggregation of node disk I/O.
-- Sums across processes per (slot, node, rw) to get total node disk I/O, then aggregates into hourly stats.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(wallclock_slot_start_date_time)) AS min_hour,
            toStartOfHour(max(wallclock_slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_node_disk_io_by_process" "helpers" "from" }} FINAL
        WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    slot_node_totals AS (
        SELECT
            wallclock_slot_start_date_time,
            meta_client_name,
            any(meta_network_name) AS meta_network_name,
            any(node_class) AS node_class,
            rw,
            sum(io_bytes) AS total_io_bytes,
            sum(io_ops) AS total_io_ops
        FROM {{ index .dep "{{transformation}}" "fct_node_disk_io_by_process" "helpers" "from" }} FINAL
        WHERE wallclock_slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND wallclock_slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
        GROUP BY wallclock_slot_start_date_time, meta_client_name, rw
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(wallclock_slot_start_date_time) AS hour_start_date_time,
    meta_client_name,
    any(meta_network_name) AS meta_network_name,
    any(node_class) AS node_class,
    rw,
    count() AS slot_count,
    sum(total_io_bytes) AS sum_io_bytes,
    round(avg(total_io_bytes), 4) AS avg_io_bytes,
    sum(total_io_ops) AS sum_io_ops,
    toUInt32(round(avg(total_io_ops))) AS avg_io_ops
FROM slot_node_totals
GROUP BY toStartOfHour(wallclock_slot_start_date_time), meta_client_name, rw
