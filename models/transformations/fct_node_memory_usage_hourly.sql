---
table: fct_node_memory_usage_hourly
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
  - memory
dependencies:
  - "{{transformation}}.fct_node_memory_usage_by_process"
---
-- Hourly aggregation of node memory usage.
-- Sums across processes per (slot, node) to get total node memory, then aggregates into hourly stats.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(wallclock_slot_start_date_time)) AS min_hour,
            toStartOfHour(max(wallclock_slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_node_memory_usage_by_process" "helpers" "from" }} FINAL
        WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    slot_node_totals AS (
        SELECT
            wallclock_slot_start_date_time,
            meta_client_name,
            any(meta_network_name) AS meta_network_name,
            any(node_class) AS node_class,
            sum(vm_rss_bytes) AS total_vm_rss_bytes,
            sum(rss_anon_bytes) AS total_rss_anon_bytes,
            sum(rss_file_bytes) AS total_rss_file_bytes,
            sum(vm_swap_bytes) AS total_vm_swap_bytes
        FROM {{ index .dep "{{transformation}}" "fct_node_memory_usage_by_process" "helpers" "from" }} FINAL
        WHERE wallclock_slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND wallclock_slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
        GROUP BY wallclock_slot_start_date_time, meta_client_name
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(wallclock_slot_start_date_time) AS hour_start_date_time,
    meta_client_name,
    any(meta_network_name) AS meta_network_name,
    any(node_class) AS node_class,
    count() AS slot_count,
    toUInt64(round(avg(total_vm_rss_bytes))) AS avg_vm_rss_bytes,
    min(total_vm_rss_bytes) AS min_vm_rss_bytes,
    max(total_vm_rss_bytes) AS max_vm_rss_bytes,
    toUInt64(round(avg(total_rss_anon_bytes))) AS avg_rss_anon_bytes,
    toUInt64(round(avg(total_rss_file_bytes))) AS avg_rss_file_bytes,
    toUInt64(round(avg(total_vm_swap_bytes))) AS avg_vm_swap_bytes
FROM slot_node_totals
GROUP BY toStartOfHour(wallclock_slot_start_date_time), meta_client_name
