---
table: fct_node_cpu_utilization_hourly
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
  - cpu
dependencies:
  - "{{transformation}}.fct_node_cpu_utilization_by_process"
---
-- Hourly aggregation of node CPU utilization.
-- Sums across processes per (slot, node) to get total node CPU, then aggregates into hourly stats.
--
-- This query expands the slot range to complete hour boundaries to handle partial
-- hour aggregations at the head of incremental processing.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(wallclock_slot_start_date_time)) AS min_hour,
            toStartOfHour(max(wallclock_slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "fct_node_cpu_utilization_by_process" "helpers" "from" }} FINAL
        WHERE wallclock_slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    -- Sum across processes per (slot, node) to get total node CPU
    slot_node_totals AS (
        SELECT
            wallclock_slot_start_date_time,
            meta_client_name,
            any(meta_network_name) AS meta_network_name,
            any(node_class) AS node_class,
            max(system_cores) AS system_cores,
            sum(mean_core_pct) AS total_mean_core_pct
        FROM {{ index .dep "{{transformation}}" "fct_node_cpu_utilization_by_process" "helpers" "from" }} FINAL
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
    max(system_cores) AS system_cores,
    count() AS slot_count,
    round(avg(total_mean_core_pct), 4) AS avg_core_pct,
    round(min(total_mean_core_pct), 4) AS min_core_pct,
    round(max(total_mean_core_pct), 4) AS max_core_pct,
    round(quantile(0.50)(total_mean_core_pct), 4) AS p50_core_pct,
    round(quantile(0.95)(total_mean_core_pct), 4) AS p95_core_pct
FROM slot_node_totals
GROUP BY toStartOfHour(wallclock_slot_start_date_time), meta_client_name
