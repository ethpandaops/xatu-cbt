---
table: fct_node_cpu_utilization_daily
type: incremental
interval:
  type: slot
  max: 604800
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - observoor
  - cpu
dependencies:
  - "{{transformation}}.fct_node_cpu_utilization_hourly"
---
-- Daily aggregation of node CPU utilization from hourly data.
-- Re-aggregates hourly stats into daily using weighted averages (by slot_count).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(hour_start_date_time)) AS min_day,
            toDate(max(hour_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_node_cpu_utilization_hourly" "helpers" "from" }} FINAL
        WHERE hour_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    hours_in_days AS (
        SELECT
            hour_start_date_time,
            meta_client_name,
            meta_network_name,
            node_class,
            system_cores,
            slot_count,
            avg_core_pct,
            min_core_pct,
            max_core_pct,
            p50_core_pct,
            p95_core_pct
        FROM {{ index .dep "{{transformation}}" "fct_node_cpu_utilization_hourly" "helpers" "from" }} FINAL
        WHERE toDate(hour_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(hour_start_date_time) <= (SELECT max_day FROM day_bounds)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(hour_start_date_time) AS day_start_date,
    meta_client_name,
    any(meta_network_name) AS meta_network_name,
    any(node_class) AS node_class,
    max(system_cores) AS system_cores,
    sum(slot_count) AS hour_count,
    round(sum(avg_core_pct * slot_count) / sum(slot_count), 4) AS avg_core_pct,
    round(min(min_core_pct), 4) AS min_core_pct,
    round(max(max_core_pct), 4) AS max_core_pct,
    round(sum(p50_core_pct * slot_count) / sum(slot_count), 4) AS p50_core_pct,
    round(max(p95_core_pct), 4) AS p95_core_pct
FROM hours_in_days
GROUP BY toDate(hour_start_date_time), meta_client_name
