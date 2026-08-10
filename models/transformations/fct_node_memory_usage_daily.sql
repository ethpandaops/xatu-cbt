---
table: fct_node_memory_usage_daily
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
  - memory
dependencies:
  - "{{transformation}}.fct_node_memory_usage_hourly"
---
-- Daily aggregation of node memory usage from hourly data.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(hour_start_date_time)) AS min_day,
            toDate(max(hour_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_node_memory_usage_hourly" "helpers" "from" }} FINAL
        WHERE hour_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    hours_in_days AS (
        SELECT
            hour_start_date_time,
            meta_client_name,
            meta_network_name,
            node_class,
            slot_count,
            avg_vm_rss_bytes,
            min_vm_rss_bytes,
            max_vm_rss_bytes,
            avg_rss_anon_bytes,
            avg_rss_file_bytes,
            avg_vm_swap_bytes
        FROM {{ index .dep "{{transformation}}" "fct_node_memory_usage_hourly" "helpers" "from" }} FINAL
        WHERE toDate(hour_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(hour_start_date_time) <= (SELECT max_day FROM day_bounds)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(hour_start_date_time) AS day_start_date,
    meta_client_name,
    any(meta_network_name) AS meta_network_name,
    any(node_class) AS node_class,
    sum(slot_count) AS hour_count,
    toUInt64(round(sum(avg_vm_rss_bytes * slot_count) / sum(slot_count))) AS avg_vm_rss_bytes,
    min(min_vm_rss_bytes) AS min_vm_rss_bytes,
    max(max_vm_rss_bytes) AS max_vm_rss_bytes,
    toUInt64(round(sum(avg_rss_anon_bytes * slot_count) / sum(slot_count))) AS avg_rss_anon_bytes,
    toUInt64(round(sum(avg_rss_file_bytes * slot_count) / sum(slot_count))) AS avg_rss_file_bytes,
    toUInt64(round(sum(avg_vm_swap_bytes * slot_count) / sum(slot_count))) AS avg_vm_swap_bytes
FROM hours_in_days
GROUP BY toDate(hour_start_date_time), meta_client_name
