---
table: fct_node_network_io_daily
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
  - network
dependencies:
  - "{{transformation}}.fct_node_network_io_hourly"
---
-- Daily aggregation of node network I/O from hourly data.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(hour_start_date_time)) AS min_day,
            toDate(max(hour_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "fct_node_network_io_hourly" "helpers" "from" }} FINAL
        WHERE hour_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    hours_in_days AS (
        SELECT
            hour_start_date_time,
            meta_client_name,
            meta_network_name,
            node_class,
            port_label,
            direction,
            slot_count,
            sum_io_bytes,
            avg_io_bytes,
            sum_io_count,
            avg_io_count
        FROM {{ index .dep "{{transformation}}" "fct_node_network_io_hourly" "helpers" "from" }} FINAL
        WHERE toDate(hour_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(hour_start_date_time) <= (SELECT max_day FROM day_bounds)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(hour_start_date_time) AS day_start_date,
    meta_client_name,
    any(meta_network_name) AS meta_network_name,
    any(node_class) AS node_class,
    port_label,
    direction,
    sum(slot_count) AS hour_count,
    sum(sum_io_bytes) AS sum_io_bytes,
    round(sum(avg_io_bytes * slot_count) / sum(slot_count), 4) AS avg_io_bytes,
    sum(sum_io_count) AS sum_io_count,
    toUInt32(round(sum(avg_io_count * slot_count) / sum(slot_count))) AS avg_io_count
FROM hours_in_days
GROUP BY toDate(hour_start_date_time), meta_client_name, port_label, direction
