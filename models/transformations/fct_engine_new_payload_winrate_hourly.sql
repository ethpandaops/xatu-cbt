---
table: fct_engine_new_payload_winrate_hourly
type: incremental
interval:
  type: slot
  max: 25200
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - execution
  - engine
dependencies:
  - "{{transformation}}.int_engine_new_payload_fastest_execution_by_node_class"
---
-- Hourly execution client winrate based on fastest engine_newPayload duration per slot.
-- Reads pre-computed winners from int_engine_new_payload_fastest_execution_by_node_class and aggregates by hour.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM {{ index .dep "{{transformation}}" "int_engine_new_payload_fastest_execution_by_node_class" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    winners AS (
        SELECT
            slot_start_date_time,
            node_class,
            meta_execution_implementation
        FROM {{ index .dep "{{transformation}}" "int_engine_new_payload_fastest_execution_by_node_class" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    ),
    target_hours AS (
        SELECT DISTINCT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time
        FROM winners
    ),
    dim AS (
        SELECT DISTINCT
            node_class,
            meta_execution_implementation
        FROM winners
    ),
    win_counts AS (
        SELECT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time,
            node_class,
            meta_execution_implementation,
            toUInt32(count()) AS win_count
        FROM winners
        GROUP BY hour_start_date_time, node_class, meta_execution_implementation
    ),
    candidate_rows AS (
        SELECT
            h.hour_start_date_time,
            d.node_class,
            d.meta_execution_implementation
        FROM target_hours h
        CROSS JOIN dim d
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.hour_start_date_time,
    c.node_class,
    c.meta_execution_implementation,
    toUInt32(coalesce(wc.win_count, toUInt32(0))) AS win_count
FROM candidate_rows c
LEFT JOIN win_counts wc
    ON c.hour_start_date_time = wc.hour_start_date_time
    AND c.node_class = wc.node_class
    AND c.meta_execution_implementation = wc.meta_execution_implementation
