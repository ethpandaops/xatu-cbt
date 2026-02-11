---
table: fct_engine_new_payload_winrate_daily
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
  - execution
  - engine
dependencies:
  - "{{transformation}}.int_engine_new_payload_fastest_execution_by_node_class"
---
-- Daily execution client winrate based on fastest engine_newPayload duration per slot.
-- Reads pre-computed winners from int_engine_new_payload_fastest_execution_by_node_class and aggregates by day.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "int_engine_new_payload_fastest_execution_by_node_class" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    winners AS (
        SELECT
            slot_start_date_time,
            node_class,
            meta_execution_implementation
        FROM {{ index .dep "{{transformation}}" "int_engine_new_payload_fastest_execution_by_node_class" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
    ),
    target_days AS (
        SELECT DISTINCT
            toDate(slot_start_date_time) AS day_start_date
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
            toDate(slot_start_date_time) AS day_start_date,
            node_class,
            meta_execution_implementation,
            toUInt32(count()) AS win_count
        FROM winners
        GROUP BY day_start_date, node_class, meta_execution_implementation
    ),
    candidate_rows AS (
        SELECT
            d.day_start_date,
            dm.node_class,
            dm.meta_execution_implementation
        FROM target_days d
        CROSS JOIN dim dm
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.day_start_date,
    c.node_class,
    c.meta_execution_implementation,
    toUInt32(coalesce(wc.win_count, toUInt32(0))) AS win_count
FROM candidate_rows c
LEFT JOIN win_counts wc
    ON c.day_start_date = wc.day_start_date
    AND c.node_class = wc.node_class
    AND c.meta_execution_implementation = wc.meta_execution_implementation
