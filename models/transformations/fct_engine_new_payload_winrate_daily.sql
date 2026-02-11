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
  - "{{transformation}}.int_engine_new_payload"
---
-- Daily execution client winrate based on fastest engine_newPayload duration per slot.
-- For each slot, the observation with the lowest duration_ms wins.
-- Only considers 7870 nodes with VALID status and known execution implementation.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM {{ index .dep "{{transformation}}" "int_engine_new_payload" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
            AND node_class = 'eip7870-block-builder'
            AND status = 'VALID'
            AND meta_execution_implementation != ''
    ),
    fastest_per_slot AS (
        SELECT
            slot,
            slot_start_date_time,
            meta_execution_implementation,
            duration_ms,
            ROW_NUMBER() OVER (PARTITION BY slot ORDER BY duration_ms ASC) AS rn
        FROM {{ index .dep "{{transformation}}" "int_engine_new_payload" "helpers" "from" }} FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
          AND node_class = 'eip7870-block-builder'
          AND status = 'VALID'
          AND meta_execution_implementation != ''
    ),
    target_days AS (
        SELECT DISTINCT
            toDate(slot_start_date_time) AS day_start_date
        FROM fastest_per_slot
        WHERE rn = 1
    ),
    impl_dim AS (
        SELECT DISTINCT meta_execution_implementation
        FROM fastest_per_slot
        WHERE rn = 1
    ),
    win_counts AS (
        SELECT
            toDate(slot_start_date_time) AS day_start_date,
            meta_execution_implementation,
            toUInt32(count()) AS win_count
        FROM fastest_per_slot
        WHERE rn = 1
        GROUP BY day_start_date, meta_execution_implementation
    ),
    candidate_rows AS (
        SELECT
            d.day_start_date,
            i.meta_execution_implementation
        FROM target_days d
        CROSS JOIN impl_dim i
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.day_start_date,
    c.meta_execution_implementation,
    toUInt32(coalesce(wc.win_count, toUInt32(0))) AS win_count
FROM candidate_rows c
LEFT JOIN win_counts wc
    ON c.day_start_date = wc.day_start_date
    AND c.meta_execution_implementation = wc.meta_execution_implementation
