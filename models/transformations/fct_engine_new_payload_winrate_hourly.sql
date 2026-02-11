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
  - "{{transformation}}.int_engine_new_payload"
---
-- Hourly execution client winrate based on fastest engine_newPayload duration per slot.
-- For each slot, the observation with the lowest duration_ms wins.
-- Only considers 7870 nodes with VALID status and known execution implementation.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
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
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
          AND node_class = 'eip7870-block-builder'
          AND status = 'VALID'
          AND meta_execution_implementation != ''
    ),
    target_hours AS (
        SELECT DISTINCT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time
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
            toStartOfHour(slot_start_date_time) AS hour_start_date_time,
            meta_execution_implementation,
            toUInt32(count()) AS win_count
        FROM fastest_per_slot
        WHERE rn = 1
        GROUP BY hour_start_date_time, meta_execution_implementation
    ),
    candidate_rows AS (
        SELECT
            h.hour_start_date_time,
            i.meta_execution_implementation
        FROM target_hours h
        CROSS JOIN impl_dim i
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.hour_start_date_time,
    c.meta_execution_implementation,
    toUInt32(coalesce(wc.win_count, toUInt32(0))) AS win_count
FROM candidate_rows c
LEFT JOIN win_counts wc
    ON c.hour_start_date_time = wc.hour_start_date_time
    AND c.meta_execution_implementation = wc.meta_execution_implementation
