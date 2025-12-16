---
table: fct_engine_get_blobs_status_daily
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - daily
  - engine_api
  - get_blobs
dependencies:
  - "{{transformation}}.fct_engine_get_blobs_by_slot"
  - "{{external}}.consensus_engine_api_get_blobs"
---
-- This query aggregates slot-level engine API data to daily granularity.
-- It finds day boundaries from available transformation data, then expands to
-- complete days to ensure accurate daily aggregations even at processing boundaries.
-- The ReplacingMergeTree will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the day boundaries from the transformation data within the current bounds
    day_bounds AS (
        SELECT
            toDate(min(slot_start_date_time)) AS min_day,
            toDate(max(slot_start_date_time)) AS max_day
        FROM `{{ .self.database }}`.`fct_engine_get_blobs_by_slot` FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    -- Find ALL slots that fall within those day boundaries (expanding to complete days)
    slots_in_days AS (
        SELECT
            slot_start_date_time,
            block_root,
            status,
            node_class,
            observation_count,
            avg_duration_ms,
            median_duration_ms,
            p95_duration_ms,
            max_duration_ms
        FROM `{{ .self.database }}`.`fct_engine_get_blobs_by_slot` FINAL
        WHERE toDate(slot_start_date_time) >= (SELECT min_day FROM day_bounds)
          AND toDate(slot_start_date_time) <= (SELECT max_day FROM day_bounds)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    day_start_date,
    node_class,
    slot_count,
    observation_count,
    -- Status distribution
    success_count,
    partial_count,
    empty_count,
    unsupported_count,
    error_count,
    round((success_count * 100.0 / observation_count), 2) AS success_pct,
    -- Duration statistics (SUCCESS status only)
    avg_duration_ms,
    avg_p50_duration_ms,
    avg_p95_duration_ms,
    max_duration_ms
FROM (
    SELECT
        toDate(slot_start_date_time) AS day_start_date,
        node_class,
        count(DISTINCT slot_start_date_time, block_root) AS slot_count,
        sum(slots_in_days.observation_count) AS observation_count,
        sumIf(slots_in_days.observation_count, status = 'SUCCESS') AS success_count,
        sumIf(slots_in_days.observation_count, status = 'PARTIAL') AS partial_count,
        sumIf(slots_in_days.observation_count, status = 'EMPTY') AS empty_count,
        sumIf(slots_in_days.observation_count, status = 'UNSUPPORTED') AS unsupported_count,
        sumIf(slots_in_days.observation_count, status = 'ERROR') AS error_count,
        round(avgIf(slots_in_days.avg_duration_ms, status = 'SUCCESS')) AS avg_duration_ms,
        round(avgIf(slots_in_days.median_duration_ms, status = 'SUCCESS')) AS avg_p50_duration_ms,
        round(avgIf(slots_in_days.p95_duration_ms, status = 'SUCCESS')) AS avg_p95_duration_ms,
        maxIf(slots_in_days.max_duration_ms, status = 'SUCCESS') AS max_duration_ms
    FROM slots_in_days
    GROUP BY toDate(slot_start_date_time), node_class
)
