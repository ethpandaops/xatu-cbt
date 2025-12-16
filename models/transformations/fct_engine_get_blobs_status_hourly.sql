---
table: fct_engine_get_blobs_status_hourly
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - hourly
  - engine_api
  - get_blobs
dependencies:
  - "{{transformation}}.fct_engine_get_blobs_by_slot"
  - "{{external}}.consensus_engine_api_get_blobs"
---
-- This query aggregates slot-level engine API data to hourly granularity.
-- It finds hour boundaries from available transformation data, then expands to
-- complete hours to ensure accurate hourly aggregations even at processing boundaries.
-- The ReplacingMergeTree will merge duplicates keeping the latest row.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    -- Find the hour boundaries from the transformation data within the current bounds
    hour_bounds AS (
        SELECT
            toStartOfHour(min(slot_start_date_time)) AS min_hour,
            toStartOfHour(max(slot_start_date_time)) AS max_hour
        FROM `{{ .self.database }}`.`fct_engine_get_blobs_by_slot` FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    -- Find ALL slots that fall within those hour boundaries (expanding to complete hours)
    slots_in_hours AS (
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
        WHERE slot_start_date_time >= (SELECT min_hour FROM hour_bounds)
          AND slot_start_date_time < (SELECT max_hour FROM hour_bounds) + INTERVAL 1 HOUR
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    hour_start_date_time,
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
        toStartOfHour(slot_start_date_time) AS hour_start_date_time,
        node_class,
        count(DISTINCT slot_start_date_time, block_root) AS slot_count,
        sum(slots_in_hours.observation_count) AS observation_count,
        sumIf(slots_in_hours.observation_count, status = 'SUCCESS') AS success_count,
        sumIf(slots_in_hours.observation_count, status = 'PARTIAL') AS partial_count,
        sumIf(slots_in_hours.observation_count, status = 'EMPTY') AS empty_count,
        sumIf(slots_in_hours.observation_count, status = 'UNSUPPORTED') AS unsupported_count,
        sumIf(slots_in_hours.observation_count, status = 'ERROR') AS error_count,
        ifNotFinite(round(avgIf(slots_in_hours.avg_duration_ms, status = 'SUCCESS')), 0) AS avg_duration_ms,
        ifNotFinite(round(avgIf(slots_in_hours.median_duration_ms, status = 'SUCCESS')), 0) AS avg_p50_duration_ms,
        ifNotFinite(round(avgIf(slots_in_hours.p95_duration_ms, status = 'SUCCESS')), 0) AS avg_p95_duration_ms,
        ifNotFinite(maxIf(slots_in_hours.max_duration_ms, status = 'SUCCESS'), 0) AS max_duration_ms
    FROM slots_in_hours
    GROUP BY toStartOfHour(slot_start_date_time), node_class
)
