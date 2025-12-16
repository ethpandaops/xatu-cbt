---
table: fct_engine_new_payload_status_hourly
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
  - new_payload
dependencies:
  - "{{transformation}}.fct_engine_new_payload_by_slot"
  - "{{external}}.consensus_engine_api_new_payload"
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
        FROM `{{ .self.database }}`.`fct_engine_new_payload_by_slot` FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    -- Find ALL slots that fall within those hour boundaries (expanding to complete hours)
    slots_in_hours AS (
        SELECT
            slot_start_date_time,
            block_hash,
            status,
            node_class,
            observation_count,
            avg_duration_ms,
            median_duration_ms,
            p95_duration_ms,
            max_duration_ms
        FROM `{{ .self.database }}`.`fct_engine_new_payload_by_slot` FINAL
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
    valid_count,
    invalid_count,
    syncing_count,
    accepted_count,
    invalid_block_hash_count,
    round((valid_count * 100.0 / observation_count), 2) AS valid_pct,
    -- Duration statistics (VALID status only)
    avg_duration_ms,
    avg_p50_duration_ms,
    avg_p95_duration_ms,
    max_duration_ms
FROM (
    SELECT
        toStartOfHour(slot_start_date_time) AS hour_start_date_time,
        node_class,
        count(DISTINCT slot_start_date_time, block_hash) AS slot_count,
        sum(slots_in_hours.observation_count) AS observation_count,
        sumIf(slots_in_hours.observation_count, status = 'VALID') AS valid_count,
        sumIf(slots_in_hours.observation_count, status = 'INVALID') AS invalid_count,
        sumIf(slots_in_hours.observation_count, status = 'SYNCING') AS syncing_count,
        sumIf(slots_in_hours.observation_count, status = 'ACCEPTED') AS accepted_count,
        sumIf(slots_in_hours.observation_count, status = 'INVALID_BLOCK_HASH') AS invalid_block_hash_count,
        round(avgIf(slots_in_hours.avg_duration_ms, status = 'VALID')) AS avg_duration_ms,
        round(avgIf(slots_in_hours.median_duration_ms, status = 'VALID')) AS avg_p50_duration_ms,
        round(avgIf(slots_in_hours.p95_duration_ms, status = 'VALID')) AS avg_p95_duration_ms,
        maxIf(slots_in_hours.max_duration_ms, status = 'VALID') AS max_duration_ms
    FROM slots_in_hours
    GROUP BY toStartOfHour(slot_start_date_time), node_class
)
