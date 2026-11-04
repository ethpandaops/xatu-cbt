---
table: fct_engine_get_blobs_duration_chunked_50ms_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - engine_api
  - get_blobs
  - chunked
dependencies:
  - "{{transformation}}.fct_engine_get_blobs_duration_chunked_50ms"
---
-- Hourly twin of fct_engine_get_blobs_duration_chunked_50ms: engine_getBlobs durations in 50ms chunks per node
-- class, summed over the hour. fct_engine_get_blobs_duration_chunked_50ms rewrites slots up to 6 minutes before
-- its bounds, so each run recounts every hour from 6 minutes before its bounds to their end in full. Keys stored
-- for those hours that the recount no longer produces are rewritten with zero counts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    period_bounds AS (
        SELECT
            toStartOfHour(fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 6 MINUTE) AS min_period,
            toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) AS max_period
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    node_class,
    chunk_duration_ms,
    toUInt32(sum(observation_count)) AS observation_count,
    toUInt32(sum(success_count)) AS success_count,
    toUInt32(sum(partial_count)) AS partial_count,
    toUInt32(sum(empty_count)) AS empty_count,
    toUInt32(sum(error_count)) AS error_count
FROM (
    SELECT
        toStartOfHour(slot_start_date_time) AS hour_start_date_time,
        node_class,
        chunk_duration_ms,
        sum(observation_count) AS observation_count,
        sum(success_count) AS success_count,
        sum(partial_count) AS partial_count,
        sum(empty_count) AS empty_count,
        sum(error_count) AS error_count
    FROM {{ index .dep "{{transformation}}" "fct_engine_get_blobs_duration_chunked_50ms" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_period FROM period_bounds)
      AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 HOUR
    GROUP BY hour_start_date_time, node_class, chunk_duration_ms

    UNION ALL

    SELECT
        hour_start_date_time,
        node_class,
        chunk_duration_ms,
        toUInt64(0) AS observation_count,
        toUInt64(0) AS success_count,
        toUInt64(0) AS partial_count,
        toUInt64(0) AS empty_count,
        toUInt64(0) AS error_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE hour_start_date_time >= (SELECT min_period FROM period_bounds)
      AND hour_start_date_time <= (SELECT max_period FROM period_bounds)
      AND observation_count > 0
)
GROUP BY hour_start_date_time, node_class, chunk_duration_ms
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
