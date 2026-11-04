---
table: fct_engine_get_blobs_duration_chunked_50ms_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - engine_api
  - get_blobs
  - chunked
dependencies:
  - "{{transformation}}.int_engine_get_blobs"
---
-- engine_getBlobs durations in 50ms chunks per day, node class and execution client version, from
-- int_engine_get_blobs, so histograms and percentiles over any range add up exactly. Rewrites every day the bounds
-- touch so partial days are recounted in full. Keys stored for those days that the recount no longer produces are
-- rewritten with zero counts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    period_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_period,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_period
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    node_class,
    meta_execution_implementation,
    meta_execution_version,
    chunk_duration_ms,
    toUInt32(sum(observation_count_n)) AS observation_count,
    toUInt32(sum(success_count_n)) AS success_count,
    toUInt32(sum(partial_count_n)) AS partial_count,
    toUInt32(sum(empty_count_n)) AS empty_count,
    toUInt32(sum(error_count_n)) AS error_count
FROM (
    SELECT
        toDate(slot_start_date_time) AS day_start_date,
        node_class,
        meta_execution_implementation,
        meta_execution_version,
        toInt64(floor(duration_ms / 50) * 50) AS chunk_duration_ms,
        count() AS observation_count_n,
        countIf(status = 'SUCCESS') AS success_count_n,
        countIf(status = 'PARTIAL') AS partial_count_n,
        countIf(status = 'EMPTY') AS empty_count_n,
        countIf(status = 'ERROR' OR status = 'UNSUPPORTED') AS error_count_n
    FROM {{ index .dep "{{transformation}}" "int_engine_get_blobs" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_period FROM period_bounds)
      AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 DAY
    GROUP BY day_start_date, node_class, meta_execution_implementation, meta_execution_version, chunk_duration_ms

    UNION ALL

    SELECT
        day_start_date,
        node_class,
        meta_execution_implementation,
        meta_execution_version,
        chunk_duration_ms,
        toUInt64(0) AS observation_count_n,
        toUInt64(0) AS success_count_n,
        toUInt64(0) AS partial_count_n,
        toUInt64(0) AS empty_count_n,
        toUInt64(0) AS error_count_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_period FROM period_bounds)
      AND day_start_date <= (SELECT max_period FROM period_bounds)
      AND observation_count > 0
)
GROUP BY day_start_date, node_class, meta_execution_implementation, meta_execution_version, chunk_duration_ms
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
