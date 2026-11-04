---
table: fct_engine_new_payload_duration_chunked_50ms_hourly
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
  - new_payload
  - chunked
dependencies:
  - "{{transformation}}.fct_engine_new_payload_duration_chunked_50ms"
---
-- Hourly twin of fct_engine_new_payload_duration_chunked_50ms: engine_newPayload durations in 50ms chunks per node
-- class, summed over the hour. fct_engine_new_payload_duration_chunked_50ms rewrites slots up to 6 minutes before
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
    toUInt32(sum(valid_count)) AS valid_count,
    toUInt32(sum(invalid_count)) AS invalid_count
FROM (
    SELECT
        toStartOfHour(slot_start_date_time) AS hour_start_date_time,
        node_class,
        chunk_duration_ms,
        sum(observation_count) AS observation_count,
        sum(valid_count) AS valid_count,
        sum(invalid_count) AS invalid_count
    FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_duration_chunked_50ms" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_period FROM period_bounds)
      AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 HOUR
    GROUP BY hour_start_date_time, node_class, chunk_duration_ms

    UNION ALL

    SELECT
        hour_start_date_time,
        node_class,
        chunk_duration_ms,
        toUInt64(0) AS observation_count,
        toUInt64(0) AS valid_count,
        toUInt64(0) AS invalid_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE hour_start_date_time >= (SELECT min_period FROM period_bounds)
      AND hour_start_date_time <= (SELECT max_period FROM period_bounds)
      AND observation_count > 0
)
GROUP BY hour_start_date_time, node_class, chunk_duration_ms
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
