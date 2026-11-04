---
table: fct_engine_new_payload_duration_chunked_50ms_daily
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
  - new_payload
  - chunked
dependencies:
  - "{{transformation}}.fct_engine_new_payload_duration_chunked_50ms"
---
-- Daily twin of fct_engine_new_payload_duration_chunked_50ms: engine_newPayload durations in 50ms chunks per node
-- class, summed over the day. fct_engine_new_payload_duration_chunked_50ms rewrites slots up to 6 minutes before
-- its bounds, so each run recounts every day from 6 minutes before its bounds to their end in full. Keys stored
-- for those days that the recount no longer produces are rewritten with zero counts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    period_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 6 MINUTE) AS min_period,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_period
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    node_class,
    chunk_duration_ms,
    toUInt32(sum(observation_count)) AS observation_count,
    toUInt32(sum(valid_count)) AS valid_count,
    toUInt32(sum(invalid_count)) AS invalid_count
FROM (
    SELECT
        toDate(slot_start_date_time) AS day_start_date,
        node_class,
        chunk_duration_ms,
        sum(observation_count) AS observation_count,
        sum(valid_count) AS valid_count,
        sum(invalid_count) AS invalid_count
    FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_duration_chunked_50ms" "helpers" "from" }} FINAL
    WHERE slot_start_date_time >= (SELECT min_period FROM period_bounds)
      AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 DAY
    GROUP BY day_start_date, node_class, chunk_duration_ms

    UNION ALL

    SELECT
        day_start_date,
        node_class,
        chunk_duration_ms,
        toUInt64(0) AS observation_count,
        toUInt64(0) AS valid_count,
        toUInt64(0) AS invalid_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_period FROM period_bounds)
      AND day_start_date <= (SELECT max_period FROM period_bounds)
      AND observation_count > 0
)
GROUP BY day_start_date, node_class, chunk_duration_ms
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
