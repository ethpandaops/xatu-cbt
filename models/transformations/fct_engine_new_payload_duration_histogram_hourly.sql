---
table: fct_engine_new_payload_duration_histogram_hourly
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
  - histogram
dependencies:
  - "{{transformation}}.int_engine_new_payload"
---
-- engine_newPayload durations per hour, node class, execution client and status, as a histogram with log-scaled buckets
-- that are 1ms wide below 25ms and about 4% wide above (bucket k from 25 starts at floor(25 * 1.04^(k - 25)) ms).
-- Summing the counts over any range and reading the quantile from the cumulative counts gives the range's
-- percentiles within one bucket, which averaging per-period percentiles does not.
-- Rewrites every hour the bounds touch so partial ones are recounted in full. Keys stored for those periods that
-- the recount no longer produces are rewritten with zero counts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    period_bounds AS (
        SELECT
            toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_period,
            toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) AS max_period
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    node_class,
    meta_execution_implementation,
    status,
    bucket_index,
    toUInt32(if(bucket_index < 25, bucket_index, floor(25 * pow(1.04, bucket_index - 25)))) AS bucket_lower_ms,
    toUInt32(if(bucket_index + 1 < 25, bucket_index + 1, floor(25 * pow(1.04, bucket_index + 1 - 25)))) AS bucket_upper_ms,
    toUInt32(sum(n)) AS observation_count
FROM (
    SELECT
        hour_start_date_time,
        node_class,
        meta_execution_implementation,
        status,
        bucket_index,
        count() AS n
    FROM (
        SELECT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time,
            node_class,
        meta_execution_implementation,
        status,
            duration_ms,
            toInt32(floor(log(greatest(duration_ms, 1) / 25) / log(1.04))) AS k0,
            -- The float log can land one bucket off at a bucket edge, so step to the bucket whose bounds hold the duration
            toUInt16(multiIf(
                duration_ms < 25, toInt64(duration_ms),
                floor(25 * pow(1.04, k0 + 1)) <= duration_ms, 25 + k0 + 1,
                floor(25 * pow(1.04, k0)) > duration_ms, 25 + k0 - 1,
                25 + k0
            )) AS bucket_index
        FROM {{ index .dep "{{transformation}}" "int_engine_new_payload" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_period FROM period_bounds)
          AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 HOUR
    )
    GROUP BY hour_start_date_time, node_class, meta_execution_implementation, status, bucket_index

    UNION ALL

    SELECT
        hour_start_date_time,
        node_class,
        meta_execution_implementation,
        status,
        bucket_index,
        toUInt64(0) AS n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE hour_start_date_time >= (SELECT min_period FROM period_bounds)
      AND hour_start_date_time <= (SELECT max_period FROM period_bounds)
      AND observation_count > 0
)
GROUP BY hour_start_date_time, node_class, meta_execution_implementation, status, bucket_index
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
