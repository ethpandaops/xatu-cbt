---
table: fct_engine_new_payload_duration_histogram_window
type: scheduled
schedule: "@every 1h"
tags:
  - engine_api
  - new_payload
  - histogram
dependencies:
  - "{{transformation}}.int_engine_new_payload"
  - "{{transformation}}.fct_engine_new_payload_duration_histogram_by_version_daily"
---
-- engine_newPayload duration histograms per window, node class, execution client, version and status, recomputed
-- every hour, so a page can read a range's true percentiles for each client and version in one small request instead
-- of summing per-period buckets. Buckets are those of fct_engine_new_payload_duration_histogram_*.
--
-- Windows match Lab's ranges. Hour windows (1, 6, 24, 168 and 744 hours) end at the last complete hour and count raw
-- calls from int_engine_new_payload. Day windows (7, 14, 30, 60, 90, 180 and 365 days, and 0 for all history) are
-- complete UTC days ending yesterday, summed from the daily per-version histogram. Keys stored for a window that the recount no longer produces are rewritten with zero counts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    toStartOfHour(fromUnixTimestamp({{ .task.start }})) AS hour_end,
    toDate(fromUnixTimestamp({{ .task.start }})) AS day_end,
    (
        SELECT min(day_start_date)
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_duration_histogram_by_version_daily" "helpers" "from" }}
    ) AS first_day,
    -- Hour windows end at the last complete hour, day windows at the end of yesterday (UTC)
    windows AS (
        SELECT 'hour' AS grain, length, hour_end - toIntervalHour(length) AS window_start, hour_end AS window_end
        FROM (SELECT arrayJoin([1, 6, 24, 168, 744]) AS length)
        UNION ALL
        SELECT 'day' AS grain, length, toDateTime(if(length = 0, first_day, day_end - length)) AS window_start, toDateTime(day_end) AS window_end
        FROM (SELECT arrayJoin([7, 14, 30, 60, 90, 180, 365, 0]) AS length)
    ),
    raw AS (
        SELECT
            slot_start_date_time,
            node_class,
            meta_execution_implementation,
            meta_execution_version,
            status,
            toInt32(floor(log(greatest(duration_ms, 1) / 25) / log(1.04))) AS k0,
            -- The float log can land one bucket off at a bucket edge, so step to the bucket whose bounds hold the duration
            toUInt16(multiIf(
                duration_ms < 25, toInt64(duration_ms),
                floor(25 * pow(1.04, k0 + 1)) <= duration_ms, 25 + k0 + 1,
                floor(25 * pow(1.04, k0)) > duration_ms, 25 + k0 - 1,
                25 + k0
            )) AS bucket_index
        FROM {{ index .dep "{{transformation}}" "int_engine_new_payload" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= hour_end - INTERVAL 744 HOUR
            AND slot_start_date_time < hour_end
    ),
    counts AS (
        SELECT w.grain AS window_grain, w.length AS window_length, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index, toUInt64(count()) AS n
        FROM raw AS r
        CROSS JOIN (SELECT * FROM windows WHERE grain = 'hour') AS w
        WHERE r.slot_start_date_time >= w.window_start
        GROUP BY window_grain, window_length, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index

        UNION ALL

        SELECT w.grain AS window_grain, w.length AS window_length, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index, toUInt64(sum(observation_count)) AS n
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_duration_histogram_by_version_daily" "helpers" "from" }} AS d FINAL
        CROSS JOIN (SELECT * FROM windows WHERE grain = 'day') AS w
        WHERE toDateTime(d.day_start_date) >= w.window_start
            AND d.day_start_date < day_end
            AND d.observation_count > 0
        GROUP BY window_grain, window_length, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index

        UNION ALL

        SELECT window_grain, window_length, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index, toUInt64(0) AS n
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE observation_count > 0
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.window_grain AS window_grain,
    c.window_length AS window_length,
    w.window_start AS window_start_date_time,
    w.window_end AS window_end_date_time,
    c.node_class AS node_class,
    c.meta_execution_implementation AS meta_execution_implementation,
    c.meta_execution_version AS meta_execution_version,
    c.status AS status,
    c.bucket_index AS bucket_index,
    toUInt32(if(c.bucket_index < 25, c.bucket_index, floor(25 * pow(1.04, c.bucket_index - 25)))) AS bucket_lower_ms,
    toUInt32(if(c.bucket_index + 1 < 25, c.bucket_index + 1, floor(25 * pow(1.04, c.bucket_index + 1 - 25)))) AS bucket_upper_ms,
    toUInt32(sum(c.n)) AS observation_count
FROM counts AS c
INNER JOIN windows AS w ON c.window_grain = w.grain AND c.window_length = w.length
GROUP BY window_grain, window_length, window_start_date_time, window_end_date_time, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
