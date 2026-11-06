---
table: fct_engine_new_payload_duration_percentile_window
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
-- engine_newPayload duration P50 and P95 per window, node class, execution client and status, for each version, each
-- build line and all versions of the client together, recomputed every hour. Besides each node class, every Xatu node
-- together is written under node_class 'all', since percentiles can't be combined afterwards. Percentiles are read
-- from the summed log-scaled bucket counts of fct_engine_new_payload_duration_histogram_*, interpolated inside the
-- bucket, so they are true percentiles of the window to within one bucket (about 4%).
--
-- Windows match Lab's ranges. Hour windows (1, 6, 24, 168 and 744 hours) end at the last complete hour and count raw
-- calls from int_engine_new_payload. Day windows (7, 14, 30, 60, 90, 180 and 365 days, and 0 for all history) are
-- complete UTC days ending yesterday, summed from the daily per-version histogram. Keys stored for a window that the recount no longer produces are rewritten with a zero count.
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
    ),
    -- Each node class on its own, and every Xatu node together under node_class 'all'
    classed AS (
        SELECT window_grain, window_length, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index, n
        FROM counts
        UNION ALL
        SELECT window_grain, window_length, 'all' AS node_class, meta_execution_implementation, meta_execution_version, status, bucket_index, n
        FROM counts
    ),
    -- Each version on its own, each build line (the version without its trailing commit, as Lab groups builds) under an
    -- empty version, and all versions of a client together under an empty build line and version. Calls without a
    -- version only count towards the client.
    lined AS (
        SELECT
            *,
            replaceRegexpOne(trimBoth(meta_execution_version), '(?i)[-+](0x)?[0-9a-f]{7,40}$', '') AS meta_execution_build_line,
            meta_execution_version != '' AS has_version
        FROM classed
    ),
    buckets AS (
        SELECT window_grain, window_length, node_class, meta_execution_implementation, meta_execution_build_line, meta_execution_version, status, bucket_index, sum(n) AS n
        FROM lined
        WHERE has_version
        GROUP BY window_grain, window_length, node_class, meta_execution_implementation, meta_execution_build_line, meta_execution_version, status, bucket_index
        UNION ALL
        SELECT window_grain, window_length, node_class, meta_execution_implementation, meta_execution_build_line, '' AS meta_execution_version, status, bucket_index, sum(n) AS n
        FROM lined
        WHERE has_version
        GROUP BY window_grain, window_length, node_class, meta_execution_implementation, meta_execution_build_line, status, bucket_index
        UNION ALL
        SELECT window_grain, window_length, node_class, meta_execution_implementation, '' AS meta_execution_build_line, '' AS meta_execution_version, status, bucket_index, sum(n) AS n
        FROM lined
        GROUP BY window_grain, window_length, node_class, meta_execution_implementation, status, bucket_index
    ),
    percentiles AS (
        SELECT
            window_grain,
            window_length,
            node_class,
            meta_execution_implementation,
            meta_execution_build_line,
            meta_execution_version,
            status,
            -- (lower ms, upper ms, count) per bucket, in duration order
            arraySort(x -> x.1, groupArray((
                toFloat64(if(bucket_index < 25, bucket_index, floor(25 * pow(1.04, bucket_index - 25)))),
                toFloat64(if(bucket_index + 1 < 25, bucket_index + 1, floor(25 * pow(1.04, bucket_index + 1 - 25)))),
                toFloat64(n)
            ))) AS b,
            arrayCumSum(arrayMap(x -> x.3, b)) AS cum,
            cum[-1] AS total,
            arrayFirstIndex(c -> c >= 0.5 * total, cum) AS i50,
            arrayFirstIndex(c -> c >= 0.95 * total, cum) AS i95,
            toUInt64(total) AS observation_count,
            b[i50].1 + (b[i50].2 - b[i50].1) * (0.5 * total - (cum[i50] - b[i50].3)) / b[i50].3 AS p50,
            b[i95].1 + (b[i95].2 - b[i95].1) * (0.95 * total - (cum[i95] - b[i95].3)) / b[i95].3 AS p95
        FROM buckets
        WHERE n > 0
        GROUP BY window_grain, window_length, node_class, meta_execution_implementation, meta_execution_build_line, meta_execution_version, status
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    k.window_grain AS window_grain,
    k.window_length AS window_length,
    w.window_start AS window_start_date_time,
    w.window_end AS window_end_date_time,
    k.node_class AS node_class,
    k.meta_execution_implementation AS meta_execution_implementation,
    k.meta_execution_build_line AS meta_execution_build_line,
    k.meta_execution_version AS meta_execution_version,
    k.status AS status,
    toUInt64(max(k.observation_count)) AS observation_count,
    toFloat32(max(k.p50)) AS p50_duration_ms,
    toFloat32(max(k.p95)) AS p95_duration_ms
FROM (
    SELECT window_grain, window_length, node_class, meta_execution_implementation, meta_execution_build_line, meta_execution_version, status, observation_count, p50, p95
    FROM percentiles
    UNION ALL
    SELECT window_grain, window_length, node_class, meta_execution_implementation, meta_execution_build_line, meta_execution_version, status, toUInt64(0), 0, 0
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE observation_count > 0
) AS k
INNER JOIN windows AS w ON k.window_grain = w.grain AND k.window_length = w.length
GROUP BY window_grain, window_length, window_start_date_time, window_end_date_time, node_class, meta_execution_implementation, meta_execution_build_line, meta_execution_version, status
SETTINGS parallel_distributed_insert_select = 0
