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
-- Windows match Lab's ranges: 1h, 6h, 24h, 7d and 31d end at the last complete hour and count raw calls from
-- int_engine_new_payload. 90d, 180d, 1y and all are complete UTC days ending yesterday, summed from the daily
-- per-version histogram. Keys stored for a window that the recount no longer produces are rewritten with zero counts.
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
        SELECT window_name, hour_end - toIntervalHour(hours) AS window_start, hour_end AS window_end, 'hour' AS grain
        FROM VALUES('window_name String, hours UInt32', ('1h', 1), ('6h', 6), ('24h', 24), ('7d', 168), ('31d', 744))
        UNION ALL
        SELECT
            window_name,
            toDateTime(if(days = 0, first_day, day_end - days)) AS window_start,
            toDateTime(day_end) AS window_end,
            'day' AS grain
        FROM VALUES('window_name String, days UInt32', ('90d', 90), ('180d', 180), ('1y', 365), ('all', 0))
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
        SELECT w.window_name AS window_name, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index, toUInt64(count()) AS n
        FROM raw AS r
        CROSS JOIN (SELECT * FROM windows WHERE grain = 'hour') AS w
        WHERE r.slot_start_date_time >= w.window_start
        GROUP BY window_name, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index

        UNION ALL

        SELECT w.window_name AS window_name, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index, toUInt64(sum(observation_count)) AS n
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_duration_histogram_by_version_daily" "helpers" "from" }} AS d FINAL
        CROSS JOIN (SELECT * FROM windows WHERE grain = 'day') AS w
        WHERE toDateTime(d.day_start_date) >= w.window_start
            AND d.day_start_date < day_end
            AND d.observation_count > 0
        GROUP BY window_name, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index

        UNION ALL

        SELECT window_name, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index, toUInt64(0) AS n
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE observation_count > 0
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.window_name AS window_name,
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
INNER JOIN windows AS w ON c.window_name = w.window_name
GROUP BY window_name, window_start_date_time, window_end_date_time, node_class, meta_execution_implementation, meta_execution_version, status, bucket_index
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
