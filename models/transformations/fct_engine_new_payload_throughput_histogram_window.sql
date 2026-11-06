---
table: fct_engine_new_payload_throughput_histogram_window
type: scheduled
schedule: "@every 1h"
tags:
  - engine_api
  - new_payload
  - performance
dependencies:
  - "{{transformation}}.fct_engine_new_payload_throughput_histogram_hourly"
  - "{{transformation}}.fct_engine_new_payload_throughput_histogram_daily"
---
-- fct_engine_new_payload_throughput_histogram_hourly and _daily (per-block throughput histogram) summed per window, so a page reads one small set of rows per range
-- instead of every hour or day of it. Hour windows (24, 168 and 744 hours) are summed from the hourly table, the all
-- history window from complete UTC days of the daily table. Every column is a count or a sum, so the window sums are
-- exact. Keys stored for a window that the recount no longer produces are rewritten with zeros.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    toStartOfHour(fromUnixTimestamp({{ .task.start }})) AS hour_end,
    toDate(fromUnixTimestamp({{ .task.start }})) AS day_end,
    (SELECT min(day_start_date) FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_throughput_histogram_daily" "helpers" "from" }}) AS first_day,
    windows AS (
        SELECT 'hour' AS grain, length, hour_end - toIntervalHour(length) AS window_start, hour_end AS window_end
        FROM (SELECT arrayJoin([24, 168, 744]) AS length)
        UNION ALL
        SELECT 'day' AS grain, toUInt16(0) AS length, toDateTime(first_day) AS window_start, toDateTime(day_end) AS window_end
    ),
    counts AS (
        SELECT 'hour' AS window_grain, w.length AS window_length, node_scope, client, bucket_index,
            sum(block_count) AS block_count
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_throughput_histogram_hourly" "helpers" "from" }} AS h FINAL
        CROSS JOIN (SELECT * FROM windows WHERE grain = 'hour') AS w
        WHERE h.hour_start_date_time >= w.window_start AND h.hour_start_date_time < w.window_end
        GROUP BY window_grain, window_length, node_scope, client, bucket_index

        UNION ALL

        SELECT 'day' AS window_grain, toUInt16(0) AS window_length, node_scope, client, bucket_index,
            sum(block_count) AS block_count
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_throughput_histogram_daily" "helpers" "from" }} FINAL
        WHERE day_start_date < day_end
        GROUP BY window_grain, window_length, node_scope, client, bucket_index

        UNION ALL

        SELECT window_grain, window_length, node_scope, client, bucket_index, toUInt64(0)
        -- Unaliased zeros, so block_count below is the stored column
        FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
        WHERE block_count > 0
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.window_grain AS window_grain,
    toUInt16(c.window_length) AS window_length,
    w.window_start AS window_start_date_time,
    w.window_end AS window_end_date_time,
    c.node_scope AS node_scope,
    c.client AS client,
    c.bucket_index AS bucket_index,
    toFloat32(pow(1.04, c.bucket_index)) AS bucket_lower_mgas_per_second,
    toFloat32(pow(1.04, c.bucket_index + 1)) AS bucket_upper_mgas_per_second,
    toUInt32(sum(c.block_count)) AS block_count
FROM counts AS c
INNER JOIN windows AS w ON c.window_grain = w.grain AND toUInt16(c.window_length) = w.length
GROUP BY window_grain, window_length, window_start_date_time, window_end_date_time, node_scope, client, bucket_index
-- Merge sums across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0
