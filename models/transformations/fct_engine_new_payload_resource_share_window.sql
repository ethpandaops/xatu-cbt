---
table: fct_engine_new_payload_resource_share_window
type: scheduled
schedule: "@every 1h"
tags:
  - engine_api
  - new_payload
  - performance
dependencies:
  - "{{transformation}}.fct_engine_new_payload_resource_share_hourly"
  - "{{transformation}}.fct_engine_new_payload_resource_share_daily"
---
-- fct_engine_new_payload_resource_share_hourly and _daily (throughput per gas resource and 10% resource share bucket) summed per window, so a page reads one small set of rows per range
-- instead of every hour or day of it. Hour windows (24, 168 and 744 hours) are summed from the hourly table, the all
-- history window from complete UTC days of the daily table. Every column is a count or a sum, so the window sums are
-- exact. Keys stored for a window that the recount no longer produces are rewritten with zeros.
-- The resource-share models run 4 hours behind the head, so hour windows end at the last hour the hourly table has
-- counted rather than the last complete hour.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    (SELECT max(hour_start_date_time) FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_resource_share_hourly" "helpers" "from" }}) AS hour_end,
    toDate(fromUnixTimestamp({{ .task.start }})) AS day_end,
    (SELECT min(day_start_date) FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_resource_share_daily" "helpers" "from" }}) AS first_day,
    windows AS (
        SELECT 'hour' AS grain, length, hour_end - toIntervalHour(length) AS window_start, hour_end AS window_end
        FROM (SELECT arrayJoin([24, 168, 744]) AS length)
        UNION ALL
        SELECT 'day' AS grain, toUInt16(0) AS length, toDateTime(first_day) AS window_start, toDateTime(day_end) AS window_end
    ),
    counts AS (
        SELECT 'hour' AS window_grain, w.length AS window_length, node_scope, client, resource, share_bucket_pct,
            sum(block_count) AS block_count,
            sum(gas_mgas_sum) AS gas_mgas_sum,
            sum(p50_ms_sum) AS p50_ms_sum
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_resource_share_hourly" "helpers" "from" }} AS h FINAL
        CROSS JOIN (SELECT * FROM windows WHERE grain = 'hour') AS w
        WHERE h.hour_start_date_time >= w.window_start AND h.hour_start_date_time < w.window_end
        GROUP BY window_grain, window_length, node_scope, client, resource, share_bucket_pct

        UNION ALL

        SELECT 'day' AS window_grain, toUInt16(0) AS window_length, node_scope, client, resource, share_bucket_pct,
            sum(block_count) AS block_count,
            sum(gas_mgas_sum) AS gas_mgas_sum,
            sum(p50_ms_sum) AS p50_ms_sum
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_resource_share_daily" "helpers" "from" }} FINAL
        WHERE day_start_date < day_end
        GROUP BY window_grain, window_length, node_scope, client, resource, share_bucket_pct

        UNION ALL

        SELECT window_grain, window_length, node_scope, client, resource, share_bucket_pct, toUInt64(0), toFloat64(0), toFloat64(0)
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
    c.resource AS resource, c.share_bucket_pct AS share_bucket_pct,
    toUInt32(sum(c.block_count)) AS block_count,
    toFloat64(sum(c.gas_mgas_sum)) AS gas_mgas_sum,
    toFloat64(sum(c.p50_ms_sum)) AS p50_ms_sum
FROM counts AS c
INNER JOIN windows AS w ON c.window_grain = w.grain AND toUInt16(c.window_length) = w.length
GROUP BY window_grain, window_length, window_start_date_time, window_end_date_time, node_scope, client, resource, share_bucket_pct
-- Merge sums across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0
