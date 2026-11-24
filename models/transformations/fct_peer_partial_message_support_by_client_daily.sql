---
table: fct_peer_partial_message_support_by_client_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - peer
  - client
  - data_column
  - partial_messages
dependencies:
  - "{{transformation}}.fct_peer_partial_message_support_by_client_hourly"
---
-- Rewrites every day the bounds touch so partial days are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
day_bounds AS (
    SELECT
        toStartOfDay(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
        toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY AS max_day
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    peer_implementation,
    peer_version,
    toUInt32(any(day_hour_count)) AS hour_count,
    round(sum(connected_peer_count) / any(day_hour_count), 2) AS avg_connected_peer_count,
    round(sum(gossipsub_v13_peer_count) / any(day_hour_count), 2) AS avg_gossipsub_v13_peer_count,
    round(sum(subscribed_peer_count) / any(day_hour_count), 2) AS avg_subscribed_peer_count,
    round(sum(requests_partial_peer_count) / any(day_hour_count), 2) AS avg_requests_partial_peer_count,
    round(sum(supports_sending_partial_peer_count) / any(day_hour_count), 2) AS avg_supports_sending_partial_peer_count
FROM (
    SELECT
        toDate(hour_start_date_time) AS day_start_date,
        -- Averages divide by every hour of the day with data, so a version seen for one hour weighs one hour.
        uniqExact(hour_start_date_time) OVER (PARTITION BY toDate(hour_start_date_time)) AS day_hour_count,
        peer_implementation,
        peer_version,
        connected_peer_count,
        gossipsub_v13_peer_count,
        subscribed_peer_count,
        requests_partial_peer_count,
        supports_sending_partial_peer_count
    FROM {{ index .dep "{{transformation}}" "fct_peer_partial_message_support_by_client_hourly" "helpers" "from" }} FINAL
    WHERE hour_start_date_time >= (SELECT min_day FROM day_bounds)
        AND hour_start_date_time < (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, peer_implementation, peer_version
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
