---
table: fct_transaction_blob_announcement_by_el_client_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - transaction
  - mempool
  - blob
  - execution
dependencies:
  - "{{external}}.mempool_blob_transaction_announcement"
  - "{{transformation}}.int_transaction_blob_announcement_hourly"
---
-- Hourly blob transaction announcements received by mimicry clients, per announcing
-- peer's execution client. first_announcer_hash_count counts hashes this client was the
-- first to announce network-wide that hour, from int_transaction_blob_announcement_hourly.
-- The scan expands to complete hour boundaries so partial boundary hours get
-- re-aggregated and replaced.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH announcements AS (
    SELECT
        toStartOfHour(event_date_time) AS hour_start_date_time,
        peer_implementation,
        count() AS announcement_count,
        uniqExact(hash) AS hash_count,
        uniqExact(meta_client_name) AS unique_sentries
    FROM {{ index .dep "{{external}}" "mempool_blob_transaction_announcement" "helpers" "from" }}
    WHERE
        meta_network_name = '{{ .env.NETWORK }}'
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
            AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
        AND toStartOfHour(event_date_time) >= toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
        AND toStartOfHour(event_date_time) <= toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
    GROUP BY hour_start_date_time, peer_implementation
),
firsts AS (
    SELECT
        hour_start_date_time,
        first_peer_implementation AS peer_implementation,
        count() AS first_announcer_hash_count
    FROM {{ index .dep "{{transformation}}" "int_transaction_blob_announcement_hourly" "helpers" "from" }} FINAL
    WHERE hour_start_date_time BETWEEN toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
        AND toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
    GROUP BY hour_start_date_time, peer_implementation
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    a.hour_start_date_time AS hour_start_date_time,
    a.peer_implementation AS peer_implementation,
    a.announcement_count AS announcement_count,
    a.hash_count AS hash_count,
    a.unique_sentries AS unique_sentries,
    ifNull(f.first_announcer_hash_count, 0) AS first_announcer_hash_count
FROM announcements a
GLOBAL LEFT JOIN firsts f
    ON f.hour_start_date_time = a.hour_start_date_time AND f.peer_implementation = a.peer_implementation
SETTINGS join_use_nulls = 1
