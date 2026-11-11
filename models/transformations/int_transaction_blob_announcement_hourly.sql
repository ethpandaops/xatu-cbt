---
table: int_transaction_blob_announcement_hourly
type: incremental
interval:
  type: slot
  max: 3600
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 30s"
tags:
  - hourly
  - transaction
  - mempool
  - blob
  - execution
dependencies:
  - "{{external}}.mempool_blob_transaction_announcement"
---
-- One row per blob transaction hash per hour in which mimicry clients received an
-- announcement of it from an execution layer peer. Each raw row is one peer announcing
-- one hash, so announcement_count approximates how many peers announced it. The first
-- announcer is chosen deterministically by (event_date_time, meta_client_name,
-- peer_implementation). The scan expands to complete hour boundaries so partial
-- boundary hours get re-aggregated and replaced.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(event_date_time) AS hour_start_date_time,
    hash,
    min(event_date_time) AS first_announced_date_time,
    max(event_date_time) AS last_announced_date_time,
    toUInt32(count()) AS announcement_count,
    toUInt32(uniqExact(meta_client_name)) AS unique_sentries,
    max(size) AS size,
    arraySort(groupUniqArray(peer_implementation)) AS peer_implementations,
    argMin(peer_implementation, (event_date_time, meta_client_name, peer_implementation)) AS first_peer_implementation,
    argMin(peer_version, (event_date_time, meta_client_name, peer_implementation)) AS first_peer_version,
    argMin(meta_client_name, (event_date_time, meta_client_name, peer_implementation)) AS first_sentry_name,
    argMin(meta_client_geo_city, (event_date_time, meta_client_name, peer_implementation)) AS first_sentry_geo_city,
    argMin(meta_client_geo_country_code, (event_date_time, meta_client_name, peer_implementation)) AS first_sentry_geo_country_code,
    argMin(meta_client_geo_continent_code, (event_date_time, meta_client_name, peer_implementation)) AS first_sentry_geo_continent_code
FROM {{ index .dep "{{external}}" "mempool_blob_transaction_announcement" "helpers" "from" }}
WHERE
    meta_network_name = '{{ .env.NETWORK }}'
    AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
    AND toStartOfHour(event_date_time) >= toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
    AND toStartOfHour(event_date_time) <= toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
GROUP BY hour_start_date_time, hash
