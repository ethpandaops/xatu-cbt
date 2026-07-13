---
table: int_transaction_mempool_observation_hourly
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
  - execution
dependencies:
  - "{{external}}.mempool_transaction"
---
-- One row per transaction hash per hour in which it was sighted in the public mempool.
-- Each raw sighting is scanned exactly once per processed hour. The scan expands to
-- complete hour boundaries so partial boundary hours from a previous run get
-- re-aggregated with complete data and replaced via ReplacingMergeTree.
-- Identity fields are constant per hash so any() is safe. The first observer is
-- chosen deterministically by (event_date_time, meta_client_name).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(event_date_time) AS hour_start_date_time,
    hash,
    min(event_date_time) AS first_seen_date_time,
    max(event_date_time) AS last_seen_date_time,
    toUInt32(count()) AS sighting_count,
    toUInt32(uniqExact(meta_client_name)) AS unique_sentries,
    any(`from`) AS `from`,
    any(`to`) AS `to`,
    any(nonce) AS nonce,
    any(type) AS type,
    any(gas) AS gas,
    any(gas_price) AS gas_price,
    any(gas_tip_cap) AS gas_tip_cap,
    any(gas_fee_cap) AS gas_fee_cap,
    any(value) AS value,
    any(size) AS size,
    any(call_data_size) AS call_data_size,
    any(blob_gas) AS blob_gas,
    any(blob_gas_fee_cap) AS blob_gas_fee_cap,
    any(blob_hashes) AS blob_hashes,
    any(ifNull(m.`to` = m.`from`, 0) AND (m.value = 0)) AS is_cancel_shape,
    argMin(meta_client_name, (event_date_time, meta_client_name)) AS first_sentry_name,
    argMin(meta_client_geo_city, (event_date_time, meta_client_name)) AS first_sentry_geo_city,
    argMin(meta_client_geo_country, (event_date_time, meta_client_name)) AS first_sentry_geo_country,
    argMin(meta_client_geo_country_code, (event_date_time, meta_client_name)) AS first_sentry_geo_country_code,
    argMin(meta_client_geo_continent_code, (event_date_time, meta_client_name)) AS first_sentry_geo_continent_code
FROM {{ index .dep "{{external}}" "mempool_transaction" "helpers" "from" }} m
WHERE
    meta_network_name = '{{ .env.NETWORK }}'
    AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
    AND toStartOfHour(event_date_time) >= toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
    AND toStartOfHour(event_date_time) <= toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
GROUP BY hour_start_date_time, hash
