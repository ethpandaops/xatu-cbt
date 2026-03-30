---
table: fct_block_first_seen_by_node_daily
type: incremental
interval:
  type: slot
  max: 15000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - daily
  - block
dependencies:
  - "{{transformation}}.fct_block_first_seen_by_node"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    day,
    day_start_date_time,
    username,
    node_id,
    classification,
    meta_client_name,
    meta_client_implementation,
    meta_client_geo_city,
    meta_client_geo_country,
    meta_client_geo_country_code,
    meta_client_geo_continent_code,
    meta_client_geo_longitude,
    meta_client_geo_latitude,
    meta_client_geo_autonomous_system_number,
    meta_client_geo_autonomous_system_organization,
    min_slot,
    max_slot,
    epoch_count,
    slot_count,
    min_seen_slot_start_diff_ms,
    p05_seen_slot_start_diff_ms,
    p50_seen_slot_start_diff_ms,
    avg_seen_slot_start_diff_ms,
    p90_seen_slot_start_diff_ms,
    p95_seen_slot_start_diff_ms,
    p99_seen_slot_start_diff_ms,
    max_seen_slot_start_diff_ms
FROM (
    SELECT
    toDate(slot_start_date_time) as day,
    toStartOfDay(slot_start_date_time) as day_start_date_time,
    username,
    node_id,
    classification,
    meta_client_name,
    argMin(meta_client_implementation, seen_slot_start_diff) AS meta_client_implementation,
    argMin(meta_client_geo_city, seen_slot_start_diff) AS meta_client_geo_city,
    argMin(meta_client_geo_country, seen_slot_start_diff) AS meta_client_geo_country,
    argMin(meta_client_geo_country_code, seen_slot_start_diff) AS meta_client_geo_country_code,
    argMin(meta_client_geo_continent_code, seen_slot_start_diff) AS meta_client_geo_continent_code,
    argMin(meta_client_geo_longitude, seen_slot_start_diff) AS meta_client_geo_longitude,
    argMin(meta_client_geo_latitude, seen_slot_start_diff) AS meta_client_geo_latitude,
    argMin(meta_client_geo_autonomous_system_number, seen_slot_start_diff) AS meta_client_geo_autonomous_system_number,
    argMin(meta_client_geo_autonomous_system_organization, seen_slot_start_diff) AS meta_client_geo_autonomous_system_organization,
    min(slot) AS min_slot,
    max(slot) AS max_slot,
    countDistinct(epoch) AS epoch_count,
    count(*) AS slot_count,
    min(seen_slot_start_diff) AS min_seen_slot_start_diff_ms,
    quantile(0.05)(seen_slot_start_diff) AS p05_seen_slot_start_diff_ms,
    quantile(0.50)(seen_slot_start_diff) AS p50_seen_slot_start_diff_ms,
    round(avg(seen_slot_start_diff)) AS avg_seen_slot_start_diff_ms,
    quantile(0.90)(seen_slot_start_diff) AS p90_seen_slot_start_diff_ms,
    quantile(0.95)(seen_slot_start_diff) AS p95_seen_slot_start_diff_ms,
    quantile(0.99)(seen_slot_start_diff) AS p99_seen_slot_start_diff_ms,
    max(seen_slot_start_diff) AS max_seen_slot_start_diff_ms
FROM {{ index .dep "{{transformation}}" "fct_block_first_seen_by_node" "helpers" "from" }} FINAL
WHERE slot_start_date_time >= toStartOfDay(fromUnixTimestamp({{ .bounds.start }}))
  AND slot_start_date_time < toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY
GROUP BY
    toDate(slot_start_date_time),
    toStartOfDay(slot_start_date_time),
    username,
    node_id,
    classification,
    meta_client_name
)
