---
table: int_custody_probe_order_by_slot
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - data_column
  - peerdas
  - custody
  - probe
dependencies:
  - "{{transformation}}.int_custody_probe"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    probe_date_time,
    slot,
    slot_start_date_time,
    peer_id_unique_key,
    result,
    error,
    column_indices,
    blob_submitters,
    response_time_ms,
    username,
    node_id,
    classification,
    meta_client_version,
    meta_client_implementation,
    meta_client_os,
    meta_client_geo_city,
    meta_client_geo_country,
    meta_client_geo_country_code,
    meta_client_geo_continent_code,
    meta_client_geo_longitude,
    meta_client_geo_latitude,
    meta_client_geo_autonomous_system_number,
    meta_client_geo_autonomous_system_organization,
    meta_peer_implementation,
    meta_peer_version,
    meta_peer_platform,
    meta_peer_geo_city,
    meta_peer_geo_country,
    meta_peer_geo_country_code,
    meta_peer_geo_continent_code,
    meta_peer_geo_longitude,
    meta_peer_geo_latitude,
    meta_peer_geo_autonomous_system_number,
    meta_peer_geo_autonomous_system_organization
FROM {{ index .dep "{{transformation}}" "int_custody_probe" "helpers" "from" }} FINAL
WHERE
    probe_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
