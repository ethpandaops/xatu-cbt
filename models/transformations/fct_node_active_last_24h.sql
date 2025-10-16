---
table: fct_node_active_last_24h
type: scheduled
schedule: "@every 5m"
tags:
  - xatu
  - active
  - nodes
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    max(slot_start_date_time) AS last_seen_date_time,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN
            splitByChar('/', meta_client_name)[2]
        WHEN startsWith(meta_client_name, 'corp-') THEN
            splitByChar('/', meta_client_name)[2]
        ELSE
            'ethpandaops'
    END AS username,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN
            splitByChar('/', meta_client_name)[3]
        WHEN startsWith(meta_client_name, 'corp-') THEN
            splitByChar('/', meta_client_name)[3]
        ELSE
            splitByChar('/', meta_client_name)[-1]
    END AS node_id,
    CASE
        WHEN startsWith(meta_client_name, 'pub-') THEN
            'individual'
        WHEN startsWith(meta_client_name, 'corp-') THEN
            'corporate'
        WHEN startsWith(meta_client_name, 'ethpandaops') THEN
            'internal'
        ELSE
            'unclassified'
    END AS classification,
    meta_client_name,
    argMax(meta_client_version, slot_start_date_time) AS meta_client_version,
    argMax(meta_client_implementation, slot_start_date_time) AS meta_client_implementation,
    argMax(meta_client_geo_city, slot_start_date_time) AS meta_client_geo_city,
    argMax(meta_client_geo_country, slot_start_date_time) AS meta_client_geo_country,
    argMax(meta_client_geo_country_code, slot_start_date_time) AS meta_client_geo_country_code,
    argMax(meta_client_geo_continent_code, slot_start_date_time) AS meta_client_geo_continent_code,
    argMax(meta_client_geo_longitude, slot_start_date_time) AS meta_client_geo_longitude,
    argMax(meta_client_geo_latitude, slot_start_date_time) AS meta_client_geo_latitude,
    argMax(meta_client_geo_autonomous_system_number, slot_start_date_time) AS meta_client_geo_autonomous_system_number,
    argMax(meta_client_geo_autonomous_system_organization, slot_start_date_time) AS meta_client_geo_autonomous_system_organization,
    argMax(meta_consensus_version, slot_start_date_time) AS meta_consensus_version,
    argMax(meta_consensus_implementation, slot_start_date_time) AS meta_consensus_implementation
FROM `{{ .self.database }}`.`beacon_api_eth_v1_events_block` FINAL
WHERE slot_start_date_time >= NOW() - INTERVAL '24 HOUR'
GROUP BY meta_client_name
ORDER BY last_seen_date_time DESC;

DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
