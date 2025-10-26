---
table: fct_head_first_seen_by_node
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - head
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_head"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    'beacon_api_eth_v1_events_head' AS source,
    argMin(slot, propagation_slot_start_diff) AS slot,
    slot_start_date_time,
    argMin(epoch, propagation_slot_start_diff) AS epoch,
    argMin(epoch_start_date_time, propagation_slot_start_diff) AS epoch_start_date_time,
    MIN(propagation_slot_start_diff) as seen_slot_start_diff,
    argMin(block, propagation_slot_start_diff) AS block_root,
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
    argMin(meta_client_version, propagation_slot_start_diff) AS meta_client_version,
    argMin(meta_client_implementation, propagation_slot_start_diff) AS meta_client_implementation,
    argMin(meta_client_geo_city, propagation_slot_start_diff) AS meta_client_geo_city,
    argMin(meta_client_geo_country, propagation_slot_start_diff) AS meta_client_geo_country,
    argMin(meta_client_geo_country_code, propagation_slot_start_diff) AS meta_client_geo_country_code,
    argMin(meta_client_geo_continent_code, propagation_slot_start_diff) AS meta_client_geo_continent_code,
    argMin(meta_client_geo_longitude, propagation_slot_start_diff) AS meta_client_geo_longitude,
    argMin(meta_client_geo_latitude, propagation_slot_start_diff) AS meta_client_geo_latitude,
    argMin(meta_client_geo_autonomous_system_number, propagation_slot_start_diff) AS meta_client_geo_autonomous_system_number,
    argMin(meta_client_geo_autonomous_system_organization, propagation_slot_start_diff) AS meta_client_geo_autonomous_system_organization,
    argMin(meta_consensus_version, propagation_slot_start_diff) AS meta_consensus_version,
    argMin(meta_consensus_implementation, propagation_slot_start_diff) AS meta_consensus_implementation
FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_head" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_network_name = '{{ .env.NETWORK }}'
GROUP BY slot_start_date_time, meta_client_name
