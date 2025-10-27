---
table: fct_attestation_observation_by_node
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - attestation
dependencies:
  - "{{transformation}}.int_attestation_first_seen"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(slot, seen_slot_start_diff) AS slot,
    slot_start_date_time,
    argMin(epoch, seen_slot_start_diff) AS epoch,
    argMin(epoch_start_date_time, seen_slot_start_diff) AS epoch_start_date_time,
    COUNT(*) as attestation_count,
    round(AVG(seen_slot_start_diff)) as avg_seen_slot_start_diff,
    round(quantile(0.5)(seen_slot_start_diff)) as median_seen_slot_start_diff,
    MIN(seen_slot_start_diff) as min_seen_slot_start_diff,
    MAX(seen_slot_start_diff) as max_seen_slot_start_diff,
    argMax(block_root, seen_slot_start_diff) AS block_root,
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
    argMin(meta_client_version, seen_slot_start_diff) AS meta_client_version,
    argMin(meta_client_implementation, seen_slot_start_diff) AS meta_client_implementation,
    argMin(meta_client_geo_city, seen_slot_start_diff) AS meta_client_geo_city,
    argMin(meta_client_geo_country, seen_slot_start_diff) AS meta_client_geo_country,
    argMin(meta_client_geo_country_code, seen_slot_start_diff) AS meta_client_geo_country_code,
    argMin(meta_client_geo_continent_code, seen_slot_start_diff) AS meta_client_geo_continent_code,
    argMin(meta_client_geo_longitude, seen_slot_start_diff) AS meta_client_geo_longitude,
    argMin(meta_client_geo_latitude, seen_slot_start_diff) AS meta_client_geo_latitude,
    argMin(meta_client_geo_autonomous_system_number, seen_slot_start_diff) AS meta_client_geo_autonomous_system_number,
    argMin(meta_client_geo_autonomous_system_organization, seen_slot_start_diff) AS meta_client_geo_autonomous_system_organization,
    argMin(meta_consensus_version, seen_slot_start_diff) AS meta_consensus_version,
    argMin(meta_consensus_implementation, seen_slot_start_diff) AS meta_consensus_implementation
FROM {{ index .dep "{{transformation}}" "int_attestation_first_seen" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot_start_date_time, meta_client_name
