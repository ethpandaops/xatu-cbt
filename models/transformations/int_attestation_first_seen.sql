---
table: int_attestation_first_seen
interval:
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - attestation
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_attestation"
  - "{{external}}.libp2p_gossipsub_beacon_attestation"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH combined_events AS (
    SELECT
        'beacon_api_eth_v1_events_attestation' AS source,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        propagation_slot_start_diff,
        beacon_block_root,
        attesting_validator_index,
        attesting_validator_committee_index,
        meta_client_name,
        meta_client_version,
        meta_client_implementation,
        meta_client_geo_city,
        meta_client_geo_country,
        meta_client_geo_country_code,
        meta_client_geo_continent_code,
        meta_client_geo_longitude,
        meta_client_geo_latitude,
        meta_client_geo_autonomous_system_number,
        meta_client_geo_autonomous_system_organization,
        meta_consensus_version,
        meta_consensus_implementation
    FROM `{{ index .dep "{{external}}" "beacon_api_eth_v1_events_attestation" "database" }}`.`beacon_api_eth_v1_events_attestation` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND aggregation_bits = ''
        AND attesting_validator_index IS NOT NULL

    UNION ALL

    SELECT
        'libp2p_gossipsub_beacon_attestation' AS source,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        propagation_slot_start_diff,
        beacon_block_root,
        attesting_validator_index,
        attesting_validator_committee_index,
        meta_client_name,
        meta_client_version,
        meta_client_implementation,
        meta_client_geo_city,
        meta_client_geo_country,
        meta_client_geo_country_code,
        meta_client_geo_continent_code,
        meta_client_geo_longitude,
        meta_client_geo_latitude,
        meta_client_geo_autonomous_system_number,
        meta_client_geo_autonomous_system_organization,
        '' AS meta_consensus_version,
        CASE
            WHEN hasSubsequence(meta_client_implementation, 'tysm') THEN
                'prysm'
            WHEN hasSubsequence(meta_client_implementation, 'dimhouse') THEN
                'lighthouse'
            WHEN hasSubsequence(meta_client_implementation, 'temu') THEN
                'teku'
            ELSE
                ''
        END AS meta_consensus_implementation
    FROM `{{ index .dep "{{external}}" "libp2p_gossipsub_beacon_attestation" "database" }}`.`libp2p_gossipsub_beacon_attestation` FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND aggregation_bits = ''
        AND attesting_validator_index IS NOT NULL
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(source, propagation_slot_start_diff) AS source,
    argMin(slot, propagation_slot_start_diff) AS slot,
    slot_start_date_time,
    argMin(epoch, propagation_slot_start_diff) AS epoch,
    argMin(epoch_start_date_time, propagation_slot_start_diff) AS epoch_start_date_time,
    MIN(propagation_slot_start_diff) as seen_slot_start_diff,
    argMin(beacon_block_root, propagation_slot_start_diff) AS block_root,
    attesting_validator_index,
    argMin(attesting_validator_committee_index, propagation_slot_start_diff) AS attesting_validator_committee_index,
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
    argMin(meta_client_name, propagation_slot_start_diff) AS meta_client_name,
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
FROM combined_events
GROUP BY slot_start_date_time, attesting_validator_index
