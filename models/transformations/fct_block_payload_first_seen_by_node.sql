---
table: fct_block_payload_first_seen_by_node
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - block
  - payload
  - epbs
dependencies:
  - "{{external}}.beacon_api_eth_v1_events_execution_payload_gossip"
  - - "{{external}}.beacon_api_eth_v1_events_execution_payload"
    - "{{external}}.libp2p_gossipsub_execution_payload_envelope"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
-- Gloas (ePBS): the execution payload envelope is revealed by the builder
-- separately from the beacon block. This mirrors fct_block_first_seen_by_node
-- but for the envelope: one row per node per slot, first arrival wins.
-- The libp2p gossip pipeline is OR-grouped with the beacon API import event so
-- a stalled hermes deployment cannot stall the model.
WITH combined_events AS (
    SELECT
        'beacon_api_eth_v1_events_execution_payload_gossip' AS source,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        propagation_slot_start_diff,
        block_root,
        block_hash,
        builder_index,
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
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload_gossip" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'

    UNION ALL

    SELECT
        'beacon_api_eth_v1_events_execution_payload' AS source,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        propagation_slot_start_diff,
        block_root,
        block_hash,
        builder_index,
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
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v1_events_execution_payload" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'

    UNION ALL

    SELECT
        'libp2p_gossipsub_execution_payload_envelope' AS source,
        slot,
        slot_start_date_time,
        epoch,
        epoch_start_date_time,
        propagation_slot_start_diff,
        block_root,
        block_hash,
        builder_index,
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
    FROM {{ index .dep "{{external}}" "libp2p_gossipsub_execution_payload_envelope" "helpers" "from" }} FINAL
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
)
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    argMin(source, propagation_slot_start_diff) AS source,
    argMin(slot, propagation_slot_start_diff) AS slot,
    slot_start_date_time,
    argMin(epoch, propagation_slot_start_diff) AS epoch,
    argMin(epoch_start_date_time, propagation_slot_start_diff) AS epoch_start_date_time,
    MIN(propagation_slot_start_diff) as seen_slot_start_diff,
    argMin(block_root, propagation_slot_start_diff) AS block_root,
    argMin(block_hash, propagation_slot_start_diff) AS block_hash,
    argMin(builder_index, propagation_slot_start_diff) AS builder_index,
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
FROM combined_events
GROUP BY slot_start_date_time, meta_client_name
