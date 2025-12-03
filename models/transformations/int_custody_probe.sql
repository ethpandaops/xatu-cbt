---
table: int_custody_probe
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
  - "{{external}}.libp2p_rpc_data_column_custody_probe"
  - "{{external}}.libp2p_synthetic_heartbeat"
  - "{{external}}.beacon_api_eth_v2_beacon_block"
  - "{{transformation}}.dim_block_blob_submitter"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Step 1: Filter probes by time bounds (scan probe table once)
filtered_probes AS (
    SELECT
        event_date_time,
        slot,
        slot_start_date_time,
        column_index,
        peer_id_unique_key,
        result,
        response_time_ms,
        COALESCE(error, '') AS error,
        meta_client_name,
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
        meta_network_name
    FROM {{ index .dep "{{external}}" "libp2p_rpc_data_column_custody_probe" "helpers" "from" }} FINAL
    WHERE
        event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
),

-- Step 2: Get unique slot_start_date_times from filtered probes
probe_slot_times AS (
    SELECT DISTINCT slot_start_date_time
    FROM filtered_probes
),

-- Step 3: Get beacon blocks only for probed slots
beacon_blocks AS (
    SELECT DISTINCT
        slot_start_date_time,
        execution_payload_block_number
    FROM {{ index .dep "{{external}}" "beacon_api_eth_v2_beacon_block" "helpers" "from" }} FINAL
    WHERE
        meta_network_name = '{{ .env.NETWORK }}'
        AND slot_start_date_time GLOBAL IN (SELECT slot_start_date_time FROM probe_slot_times)
),

-- Step 4: Get blob submitters for the block numbers we need
blob_submitters AS (
    SELECT
        block_number,
        name
    FROM {{ index .dep "{{transformation}}" "dim_block_blob_submitter" "helpers" "from" }} FINAL
    WHERE
        block_number GLOBAL IN (SELECT execution_payload_block_number FROM beacon_blocks)
),

-- Step 5: Get heartbeat data for peer metadata
heartbeat_data AS (
    SELECT
        remote_peer_id_unique_key,
        meta_network_name,
        argMax(remote_agent_implementation, event_date_time) AS remote_agent_implementation,
        argMax(remote_agent_version, event_date_time) AS remote_agent_version,
        argMax(remote_agent_platform, event_date_time) AS remote_agent_platform,
        argMax(remote_geo_city, event_date_time) AS remote_geo_city,
        argMax(remote_geo_country, event_date_time) AS remote_geo_country,
        argMax(remote_geo_country_code, event_date_time) AS remote_geo_country_code,
        argMax(remote_geo_continent_code, event_date_time) AS remote_geo_continent_code,
        argMax(remote_geo_longitude, event_date_time) AS remote_geo_longitude,
        argMax(remote_geo_latitude, event_date_time) AS remote_geo_latitude,
        argMax(remote_geo_autonomous_system_number, event_date_time) AS remote_geo_autonomous_system_number,
        argMax(remote_geo_autonomous_system_organization, event_date_time) AS remote_geo_autonomous_system_organization
    FROM {{ index .dep "{{external}}" "libp2p_synthetic_heartbeat" "helpers" "from" }} FINAL
    WHERE
        event_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 5 MINUTE
        AND event_date_time <= fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY remote_peer_id_unique_key, meta_network_name
),

-- Step 6: Enrich probes with beacon block, blob submitter, and heartbeat data
enriched_probes AS (
    SELECT
        p.event_date_time AS event_date_time,
        p.slot AS slot,
        p.slot_start_date_time AS slot_start_date_time,
        p.column_index AS column_index,
        p.peer_id_unique_key AS peer_id_unique_key,
        p.result AS result,
        p.response_time_ms AS response_time_ms,
        p.error AS error,
        -- Blob submitter name
        COALESCE(bs.name, '') AS blob_submitter_name,
        -- Client classification
        CASE
            WHEN startsWith(p.meta_client_name, 'pub-') THEN splitByChar('/', p.meta_client_name)[2]
            WHEN startsWith(p.meta_client_name, 'corp-') THEN splitByChar('/', p.meta_client_name)[2]
            ELSE 'ethpandaops'
        END AS username,
        CASE
            WHEN startsWith(p.meta_client_name, 'pub-') THEN splitByChar('/', p.meta_client_name)[3]
            WHEN startsWith(p.meta_client_name, 'corp-') THEN splitByChar('/', p.meta_client_name)[3]
            ELSE splitByChar('/', p.meta_client_name)[-1]
        END AS node_id,
        CASE
            WHEN startsWith(p.meta_client_name, 'pub-') THEN 'individual'
            WHEN startsWith(p.meta_client_name, 'corp-') THEN 'corporate'
            WHEN startsWith(p.meta_client_name, 'ethpandaops') THEN 'internal'
            ELSE 'unclassified'
        END AS classification,
        -- Client metadata
        p.meta_client_version AS meta_client_version,
        p.meta_client_implementation AS meta_client_implementation,
        p.meta_client_os AS meta_client_os,
        p.meta_client_geo_city AS meta_client_geo_city,
        p.meta_client_geo_country AS meta_client_geo_country,
        p.meta_client_geo_country_code AS meta_client_geo_country_code,
        p.meta_client_geo_continent_code AS meta_client_geo_continent_code,
        p.meta_client_geo_longitude AS meta_client_geo_longitude,
        p.meta_client_geo_latitude AS meta_client_geo_latitude,
        p.meta_client_geo_autonomous_system_number AS meta_client_geo_autonomous_system_number,
        p.meta_client_geo_autonomous_system_organization AS meta_client_geo_autonomous_system_organization,
        -- Peer metadata from heartbeat
        COALESCE(h.remote_agent_implementation, '') AS meta_peer_implementation,
        COALESCE(h.remote_agent_version, '') AS meta_peer_version,
        COALESCE(h.remote_agent_platform, '') AS meta_peer_platform,
        COALESCE(h.remote_geo_city, '') AS meta_peer_geo_city,
        COALESCE(h.remote_geo_country, '') AS meta_peer_geo_country,
        COALESCE(h.remote_geo_country_code, '') AS meta_peer_geo_country_code,
        COALESCE(h.remote_geo_continent_code, '') AS meta_peer_geo_continent_code,
        h.remote_geo_longitude AS meta_peer_geo_longitude,
        h.remote_geo_latitude AS meta_peer_geo_latitude,
        h.remote_geo_autonomous_system_number AS meta_peer_geo_autonomous_system_number,
        h.remote_geo_autonomous_system_organization AS meta_peer_geo_autonomous_system_organization
    FROM filtered_probes AS p
    LEFT JOIN heartbeat_data AS h
        ON p.peer_id_unique_key = h.remote_peer_id_unique_key
        AND p.meta_network_name = h.meta_network_name
    LEFT JOIN beacon_blocks AS bb
        ON p.slot_start_date_time = bb.slot_start_date_time
    LEFT JOIN blob_submitters AS bs
        ON bb.execution_payload_block_number = bs.block_number
)

-- Final aggregation
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    any(event_date_time) AS probe_date_time,
    slot,
    any(slot_start_date_time) AS slot_start_date_time,
    peer_id_unique_key,
    result,
    error,
    groupUniqArray(column_index) AS column_indices,
    arrayFilter(x -> x != '', groupUniqArray(blob_submitter_name)) AS blob_submitters,
    any(response_time_ms) AS response_time_ms,
    any(username) AS username,
    any(node_id) AS node_id,
    any(classification) AS classification,
    any(meta_client_version) AS meta_client_version,
    any(meta_client_implementation) AS meta_client_implementation,
    any(meta_client_os) AS meta_client_os,
    any(meta_client_geo_city) AS meta_client_geo_city,
    any(meta_client_geo_country) AS meta_client_geo_country,
    any(meta_client_geo_country_code) AS meta_client_geo_country_code,
    any(meta_client_geo_continent_code) AS meta_client_geo_continent_code,
    any(meta_client_geo_longitude) AS meta_client_geo_longitude,
    any(meta_client_geo_latitude) AS meta_client_geo_latitude,
    any(meta_client_geo_autonomous_system_number) AS meta_client_geo_autonomous_system_number,
    any(meta_client_geo_autonomous_system_organization) AS meta_client_geo_autonomous_system_organization,
    any(meta_peer_implementation) AS meta_peer_implementation,
    any(meta_peer_version) AS meta_peer_version,
    any(meta_peer_platform) AS meta_peer_platform,
    any(meta_peer_geo_city) AS meta_peer_geo_city,
    any(meta_peer_geo_country) AS meta_peer_geo_country,
    any(meta_peer_geo_country_code) AS meta_peer_geo_country_code,
    any(meta_peer_geo_continent_code) AS meta_peer_geo_continent_code,
    any(meta_peer_geo_longitude) AS meta_peer_geo_longitude,
    any(meta_peer_geo_latitude) AS meta_peer_geo_latitude,
    any(meta_peer_geo_autonomous_system_number) AS meta_peer_geo_autonomous_system_number,
    any(meta_peer_geo_autonomous_system_organization) AS meta_peer_geo_autonomous_system_organization
FROM enriched_probes
GROUP BY
    slot,
    peer_id_unique_key,
    result,
    error
