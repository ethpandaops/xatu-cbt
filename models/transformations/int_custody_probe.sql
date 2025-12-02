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
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    -- Time bucket (3 second window to group probes together)
    toStartOfInterval(event_date_time, INTERVAL 3 SECOND) AS probe_date_time,
    -- Group key fields
    peer_id_unique_key,
    result,
    error,
    -- Aggregated arrays
    groupUniqArray(slot) AS slots,
    groupUniqArray(column_index) AS column_indices,
    -- Response time (same for all probes in a grouped request)
    any(response_time_ms) AS response_time_ms,
    -- Client classification
    any(username) AS username,
    any(node_id) AS node_id,
    any(classification) AS classification,
    -- Client metadata (who ran the probe)
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
    -- Peer metadata (who was probed)
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
FROM (
    SELECT
        probe.event_date_time,
        probe.slot,
        probe.column_index,
        probe.peer_id_unique_key,
        probe.result,
        probe.response_time_ms,
        COALESCE(probe.error, '') AS error,
        -- Client classification and parsing
        CASE
            WHEN startsWith(probe.meta_client_name, 'pub-') THEN
                splitByChar('/', probe.meta_client_name)[2]
            WHEN startsWith(probe.meta_client_name, 'corp-') THEN
                splitByChar('/', probe.meta_client_name)[2]
            ELSE
                'ethpandaops'
        END AS username,
        CASE
            WHEN startsWith(probe.meta_client_name, 'pub-') THEN
                splitByChar('/', probe.meta_client_name)[3]
            WHEN startsWith(probe.meta_client_name, 'corp-') THEN
                splitByChar('/', probe.meta_client_name)[3]
            ELSE
                splitByChar('/', probe.meta_client_name)[-1]
        END AS node_id,
        CASE
            WHEN startsWith(probe.meta_client_name, 'pub-') THEN
                'individual'
            WHEN startsWith(probe.meta_client_name, 'corp-') THEN
                'corporate'
            WHEN startsWith(probe.meta_client_name, 'ethpandaops') THEN
                'internal'
            ELSE
                'unclassified'
        END AS classification,
        -- Client metadata
        probe.meta_client_version,
        probe.meta_client_implementation,
        probe.meta_client_os,
        probe.meta_client_geo_city,
        probe.meta_client_geo_country,
        probe.meta_client_geo_country_code,
        probe.meta_client_geo_continent_code,
        probe.meta_client_geo_longitude,
        probe.meta_client_geo_latitude,
        probe.meta_client_geo_autonomous_system_number,
        probe.meta_client_geo_autonomous_system_organization,
        -- Peer metadata from heartbeat
        COALESCE(heartbeat.remote_agent_implementation, '') AS meta_peer_implementation,
        COALESCE(heartbeat.remote_agent_version, '') AS meta_peer_version,
        COALESCE(heartbeat.remote_agent_platform, '') AS meta_peer_platform,
        COALESCE(heartbeat.remote_geo_city, '') AS meta_peer_geo_city,
        COALESCE(heartbeat.remote_geo_country, '') AS meta_peer_geo_country,
        COALESCE(heartbeat.remote_geo_country_code, '') AS meta_peer_geo_country_code,
        COALESCE(heartbeat.remote_geo_continent_code, '') AS meta_peer_geo_continent_code,
        heartbeat.remote_geo_longitude AS meta_peer_geo_longitude,
        heartbeat.remote_geo_latitude AS meta_peer_geo_latitude,
        heartbeat.remote_geo_autonomous_system_number AS meta_peer_geo_autonomous_system_number,
        heartbeat.remote_geo_autonomous_system_organization AS meta_peer_geo_autonomous_system_organization
    FROM {{ index .dep "{{external}}" "libp2p_rpc_data_column_custody_probe" "helpers" "from" }} AS probe FINAL
    GLOBAL LEFT JOIN (
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
    ) AS heartbeat ON probe.peer_id_unique_key = heartbeat.remote_peer_id_unique_key
        AND probe.meta_network_name = heartbeat.meta_network_name
    WHERE
        probe.event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND probe.meta_network_name = '{{ .env.NETWORK }}'
)
GROUP BY
    probe_date_time,
    peer_id_unique_key,
    result,
    error
