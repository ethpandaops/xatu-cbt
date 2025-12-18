---
table: int_peer_custody_count_by_epoch
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - epoch
  - peerdas
  - custody
  - peer
dependencies:
  - "{{external}}.libp2p_handle_metadata"
  - "{{external}}.libp2p_synthetic_heartbeat"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Step 1: Filter metadata by time bounds
-- MUST filter by event_date_time first for partition pruning (partition key is toYYYYMM(event_date_time))
filtered_metadata AS (
    SELECT
        event_date_time,
        -- Derive epoch from event_date_time (384 seconds per epoch)
        toUInt64((toUnixTimestamp(event_date_time) - {{ .env.GENESIS_TIME }}) / 384) AS epoch,
        toDateTime({{ .env.GENESIS_TIME }} + (toUInt64((toUnixTimestamp(event_date_time) - {{ .env.GENESIS_TIME }}) / 384) * 384)) AS epoch_start_date_time,
        peer_id_unique_key,
        custody_group_count
    FROM {{ index .dep "{{external}}" "libp2p_handle_metadata" "helpers" "from" }} FINAL
    WHERE
        event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND meta_network_name = '{{ .env.NETWORK }}'
        AND direction = 'outbound'
        AND custody_group_count IS NOT NULL
),

-- Step 2: Get heartbeat data within ±5 min window for peer metadata enrichment
-- MUST filter by event_date_time first for partition pruning
-- Using argMax to get the most recent value for each field
heartbeat_data AS (
    SELECT
        remote_peer_id_unique_key,
        -- Agent metadata
        argMax(remote_agent_implementation, event_date_time) AS peer_agent_implementation,
        argMax(remote_agent_version, event_date_time) AS peer_agent_version,
        argMax(remote_agent_version_major, event_date_time) AS peer_agent_version_major,
        argMax(remote_agent_version_minor, event_date_time) AS peer_agent_version_minor,
        argMax(remote_agent_version_patch, event_date_time) AS peer_agent_version_patch,
        argMax(remote_agent_platform, event_date_time) AS peer_agent_platform,
        -- Network metadata
        argMax(remote_ip, event_date_time) AS peer_ip,
        argMax(remote_port, event_date_time) AS peer_port,
        -- Geolocation metadata
        argMax(remote_geo_city, event_date_time) AS peer_geo_city,
        argMax(remote_geo_country, event_date_time) AS peer_geo_country,
        argMax(remote_geo_country_code, event_date_time) AS peer_geo_country_code,
        argMax(remote_geo_continent_code, event_date_time) AS peer_geo_continent_code,
        argMax(remote_geo_longitude, event_date_time) AS peer_geo_longitude,
        argMax(remote_geo_latitude, event_date_time) AS peer_geo_latitude,
        -- ISP/ASN metadata
        argMax(remote_geo_autonomous_system_number, event_date_time) AS peer_geo_autonomous_system_number,
        argMax(remote_geo_autonomous_system_organization, event_date_time) AS peer_geo_autonomous_system_organization
    FROM {{ index .dep "{{external}}" "libp2p_synthetic_heartbeat" "helpers" "from" }} FINAL
    WHERE
        event_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 5 MINUTE
        AND event_date_time <= fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 5 MINUTE
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY remote_peer_id_unique_key
)

-- Step 3: Deduplicate per epoch (peer may report metadata multiple times within an epoch)
-- Take the max custody_group_count if it changes within the epoch
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    epoch,
    any(epoch_start_date_time) AS epoch_start_date_time,
    toUInt8(max(custody_group_count)) AS custody_group_count,
    peer_id_unique_key,
    -- Agent metadata
    any(COALESCE(h.peer_agent_implementation, '')) AS peer_agent_implementation,
    any(COALESCE(h.peer_agent_version, '')) AS peer_agent_version,
    any(COALESCE(h.peer_agent_version_major, '')) AS peer_agent_version_major,
    any(COALESCE(h.peer_agent_version_minor, '')) AS peer_agent_version_minor,
    any(COALESCE(h.peer_agent_version_patch, '')) AS peer_agent_version_patch,
    any(COALESCE(h.peer_agent_platform, '')) AS peer_agent_platform,
    -- Network metadata
    any(h.peer_ip) AS peer_ip,
    any(h.peer_port) AS peer_port,
    -- Geolocation metadata
    any(COALESCE(h.peer_geo_city, '')) AS peer_geo_city,
    any(COALESCE(h.peer_geo_country, '')) AS peer_geo_country,
    any(COALESCE(h.peer_geo_country_code, '')) AS peer_geo_country_code,
    any(COALESCE(h.peer_geo_continent_code, '')) AS peer_geo_continent_code,
    any(h.peer_geo_longitude) AS peer_geo_longitude,
    any(h.peer_geo_latitude) AS peer_geo_latitude,
    -- ISP/ASN metadata
    any(h.peer_geo_autonomous_system_number) AS peer_geo_autonomous_system_number,
    any(h.peer_geo_autonomous_system_organization) AS peer_geo_autonomous_system_organization
FROM filtered_metadata m
LEFT JOIN heartbeat_data h ON m.peer_id_unique_key = h.remote_peer_id_unique_key
GROUP BY
    epoch,
    peer_id_unique_key
