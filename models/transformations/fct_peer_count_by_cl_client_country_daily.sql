---
table: fct_peer_count_by_cl_client_country_daily
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 1m"
tags:
  - daily
  - peer
  - client
  - geo
  - libp2p
dependencies:
  - "{{external}}.libp2p_synthetic_heartbeat"
  - "{{external}}.libp2p_identify"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and NULL percentiles.
-- Consensus peers our libp2p sentries saw in the day by client and the country of their latest address.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    cl_rows AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            remote_peer_id_unique_key AS peer_id_unique_key,
            event_date_time,
            lower(remote_agent_implementation) AS implementation,
            remote_agent_version AS agent_version,
            remote_agent_platform AS agent_platform,
            remote_ip AS ip,
            remote_geo_country_code AS country_code,
            remote_geo_country AS country,
            remote_geo_continent_code AS continent_code,
            remote_geo_autonomous_system_number AS asn,
            remote_geo_autonomous_system_organization AS asn_organization,
            lower(direction) = 'outbound' AS dialled,
            position(remote_maddrs, '/quic') > 0 AS quic_connected,
            false AS quic_listen,
            latency_ms,
            connection_age_ms
        FROM {{ index .dep "{{external}}" "libp2p_synthetic_heartbeat" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'

        UNION ALL

        SELECT
            toDate(event_date_time) AS day_start_date,
            remote_peer_id_unique_key AS peer_id_unique_key,
            event_date_time,
            lower(remote_agent_implementation) AS implementation,
            remote_agent_version AS agent_version,
            remote_agent_platform AS agent_platform,
            remote_ip AS ip,
            remote_geo_country_code AS country_code,
            remote_geo_country AS country,
            remote_geo_continent_code AS continent_code,
            remote_geo_autonomous_system_number AS asn,
            remote_geo_autonomous_system_organization AS asn_organization,
            lower(direction) = 'outbound' AS dialled,
            transport = 'quic-v1' AS quic_connected,
            arrayExists(a -> position(a, '/quic') > 0, listen_addrs) AS quic_listen,
            NULL AS latency_ms,
            NULL AS connection_age_ms
        FROM {{ index .dep "{{external}}" "libp2p_identify" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND success
          AND remote_agent_implementation != ''
    ),
    cl_peer_days AS (
        SELECT
            day_start_date,
            peer_id_unique_key,
            -- Heartbeats taken before identify completed carry no agent, so rows with one win.
            argMax((implementation, agent_version, agent_platform), (implementation != '', event_date_time, agent_version, agent_platform)) AS agent,
            argMax((ip, country_code, country, continent_code, asn, asn_organization), (ip IS NOT NULL, event_date_time, ifNull(ip, toIPv6('::')))) AS location,
            max(dialled) AS dialled,
            max(quic_connected) AS quic_connected,
            max(quic_listen) AS quic_listen,
            quantileIf(0.5)(latency_ms, latency_ms IS NOT NULL) AS latency_ms,
            maxIf(connection_age_ms, connection_age_ms IS NOT NULL) AS connection_age_ms
        FROM cl_rows
        GROUP BY day_start_date, peer_id_unique_key
    ),
    cl_peers AS (
        SELECT
            p.day_start_date AS day_start_date,
            p.peer_id_unique_key AS peer_id_unique_key,
            toDateTime(p.day_start_date) + INTERVAL 1 DAY AS seen_at,
            agent.1 AS implementation,
            multiIf(
                agent.1 = '', 'unknown',
                agent.1 = 'erigon', 'caplin',
                match(agent.1, '^[a-z][a-z0-9-]*$'), agent.1,
                'other'
            ) AS client,
            -- Caplin reports its Erigon version and commit in the platform field.
            if(agent.1 = 'erigon', agent.3, agent.2) AS version_text,
            extract(version_text, '^v?\\d+\\.\\d+\\.\\d+(?:-(?:rc|beta|alpha)[.0-9]*)?') AS semver,
            multiIf(
                agent.1 = 'lodestar', extract(agent.3, '^([0-9a-f]{7,40})$'),
                extract(version_text, '[-+]g?([0-9a-f]{7,40})(?:-[a-z]+)?$') != '', extract(version_text, '[-+]g?([0-9a-f]{7,40})(?:-[a-z]+)?$'),
                semver != '' AND extract(enr_version_segment, '^v?\\d+\\.\\d+\\.\\d+(?:-(?:rc|beta|alpha)[.0-9]*)?') = semver, enr_commit,
                ''
            ) AS commit,
            '' AS enr_version_segment,
            '' AS enr_commit,
            location,
            dialled,
            quic_connected,
            quic_listen,
            latency_ms,
            connection_age_ms
        FROM cl_peer_days AS p
        WHERE NOT (agent.1 IN ('tysm', 'hermes', 'nebula', 'eth2-crawler', 'enrscout', 'oft-probe', 'migalabs-kumo', 'xrpc-basel1lc', 'geth-blsync', 'nimbus-verified-proxy') OR match(agent.1, 'crawler|probe|scout|armiarma'))
    ),
    peer_locations AS (
        SELECT
            day_start_date,
            client,
            location.4 AS continent_code,
            location.2 AS country_code,
            location.3 AS country
        FROM cl_peers
    ),
    country_counts AS (
        SELECT
            day_start_date,
            client, if(continent_code = '', 'unknown', continent_code) AS continent_code,
            if(country_code = '', 'unknown', country_code) AS country_code,
            if(country = '', 'Unknown', country) AS country,
            count() AS peers
        FROM peer_locations
        GROUP BY day_start_date, client, continent_code, country_code, country
    ),
    continent_rollup AS (
        SELECT
            day_start_date,
            client, continent_code,
            if(peers >= 3, country_code, 'other') AS country_code,
            if(peers >= 3, country, 'Other') AS country,
            sum(peers) AS continent_peers
        FROM country_counts
        GROUP BY day_start_date, client, continent_code, country_code, country
    ),
    country_rollup AS (
        SELECT
            day_start_date,
            client, if(continent_peers >= 3, continent_code, 'other') AS continent_code,
            if(continent_peers >= 3, country_code, 'other') AS country_code,
            if(continent_peers >= 3, country, 'Other') AS country,
            sum(continent_peers) AS rolled_peers
        FROM continent_rollup
        GROUP BY day_start_date, client, continent_code, country_code, country
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    client,
    continent_code,
    country_code,
    country,
    toUInt32(sum(peer_count)) AS peer_count
FROM (
    SELECT
        day_start_date,
        client,
        continent_code,
        country_code,
        country,
        rolled_peers AS peer_count
    FROM country_rollup

    UNION ALL

    SELECT
        day_start_date,
        client,
        continent_code,
        country_code,
        country,
        0 AS peer_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, client, continent_code, country_code, country
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
