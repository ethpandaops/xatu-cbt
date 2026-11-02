---
table: fct_peer_count_by_client_pair_daily
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
  - pairing
dependencies:
  - "{{external}}.libp2p_synthetic_heartbeat"
  - "{{external}}.libp2p_identify"
  - "{{external}}.node_record_consensus"
  - "{{external}}.node_record_execution"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and NULL percentiles.
-- Consensus and execution client pairs from nodes sharing an IPv4 address in the day: consensus peers from
-- libp2p and ENRs, execution nodes from discovery. Only addresses with exactly one consensus and one execution
-- client count as a pair. Addresses are only compared inside this query, nothing per address is stored.
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
    cl_enr_peers AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            node_id,
            argMax((lower(implementation), name, fork_digest, next_fork_digest, ip), (event_date_time, name, ifNull(ip, toIPv6('::')))) AS record
        FROM {{ index .dep "{{external}}" "node_record_consensus" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY day_start_date, node_id
    ),
    el_rows AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            node_id,
            event_date_time,
            lower(implementation) AS implementation,
            name,
            genesis,
            fork_id_hash,
            fork_id_next,
            ip,
            geo_country_code AS country_code,
            geo_country AS country,
            geo_continent_code AS continent_code,
            geo_autonomous_system_number AS asn,
            geo_autonomous_system_organization AS asn_organization
        FROM {{ index .dep "{{external}}" "node_record_execution" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
    ),
    network_genesis AS (
        SELECT day_start_date, topK(1)(genesis)[1] AS genesis
        FROM el_rows
        GROUP BY day_start_date
    ),
    el_node_days AS (
        SELECT
            day_start_date,
            node_id,
            argMax(
                (implementation, name, fork_id_hash, fork_id_next, ip, country_code, country, continent_code, asn, asn_organization),
                (event_date_time, name, ifNull(ip, toIPv6('::')))
            ) AS record
        FROM el_rows
        WHERE (day_start_date, genesis) GLOBAL IN (SELECT day_start_date, genesis FROM network_genesis)
        GROUP BY day_start_date, node_id
    ),
    el_peers AS (
        SELECT
            day_start_date,
            node_id,
            toDateTime(day_start_date) + INTERVAL 1 DAY AS seen_at,
            record.1 AS implementation,
            multiIf(
                record.1 = 'nimbus', 'nimbusel',
                match(record.1, '^[a-z][a-z0-9_-]*$'), record.1,
                'other'
            ) AS client,
            arrayFirst(s -> match(s, '\\d+\\.\\d+\\.\\d+'), splitByChar('/', record.2)) AS version_segment,
            extract(version_segment, '^v?\\d+\\.\\d+\\.\\d+(?:-(?:rc|beta|alpha)[.0-9]*)?') AS semver,
            extract(version_segment, '[-+]g?([0-9a-f]{7,40})(?:-[a-z]+)?$') AS commit,
            record.3 AS fork_id_hash,
            record.4 AS fork_id_next,
            record.5 AS ip,
            record.6 AS country_code,
            record.7 AS country,
            record.8 AS continent_code,
            record.9 AS asn,
            record.10 AS asn_organization
        FROM el_node_days
        WHERE record.1 != '' AND NOT match(lower(record.2), 'probe|scout|sniff|leech|observer|listener|crawler|xatu')
    ),
    ip_layers AS (
        SELECT day_start_date, location.1 AS ip, 'consensus' AS layer, client
        FROM cl_peers
        WHERE client NOT IN ('unknown', 'other') AND location.1 IS NOT NULL AND startsWith(IPv6NumToString(location.1), '::ffff:')

        UNION ALL

        SELECT day_start_date, record.5 AS ip, 'consensus' AS layer, multiIf(
            record.1 = '', 'unknown',
            record.1 = 'erigon', 'caplin',
            match(record.1, '^[a-z][a-z0-9-]*$'), record.1,
            'other'
        ) AS client
        FROM cl_enr_peers
        WHERE record.1 != '' AND NOT (record.1 IN ('tysm', 'hermes', 'nebula', 'eth2-crawler', 'enrscout', 'oft-probe', 'migalabs-kumo', 'xrpc-basel1lc', 'geth-blsync', 'nimbus-verified-proxy') OR match(record.1, 'crawler|probe|scout|armiarma')) AND record.5 IS NOT NULL AND startsWith(IPv6NumToString(record.5), '::ffff:')

        UNION ALL

        SELECT day_start_date, ip, 'execution' AS layer, client
        FROM el_peers
        WHERE client != 'other' AND ip IS NOT NULL AND startsWith(IPv6NumToString(ip), '::ffff:')
    ),
    ip_clients AS (
        SELECT
            day_start_date,
            ip,
            groupUniqArrayIf(client, layer = 'consensus') AS cl_clients,
            groupUniqArrayIf(client, layer = 'execution') AS el_clients
        FROM ip_layers
        GROUP BY day_start_date, ip
    ),
    day_totals AS (
        SELECT
            day_start_date,
            countIf(length(cl_clients) > 0) AS cl_ip_count,
            countIf(length(el_clients) > 0) AS el_ip_count,
            countIf(length(cl_clients) > 0 AND length(el_clients) > 0) AS matched_ip_count,
            countIf(length(cl_clients) = 1 AND length(el_clients) = 1) AS paired_ip_count
        FROM ip_clients
        GROUP BY day_start_date
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    consensus_client,
    execution_client,
    toUInt32(sum(ip_count)) AS ip_count,
    toUInt32(max(cl_ip_count)) AS cl_ip_count,
    toUInt32(max(el_ip_count)) AS el_ip_count,
    toUInt32(max(matched_ip_count)) AS matched_ip_count,
    toUInt32(max(paired_ip_count)) AS paired_ip_count
FROM (
    SELECT
        pairs.day_start_date AS day_start_date,
        consensus_client,
        execution_client,
        ip_count,
        cl_ip_count,
        el_ip_count,
        matched_ip_count,
        paired_ip_count
    FROM (
        SELECT
            day_start_date,
            cl_clients[1] AS consensus_client,
            el_clients[1] AS execution_client,
            count() AS ip_count
        FROM ip_clients
        WHERE length(cl_clients) = 1 AND length(el_clients) = 1
        GROUP BY day_start_date, consensus_client, execution_client
    ) AS pairs
    GLOBAL INNER JOIN day_totals AS t ON pairs.day_start_date = t.day_start_date

    UNION ALL

    SELECT
        day_start_date,
        consensus_client,
        execution_client,
        0 AS ip_count,
        0 AS cl_ip_count,
        0 AS el_ip_count,
        0 AS matched_ip_count,
        0 AS paired_ip_count
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, consensus_client, execution_client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
