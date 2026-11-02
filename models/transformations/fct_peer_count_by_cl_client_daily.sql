---
table: fct_peer_count_by_cl_client_daily
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
  - release
  - libp2p
dependencies:
  - "{{external}}.libp2p_synthetic_heartbeat"
  - "{{external}}.libp2p_identify"
  - "{{external}}.libp2p_handle_metadata"
  - "{{external}}.node_record_consensus"
  - "{{transformation}}.dim_client_release"
  - "{{transformation}}.dim_client_commit"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and NULL percentiles.
-- One count per consensus peer our libp2p sentries saw in the day (heartbeats and identify exchanges), with its
-- latest agent. Crawlers, probes, light clients and our own sentries are left out. Custody group counts come
-- from the peer's own metadata responses to our outbound requests.
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
    enr_names AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            assumeNotNull(peer_id_unique_key) AS peer_id_unique_key,
            argMax(name, (event_date_time, name)) AS enr_name
        FROM {{ index .dep "{{external}}" "node_record_consensus" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND peer_id_unique_key IS NOT NULL
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
            arrayFirst(s -> match(s, '\\d+\\.\\d+\\.\\d+'), splitByChar('/', ifNull(e.enr_name, ''))) AS enr_version_segment,
            if(
                extract(enr_version_segment, '[-+]g?([0-9a-f]{7,40})(?:-[a-z]+)?$') != '',
                extract(enr_version_segment, '[-+]g?([0-9a-f]{7,40})(?:-[a-z]+)?$'),
                arrayFirst(s -> match(s, '^[0-9a-f]{7,40}$'), splitByChar('/', ifNull(e.enr_name, '')))
            ) AS enr_commit,
            location,
            dialled,
            quic_connected,
            quic_listen,
            latency_ms,
            connection_age_ms
        FROM cl_peer_days AS p
        GLOBAL LEFT JOIN enr_names AS e ON p.day_start_date = e.day_start_date AND p.peer_id_unique_key = e.peer_id_unique_key
        WHERE NOT (agent.1 IN ('tysm', 'hermes', 'nebula', 'eth2-crawler', 'enrscout', 'oft-probe', 'migalabs-kumo', 'xrpc-basel1lc', 'geth-blsync', 'nimbus-verified-proxy') OR match(agent.1, 'crawler|probe|scout|armiarma'))
    ),
    custody AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            peer_id_unique_key,
            argMax(custody_group_count, (event_date_time, custody_group_count)) AS peer_custody_group_count
        FROM {{ index .dep "{{external}}" "libp2p_handle_metadata" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND direction = 'outbound'
          AND error IS NULL
          AND custody_group_count IS NOT NULL
        GROUP BY day_start_date, peer_id_unique_key
    ),
    releases AS (
        SELECT
            client,
            version,
            commit_sha,
            release_date,
            -- Stable before pre-release, then the numerically highest version.
            (NOT prerelease, arrayMap(x -> toUInt32OrZero(x), extractAll(version, '\\d+'))) AS version_rank
        FROM {{ index .dep "{{transformation}}" "dim_client_release" "helpers" "from" }} FINAL
        -- Fail until both dimension tables have run once, so a fresh deployment retries instead of storing unresolved rows.
        WHERE throwIf((SELECT count() FROM {{ index .dep "{{transformation}}" "dim_client_release" "helpers" "from" }}) = 0, 'dim_client_release is empty') = 0
          AND throwIf((SELECT count() FROM {{ index .dep "{{transformation}}" "dim_client_commit" "helpers" "from" }}) = 0, 'dim_client_commit is empty') = 0
    ),
    -- One row per release and commit prefix length, so a prefix lookup is an equality join.
    release_prefixes AS (
        SELECT
            client,
            substring(commit_sha, 1, prefix_length) AS commit_prefix,
            commit_sha,
            version,
            release_date,
            version_rank
        FROM releases
        ARRAY JOIN arrayConcat(range(4, 13), [40]) AS prefix_length
    ),
    -- Several releases on a prefix keep only those published by the end of the day, like Lab's slot page.
    commit_releases AS (
        SELECT
            l.client AS release_client,
            l.commit AS release_commit,
            l.seen_at AS release_seen_at,
            uniqExact(r.commit_sha) AS commit_count,
            uniqExactIf(r.commit_sha, r.release_date IS NULL OR r.release_date <= l.seen_at) AS published_commit_count,
            argMax(r.version, (r.version_rank, r.version)) AS any_version,
            argMaxIf(r.version, (r.version_rank, r.version), r.release_date IS NULL OR r.release_date <= l.seen_at) AS published_version
        FROM (SELECT DISTINCT client, commit, seen_at FROM cl_peers WHERE commit != '') AS l
        GLOBAL LEFT JOIN release_prefixes AS r ON l.client = r.client AND l.commit = r.commit_prefix
        GROUP BY release_client, release_commit, release_seen_at
    ),
    -- A commit no release tag points at takes the release it builds on from dim_client_commit (default and release
    -- branch commits). Prefixes under 7 characters also need a commit from the 180 days before.
    commit_branches AS (
        SELECT
            l.client AS branch_client,
            l.commit AS branch_commit,
            l.seen_at AS branch_seen_at,
            uniqExact(c.commit_sha) AS branch_commit_count,
            any(c.base_version) AS branch_base_version
        FROM (SELECT DISTINCT client, commit, seen_at FROM cl_peers WHERE length(commit) >= 4) AS l
        GLOBAL INNER JOIN (
            SELECT client, commit_sha, commit_date, base_version FROM {{ index .dep "{{transformation}}" "dim_client_commit" "helpers" "from" }} FINAL
        ) AS c ON l.client = c.client AND substring(l.commit, 1, 4) = substring(c.commit_sha, 1, 4)
        WHERE startsWith(c.commit_sha, l.commit)
          AND c.commit_date <= l.seen_at
          AND (length(l.commit) >= 7 OR c.commit_date >= l.seen_at - INTERVAL 180 DAY)
        GROUP BY branch_client, branch_commit, branch_seen_at
    ),
    -- Versions without a commit match tags with or without the leading v.
    version_releases AS (
        SELECT
            client AS version_client,
            replaceRegexpOne(version, '^[vV]', '') AS normalized_version,
            argMax(version, (version_rank, version)) AS release_version
        FROM releases
        GROUP BY version_client, normalized_version
    ),
    resolved AS (
        SELECT
            p.*,
            multiIf(
                match(p.commit, '^0+$'), 'commit_zero',
                p.commit != '' AND (c.commit_count = 1 OR c.published_commit_count = 1), 'commit_match',
                p.commit != '' AND c.commit_count > 1, 'commit_ambiguous',
                p.commit != '' AND b.branch_commit_count = 1, 'commit_untagged',
                p.commit != '' AND b.branch_commit_count > 1, 'commit_ambiguous',
                p.commit != '', 'commit_unmatched',
                p.semver != '' AND v.release_version IS NOT NULL, 'version_match',
                p.semver != '', 'version_unmatched',
                'none'
            ) AS release_status,
            ifNull(multiIf(
                release_status = 'commit_match', if(c.commit_count = 1, c.any_version, c.published_version),
                release_status = 'commit_untagged' AND b.branch_base_version != '', b.branch_base_version,
                release_status = 'version_match', v.release_version,
                p.semver
            ), '') AS version
        FROM cl_peers AS p
        GLOBAL LEFT JOIN commit_releases AS c
            ON p.client = c.release_client AND p.commit = c.release_commit AND p.seen_at = c.release_seen_at
        GLOBAL LEFT JOIN commit_branches AS b
            ON p.client = b.branch_client AND p.commit = b.branch_commit AND p.seen_at = b.branch_seen_at
        GLOBAL LEFT JOIN version_releases AS v
            ON p.client = v.version_client AND replaceRegexpOne(p.semver, '^[vV]', '') = v.normalized_version
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    client,
    version,
    release_status,
    toUInt32(sum(peer_count)) AS peer_count,
    toUInt32(sum(dialled_peer_count)) AS dialled_peer_count,
    toUInt32(sum(quic_connected_peer_count)) AS quic_connected_peer_count,
    toUInt32(sum(quic_listen_peer_count)) AS quic_listen_peer_count,
    toUInt32(sum(custody_known_peer_count)) AS custody_known_peer_count,
    toUInt32(sum(custody_4_peer_count)) AS custody_4_peer_count,
    toUInt32(sum(custody_5_8_peer_count)) AS custody_5_8_peer_count,
    toUInt32(sum(custody_9_127_peer_count)) AS custody_9_127_peer_count,
    toUInt32(sum(custody_128_peer_count)) AS custody_128_peer_count,
    max(latency_p50_ms) AS latency_p50_ms,
    max(latency_p90_ms) AS latency_p90_ms,
    max(connection_age_p50_ms) AS connection_age_p50_ms
FROM (
    SELECT
        r.day_start_date AS day_start_date,
        client,
        version,
        release_status,
        count() AS peer_count,
        countIf(dialled) AS dialled_peer_count,
        countIf(quic_connected) AS quic_connected_peer_count,
        countIf(quic_listen) AS quic_listen_peer_count,
        countIf(peer_custody_group_count IS NOT NULL) AS custody_known_peer_count,
        countIf(peer_custody_group_count <= 4) AS custody_4_peer_count,
        countIf(peer_custody_group_count BETWEEN 5 AND 8) AS custody_5_8_peer_count,
        countIf(peer_custody_group_count BETWEEN 9 AND 127) AS custody_9_127_peer_count,
        countIf(peer_custody_group_count >= 128) AS custody_128_peer_count,
        if(isNaN(quantileIf(0.5)(latency_ms, isFinite(latency_ms))), NULL, quantileIf(0.5)(latency_ms, isFinite(latency_ms))) AS latency_p50_ms,
        if(isNaN(quantileIf(0.9)(latency_ms, isFinite(latency_ms))), NULL, quantileIf(0.9)(latency_ms, isFinite(latency_ms))) AS latency_p90_ms,
        if(isNaN(quantileIf(0.5)(connection_age_ms, isFinite(latency_ms))), NULL, quantileIf(0.5)(connection_age_ms, isFinite(latency_ms))) AS connection_age_p50_ms
    FROM resolved AS r
    GLOBAL LEFT JOIN custody AS c ON r.day_start_date = c.day_start_date AND r.peer_id_unique_key = c.peer_id_unique_key
    GROUP BY day_start_date, client, version, release_status

    UNION ALL

    SELECT
        day_start_date,
        client,
        version,
        release_status,
        0 AS peer_count,
        0 AS dialled_peer_count,
        0 AS quic_connected_peer_count,
        0 AS quic_listen_peer_count,
        0 AS custody_known_peer_count,
        0 AS custody_4_peer_count,
        0 AS custody_5_8_peer_count,
        0 AS custody_9_127_peer_count,
        0 AS custody_128_peer_count,
        NULL AS latency_p50_ms,
        NULL AS latency_p90_ms,
        NULL AS connection_age_p50_ms
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, client, version, release_status
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
