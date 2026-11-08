---
table: int_peer_daily
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
  - libp2p
  - discovery
dependencies:
  - "{{external}}.libp2p_synthetic_heartbeat"
  - "{{external}}.libp2p_identify"
  # Status exchanges, metadata, ENRs and peer IDs start later than heartbeats or stall on their own, so each is
  # paired with heartbeats and never holds the day back.
  - - "{{external}}.libp2p_handle_status"
    - "{{external}}.libp2p_synthetic_heartbeat"
  - - "{{external}}.libp2p_handle_metadata"
    - "{{external}}.libp2p_synthetic_heartbeat"
  - - "{{external}}.node_record_consensus"
    - "{{external}}.libp2p_synthetic_heartbeat"
  - - "{{external}}.libp2p_peer"
    - "{{external}}.libp2p_synthetic_heartbeat"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Peers stored for those days that the
-- recount no longer produces keep their key with seen_by_sentry and seen_by_enr false and every other value cleared.
-- One row per consensus peer per day, from our libp2p sentries (heartbeats, identify, metadata and status exchanges)
-- and the discovery crawler (ENRs). Sentry values win over ENR values. Crawlers, probes, light clients and our own
-- sentries are left out. Addresses are only compared inside the model and are never stored.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    sentry_rows AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            remote_peer_id_unique_key AS peer_id_unique_key,
            event_date_time,
            lower(remote_agent_implementation) AS implementation,
            remote_agent_version AS agent_version,
            remote_agent_platform AS agent_platform,
            remote_ip AS ip,
            remote_geo_city AS city,
            remote_geo_country_code AS country_code,
            remote_geo_country AS country,
            remote_geo_continent_code AS continent_code,
            remote_geo_autonomous_system_number AS asn,
            remote_geo_autonomous_system_organization AS asn_organization,
            lower(direction) = 'outbound' AS dialled,
            lower(direction) = 'inbound' AS inbound,
            position(remote_maddrs, '/quic') > 0 AS quic_connected,
            false AS quic_listen,
            latency_ms
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
            remote_geo_city AS city,
            remote_geo_country_code AS country_code,
            remote_geo_country AS country,
            remote_geo_continent_code AS continent_code,
            remote_geo_autonomous_system_number AS asn,
            remote_geo_autonomous_system_organization AS asn_organization,
            lower(direction) = 'outbound' AS dialled,
            lower(direction) = 'inbound' AS inbound,
            transport = 'quic-v1' AS quic_connected,
            arrayExists(a -> position(a, '/quic') > 0, listen_addrs) AS quic_listen,
            NULL AS latency_ms
        FROM {{ index .dep "{{external}}" "libp2p_identify" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND success
          AND remote_agent_implementation != ''
    ),
    sentry_peers AS (
        SELECT
            day_start_date,
            peer_id_unique_key,
            -- Heartbeats taken before identify completed carry no agent, so rows with one win.
            argMax((implementation, agent_version, agent_platform), (implementation != '', event_date_time, agent_version, agent_platform)) AS agent,
            argMax((ip, city, country_code, country, continent_code, asn, asn_organization), (ip IS NOT NULL, event_date_time, ifNull(ip, toIPv6('::')))) AS location,
            max(dialled) AS dialled,
            max(inbound) AS inbound,
            max(quic_connected) AS quic_connected,
            max(quic_listen) AS quic_listen,
            quantileExactIf(0.5)(latency_ms, latency_ms IS NOT NULL AND latency_ms > 0) AS median_latency_ms,
            countIf(latency_ms IS NOT NULL AND latency_ms > 0) AS latency_count
        FROM sentry_rows
        GROUP BY day_start_date, peer_id_unique_key
    ),
    enr_peers AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            assumeNotNull(peer_id_unique_key) AS peer_id_unique_key,
            argMax(
                (lower(implementation), version, ip, geo_city, geo_country_code, geo_country, geo_continent_code, geo_autonomous_system_number, geo_autonomous_system_organization, fork_digest, cgc),
                (event_date_time, name, ifNull(ip, toIPv6('::')))
            ) AS record
        FROM {{ index .dep "{{external}}" "node_record_consensus" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND peer_id_unique_key IS NOT NULL
        GROUP BY day_start_date, peer_id_unique_key
    ),
    -- The peer's own status: the response to our request, or the request it sent us. The other side is our sentry's
    -- status at the same moment, so lags are measured against the sentry that asked.
    status_rows AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            peer_id_unique_key,
            event_date_time,
            direction = 'outbound' AS outbound,
            if(outbound, response_fork_digest, request_fork_digest) AS peer_fork_digest,
            if(outbound, response_head_slot, request_head_slot) AS peer_head_slot,
            if(outbound, request_head_slot, response_head_slot) AS our_head_slot,
            if(outbound, response_finalized_epoch, request_finalized_epoch) AS peer_finalized_epoch,
            if(outbound, request_finalized_epoch, response_finalized_epoch) AS our_finalized_epoch,
            if(outbound, response_earliest_available_slot, request_earliest_available_slot) AS peer_earliest_available_slot
        FROM {{ index .dep "{{external}}" "libp2p_handle_status" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND direction IN ('inbound', 'outbound')
          AND error IS NULL
    ),
    status_peers AS (
        SELECT
            day_start_date,
            peer_id_unique_key,
            count() AS status_count,
            argMax(peer_fork_digest, (event_date_time, peer_fork_digest)) AS fork_digest,
            quantileExact(0.5)(toInt32(toInt64(assumeNotNull(our_head_slot)) - toInt64(assumeNotNull(peer_head_slot)))) AS head_lag_slots,
            quantileExact(0.5)(toInt32(toInt64(assumeNotNull(our_finalized_epoch)) - toInt64(assumeNotNull(peer_finalized_epoch)))) AS finalized_lag_epochs,
            argMaxIf(
                (peer_earliest_available_slot, toUInt32(greatest(toInt64(assumeNotNull(our_head_slot)) - toInt64(ifNull(peer_earliest_available_slot, 0)), 0))),
                (event_date_time, ifNull(peer_earliest_available_slot, 0)),
                peer_earliest_available_slot IS NOT NULL
            ) AS history,
            countIf(peer_earliest_available_slot IS NOT NULL) AS earliest_available_count
        FROM status_rows
        WHERE peer_head_slot IS NOT NULL
          AND our_head_slot IS NOT NULL
          AND peer_finalized_epoch IS NOT NULL
          AND our_finalized_epoch IS NOT NULL
        GROUP BY day_start_date, peer_id_unique_key
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
    peers AS (
        SELECT
            if(s.peer_id_unique_key IS NOT NULL, s.day_start_date, e.day_start_date) AS day_start_date,
            if(s.peer_id_unique_key IS NOT NULL, s.peer_id_unique_key, e.peer_id_unique_key) AS peer_id_unique_key,
            s.peer_id_unique_key IS NOT NULL AS seen_by_sentry,
            e.peer_id_unique_key IS NOT NULL AS seen_by_enr,
            -- Sentry peers without an agent fall back to the ENR client name.
            if(ifNull(s.agent.1, '') != '', s.agent.1, ifNull(e.record.1, '')) AS implementation,
            multiIf(
                implementation = '', 'unknown',
                implementation = 'erigon', 'caplin',
                match(implementation, '^[a-z][a-z0-9-]*$'), implementation,
                'other'
            ) AS client,
            -- Caplin reports its Erigon version and commit in the platform field.
            multiIf(
                ifNull(s.agent.1, '') = '', ifNull(e.record.2, ''),
                s.agent.1 = 'erigon', s.agent.3,
                s.agent.2
            ) AS agent_version,
            extract(agent_version, '^v?\\d+\\.\\d+\\.\\d+(?:-(?:rc|beta|alpha)[.0-9]*)?') AS version,
            s.location.1 IS NOT NULL AS sentry_located,
            if(sentry_located, s.location.1, e.record.3) AS ip,
            if(sentry_located, s.location.2, ifNull(e.record.4, '')) AS city,
            if(sentry_located, s.location.3, ifNull(e.record.5, '')) AS country_code,
            if(sentry_located, s.location.4, ifNull(e.record.6, '')) AS country,
            if(sentry_located, s.location.5, ifNull(e.record.7, '')) AS continent_code,
            if(sentry_located, s.location.6, e.record.8) AS asn,
            if(sentry_located, s.location.7, e.record.9) AS asn_organization,
            -- Only an IP the sentries connected to and one the ENR advertises can be compared.
            if(sentry_located AND e.record.3 IS NOT NULL, s.location.1 != e.record.3, NULL) AS enr_ip_mismatch,
            ifNull(s.dialled, false) AS dialled,
            ifNull(s.inbound, false) AS inbound,
            ifNull(s.quic_connected, false) AS quic_connected,
            ifNull(s.quic_listen, false) AS quic_listen,
            if(ifNull(s.latency_count, 0) > 0, s.median_latency_ms, NULL) AS latency_ms,
            -- ENR custody group counts are big-endian hex, 0x when unset.
            if(
                length(ifNull(e.record.11, '')) > 2,
                toUInt8OrNull(toString(reinterpretAsUInt64(reverse(unhex(substring(e.record.11, 3)))))),
                NULL
            ) AS enr_custody_group_count,
            e.record.10 AS enr_fork_digest
        FROM sentry_peers AS s
        GLOBAL FULL OUTER JOIN enr_peers AS e ON s.day_start_date = e.day_start_date AND s.peer_id_unique_key = e.peer_id_unique_key
        WHERE NOT (implementation IN ('tysm', 'hermes', 'nebula', 'eth2-crawler', 'enrscout', 'oft-probe', 'migalabs-kumo', 'xrpc-basel1lc', 'geth-blsync', 'nimbus-verified-proxy') OR match(implementation, 'crawler|probe|scout|armiarma'))
    ),
    peer_ids AS (
        SELECT unique_key, argMax(peer_id, updated_date_time) AS peer_id
        FROM {{ index .dep "{{external}}" "libp2p_peer" "helpers" "from" }}
        WHERE meta_network_name = '{{ .env.NETWORK }}'
        GROUP BY unique_key
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    peer_id_unique_key,
    argMax(peer_id, seen) AS peer_id,
    max(seen_by_sentry) AS seen_by_sentry,
    max(seen_by_enr) AS seen_by_enr,
    argMax(client, seen) AS client,
    argMax(version, seen) AS version,
    argMax(agent_version, seen) AS agent_version,
    argMax(continent_code, seen) AS continent_code,
    argMax(country_code, seen) AS country_code,
    argMax(country, seen) AS country,
    argMax(city, seen) AS city,
    argMax(autonomous_system_number, seen) AS autonomous_system_number,
    argMax(autonomous_system_organization, seen) AS autonomous_system_organization,
    argMax(network_type, seen) AS network_type,
    argMax(provider, seen) AS provider,
    max(ipv6) AS ipv6,
    max(dialled) AS dialled,
    max(inbound) AS inbound,
    max(quic_connected) AS quic_connected,
    max(quic_listen) AS quic_listen,
    argMax(enr_ip_mismatch, seen) AS enr_ip_mismatch,
    argMax(custody_group_count, seen) AS custody_group_count,
    argMax(latency_ms, seen) AS latency_ms,
    max(status_count) AS status_count,
    argMax(fork_digest, seen) AS fork_digest,
    argMax(head_lag_slots, seen) AS head_lag_slots,
    argMax(finalized_lag_epochs, seen) AS finalized_lag_epochs,
    argMax(earliest_available_slot, seen) AS earliest_available_slot,
    argMax(served_history_slots, seen) AS served_history_slots
FROM (
    SELECT
        p.day_start_date AS day_start_date,
        p.peer_id_unique_key AS peer_id_unique_key,
        true AS seen,
        ifNull(i.peer_id, '') AS peer_id,
        p.seen_by_sentry AS seen_by_sentry,
        p.seen_by_enr AS seen_by_enr,
        p.client AS client,
        p.version AS version,
        p.agent_version AS agent_version,
        p.continent_code AS continent_code,
        p.country_code AS country_code,
        p.country AS country,
        p.city AS city,
        ifNull(p.asn, 0) AS autonomous_system_number,
        ifNull(p.asn_organization, '') AS autonomous_system_organization,
        -- The same hosting rules as fct_peer_count_by_cl_client_asn_daily.
        multiIf(
            p.asn IS NULL, 'unknown',
            p.asn IN (24940, 213230, 212317, 16276, 35540, 16509, 14618, 8987, 396982, 15169, 19527, 8075, 8068, 31898, 45102, 37963, 132203, 45090, 14061, 63949, 20473, 51167, 40021, 141995, 60781, 16265, 28753, 30633, 59253, 12876, 54825, 396356, 20326, 395201, 197540, 46475, 7979, 19318, 212238, 204770, 23470, 29802, 29066, 9009, 62240, 136907, 8560, 53667, 13335, 47583, 36352, 32613, 55286), 'hosting',
            match(ifNull(p.asn_organization, ''), '(?i)(hosting|cloud|server|data ?cent(er|re)|datacamp|colo(cation|crossing)|vps|dedicated|amazon|google llc|microsoft|digitalocean|hetzner|ovh|contabo|linode|akamai|vultr|leaseweb|scaleway|equinix|oracle|alibaba|tencent|hostinger|ionos|latitude|teraswitch|allnodes|netcup|limestone|hivelocity)'), 'hosting',
            'other'
        ) AS network_type,
        -- Providers that span several networks or organisation names get one name, the rest drop legal suffixes.
        multiIf(
            ifNull(p.asn_organization, '') = '', '',
            match(p.asn_organization, '(?i)^amazon'), 'Amazon',
            match(p.asn_organization, '(?i)^google (llc|cloud)'), 'Google',
            match(p.asn_organization, '(?i)^microsoft'), 'Microsoft',
            match(p.asn_organization, '(?i)alibaba'), 'Alibaba',
            match(p.asn_organization, '(?i)^oracle'), 'Oracle',
            match(p.asn_organization, '(?i)^ovh'), 'OVHcloud',
            match(p.asn_organization, '(?i)^hetzner'), 'Hetzner',
            match(p.asn_organization, '(?i)^contabo'), 'Contabo',
            match(p.asn_organization, '(?i)^(akamai|linode)'), 'Akamai',
            match(p.asn_organization, '(?i)^tencent'), 'Tencent',
            trimRight(
                replaceRegexpOne(
                    replaceRegexpOne(p.asn_organization, '(?i)[,.]?\\s+(inc|llc|l\\.l\\.c|ltd|ltda|limited|private limited|pte|pty|gmbh|corp|corporation|co|company|sas|sa|s\\.a|s\\.a\\.u|bv|b\\.v|a\\.s|aps|ag|plc|oy|ab|srl|ltee|sp\\. z o\\.o)\\.?$', ''),
                    '(?i)[,.]?\\s+(inc|llc|ltd|limited|private|pte|pty|gmbh|corp|corporation|co|company|sas|sa|s\\.a|bv|b\\.v|ag|plc)\\.?$', ''
                ),
                ' ,.'
            )
        ) AS provider,
        ifNull(NOT (p.ip BETWEEN toIPv6('::ffff:0.0.0.0') AND toIPv6('::ffff:255.255.255.255')), false) AS ipv6,
        p.dialled AS dialled,
        p.inbound AS inbound,
        p.quic_connected AS quic_connected,
        p.quic_listen AS quic_listen,
        p.enr_ip_mismatch AS enr_ip_mismatch,
        if(c.peer_id_unique_key IS NOT NULL, c.peer_custody_group_count, p.enr_custody_group_count) AS custody_group_count,
        p.latency_ms AS latency_ms,
        toUInt32(ifNull(st.status_count, 0)) AS status_count,
        multiIf(
            ifNull(st.fork_digest, '') != '', concat('0x', st.fork_digest),
            ifNull(p.enr_fork_digest, '')
        ) AS fork_digest,
        if(st.peer_id_unique_key IS NOT NULL, st.head_lag_slots, NULL) AS head_lag_slots,
        if(st.peer_id_unique_key IS NOT NULL, st.finalized_lag_epochs, NULL) AS finalized_lag_epochs,
        if(ifNull(st.earliest_available_count, 0) > 0, st.history.1, NULL) AS earliest_available_slot,
        if(ifNull(st.earliest_available_count, 0) > 0, st.history.2, NULL) AS served_history_slots
    FROM peers AS p
    GLOBAL LEFT JOIN peer_ids AS i ON p.peer_id_unique_key = i.unique_key
    GLOBAL LEFT JOIN custody AS c ON p.day_start_date = c.day_start_date AND p.peer_id_unique_key = c.peer_id_unique_key
    GLOBAL LEFT JOIN status_peers AS st ON p.day_start_date = st.day_start_date AND p.peer_id_unique_key = st.peer_id_unique_key

    UNION ALL

    -- Peers stored for these days that the recount no longer produces.
    SELECT
        day_start_date,
        peer_id_unique_key,
        false AS seen,
        peer_id,
        false AS seen_by_sentry,
        false AS seen_by_enr,
        '' AS client,
        '' AS version,
        '' AS agent_version,
        '' AS continent_code,
        '' AS country_code,
        '' AS country,
        '' AS city,
        0 AS autonomous_system_number,
        '' AS autonomous_system_organization,
        '' AS network_type,
        '' AS provider,
        false AS ipv6,
        false AS dialled,
        false AS inbound,
        false AS quic_connected,
        false AS quic_listen,
        NULL AS enr_ip_mismatch,
        NULL AS custody_group_count,
        NULL AS latency_ms,
        0 AS status_count,
        '' AS fork_digest,
        NULL AS head_lag_slots,
        NULL AS finalized_lag_epochs,
        NULL AS earliest_available_slot,
        NULL AS served_history_slots
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, peer_id_unique_key
-- Merge peers across shards before inserting instead of each shard inserting its partial rows.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
