---
table: fct_peer_count_by_asn_country_daily
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
  - geo
dependencies:
  - "{{transformation}}.int_peer_daily"
  - "{{external}}.libp2p_synthetic_heartbeat"
  - "{{external}}.libp2p_identify"
  - - "{{external}}.node_record_consensus"
    - "{{external}}.libp2p_synthetic_heartbeat"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts.
-- Consensus peers our libp2p sentries or our discovery crawler saw in the day by autonomous system and country,
-- with reachability and custody counts. Cities are not crossed with autonomous systems.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    -- Peer locations are read from raw because int_peer_daily keeps city and autonomous system for hosting peers only.
    -- The choice of location matches int_peer_daily: the latest geolocated sentry row, else the latest ENR.
    sentry_locations AS (
        SELECT
            day_start_date,
            peer_id_unique_key,
            argMax((ip, city, asn, asn_organization), (ip IS NOT NULL, event_date_time, ifNull(ip, toIPv6('::')))) AS location
        FROM (
            SELECT
                toDate(event_date_time) AS day_start_date,
                remote_peer_id_unique_key AS peer_id_unique_key,
                event_date_time,
                remote_ip AS ip,
                remote_geo_city AS city,
                remote_geo_autonomous_system_number AS asn,
                remote_geo_autonomous_system_organization AS asn_organization
            FROM {{ index .dep "{{external}}" "libp2p_synthetic_heartbeat" "helpers" "from" }}
            WHERE event_date_time >= (SELECT min_day FROM day_bounds)
              AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
              AND meta_network_name = '{{ .env.NETWORK }}'

            UNION ALL

            SELECT
                toDate(event_date_time) AS day_start_date,
                remote_peer_id_unique_key AS peer_id_unique_key,
                event_date_time,
                remote_ip AS ip,
                remote_geo_city AS city,
                remote_geo_autonomous_system_number AS asn,
                remote_geo_autonomous_system_organization AS asn_organization
            FROM {{ index .dep "{{external}}" "libp2p_identify" "helpers" "from" }}
            WHERE event_date_time >= (SELECT min_day FROM day_bounds)
              AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
              AND meta_network_name = '{{ .env.NETWORK }}'
              AND success
              AND remote_agent_implementation != ''
        )
        GROUP BY day_start_date, peer_id_unique_key
    ),
    enr_locations AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            assumeNotNull(peer_id_unique_key) AS peer_id_unique_key,
            argMax((geo_city, geo_autonomous_system_number, geo_autonomous_system_organization), (event_date_time, name, ifNull(ip, toIPv6('::')))) AS location
        FROM {{ index .dep "{{external}}" "node_record_consensus" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND peer_id_unique_key IS NOT NULL
        GROUP BY day_start_date, peer_id_unique_key
    ),
    -- The peers of the day, their client, country and network type come from int_peer_daily, which also leaves
    -- crawlers out.
    located_peers AS (
        SELECT
            p.day_start_date AS day_start_date,
            p.peer_id_unique_key AS peer_id_unique_key,
            p.*,
            s.location.1 IS NOT NULL AS sentry_located,
            if(sentry_located, s.location.2, ifNull(e.location.1, '')) AS raw_city,
            if(sentry_located, s.location.3, e.location.2) AS raw_asn,
            if(sentry_located, s.location.4, e.location.3) AS raw_asn_organization
        FROM (
            SELECT *
            FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
            WHERE day_start_date >= (SELECT min_day FROM day_bounds)
              AND day_start_date <= (SELECT max_day FROM day_bounds)
              AND (seen_by_sentry OR seen_by_enr)
        ) AS p
        GLOBAL LEFT JOIN sentry_locations AS s ON p.day_start_date = s.day_start_date AND p.peer_id_unique_key = s.peer_id_unique_key
        GLOBAL LEFT JOIN enr_locations AS e ON p.day_start_date = e.day_start_date AND p.peer_id_unique_key = e.peer_id_unique_key
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    autonomous_system_number,
    autonomous_system_organization,
    network_type,
    continent_code,
    country_code,
    country,
    toUInt32(sum(peer_count_n)) AS peer_count,
    toUInt32(sum(sentry_peer_count_n)) AS sentry_peer_count,
    toUInt32(sum(enr_peer_count_n)) AS enr_peer_count,
    toUInt32(sum(dialled_peer_count_n)) AS dialled_peer_count,
    toUInt32(sum(inbound_only_peer_count_n)) AS inbound_only_peer_count,
    toUInt32(sum(quic_peer_count_n)) AS quic_peer_count,
    toUInt32(sum(custody_known_peer_count_n)) AS custody_known_peer_count,
    toUInt32(sum(custody_4_peer_count_n)) AS custody_4_peer_count,
    toUInt32(sum(custody_5_8_peer_count_n)) AS custody_5_8_peer_count,
    toUInt32(sum(custody_9_127_peer_count_n)) AS custody_9_127_peer_count,
    toUInt32(sum(custody_128_peer_count_n)) AS custody_128_peer_count
FROM (
    SELECT
        day_start_date,
        ifNull(raw_asn, 0) AS autonomous_system_number,
        if(raw_asn IS NULL, 'unknown', ifNull(raw_asn_organization, '')) AS autonomous_system_organization,
        network_type AS network_type,
        if(continent_code = '', 'unknown', continent_code) AS continent_code,
        if(country_code = '', 'unknown', country_code) AS country_code,
        if(country = '', 'Unknown', country) AS country,
        count() AS peer_count_n,
        countIf(seen_by_sentry) AS sentry_peer_count_n,
        countIf(seen_by_enr) AS enr_peer_count_n,
        countIf(dialled) AS dialled_peer_count_n,
        countIf(inbound AND NOT dialled) AS inbound_only_peer_count_n,
        countIf(quic_connected OR quic_listen) AS quic_peer_count_n,
        countIf(custody_group_count IS NOT NULL) AS custody_known_peer_count_n,
        countIf(custody_group_count <= 4) AS custody_4_peer_count_n,
        countIf(custody_group_count BETWEEN 5 AND 8) AS custody_5_8_peer_count_n,
        countIf(custody_group_count BETWEEN 9 AND 127) AS custody_9_127_peer_count_n,
        countIf(custody_group_count >= 128) AS custody_128_peer_count_n
    FROM located_peers
    GROUP BY day_start_date, autonomous_system_number, autonomous_system_organization, network_type, continent_code, country_code, country

    UNION ALL

    SELECT
        day_start_date,
        autonomous_system_number, autonomous_system_organization, network_type, continent_code, country_code, country,
        0 AS peer_count_n,
        0 AS sentry_peer_count_n,
        0 AS enr_peer_count_n,
        0 AS dialled_peer_count_n,
        0 AS inbound_only_peer_count_n,
        0 AS quic_peer_count_n,
        0 AS custody_known_peer_count_n,
        0 AS custody_4_peer_count_n,
        0 AS custody_5_8_peer_count_n,
        0 AS custody_9_127_peer_count_n,
        0 AS custody_128_peer_count_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, autonomous_system_number, autonomous_system_organization, network_type, continent_code, country_code, country
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
