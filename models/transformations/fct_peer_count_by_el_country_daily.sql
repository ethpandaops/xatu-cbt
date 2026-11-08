---
table: fct_peer_count_by_el_country_daily
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
  - discovery
dependencies:
  - "{{external}}.node_record_execution"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts.
-- Execution nodes our discovery crawler found in the day by country, all clients together.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
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
            has_ipv6,
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
                (implementation, name, fork_id_hash, fork_id_next, ip, country_code, country, continent_code, asn, asn_organization, has_ipv6),
                (event_date_time, name, ifNull(ip, toIPv6('::')))
            ) AS record
        FROM el_rows
        WHERE (day_start_date, genesis) GLOBAL IN (SELECT day_start_date, genesis FROM network_genesis)
        GROUP BY day_start_date, node_id
    ),
    -- Nodes on the network genesis can still be on another chain (PulseChain, ETC) or a stale fork. A node counts when
    -- its fork ID hash is held by at least 10% of the day's nodes on the genesis and that hash's usual next fork has
    -- not passed (around a fork, the hashes before and after it). A node whose own next fork (a timestamp) passed
    -- before the day is behind it and does not count.
    network_fork AS (
        SELECT day_start_date, fork_id_hash
        FROM (
            SELECT
                day_start_date,
                record.3 AS fork_id_hash,
                topK(1)(record.4)[1] AS hash_fork_id_next,
                count() AS hash_nodes,
                sum(hash_nodes) OVER (PARTITION BY day_start_date) AS day_nodes
            FROM el_node_days
            GROUP BY day_start_date, fork_id_hash
        )
        WHERE hash_nodes * 10 >= day_nodes
          AND NOT (
              toUInt64OrZero(hash_fork_id_next) BETWEEN 1000000000 AND 4294967295
              AND toDate(toDateTime(toUInt32(toUInt64OrZero(hash_fork_id_next)))) < day_start_date
          )
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
            record.10 AS asn_organization,
            record.11 AS has_ipv6
        FROM el_node_days
        WHERE record.1 != '' AND NOT match(lower(record.2), 'probe|scout|sniff|leech|observer|listener|crawler|xatu')
          AND (day_start_date, record.3) GLOBAL IN (SELECT day_start_date, fork_id_hash FROM network_fork)
          AND NOT (
              toUInt64OrZero(record.4) BETWEEN 1000000000 AND 4294967295
              AND toDate(toDateTime(toUInt32(toUInt64OrZero(record.4)))) < day_start_date
          )
    ),
    located AS (
        SELECT
            *,
            multiIf(
                asn IS NULL, 'unknown',
                asn IN (24940, 213230, 212317, 16276, 35540, 16509, 14618, 8987, 396982, 15169, 19527, 8075, 8068, 31898, 45102, 37963, 132203, 45090, 14061, 63949, 20473, 51167, 40021, 141995, 60781, 16265, 28753, 30633, 59253, 12876, 54825, 396356, 20326, 395201, 197540, 46475, 7979, 19318, 212238, 204770, 23470, 29802, 29066, 9009, 62240, 136907, 8560, 53667, 13335, 47583, 36352, 32613, 55286), 'hosting',
                match(ifNull(asn_organization, ''), '(?i)(hosting|cloud|server|data ?cent(er|re)|datacamp|colo(cation|crossing)|vps|dedicated|amazon|google llc|microsoft|digitalocean|hetzner|ovh|contabo|linode|akamai|vultr|leaseweb|scaleway|equinix|oracle|alibaba|tencent|hostinger|ionos|latitude|teraswitch|allnodes|netcup|limestone|hivelocity)'), 'hosting',
                'other'
            ) AS network_type,
            -- Discovery records the IPv4 address it dialled; IPv6 support is the record advertising an IPv6 address.
            has_ipv6 AS ipv6
        FROM el_peers
    ),
    cells AS (
        SELECT
            day_start_date,
            if(continent_code = '', 'unknown', continent_code) AS continent_code,
            if(country_code = '', 'unknown', country_code) AS country_code,
            if(country = '', 'Unknown', country) AS country,
            count() AS peer_count_n,
            countIf(network_type = 'hosting') AS hosting_peer_count_n
        FROM located
        GROUP BY day_start_date, continent_code, country_code, country
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    continent_code,
    country_code,
    country,
    toUInt32(sum(peer_count_n)) AS peer_count,
    toUInt32(sum(hosting_peer_count_n)) AS hosting_peer_count
FROM (
    SELECT
        day_start_date,
        continent_code, country_code, country,
        peer_count_n,
        hosting_peer_count_n
    FROM cells

    UNION ALL

    SELECT
        day_start_date,
        continent_code, country_code, country,
        0 AS peer_count_n,
        0 AS hosting_peer_count_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, continent_code, country_code, country
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
