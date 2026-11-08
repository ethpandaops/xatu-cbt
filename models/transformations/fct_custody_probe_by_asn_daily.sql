---
table: fct_custody_probe_by_asn_daily
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
  - custody
  - peerdas
dependencies:
  - "{{transformation}}.int_custody_probe"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts and NULL percentiles.
-- Data column custody probes of the day by the probed peer's autonomous system (its latest location in the day).
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    probes AS (
        SELECT
            toDate(probe_date_time) AS day_start_date,
            peer_id_unique_key,
            probe_date_time,
            result,
            response_time_ms,
            lower(meta_peer_implementation) AS implementation,
            meta_peer_geo_continent_code AS continent_code,
            meta_peer_geo_country_code AS country_code,
            meta_peer_geo_country AS country,
            meta_peer_geo_autonomous_system_number AS asn,
            meta_peer_geo_autonomous_system_organization AS asn_organization
        FROM {{ index .dep "{{transformation}}" "int_custody_probe" "helpers" "from" }} FINAL
        WHERE probe_date_time >= (SELECT min_day FROM day_bounds)
          AND probe_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
    ),
    -- Each probed peer takes its latest agent and location of the day, as fct_custody_probe_by_client_daily does.
    probed_peers AS (
        SELECT
            day_start_date,
            peer_id_unique_key,
            argMax(implementation, (implementation != '', probe_date_time, implementation)) AS agent,
            argMax(
                (continent_code, country_code, country, asn, asn_organization),
                (asn IS NOT NULL, probe_date_time, ifNull(asn, 0))
            ) AS location
        FROM probes
        GROUP BY day_start_date, peer_id_unique_key
        HAVING NOT (agent IN ('tysm', 'hermes', 'nebula', 'eth2-crawler', 'enrscout', 'oft-probe', 'migalabs-kumo', 'xrpc-basel1lc', 'geth-blsync', 'nimbus-verified-proxy') OR match(agent, 'crawler|probe|scout|armiarma'))
    ),
    located AS (
        SELECT
            day_start_date,
            peer_id_unique_key,
            location.1 AS continent_code,
            location.2 AS country_code,
            location.3 AS country,
            location.4 AS asn,
            location.5 AS asn_organization,
            -- The same hosting rules as fct_peer_count_by_cl_client_asn_daily.
            multiIf(
            asn IS NULL, 'unknown',
            asn IN (24940, 213230, 212317, 16276, 35540, 16509, 14618, 8987, 396982, 15169, 19527, 8075, 8068, 31898, 45102, 37963, 132203, 45090, 14061, 63949, 20473, 51167, 40021, 141995, 60781, 16265, 28753, 30633, 59253, 12876, 54825, 396356, 20326, 395201, 197540, 46475, 7979, 19318, 212238, 204770, 23470, 29802, 29066, 9009, 62240, 136907, 8560, 53667, 13335, 47583, 36352, 32613, 55286), 'hosting',
            match(ifNull(asn_organization, ''), '(?i)(hosting|cloud|server|data ?cent(er|re)|datacamp|colo(cation|crossing)|vps|dedicated|amazon|google llc|microsoft|digitalocean|hetzner|ovh|contabo|linode|akamai|vultr|leaseweb|scaleway|equinix|oracle|alibaba|tencent|hostinger|ionos|latitude|teraswitch|allnodes|netcup|limestone|hivelocity)'), 'hosting',
            'other'
            ) AS network_type
        FROM probed_peers
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    autonomous_system_number,
    autonomous_system_organization,
    network_type,
    toUInt32(sum(peer_n)) AS peer_count,
    toUInt32(sum(probe_n)) AS probe_count,
    toUInt32(sum(success_n)) AS success_count,
    toUInt32(sum(missing_n)) AS missing_count,
    toUInt32(sum(failure_n)) AS failure_count,
    toUInt32(sum(missing_peer_n)) AS missing_peer_count,
    max(response_time_p50) AS response_time_p50_ms,
    max(response_time_p90) AS response_time_p90_ms
FROM (
    SELECT
        p.day_start_date AS day_start_date,
        ifNull(l.asn, 0) AS autonomous_system_number,
        if(l.asn IS NULL, 'unknown', ifNull(l.asn_organization, '')) AS autonomous_system_organization,
        l.network_type AS network_type,
        uniqExact(p.peer_id_unique_key) AS peer_n,
        count() AS probe_n,
        countIf(p.result = 'success') AS success_n,
        countIf(p.result = 'missing') AS missing_n,
        countIf(p.result = 'failure') AS failure_n,
        uniqExactIf(p.peer_id_unique_key, p.result = 'missing') AS missing_peer_n,
        if(isNaN(quantileIf(0.5)(p.response_time_ms, p.result = 'success')), NULL, quantileIf(0.5)(p.response_time_ms, p.result = 'success')) AS response_time_p50,
        if(isNaN(quantileIf(0.9)(p.response_time_ms, p.result = 'success')), NULL, quantileIf(0.9)(p.response_time_ms, p.result = 'success')) AS response_time_p90
    FROM probes AS p
    GLOBAL INNER JOIN located AS l ON p.day_start_date = l.day_start_date AND p.peer_id_unique_key = l.peer_id_unique_key
    GROUP BY day_start_date, autonomous_system_number, autonomous_system_organization, network_type

    UNION ALL

    SELECT
        day_start_date,
        autonomous_system_number, autonomous_system_organization, network_type,
        0 AS peer_n,
        0 AS probe_n,
        0 AS success_n,
        0 AS missing_n,
        0 AS failure_n,
        0 AS missing_peer_n,
        NULL AS response_time_p50,
        NULL AS response_time_p90
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, autonomous_system_number, autonomous_system_organization, network_type
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
