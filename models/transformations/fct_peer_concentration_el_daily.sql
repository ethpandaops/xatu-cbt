---
table: fct_peer_concentration_el_daily
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
  - "{{transformation}}.fct_peer_hosting_by_el_client_daily"
  - "{{transformation}}.fct_peer_count_by_el_country_daily"
  - "{{transformation}}.fct_peer_count_by_el_asn_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Days the recount no longer produces
-- are rewritten with zero counts.
-- How concentrated the day's execution nodes are: one row per day with the countries and autonomous systems they are on,
-- the largest systems, the fewest countries and systems that hold a third and half of them, and peers per continent.
-- Shares are of the peers with a known country or system. Residential systems folded into autonomous system 0 have
-- 1 or 2 peers each, so their sizes are rebuilt exactly from the folded row's peer and system counts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    totals AS (
        SELECT
            day_start_date,
            sum(peer_count) AS peers,
            sum(located_peer_count) AS located,
            sum(hosting_peer_count) AS hosting,
            sum(ipv6_peer_count) AS ipv6
        FROM {{ index .dep "{{transformation}}" "fct_peer_hosting_by_el_client_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
        GROUP BY day_start_date
    ),
    countries AS (
        SELECT
            day_start_date,
            arrayReverseSort(groupArrayIf(peer_count, country_code != 'unknown')) AS sizes,
            sumIf(peer_count, continent_code = 'AF') AS africa,
            sumIf(peer_count, continent_code = 'AN') AS antarctica,
            sumIf(peer_count, continent_code = 'AS') AS asia,
            sumIf(peer_count, continent_code = 'EU') AS europe,
            sumIf(peer_count, continent_code = 'NA') AS north_america,
            sumIf(peer_count, continent_code = 'OC') AS oceania,
            sumIf(peer_count, continent_code = 'SA') AS south_america,
            sumIf(peer_count, continent_code NOT IN ('AF', 'AN', 'AS', 'EU', 'NA', 'OC', 'SA')) AS unknown_continent
        FROM {{ index .dep "{{transformation}}" "fct_peer_count_by_el_country_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND peer_count > 0
        GROUP BY day_start_date
    ),
    asns AS (
        SELECT
            day_start_date,
            arrayReverseSort(groupArrayArray(if(
                autonomous_system_number = 0,
                arrayConcat(
                    arrayWithConstant(toUInt32(greatest(toInt64(peer_count) - autonomous_system_count, 0)), toUInt32(2)),
                    arrayWithConstant(toUInt32(greatest(2 * toInt64(autonomous_system_count) - peer_count, 0)), toUInt32(1))
                ),
                [toUInt32(peer_count)]
            ))) AS sizes
        FROM {{ index .dep "{{transformation}}" "fct_peer_count_by_el_asn_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND peer_count > 0
          AND autonomous_system_count > 0
        GROUP BY day_start_date
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    toUInt32(sum(peer_count_n)) AS peer_count,
    toUInt32(sum(located_peer_count_n)) AS located_peer_count,
    toUInt32(sum(hosting_peer_count_n)) AS hosting_peer_count,
    toUInt32(sum(ipv6_peer_count_n)) AS ipv6_peer_count,
    toUInt32(sum(country_count_n)) AS country_count,
    toUInt32(sum(third_country_count_n)) AS third_country_count,
    toUInt32(sum(half_country_count_n)) AS half_country_count,
    toUInt32(sum(autonomous_system_count_n)) AS autonomous_system_count,
    toUInt32(sum(top_1_asn_peer_count_n)) AS top_1_asn_peer_count,
    toUInt32(sum(top_5_asn_peer_count_n)) AS top_5_asn_peer_count,
    toUInt32(sum(top_10_asn_peer_count_n)) AS top_10_asn_peer_count,
    toUInt32(sum(third_asn_count_n)) AS third_asn_count,
    toUInt32(sum(half_asn_count_n)) AS half_asn_count,
    toUInt32(sum(africa_peer_count_n)) AS africa_peer_count,
    toUInt32(sum(antarctica_peer_count_n)) AS antarctica_peer_count,
    toUInt32(sum(asia_peer_count_n)) AS asia_peer_count,
    toUInt32(sum(europe_peer_count_n)) AS europe_peer_count,
    toUInt32(sum(north_america_peer_count_n)) AS north_america_peer_count,
    toUInt32(sum(oceania_peer_count_n)) AS oceania_peer_count,
    toUInt32(sum(south_america_peer_count_n)) AS south_america_peer_count,
    toUInt32(sum(unknown_continent_peer_count_n)) AS unknown_continent_peer_count
FROM (
    SELECT
        t.day_start_date AS day_start_date,
        t.peers AS peer_count_n,
        t.located AS located_peer_count_n,
        t.hosting AS hosting_peer_count_n,
        t.ipv6 AS ipv6_peer_count_n,
        length(c.sizes) AS country_count_n,
        arrayFirstIndex(x -> x * 3 >= arraySum(c.sizes), arrayCumSum(c.sizes)) AS third_country_count_n,
        arrayFirstIndex(x -> x * 2 >= arraySum(c.sizes), arrayCumSum(c.sizes)) AS half_country_count_n,
        length(a.sizes) AS autonomous_system_count_n,
        arraySum(arraySlice(a.sizes, 1, 1)) AS top_1_asn_peer_count_n,
        arraySum(arraySlice(a.sizes, 1, 5)) AS top_5_asn_peer_count_n,
        arraySum(arraySlice(a.sizes, 1, 10)) AS top_10_asn_peer_count_n,
        arrayFirstIndex(x -> x * 3 >= arraySum(a.sizes), arrayCumSum(a.sizes)) AS third_asn_count_n,
        arrayFirstIndex(x -> x * 2 >= arraySum(a.sizes), arrayCumSum(a.sizes)) AS half_asn_count_n,
        c.africa AS africa_peer_count_n,
        c.antarctica AS antarctica_peer_count_n,
        c.asia AS asia_peer_count_n,
        c.europe AS europe_peer_count_n,
        c.north_america AS north_america_peer_count_n,
        c.oceania AS oceania_peer_count_n,
        c.south_america AS south_america_peer_count_n,
        c.unknown_continent AS unknown_continent_peer_count_n
    FROM totals AS t
    GLOBAL LEFT JOIN countries AS c ON t.day_start_date = c.day_start_date
    GLOBAL LEFT JOIN asns AS a ON t.day_start_date = a.day_start_date
    WHERE t.peers > 0

    UNION ALL

    SELECT
        day_start_date,
        0 AS peer_count_n,
        0 AS located_peer_count_n,
        0 AS hosting_peer_count_n,
        0 AS ipv6_peer_count_n,
        0 AS country_count_n,
        0 AS third_country_count_n,
        0 AS half_country_count_n,
        0 AS autonomous_system_count_n,
        0 AS top_1_asn_peer_count_n,
        0 AS top_5_asn_peer_count_n,
        0 AS top_10_asn_peer_count_n,
        0 AS third_asn_count_n,
        0 AS half_asn_count_n,
        0 AS africa_peer_count_n,
        0 AS antarctica_peer_count_n,
        0 AS asia_peer_count_n,
        0 AS europe_peer_count_n,
        0 AS north_america_peer_count_n,
        0 AS oceania_peer_count_n,
        0 AS south_america_peer_count_n,
        0 AS unknown_continent_peer_count_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
