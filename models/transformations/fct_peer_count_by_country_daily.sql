---
table: fct_peer_count_by_country_daily
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
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts.
-- Consensus peers our libp2p sentries or our discovery crawler saw in the day by country, with reachability,
-- hosting and custody counts.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    continent_code,
    country_code,
    country,
    toUInt32(sum(peer_count_n)) AS peer_count,
    toUInt32(sum(sentry_peer_count_n)) AS sentry_peer_count,
    toUInt32(sum(enr_peer_count_n)) AS enr_peer_count,
    toUInt32(sum(dialled_peer_count_n)) AS dialled_peer_count,
    toUInt32(sum(inbound_only_peer_count_n)) AS inbound_only_peer_count,
    toUInt32(sum(quic_peer_count_n)) AS quic_peer_count,
    toUInt32(sum(hosting_peer_count_n)) AS hosting_peer_count,
    toUInt32(sum(custody_known_peer_count_n)) AS custody_known_peer_count,
    toUInt32(sum(custody_4_peer_count_n)) AS custody_4_peer_count,
    toUInt32(sum(custody_5_8_peer_count_n)) AS custody_5_8_peer_count,
    toUInt32(sum(custody_9_127_peer_count_n)) AS custody_9_127_peer_count,
    toUInt32(sum(custody_128_peer_count_n)) AS custody_128_peer_count
FROM (
    SELECT
        day_start_date,
        if(continent_code = '', 'unknown', continent_code) AS continent_code,
        if(country_code = '', 'unknown', country_code) AS country_code,
        if(country = '', 'Unknown', country) AS country,
        count() AS peer_count_n,
        countIf(seen_by_sentry) AS sentry_peer_count_n,
        countIf(seen_by_enr) AS enr_peer_count_n,
        countIf(dialled) AS dialled_peer_count_n,
        countIf(inbound AND NOT dialled) AS inbound_only_peer_count_n,
        countIf(quic_connected OR quic_listen) AS quic_peer_count_n,
        countIf(network_type = 'hosting') AS hosting_peer_count_n,
        countIf(custody_group_count IS NOT NULL) AS custody_known_peer_count_n,
        countIf(custody_group_count <= 4) AS custody_4_peer_count_n,
        countIf(custody_group_count BETWEEN 5 AND 8) AS custody_5_8_peer_count_n,
        countIf(custody_group_count BETWEEN 9 AND 127) AS custody_9_127_peer_count_n,
        countIf(custody_group_count >= 128) AS custody_128_peer_count_n
    FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
      AND (seen_by_sentry OR seen_by_enr)
    GROUP BY day_start_date, continent_code, country_code, country

    UNION ALL

    SELECT
        day_start_date,
        continent_code, country_code, country,
        0 AS peer_count_n,
        0 AS sentry_peer_count_n,
        0 AS enr_peer_count_n,
        0 AS dialled_peer_count_n,
        0 AS inbound_only_peer_count_n,
        0 AS quic_peer_count_n,
        0 AS hosting_peer_count_n,
        0 AS custody_known_peer_count_n,
        0 AS custody_4_peer_count_n,
        0 AS custody_5_8_peer_count_n,
        0 AS custody_9_127_peer_count_n,
        0 AS custody_128_peer_count_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, continent_code, country_code, country
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
