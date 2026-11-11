---
table: fct_peer_custody_by_column_daily
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
  - peerdas
  - custody
dependencies:
  - "{{transformation}}.int_peer_daily"
  - "{{transformation}}.dim_peer_node"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Every column gets a row on each day
-- with peers, zero when no peer custodies it, and keys stored for those days that the recount no longer produces
-- are rewritten with zero counts.
-- Consensus peers by the data columns they custody: a peer with custody group count n custodies the first n
-- groups its node ID assigns (dim_peer_node), and group g is column g.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    peers AS (
        SELECT
            day_start_date,
            peer_id_unique_key,
            least(custody_group_count, 128) AS groups,
            country_code,
            network_type,
            autonomous_system_number
        FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND (seen_by_sentry OR seen_by_enr)
          AND custody_group_count IS NOT NULL
          AND peer_id != ''
    ),
    custodians AS (
        SELECT
            p.day_start_date AS day_start_date,
            arrayJoin(arraySlice(n.custody_group_order, 1, p.groups)) AS column_index,
            p.groups AS groups,
            p.country_code AS country_code,
            p.network_type AS network_type,
            p.autonomous_system_number AS autonomous_system_number
        FROM peers AS p
        GLOBAL INNER JOIN (
            SELECT peer_id_unique_key, custody_group_order
            FROM {{ index .dep "{{transformation}}" "dim_peer_node" "helpers" "from" }} FINAL
            WHERE peer_id_unique_key GLOBAL IN (SELECT peer_id_unique_key FROM peers)
              AND node_id != ''
        ) AS n ON p.peer_id_unique_key = n.peer_id_unique_key
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    column_index,
    toUInt32(sum(peer_n)) AS custodian_peer_count,
    toUInt32(sum(supernode_n)) AS supernode_peer_count,
    toUInt32(sum(hosting_n)) AS hosting_peer_count,
    toUInt16(sum(country_n)) AS country_count,
    toUInt32(sum(largest_country_n)) AS largest_country_peer_count,
    toUInt16(sum(asn_n)) AS hosting_autonomous_system_count,
    toUInt32(sum(largest_asn_n)) AS largest_autonomous_system_peer_count
FROM (
    SELECT
        day_start_date,
        toUInt8(column_index) AS column_index,
        count() AS peer_n,
        countIf(groups >= 128) AS supernode_n,
        countIf(network_type = 'hosting') AS hosting_n,
        uniqExactIf(country_code, country_code != '') AS country_n,
        arrayMax(arrayConcat([0], sumMapIf([country_code], [toUInt64(1)], country_code != '').2)) AS largest_country_n,
        uniqExactIf(autonomous_system_number, network_type = 'hosting' AND autonomous_system_number != 0) AS asn_n,
        arrayMax(arrayConcat([0], sumMapIf([autonomous_system_number], [toUInt64(1)], network_type = 'hosting' AND autonomous_system_number != 0).2)) AS largest_asn_n
    FROM custodians
    GROUP BY day_start_date, column_index

    UNION ALL

    SELECT
        day_start_date,
        toUInt8(arrayJoin(range(128))) AS column_index,
        0 AS peer_n,
        0 AS supernode_n,
        0 AS hosting_n,
        0 AS country_n,
        0 AS largest_country_n,
        0 AS asn_n,
        0 AS largest_asn_n
    FROM (SELECT DISTINCT day_start_date FROM peers)

    UNION ALL

    SELECT
        day_start_date,
        column_index,
        0 AS peer_n,
        0 AS supernode_n,
        0 AS hosting_n,
        0 AS country_n,
        0 AS largest_country_n,
        0 AS asn_n,
        0 AS largest_asn_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, column_index
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
