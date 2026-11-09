---
table: fct_peer_custody_coverage_daily
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
-- Rewrites every day the bounds touch so partial days are recounted in full. Days the recount no longer produces
-- are rewritten with zero counts.
-- One row a day on how evenly consensus peers custody the 128 data columns: the thinnest column, the median and
-- the thickest, and the thinnest once supernodes or each column's largest hosting network are taken away.
-- Custody follows fct_peer_custody_by_column_daily.
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
    known AS (
        SELECT
            p.day_start_date AS day_start_date,
            p.groups AS groups,
            n.custody_group_order AS custody_group_order,
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
    ),
    peer_totals AS (
        SELECT
            day_start_date,
            count() AS peer_n,
            countIf(groups >= 128) AS supernode_n
        FROM known
        GROUP BY day_start_date
    ),
    columns AS (
        SELECT
            day_start_date,
            column_index,
            sum(peer_n) AS peer_n,
            sum(largest_asn_n) AS largest_asn_n,
            sum(country_n) AS country_n
        FROM (
            SELECT
                day_start_date,
                toUInt8(arrayJoin(arraySlice(custody_group_order, 1, groups))) AS column_index,
                count() AS peer_n,
                arrayMax(arrayConcat([0], sumMapIf([autonomous_system_number], [toUInt64(1)], network_type = 'hosting' AND autonomous_system_number != 0).2)) AS largest_asn_n,
                uniqExactIf(country_code, country_code != '') AS country_n
            FROM known
            GROUP BY day_start_date, column_index

            UNION ALL

            -- Columns no peer custodies count as zero.
            SELECT
                day_start_date,
                toUInt8(arrayJoin(range(128))) AS column_index,
                0 AS peer_n,
                0 AS largest_asn_n,
                0 AS country_n
            FROM peer_totals
        )
        GROUP BY day_start_date, column_index
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    toUInt32(sum(peer_n)) AS custody_peer_count,
    toUInt32(sum(supernode_n)) AS supernode_peer_count,
    toUInt32(sum(min_n)) AS min_column_custodian_peer_count,
    toUInt8(sum(min_column)) AS min_column_index,
    toUInt32(sum(median_n)) AS median_column_custodian_peer_count,
    toUInt32(sum(max_n)) AS max_column_custodian_peer_count,
    toUInt32(sum(min_without_supernodes_n)) AS min_column_non_supernode_peer_count,
    toUInt32(sum(min_without_largest_asn_n)) AS min_column_without_largest_autonomous_system_peer_count,
    toUInt16(sum(min_country_n)) AS min_column_country_count
FROM (
    SELECT
        c.day_start_date AS day_start_date,
        any(t.peer_n) AS peer_n,
        any(t.supernode_n) AS supernode_n,
        min(c.peer_n) AS min_n,
        -- The lowest column index among the thinnest columns.
        argMin(c.column_index, (c.peer_n, c.column_index)) AS min_column,
        quantileExactLow(0.5)(c.peer_n) AS median_n,
        max(c.peer_n) AS max_n,
        -- Supernodes custody every column.
        min(c.peer_n) - any(t.supernode_n) AS min_without_supernodes_n,
        min(c.peer_n - c.largest_asn_n) AS min_without_largest_asn_n,
        min(c.country_n) AS min_country_n
    FROM columns AS c
    INNER JOIN peer_totals AS t ON c.day_start_date = t.day_start_date
    GROUP BY c.day_start_date

    UNION ALL

    SELECT
        day_start_date,
        0 AS peer_n,
        0 AS supernode_n,
        0 AS min_n,
        0 AS min_column,
        0 AS median_n,
        0 AS max_n,
        0 AS min_without_supernodes_n,
        0 AS min_without_largest_asn_n,
        0 AS min_country_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
