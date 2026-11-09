---
table: fct_peer_subnet_daily
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
  - subnet
dependencies:
  - "{{transformation}}.int_peer_subnet_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Every subnet gets a row on each day
-- with peers, zero when no peer advertises it, and keys stored for those days that the recount no longer produces
-- are rewritten with zero counts.
-- Consensus peers by the attestation (64) and sync (4) subnets they last advertised in the day.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    peers AS (
        SELECT day_start_date, attnets, syncnets
        FROM {{ index .dep "{{transformation}}" "int_peer_subnet_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND response_count > 0
    ),
    subnets AS (
        SELECT 'attestation' AS subnet_type, arrayJoin(range(64)) AS subnet_id
        UNION ALL
        SELECT 'sync' AS subnet_type, arrayJoin(range(4)) AS subnet_id
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    subnet_type,
    subnet_id,
    toUInt32(sum(peer_n)) AS peer_count,
    toUInt32(sum(all_subnets_n)) AS all_subnets_peer_count
FROM (
    SELECT
        day_start_date,
        'attestation' AS subnet_type,
        toUInt8(arrayJoin(bitPositionsToArray(attnets))) AS subnet_id,
        count() AS peer_n,
        countIf(bitCount(attnets) = 64) AS all_subnets_n
    FROM peers
    GROUP BY day_start_date, subnet_id

    UNION ALL

    SELECT
        day_start_date,
        'sync' AS subnet_type,
        toUInt8(arrayJoin(bitPositionsToArray(bitAnd(syncnets, 15)))) AS subnet_id,
        count() AS peer_n,
        countIf(bitAnd(syncnets, 15) = 15) AS all_subnets_n
    FROM peers
    GROUP BY day_start_date, subnet_id

    UNION ALL

    SELECT
        d.day_start_date AS day_start_date,
        s.subnet_type AS subnet_type,
        toUInt8(s.subnet_id) AS subnet_id,
        0 AS peer_n,
        0 AS all_subnets_n
    FROM (SELECT DISTINCT day_start_date FROM peers) AS d
    CROSS JOIN subnets AS s

    UNION ALL

    SELECT
        day_start_date,
        subnet_type,
        subnet_id,
        0 AS peer_n,
        0 AS all_subnets_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, subnet_type, subnet_id
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
