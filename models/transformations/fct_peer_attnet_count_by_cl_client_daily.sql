---
table: fct_peer_attnet_count_by_cl_client_daily
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
  - "{{transformation}}.int_peer_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that the
-- recount no longer produces are rewritten with zero counts.
-- Consensus peers that answered our sentries' metadata requests in the day, by client and by how many attestation
-- and sync subnets they last advertised.
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
    client,
    toUInt32(sum(peer_n)) AS peer_count,
    toUInt32(sum(attnets_0_n)) AS attnets_0_peer_count,
    toUInt32(sum(attnets_1_2_n)) AS attnets_1_2_peer_count,
    toUInt32(sum(attnets_3_63_n)) AS attnets_3_63_peer_count,
    toUInt32(sum(attnets_64_n)) AS attnets_64_peer_count,
    toUInt32(sum(syncnets_any_n)) AS syncnets_any_peer_count,
    toUInt32(sum(syncnets_4_n)) AS syncnets_4_peer_count
FROM (
    SELECT
        s.day_start_date AS day_start_date,
        -- Named consensus clients keep their name. Other agents (libp2p tools, crawlers we do not know) are other.
        if(p.client IN ('lighthouse', 'lodestar', 'nimbus', 'prysm', 'teku', 'grandine', 'caplin', 'silver', 'ethlambda', 'unknown'), p.client, 'other') AS client,
        count() AS peer_n,
        countIf(bitCount(s.attnets) = 0) AS attnets_0_n,
        countIf(bitCount(s.attnets) BETWEEN 1 AND 2) AS attnets_1_2_n,
        countIf(bitCount(s.attnets) BETWEEN 3 AND 63) AS attnets_3_63_n,
        countIf(bitCount(s.attnets) = 64) AS attnets_64_n,
        countIf(s.syncnets != 0) AS syncnets_any_n,
        countIf(bitCount(s.syncnets) = 4) AS syncnets_4_n
    FROM {{ index .dep "{{transformation}}" "int_peer_subnet_daily" "helpers" "from" }} AS s FINAL
    GLOBAL INNER JOIN (
        SELECT day_start_date, peer_id_unique_key, client
        FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND seen_by_sentry
    ) AS p ON s.day_start_date = p.day_start_date AND s.peer_id_unique_key = p.peer_id_unique_key
    WHERE s.day_start_date >= (SELECT min_day FROM day_bounds)
      AND s.day_start_date <= (SELECT max_day FROM day_bounds)
      AND s.response_count > 0
    GROUP BY day_start_date, client

    UNION ALL

    SELECT
        day_start_date,
        client,
        0 AS peer_n,
        0 AS attnets_0_n,
        0 AS attnets_1_2_n,
        0 AS attnets_3_63_n,
        0 AS attnets_64_n,
        0 AS syncnets_any_n,
        0 AS syncnets_4_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
