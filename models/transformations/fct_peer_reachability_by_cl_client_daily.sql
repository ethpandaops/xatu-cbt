---
table: fct_peer_reachability_by_cl_client_daily
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
  - client
  - libp2p
dependencies:
  - "{{transformation}}.int_peer_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts.
-- Consensus peers our libp2p sentries saw in the day by client: which ones our sentries could dial, which only
-- connected in, QUIC support, and whether their ENR advertises the address we reached them on.
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
    toUInt32(sum(dialled_n)) AS dialled_peer_count,
    toUInt32(sum(inbound_only_n)) AS inbound_only_peer_count,
    toUInt32(sum(quic_connected_n)) AS quic_connected_peer_count,
    toUInt32(sum(quic_listen_n)) AS quic_listen_peer_count,
    toUInt32(sum(enr_n)) AS enr_peer_count,
    toUInt32(sum(enr_ip_mismatch_n)) AS enr_ip_mismatch_peer_count
FROM (
    SELECT
        day_start_date,
        -- Named consensus clients keep their name. Other agents (libp2p tools, crawlers we do not know) are other.
        if(client IN ('lighthouse', 'lodestar', 'nimbus', 'prysm', 'teku', 'grandine', 'caplin', 'silver', 'ethlambda', 'unknown'), client, 'other') AS client,
        count() AS peer_n,
        countIf(dialled) AS dialled_n,
        countIf(inbound AND NOT dialled) AS inbound_only_n,
        countIf(quic_connected) AS quic_connected_n,
        countIf(quic_listen) AS quic_listen_n,
        countIf(seen_by_enr) AS enr_n,
        countIf(enr_ip_mismatch) AS enr_ip_mismatch_n
    FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
      AND seen_by_sentry
    GROUP BY day_start_date, client

    UNION ALL

    SELECT
        day_start_date,
        client,
        0 AS peer_n,
        0 AS dialled_n,
        0 AS inbound_only_n,
        0 AS quic_connected_n,
        0 AS quic_listen_n,
        0 AS enr_n,
        0 AS enr_ip_mismatch_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
