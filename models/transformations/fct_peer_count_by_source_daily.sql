---
table: fct_peer_count_by_source_daily
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
  - discovery
dependencies:
  - "{{transformation}}.int_peer_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Days the recount no longer produces
-- are rewritten with zero counts.
-- Consensus peers in the day by the source that saw them: our libp2p sentries, our discovery crawler, or both.
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
    toUInt32(sum(peer_n)) AS peer_count,
    toUInt32(sum(sentry_n)) AS sentry_peer_count,
    toUInt32(sum(enr_n)) AS enr_peer_count,
    toUInt32(sum(sentry_and_enr_n)) AS sentry_and_enr_peer_count,
    toUInt32(sum(dialled_n)) AS dialled_peer_count,
    toUInt32(sum(inbound_only_n)) AS inbound_only_peer_count,
    toUInt32(sum(quic_n)) AS quic_peer_count,
    toUInt32(sum(ipv6_n)) AS ipv6_peer_count,
    toUInt32(sum(hosting_n)) AS hosting_peer_count,
    toUInt32(sum(located_n)) AS located_peer_count,
    toUInt32(sum(custody_known_n)) AS custody_known_peer_count,
    toUInt32(sum(custody_4_n)) AS custody_4_peer_count,
    toUInt32(sum(custody_5_8_n)) AS custody_5_8_peer_count,
    toUInt32(sum(custody_9_127_n)) AS custody_9_127_peer_count,
    toUInt32(sum(custody_128_n)) AS custody_128_peer_count
FROM (
    SELECT
        day_start_date,
        count() AS peer_n,
        countIf(seen_by_sentry) AS sentry_n,
        countIf(seen_by_enr) AS enr_n,
        countIf(seen_by_sentry AND seen_by_enr) AS sentry_and_enr_n,
        countIf(dialled) AS dialled_n,
        countIf(inbound AND NOT dialled) AS inbound_only_n,
        countIf(quic_connected OR quic_listen) AS quic_n,
        countIf(ipv6) AS ipv6_n,
        countIf(network_type = 'hosting') AS hosting_n,
        countIf(network_type IN ('hosting', 'other')) AS located_n,
        countIf(custody_group_count IS NOT NULL) AS custody_known_n,
        countIf(custody_group_count <= 4) AS custody_4_n,
        countIf(custody_group_count BETWEEN 5 AND 8) AS custody_5_8_n,
        countIf(custody_group_count BETWEEN 9 AND 127) AS custody_9_127_n,
        countIf(custody_group_count >= 128) AS custody_128_n
    FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
      AND (seen_by_sentry OR seen_by_enr)
    GROUP BY day_start_date

    UNION ALL

    SELECT
        day_start_date,
        0 AS peer_n,
        0 AS sentry_n,
        0 AS enr_n,
        0 AS sentry_and_enr_n,
        0 AS dialled_n,
        0 AS inbound_only_n,
        0 AS quic_n,
        0 AS ipv6_n,
        0 AS hosting_n,
        0 AS located_n,
        0 AS custody_known_n,
        0 AS custody_4_n,
        0 AS custody_5_8_n,
        0 AS custody_9_127_n,
        0 AS custody_128_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
