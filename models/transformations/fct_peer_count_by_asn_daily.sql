---
table: fct_peer_count_by_asn_daily
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
  - "{{transformation}}.fct_peer_count_by_asn_country_daily"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Keys stored for those days that
-- the recount no longer produces are rewritten with zero counts.
-- Consensus peers our libp2p sentries or our discovery crawler saw in the day by autonomous system, summed over
-- countries from fct_peer_count_by_asn_country_daily (each peer has one country a day, so the sums are exact).
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
    autonomous_system_number,
    autonomous_system_organization,
    network_type,
    toUInt32(sum(peer_count_n)) AS peer_count,
    toUInt32(sum(sentry_peer_count_n)) AS sentry_peer_count,
    toUInt32(sum(enr_peer_count_n)) AS enr_peer_count,
    toUInt32(sum(dialled_peer_count_n)) AS dialled_peer_count,
    toUInt32(sum(inbound_only_peer_count_n)) AS inbound_only_peer_count,
    toUInt32(sum(quic_peer_count_n)) AS quic_peer_count,
    toUInt32(sum(custody_known_peer_count_n)) AS custody_known_peer_count,
    toUInt32(sum(custody_4_peer_count_n)) AS custody_4_peer_count,
    toUInt32(sum(custody_5_8_peer_count_n)) AS custody_5_8_peer_count,
    toUInt32(sum(custody_9_127_peer_count_n)) AS custody_9_127_peer_count,
    toUInt32(sum(custody_128_peer_count_n)) AS custody_128_peer_count
FROM (
    SELECT
        day_start_date,
        autonomous_system_number, autonomous_system_organization, network_type,
        peer_count AS peer_count_n,
        sentry_peer_count AS sentry_peer_count_n,
        enr_peer_count AS enr_peer_count_n,
        dialled_peer_count AS dialled_peer_count_n,
        inbound_only_peer_count AS inbound_only_peer_count_n,
        quic_peer_count AS quic_peer_count_n,
        custody_known_peer_count AS custody_known_peer_count_n,
        custody_4_peer_count AS custody_4_peer_count_n,
        custody_5_8_peer_count AS custody_5_8_peer_count_n,
        custody_9_127_peer_count AS custody_9_127_peer_count_n,
        custody_128_peer_count AS custody_128_peer_count_n
    FROM {{ index .dep "{{transformation}}" "fct_peer_count_by_asn_country_daily" "helpers" "from" }} FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)

    UNION ALL

    SELECT
        day_start_date,
        autonomous_system_number, autonomous_system_organization, network_type,
        0 AS peer_count_n,
        0 AS sentry_peer_count_n,
        0 AS enr_peer_count_n,
        0 AS dialled_peer_count_n,
        0 AS inbound_only_peer_count_n,
        0 AS quic_peer_count_n,
        0 AS custody_known_peer_count_n,
        0 AS custody_4_peer_count_n,
        0 AS custody_5_8_peer_count_n,
        0 AS custody_9_127_peer_count_n,
        0 AS custody_128_peer_count_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, autonomous_system_number, autonomous_system_organization, network_type
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
