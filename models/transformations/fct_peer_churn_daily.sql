---
table: fct_peer_churn_daily
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
  - churn
dependencies:
  - "{{transformation}}.int_peer_daily"
---
-- Writes the days the bounds complete, from the first day the bounds touch to the day before the one the bounds end
-- in, so no row holds part of a day. Each day reads the 29 days before it. Keys stored for those days that the
-- recount no longer produces are rewritten with zero counts.
-- Consensus peers our libp2p sentries saw in the day by network type: kept from the previous day, new in the
-- 30 days ending on the day, or back after time away, with the peers of the previous day that did not return.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) - 1 AS max_day,
            arrayMap(i -> min_day + i, range(toUInt64(greatest(toInt64(max_day) - toInt64(min_day) + 1, 0)))) AS days
    ),
    -- Each sentry peer day, once for every target day whose 30-day window holds it.
    peer_days AS (
        SELECT
            day_start_date AS day,
            peer_id_unique_key,
            network_type,
            arrayJoin(arrayFilter(t -> t >= day_start_date AND t <= day_start_date + 29, (SELECT days FROM day_bounds))) AS target_day
        FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds) - 29
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND seen_by_sentry
    ),
    history AS (
        SELECT
            target_day AS day_start_date,
            toUInt8(uniqExactIf(day, day < target_day)) AS history_days
        FROM peer_days
        GROUP BY target_day
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    c.day_start_date AS day_start_date,
    c.network_type AS network_type,
    toUInt32(sum(c.peer_n)) AS peer_count,
    toUInt32(sum(c.continuing_n)) AS continuing_peer_count,
    toUInt32(sum(c.new_n)) AS new_peer_count,
    toUInt32(sum(c.returning_n)) AS returning_peer_count,
    toUInt32(sum(c.departed_n)) AS departed_peer_count,
    toUInt32(sum(c.seen_2_7_n)) AS seen_2_7_days_peer_count,
    toUInt32(sum(c.seen_8_29_n)) AS seen_8_29_days_peer_count,
    toUInt32(sum(c.seen_30_n)) AS seen_30_days_peer_count,
    any(coalesce(h.history_days, 0)) AS history_days
FROM (
    SELECT
        day_start_date,
        network_type,
        countIf(present) AS peer_n,
        countIf(present AND previous) AS continuing_n,
        countIf(present AND days_seen = 1) AS new_n,
        countIf(present AND NOT previous AND days_seen > 1) AS returning_n,
        countIf(previous AND NOT present) AS departed_n,
        countIf(present AND days_seen BETWEEN 2 AND 7) AS seen_2_7_n,
        countIf(present AND days_seen BETWEEN 8 AND 29) AS seen_8_29_n,
        countIf(present AND days_seen = 30) AS seen_30_n
    FROM (
        SELECT
            target_day AS day_start_date,
            peer_id_unique_key,
            countIf(day = target_day) > 0 AS present,
            countIf(day = target_day - 1) > 0 AS previous,
            count() AS days_seen,
            -- A departed peer counts under its network type on the previous day.
            if(present, anyIf(network_type, day = target_day), anyIf(network_type, day = target_day - 1)) AS network_type
        FROM peer_days
        GROUP BY target_day, peer_id_unique_key
        HAVING present OR previous
    )
    GROUP BY day_start_date, network_type

    UNION ALL

    SELECT
        day_start_date,
        network_type,
        0 AS peer_n,
        0 AS continuing_n,
        0 AS new_n,
        0 AS returning_n,
        0 AS departed_n,
        0 AS seen_2_7_n,
        0 AS seen_8_29_n,
        0 AS seen_30_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
) AS c
LEFT JOIN history AS h ON c.day_start_date = h.day_start_date
GROUP BY c.day_start_date, c.network_type
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
