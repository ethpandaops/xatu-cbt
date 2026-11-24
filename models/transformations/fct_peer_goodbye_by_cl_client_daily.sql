---
table: fct_peer_goodbye_by_cl_client_daily
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
  - goodbye
dependencies:
  - "{{external}}.libp2p_goodbye"
---
-- Writes the days the bounds complete, from the first day the bounds touch to the day before the one the bounds end
-- in, so no row holds part of a day. A day is only written when the day before it holds goodbyes, which leaves out
-- the partial first day of data and a day goodbyes resume after a whole day without them. Keys stored for those days
-- that the recount no longer produces are rewritten with zero counts.
-- Goodbye messages our sentries received in the day by the consensus client of the remote peer and the reason code
-- it gave, with the unique peers that sent each code.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) - 1 AS max_day
    ),
    complete_days AS (
        SELECT DISTINCT toDate(event_date_time) + 1 AS day_start_date
        FROM {{ index .dep "{{external}}" "libp2p_goodbye" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds) - 1
          AND event_date_time < (SELECT max_day FROM day_bounds)
          AND meta_network_name = '{{ .env.NETWORK }}'
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    client,
    code,
    -- A key the recount no longer produces keeps its stored reason.
    argMax(reason, goodbye_n) AS reason,
    toUInt32(sum(goodbye_n)) AS goodbye_count,
    toUInt32(sum(peer_n)) AS peer_count
FROM (
    SELECT
        toDate(event_date_time) AS day_start_date,
        -- The client names of fct_peer_churn_by_cl_client_daily: named consensus clients keep their name, erigon is
        -- caplin, a missing agent is unknown, and other agents (libp2p tools, crawlers) are other.
        multiIf(
            lower(remote_agent_implementation) IN ('', 'unknown'), 'unknown',
            lower(remote_agent_implementation) = 'erigon', 'caplin',
            lower(remote_agent_implementation) IN ('lighthouse', 'lodestar', 'nimbus', 'prysm', 'teku', 'grandine', 'caplin', 'silver', 'ethlambda'), lower(remote_agent_implementation),
            'other'
        ) AS client,
        code,
        topK(1)(reason)[1] AS reason,
        count() AS goodbye_n,
        uniqExact(remote_peer_id_unique_key) AS peer_n
    FROM {{ index .dep "{{external}}" "libp2p_goodbye" "helpers" "from" }} FINAL
    WHERE event_date_time >= (SELECT min_day FROM day_bounds)
      AND event_date_time < (SELECT max_day FROM day_bounds) + 1
      AND meta_network_name = '{{ .env.NETWORK }}'
      -- GLOBAL: the raw shards run this subquery otherwise, and they cannot expand the {raw} cluster macro.
      AND toDate(event_date_time) GLOBAL IN (SELECT day_start_date FROM complete_days)
    GROUP BY day_start_date, client, code

    UNION ALL

    SELECT
        day_start_date,
        client,
        code,
        reason,
        toUInt64(0) AS goodbye_n,
        toUInt64(0) AS peer_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, client, code
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
