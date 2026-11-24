---
table: fct_peer_sync_status_by_cl_client_daily
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
-- Consensus peers our libp2p sentries saw in the day by client, bucketed by how far their head and finalized
-- checkpoint trailed our sentries' in status exchanges, their fork digest and how much block history they serve.
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
            -- Named consensus clients keep their name. Other agents (libp2p tools, crawlers we do not know) are other.
            if(client IN ('lighthouse', 'lodestar', 'nimbus', 'prysm', 'teku', 'grandine', 'caplin', 'silver', 'ethlambda', 'unknown'), client, 'other') AS client,
            status_count,
            fork_digest,
            head_lag_slots,
            finalized_lag_epochs,
            earliest_available_slot,
            served_history_slots
        FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND seen_by_sentry
    ),
    network_fork_digests AS (
        SELECT day_start_date, topK(1)(fork_digest)[1] AS network_fork_digest
        FROM peers
        WHERE status_count > 0
        GROUP BY day_start_date
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    client,
    toUInt32(sum(peer_n)) AS peer_count,
    toUInt32(sum(status_n)) AS status_peer_count,
    toUInt32(sum(head_lag_0_1_n)) AS head_lag_0_1_peer_count,
    toUInt32(sum(head_lag_2_32_n)) AS head_lag_2_32_peer_count,
    toUInt32(sum(head_lag_over_32_n)) AS head_lag_over_32_peer_count,
    toUInt32(sum(finalized_lag_over_1_n)) AS finalized_lag_over_1_peer_count,
    toUInt32(sum(other_fork_digest_n)) AS other_fork_digest_peer_count,
    toUInt32(sum(history_known_n)) AS history_known_peer_count,
    toUInt32(sum(history_full_n)) AS history_full_peer_count,
    toUInt32(sum(history_over_min_n)) AS history_over_min_peer_count,
    toUInt32(sum(history_min_n)) AS history_min_peer_count,
    toUInt32(sum(history_under_min_n)) AS history_under_min_peer_count
FROM (
    SELECT
        p.day_start_date AS day_start_date,
        p.client AS client,
        count() AS peer_n,
        countIf(p.status_count > 0) AS status_n,
        countIf(p.head_lag_slots <= 1) AS head_lag_0_1_n,
        countIf(p.head_lag_slots BETWEEN 2 AND 32) AS head_lag_2_32_n,
        countIf(p.head_lag_slots > 32) AS head_lag_over_32_n,
        countIf(p.finalized_lag_epochs > 1) AS finalized_lag_over_1_n,
        countIf(p.status_count > 0 AND p.fork_digest != d.network_fork_digest) AS other_fork_digest_n,
        countIf(p.earliest_available_slot IS NOT NULL) AS history_known_n,
        countIf(p.earliest_available_slot = 0) AS history_full_n,
        -- The spec minimum is MIN_EPOCHS_FOR_BLOCK_REQUESTS (33024 epochs, 1056768 slots). Within a day (7200 slots)
        -- of it counts as the minimum.
        countIf(p.earliest_available_slot > 0 AND p.served_history_slots > 1056768 + 7200) AS history_over_min_n,
        countIf(p.earliest_available_slot > 0 AND p.served_history_slots BETWEEN 1056768 - 7200 AND 1056768 + 7200) AS history_min_n,
        countIf(p.earliest_available_slot > 0 AND p.served_history_slots < 1056768 - 7200) AS history_under_min_n
    FROM peers AS p
    GLOBAL LEFT JOIN network_fork_digests AS d ON p.day_start_date = d.day_start_date
    GROUP BY day_start_date, client

    UNION ALL

    SELECT
        day_start_date,
        client,
        0 AS peer_n,
        0 AS status_n,
        0 AS head_lag_0_1_n,
        0 AS head_lag_2_32_n,
        0 AS head_lag_over_32_n,
        0 AS finalized_lag_over_1_n,
        0 AS other_fork_digest_n,
        0 AS history_known_n,
        0 AS history_full_n,
        0 AS history_over_min_n,
        0 AS history_min_n,
        0 AS history_under_min_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, client
-- Merge counts across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0, join_use_nulls = 1
