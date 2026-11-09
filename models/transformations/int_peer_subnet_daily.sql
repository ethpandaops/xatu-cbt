---
table: int_peer_subnet_daily
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
  - "{{transformation}}.int_peer_daily"
  - "{{external}}.libp2p_handle_metadata"
---
-- Rewrites every day the bounds touch so partial days are recounted in full. Peers stored for those days that the
-- recount no longer produces are rewritten with no subnets and a zero response count.
-- One row per consensus peer per day with the attestation and sync subnets it last advertised in a metadata response
-- to our sentries. Inbound metadata rows hold our own sentries' subnets, so only outbound responses count. Peers are
-- those int_peer_daily has from our sentries, which leaves out crawlers, probes, light clients and our own sentries.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    day_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_day,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_day
    ),
    peers AS (
        SELECT day_start_date, peer_id_unique_key
        FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
        WHERE day_start_date >= (SELECT min_day FROM day_bounds)
          AND day_start_date <= (SELECT max_day FROM day_bounds)
          AND seen_by_sentry
    ),
    responses AS (
        SELECT
            toDate(event_date_time) AS day_start_date,
            peer_id_unique_key,
            argMax((attnets, syncnets), (event_date_time, attnets, syncnets)) AS latest,
            count() AS response_n
        FROM {{ index .dep "{{external}}" "libp2p_handle_metadata" "helpers" "from" }}
        WHERE event_date_time >= (SELECT min_day FROM day_bounds)
          AND event_date_time < (SELECT max_day FROM day_bounds) + INTERVAL 1 DAY
          AND meta_network_name = '{{ .env.NETWORK }}'
          AND direction = 'outbound'
          AND error IS NULL
          AND length(attnets) = 16
          AND length(syncnets) = 2
        GROUP BY day_start_date, peer_id_unique_key
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    peer_id_unique_key,
    max(attnets) AS attnets,
    max(syncnets) AS syncnets,
    toUInt32(sum(response_n)) AS response_count
FROM (
    SELECT
        r.day_start_date AS day_start_date,
        r.peer_id_unique_key AS peer_id_unique_key,
        -- SSZ bitvectors: subnet i is bit i % 8 of byte i / 8, so read little-endian bit i is subnet i.
        reinterpretAsUInt64(unhex(r.latest.1)) AS attnets,
        reinterpretAsUInt8(unhex(r.latest.2)) AS syncnets,
        r.response_n AS response_n
    FROM responses AS r
    GLOBAL INNER JOIN peers AS p
        ON r.day_start_date = p.day_start_date
        AND r.peer_id_unique_key = p.peer_id_unique_key

    UNION ALL

    SELECT
        day_start_date,
        peer_id_unique_key,
        toUInt64(0) AS attnets,
        toUInt8(0) AS syncnets,
        0 AS response_n
    FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
    WHERE day_start_date >= (SELECT min_day FROM day_bounds)
      AND day_start_date <= (SELECT max_day FROM day_bounds)
)
GROUP BY day_start_date, peer_id_unique_key
-- Merge rows across shards before inserting instead of each shard inserting its partial rows.
SETTINGS parallel_distributed_insert_select = 0
