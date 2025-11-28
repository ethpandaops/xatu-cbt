---
table: fct_peer_head_last_30m
type: scheduled
schedule: "@every 30s"
tags:
  - libp2p
  - peer
  - head
dependencies:
  - "{{transformation}}.int_peer_head"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get the lookback interval from env, defaulting to 30 minutes
lookback_minutes AS (
    SELECT {{ default "30" .env.PEER_HEAD_LOOKBACK_MINUTES }} AS minutes
),

-- Get the latest observation per peer from int_peer_head within the lookback window
latest_peer_head AS (
    SELECT
        peer_id_unique_key,
        argMax(head_slot, event_date_time) AS head_slot,
        argMax(head_root, event_date_time) AS head_root,
        argMax(fork_digest, event_date_time) AS fork_digest,
        argMax(finalized_epoch, event_date_time) AS finalized_epoch,
        argMax(client, event_date_time) AS client,
        argMax(platform, event_date_time) AS platform,
        argMax(continent_code, event_date_time) AS continent_code,
        argMax(country, event_date_time) AS country
    FROM `{{ .self.database }}`.int_peer_head FINAL
    WHERE event_date_time >= NOW() - INTERVAL (SELECT minutes FROM lookback_minutes) MINUTE
    GROUP BY peer_id_unique_key
)

-- Aggregate by head_slot/head_root to get counts
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    head_slot,
    head_root,
    count(*) AS peer_count,
    sumMap(map(client, toUInt32(1))) AS count_by_client,
    sumMap(map(country, toUInt32(1))) AS count_by_country,
    sumMap(map(continent_code, toUInt32(1))) AS count_by_continent,
    sumMap(map(fork_digest, toUInt32(1))) AS count_by_fork_digest,
    sumMap(map(platform, toUInt32(1))) AS count_by_platform,
    sumMap(map(toString(coalesce(finalized_epoch, toUInt32(0))), toUInt32(1))) AS count_by_finalized_epoch
FROM latest_peer_head
WHERE head_slot IS NOT NULL
GROUP BY head_slot, head_root
ORDER BY peer_count DESC;

-- Remove previous snapshot to keep only the latest
DELETE FROM `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
