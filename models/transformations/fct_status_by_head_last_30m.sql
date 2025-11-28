---
table: fct_status_by_head_last_30m
type: scheduled
schedule: "@every 30s"
tags:
  - libp2p
  - peer
  - status
dependencies:
  - "{{external}}.libp2p_handle_status"
  - "{{external}}.libp2p_synthetic_heartbeat"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Get the lookback interval from env, defaulting to 30 minutes
lookback_minutes AS (
    SELECT {{ default "30" .env.STATUS_BY_HEAD_LOOKBACK_MINUTES }} AS minutes
),

-- Get the most recent handle_status per peer (accounting for both directions)
-- For outbound: response_* is the peer's data
-- For inbound: request_* is the peer's data
latest_status_per_peer AS (
    SELECT
        peer_id_unique_key,
        argMax(
            CASE
                WHEN direction = 'outbound' THEN response_head_slot
                ELSE request_head_slot
            END,
            event_date_time
        ) AS peer_head_slot,
        argMax(
            CASE
                WHEN direction = 'outbound' THEN response_head_root
                ELSE request_head_root
            END,
            event_date_time
        ) AS peer_head_root,
        argMax(
            CASE
                WHEN direction = 'outbound' THEN response_fork_digest
                ELSE request_fork_digest
            END,
            event_date_time
        ) AS peer_fork_digest,
        argMax(
            CASE
                WHEN direction = 'outbound' THEN response_finalized_epoch
                ELSE request_finalized_epoch
            END,
            event_date_time
        ) AS peer_finalized_epoch,
        max(event_date_time) AS last_status_time
    FROM {{ index .dep "{{external}}" "libp2p_handle_status" "helpers" "from" }}
    WHERE
        meta_network_name = '{{ .env.NETWORK }}'
        AND event_date_time >= NOW() - INTERVAL (SELECT minutes FROM lookback_minutes) MINUTE
        AND error IS NULL
        AND (
            (direction = 'outbound' AND response_head_slot IS NOT NULL) OR
            (direction != 'outbound' AND request_head_slot IS NOT NULL) OR
            (direction IS NULL AND request_head_slot IS NOT NULL)
        )
    GROUP BY peer_id_unique_key
),

-- Get the most recent peer metadata from synthetic heartbeat
latest_peer_metadata AS (
    SELECT
        remote_peer_id_unique_key,
        argMax(remote_agent_implementation, event_date_time) AS client,
        argMax(remote_agent_version, event_date_time) AS client_version,
        argMax(remote_agent_platform, event_date_time) AS platform,
        argMax(remote_geo_country, event_date_time) AS country,
        argMax(remote_geo_country_code, event_date_time) AS country_code,
        argMax(remote_geo_continent_code, event_date_time) AS continent_code
    FROM {{ index .dep "{{external}}" "libp2p_synthetic_heartbeat" "helpers" "from" }}
    WHERE
        meta_network_name = '{{ .env.NETWORK }}'
        -- Use a longer lookback for metadata since heartbeats may be less frequent
        AND event_date_time >= NOW() - INTERVAL 1 HOUR
    GROUP BY remote_peer_id_unique_key
),

-- Join status with metadata
peer_status_with_metadata AS (
    SELECT
        s.peer_id_unique_key,
        s.peer_head_slot,
        s.peer_head_root,
        s.peer_fork_digest,
        s.peer_finalized_epoch,
        s.last_status_time,
        coalesce(nullIf(m.client, ''), 'unknown') AS client,
        coalesce(nullIf(m.client_version, ''), 'unknown') AS client_version,
        coalesce(nullIf(m.platform, ''), 'unknown') AS platform,
        coalesce(nullIf(m.country, ''), 'unknown') AS country,
        coalesce(nullIf(m.country_code, ''), 'unknown') AS country_code,
        coalesce(nullIf(m.continent_code, ''), 'unknown') AS continent_code
    FROM latest_status_per_peer s
    LEFT JOIN latest_peer_metadata m ON s.peer_id_unique_key = m.remote_peer_id_unique_key
)

-- Aggregate by head_slot to get counts
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    peer_head_slot AS head_slot,
    -- Ensure 0x prefix for head_root (backwards compatible), remove null bytes from FixedString
    if(
        nullIf(replaceAll(peer_head_root, '\0', ''), '') IS NULL,
        'unknown',
        if(startsWith(replaceAll(peer_head_root, '\0', ''), '0x'), replaceAll(peer_head_root, '\0', ''), concat('0x', replaceAll(peer_head_root, '\0', '')))
    ) AS head_root,
    count(*) AS peer_count,
    -- Aggregate by client
    sumMap(map(client, toUInt32(1))) AS count_by_client,
    -- Aggregate by country
    sumMap(map(country, toUInt32(1))) AS count_by_country,
    -- Aggregate by continent
    sumMap(map(continent_code, toUInt32(1))) AS count_by_continent,
    -- Aggregate by fork digest
    sumMap(map(coalesce(nullIf(peer_fork_digest, ''), 'unknown'), toUInt32(1))) AS count_by_fork_digest,
    -- Aggregate by platform
    sumMap(map(platform, toUInt32(1))) AS count_by_platform,
    -- Aggregate by finalized epoch
    sumMap(map(coalesce(peer_finalized_epoch, toUInt32(0)), toUInt32(1))) AS count_by_finalized_epoch
FROM peer_status_with_metadata
WHERE peer_head_slot IS NOT NULL
GROUP BY
    peer_head_slot,
    if(
        nullIf(replaceAll(peer_head_root, '\0', ''), '') IS NULL,
        'unknown',
        if(startsWith(replaceAll(peer_head_root, '\0', ''), '0x'), replaceAll(peer_head_root, '\0', ''), concat('0x', replaceAll(peer_head_root, '\0', '')))
    )
ORDER BY peer_count DESC;

-- Remove previous snapshot to keep only the latest
DELETE FROM `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE updated_date_time != fromUnixTimestamp({{ .task.start }});
