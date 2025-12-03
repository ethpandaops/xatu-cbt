---
table: int_peer_head
type: incremental
interval:
  type: datetime
  max: 3600
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 60s"
tags:
  - libp2p
  - peer
  - head
dependencies:
  - "{{external}}.libp2p_handle_status"
  - "{{external}}.libp2p_synthetic_heartbeat"
---
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- Extract peer head data from handle_status events
-- For outbound: response_* is the peer's data
-- For inbound: request_* is the peer's data
peer_status AS (
    SELECT
        event_date_time,
        peer_id_unique_key,
        CASE
            WHEN direction = 'outbound' THEN response_head_slot
            ELSE request_head_slot
        END AS head_slot,
        CASE
            WHEN direction = 'outbound' THEN response_head_root
            ELSE request_head_root
        END AS head_root,
        CASE
            WHEN direction = 'outbound' THEN response_fork_digest
            ELSE request_fork_digest
        END AS fork_digest,
        CASE
            WHEN direction = 'outbound' THEN response_finalized_epoch
            ELSE request_finalized_epoch
        END AS finalized_epoch
    FROM {{ index .dep "{{external}}" "libp2p_handle_status" "helpers" "from" }}
    WHERE
        meta_network_name = '{{ .env.NETWORK }}'
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
        AND error IS NULL
        AND (
            (direction = 'outbound' AND response_head_slot IS NOT NULL) OR
            (direction != 'outbound' AND request_head_slot IS NOT NULL) OR
            (direction IS NULL AND request_head_slot IS NOT NULL)
        )
),

-- Get peer metadata from synthetic heartbeat (using a wider window for metadata)
peer_metadata AS (
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
        -- Use a wider window to ensure we have metadata for peers
        AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 1 HOUR AND fromUnixTimestamp({{ .bounds.end }})
    GROUP BY remote_peer_id_unique_key
)

-- Join status with metadata and deduplicate to latest per (head_slot, peer)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    argMax(s.event_date_time, s.event_date_time) AS event_date_time,
    s.peer_id_unique_key,
    s.head_slot,
    -- Normalize head_root: ensure 0x prefix, remove null bytes
    argMax(
        if(
            nullIf(replaceAll(s.head_root, '\0', ''), '') IS NULL,
            'unknown',
            if(
                startsWith(replaceAll(s.head_root, '\0', ''), '0x'),
                replaceAll(s.head_root, '\0', ''),
                concat('0x', replaceAll(s.head_root, '\0', ''))
            )
        ),
        s.event_date_time
    ) AS head_root,
    argMax(coalesce(nullIf(s.fork_digest, ''), 'unknown'), s.event_date_time) AS fork_digest,
    argMax(s.finalized_epoch, s.event_date_time) AS finalized_epoch,
    argMax(coalesce(nullIf(m.client, ''), 'unknown'), s.event_date_time) AS client,
    argMax(coalesce(nullIf(m.client_version, ''), 'unknown'), s.event_date_time) AS client_version,
    argMax(coalesce(nullIf(m.platform, ''), 'unknown'), s.event_date_time) AS platform,
    argMax(coalesce(nullIf(m.country, ''), 'unknown'), s.event_date_time) AS country,
    argMax(coalesce(nullIf(m.country_code, ''), 'unknown'), s.event_date_time) AS country_code,
    argMax(coalesce(nullIf(m.continent_code, ''), 'unknown'), s.event_date_time) AS continent_code
FROM peer_status s
LEFT JOIN peer_metadata m ON s.peer_id_unique_key = m.remote_peer_id_unique_key
WHERE s.head_slot IS NOT NULL
GROUP BY s.head_slot, s.peer_id_unique_key;
