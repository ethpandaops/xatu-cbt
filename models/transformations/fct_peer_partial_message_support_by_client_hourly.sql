---
table: fct_peer_partial_message_support_by_client_hourly
type: incremental
interval:
  type: slot
  max: 10000
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - peer
  - client
  - data_column
  - partial_messages
dependencies:
  - "{{external}}.libp2p_synthetic_heartbeat"
  - "{{external}}.libp2p_rpc_meta_subscription"
---
-- Rewrites every hour the bounds touch so partial hours are recounted in full.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
WITH
hour_bounds AS (
    SELECT
        toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_hour,
        toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 HOUR AS max_hour
),
heartbeat_peers AS (
    SELECT
        toStartOfHour(event_date_time) AS hour_start_date_time,
        remote_peer_id_unique_key AS peer_id_unique_key,
        argMax(remote_agent_implementation, event_date_time) AS peer_implementation,
        argMax(
            if(
                remote_agent_version_minor != '',
                concat(remote_agent_version_major, '.', remote_agent_version_minor, '.', remote_agent_version_patch),
                remote_agent_version
            ),
            event_date_time
        ) AS peer_version,
        max(has(protocols, '/meshsub/1.3.0')) AS gossipsub_v13
    FROM {{ index .dep "{{external}}" "libp2p_synthetic_heartbeat" "helpers" "from" }} FINAL
    WHERE event_date_time >= (SELECT min_hour FROM hour_bounds)
        AND event_date_time < (SELECT max_hour FROM hour_bounds)
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY hour_start_date_time, peer_id_unique_key
),
-- Only vantages that report the partial flags count, a NULL flag means the vantage could not see them.
subscription_peers AS (
    SELECT
        toStartOfHour(event_date_time) AS hour_start_date_time,
        peer_id_unique_key,
        max(assumeNotNull(requests_partial)) AS peer_requests_partial,
        max(assumeNotNull(supports_sending_partial)) AS peer_supports_sending_partial
    FROM {{ index .dep "{{external}}" "libp2p_rpc_meta_subscription" "helpers" "from" }} FINAL
    WHERE event_date_time >= (SELECT min_hour FROM hour_bounds)
        AND event_date_time < (SELECT max_hour FROM hour_bounds)
        AND startsWith(topic_name, 'data_column_sidecar_')
        AND subscribe
        AND requests_partial IS NOT NULL
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY hour_start_date_time, peer_id_unique_key
),
peer_hours AS (
    SELECT
        hour_start_date_time,
        peer_id_unique_key,
        max(peer_implementation) AS peer_implementation,
        max(peer_version) AS peer_version,
        max(connected) AS connected,
        max(gossipsub_v13) AS gossipsub_v13,
        max(subscribed) AS subscribed,
        max(requests_partial) AS requests_partial,
        max(supports_sending_partial) AS supports_sending_partial
    FROM (
        SELECT
            hour_start_date_time,
            peer_id_unique_key,
            peer_implementation,
            peer_version,
            true AS connected,
            gossipsub_v13,
            false AS subscribed,
            false AS requests_partial,
            false AS supports_sending_partial
        FROM heartbeat_peers

        UNION ALL

        SELECT
            hour_start_date_time,
            peer_id_unique_key,
            '' AS peer_implementation,
            '' AS peer_version,
            false AS connected,
            false AS gossipsub_v13,
            true AS subscribed,
            peer_requests_partial AS requests_partial,
            peer_supports_sending_partial AS supports_sending_partial
        FROM subscription_peers
    )
    GROUP BY hour_start_date_time, peer_id_unique_key
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    peer_implementation,
    peer_version,
    toUInt32(countIf(connected)) AS connected_peer_count,
    toUInt32(countIf(connected AND gossipsub_v13)) AS gossipsub_v13_peer_count,
    toUInt32(countIf(subscribed)) AS subscribed_peer_count,
    toUInt32(countIf(requests_partial)) AS requests_partial_peer_count,
    toUInt32(countIf(supports_sending_partial)) AS supports_sending_partial_peer_count
FROM peer_hours
GROUP BY hour_start_date_time, peer_implementation, peer_version
