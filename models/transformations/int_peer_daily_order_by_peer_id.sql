---
table: int_peer_daily_order_by_peer_id
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
-- int_peer_daily ordered by peer ID, so one peer's days are read without a day filter. Every day the bounds touch
-- is copied in full, including peers a recount dropped from the day.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    peer_id_unique_key,
    peer_id,
    seen_by_sentry,
    seen_by_enr,
    client,
    version,
    agent_version,
    continent_code,
    country_code,
    country,
    city,
    autonomous_system_number,
    autonomous_system_organization,
    network_type,
    ipv6,
    dialled,
    inbound,
    quic_connected,
    quic_listen,
    enr_ip_mismatch,
    custody_group_count,
    latency_ms,
    status_count,
    fork_digest,
    head_lag_slots,
    finalized_lag_epochs,
    earliest_available_slot,
    served_history_slots
FROM {{ index .dep "{{transformation}}" "int_peer_daily" "helpers" "from" }} FINAL
WHERE day_start_date >= toDate(fromUnixTimestamp({{ .bounds.start }}))
  AND day_start_date <= toDate(fromUnixTimestamp({{ .bounds.end }}))
