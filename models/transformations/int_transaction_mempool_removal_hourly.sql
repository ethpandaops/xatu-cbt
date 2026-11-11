---
table: int_transaction_mempool_removal_hourly
type: incremental
interval:
  type: slot
  max: 3600
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 30s"
tags:
  - hourly
  - transaction
  - mempool
  - execution
dependencies:
  - "{{external}}.mempool_transaction_removed"
---
-- One row per transaction hash, removal reason and hour in which Xatu sentries saw it
-- leave their txpool without being included. Sentries detect removals by diffing
-- txpool_content polls, and a queued transaction can leave and re-enter a pool many
-- times (reth does this constantly), so repeat detections by the same sentry collapse
-- into one row here and sentry_count counts distinct sentries. A dropped row means the
-- hash left at least one sentry pool, and it is usually still circulating elsewhere, so
-- join to later sightings or inclusions before calling it gone. The scan expands to
-- complete hour boundaries so partial boundary hours get re-aggregated and replaced.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toStartOfHour(event_date_time) AS hour_start_date_time,
    hash,
    reason,
    min(event_date_time) AS first_detected_date_time,
    max(event_date_time) AS last_detected_date_time,
    min(first_seen_date_time) AS first_seen_date_time,
    max(last_seen_date_time) AS last_seen_date_time,
    toUInt32(count()) AS detection_count,
    toUInt32(uniqExact(meta_client_name)) AS sentry_count,
    toUInt32(uniqExactIf(meta_client_name, pool = 'pending')) AS pending_sentry_count,
    toUInt32(uniqExactIf(meta_client_name, pool = 'queued')) AS queued_sentry_count,
    any(`from`) AS `from`,
    any(`to`) AS `to`,
    any(nonce) AS nonce,
    any(type) AS type,
    any(gas) AS gas,
    any(gas_tip_cap) AS gas_tip_cap,
    any(gas_fee_cap) AS gas_fee_cap,
    any(blob_gas_fee_cap) AS blob_gas_fee_cap,
    argMinIf(r.replacement_hash, event_date_time, r.replacement_hash IS NOT NULL) AS replacement_hash,
    argMinIf(r.replacement_gas_tip_cap, event_date_time, r.replacement_hash IS NOT NULL) AS replacement_gas_tip_cap,
    argMinIf(r.replacement_gas_fee_cap, event_date_time, r.replacement_hash IS NOT NULL) AS replacement_gas_fee_cap,
    max(account_nonce) AS max_account_nonce
FROM {{ index .dep "{{external}}" "mempool_transaction_removed" "helpers" "from" }} r
WHERE
    meta_network_name = '{{ .env.NETWORK }}'
    AND event_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
    AND toStartOfHour(event_date_time) >= toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
    AND toStartOfHour(event_date_time) <= toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
GROUP BY hour_start_date_time, hash, reason
