---
table: fct_transaction_mempool_removal_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - transaction
  - mempool
  - execution
dependencies:
  - "{{transformation}}.int_transaction_mempool_removal_hourly"
---
-- Hourly counts of transactions that left Xatu sentry txpools without being included,
-- by removal reason. replaced and nonce_consumed are reliable. dropped counts hashes that
-- left at least one sentry's pool, most of which are seen again later, so it is churn
-- rather than eviction. The scan expands to complete hour boundaries so partial boundary
-- hours get re-aggregated and replaced.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    reason,
    count() AS hash_count,
    countIf(pending_sentry_count > 0) AS pending_hash_count,
    countIf(queued_sentry_count > 0) AS queued_hash_count,
    sum(sentry_count) AS hash_sentry_count,
    sum(detection_count) AS detection_count,
    countIf(type = 3) AS blob_hash_count,
    countIf(replacement_hash IS NOT NULL) AS with_replacement_count,
    if(
        countIf(replacement_hash IS NOT NULL AND gas_tip_cap > 0) > 0,
        quantileIf(0.50)(toFloat64(replacement_gas_tip_cap) / toFloat64(gas_tip_cap), replacement_hash IS NOT NULL AND gas_tip_cap > 0),
        NULL
    ) AS replacement_tip_cap_ratio_p50,
    if(
        countIf(replacement_hash IS NOT NULL AND gas_fee_cap > 0) > 0,
        quantileIf(0.50)(toFloat64(replacement_gas_fee_cap) / toFloat64(gas_fee_cap), replacement_hash IS NOT NULL AND gas_fee_cap > 0),
        NULL
    ) AS replacement_fee_cap_ratio_p50
FROM {{ index .dep "{{transformation}}" "int_transaction_mempool_removal_hourly" "helpers" "from" }} FINAL
WHERE hour_start_date_time BETWEEN toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
    AND toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
GROUP BY hour_start_date_time, reason
