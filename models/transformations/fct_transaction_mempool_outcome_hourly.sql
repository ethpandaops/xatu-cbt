---
table: fct_transaction_mempool_outcome_hourly
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
  - "{{transformation}}.fct_transaction_mempool_outcome_7d"
  - "{{transformation}}.fct_transaction_replacement"
---
-- Hourly cohort outcomes for transactions first sighted in the public mempool,
-- bucketed by first sighting time. Buckets are complete once the upstream 7 day
-- horizon has been processed and never change afterwards. The scan expands to
-- complete hour boundaries so partial boundary hours get re-aggregated and replaced.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH outcomes AS (
    SELECT
        toStartOfHour(first_seen_date_time) AS hour_start_date_time,
        count() AS observed_count,
        countIf(outcome = 'included') AS included_count,
        countIf(outcome = 'nonce_consumed') AS nonce_consumed_count,
        countIf(outcome = 'unincluded') AS unincluded_count,
        countIf(outcome = 'included' AND ifNull(included_via_known_relay, 0)) AS included_relay_delivered_count,
        countIf(outcome = 'included' AND NOT ifNull(included_via_known_relay, 1)) AS included_unknown_build_count,
        countIf(in_mempool_at_deadline) AS in_mempool_at_deadline_count,
        countIf(is_cancel_shape) AS cancel_shape_count,
        countIf(observed_after_nonce_consumed) AS observed_after_nonce_consumed_count,
        countIf(outcome = 'included' AND wait_ms >= 0) AS wait_sample_count,
        if(wait_sample_count > 0, quantileIf(0.50)(wait_ms, outcome = 'included' AND wait_ms >= 0), NULL) AS wait_ms_p50,
        if(wait_sample_count > 0, quantileIf(0.90)(wait_ms, outcome = 'included' AND wait_ms >= 0), NULL) AS wait_ms_p90,
        if(wait_sample_count > 0, quantileIf(0.99)(wait_ms, outcome = 'included' AND wait_ms >= 0), NULL) AS wait_ms_p99,
        countIf(outcome = 'included' AND wait_ms < 0) AS negative_wait_count,
        countIf(type = 3) AS blob_observed_count,
        countIf(type = 3 AND outcome = 'included' AND wait_ms >= 0) AS blob_wait_sample_count,
        if(blob_wait_sample_count > 0, quantileIf(0.50)(wait_ms, type = 3 AND outcome = 'included' AND wait_ms >= 0), NULL) AS blob_wait_ms_p50,
        if(blob_wait_sample_count > 0, quantileIf(0.90)(wait_ms, type = 3 AND outcome = 'included' AND wait_ms >= 0), NULL) AS blob_wait_ms_p90,
        if(blob_wait_sample_count > 0, quantileIf(0.99)(wait_ms, type = 3 AND outcome = 'included' AND wait_ms >= 0), NULL) AS blob_wait_ms_p99,
        uniqExact(`from`, nonce) AS nonce_group_count
    FROM {{ index .dep "{{transformation}}" "fct_transaction_mempool_outcome_7d" "helpers" "from" }} FINAL
    WHERE first_seen_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
        AND toStartOfHour(first_seen_date_time) >= toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
        AND toStartOfHour(first_seen_date_time) <= toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
    GROUP BY hour_start_date_time
),
replacements AS (
    SELECT
        toStartOfHour(group_first_seen_date_time) AS hour_start_date_time,
        uniqExact(`from`, nonce) AS multi_attempt_nonce_group_count,
        countIf(NOT is_winner) AS replaced_attempt_count
    FROM {{ index .dep "{{transformation}}" "fct_transaction_replacement" "helpers" "from" }} FINAL
    WHERE group_first_seen_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 65 MINUTE
        AND fromUnixTimestamp({{ .bounds.end }}) + INTERVAL 65 MINUTE
        AND toStartOfHour(group_first_seen_date_time) >= toStartOfHour(fromUnixTimestamp({{ .bounds.start }}))
        AND toStartOfHour(group_first_seen_date_time) <= toStartOfHour(fromUnixTimestamp({{ .bounds.end }}))
    GROUP BY hour_start_date_time
)
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    o.hour_start_date_time AS hour_start_date_time,
    o.observed_count AS observed_count,
    o.included_count AS included_count,
    o.nonce_consumed_count AS nonce_consumed_count,
    o.unincluded_count AS unincluded_count,
    o.included_relay_delivered_count AS included_relay_delivered_count,
    o.included_unknown_build_count AS included_unknown_build_count,
    o.in_mempool_at_deadline_count AS in_mempool_at_deadline_count,
    o.cancel_shape_count AS cancel_shape_count,
    o.observed_after_nonce_consumed_count AS observed_after_nonce_consumed_count,
    o.wait_ms_p50 AS wait_ms_p50,
    o.wait_ms_p90 AS wait_ms_p90,
    o.wait_ms_p99 AS wait_ms_p99,
    o.wait_sample_count AS wait_sample_count,
    o.negative_wait_count AS negative_wait_count,
    o.blob_observed_count AS blob_observed_count,
    o.blob_wait_ms_p50 AS blob_wait_ms_p50,
    o.blob_wait_ms_p90 AS blob_wait_ms_p90,
    o.blob_wait_ms_p99 AS blob_wait_ms_p99,
    o.blob_wait_sample_count AS blob_wait_sample_count,
    o.nonce_group_count AS nonce_group_count,
    ifNull(r.multi_attempt_nonce_group_count, 0) AS multi_attempt_nonce_group_count,
    ifNull(r.replaced_attempt_count, 0) AS replaced_attempt_count
FROM outcomes o
GLOBAL LEFT JOIN replacements r ON r.hour_start_date_time = o.hour_start_date_time
SETTINGS join_use_nulls = 1
