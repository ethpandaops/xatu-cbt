---
table: fct_block_fast_confirmation_by_client_daily
type: incremental
interval:
  type: slot
  max: 604800
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - consensus
  - block
  - fast_confirmation
dependencies:
  - "{{transformation}}.fct_block_fast_confirmation_by_node"
---
-- Every day touched by the bounds is re-aggregated in full, so a partial day from
-- an earlier run is replaced by the complete one.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    toStartOfDay(fromUnixTimestamp({{ .bounds.start }})) AS window_start,
    toStartOfDay(fromUnixTimestamp({{ .bounds.end }})) + INTERVAL 1 DAY AS window_end,
    observations AS (
        SELECT
            slot,
            slot_start_date_time,
            status,
            meta_client_name,
            meta_consensus_implementation,
            confirmation_type,
            fast_confirmed_slot_start_diff,
            finalized_slot_start_diff,
            fast_confirmed_slot_start_diff IS NOT NULL AND status = 'canonical' AS confirmed,
            finalized_slot_start_diff IS NOT NULL AS finalized
        FROM {{ index .dep "{{transformation}}" "fct_block_fast_confirmation_by_node" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= window_start
            AND slot_start_date_time < window_end
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    toDate(slot_start_date_time) AS day_start_date,
    meta_consensus_implementation,
    uniqExactIf(meta_client_name, status = 'canonical') AS node_count,
    uniqExactIf(slot, status = 'canonical') AS slot_count,
    countIf(status = 'canonical') AS observation_count,
    countIf(status = 'canonical' AND confirmation_type = 'direct') AS direct_count,
    countIf(status = 'canonical' AND confirmation_type = 'descendant') AS descendant_count,
    countIf(status = 'canonical' AND confirmation_type = 'unconfirmed') AS unconfirmed_count,
    countIf(status = 'orphaned') AS orphaned_count,
    toUInt32(ifNotFinite(round(avgIf(fast_confirmed_slot_start_diff, confirmed)), 0)) AS avg_fast_confirmation_ms,
    toUInt32(ifNull(minIf(fast_confirmed_slot_start_diff, confirmed), 0)) AS min_fast_confirmation_ms,
    toUInt32(ifNotFinite(round(quantileExactIf(0.50)(fast_confirmed_slot_start_diff, confirmed)), 0)) AS p50_fast_confirmation_ms,
    toUInt32(ifNotFinite(round(quantileExactIf(0.90)(fast_confirmed_slot_start_diff, confirmed)), 0)) AS p90_fast_confirmation_ms,
    toUInt32(ifNotFinite(round(quantileExactIf(0.95)(fast_confirmed_slot_start_diff, confirmed)), 0)) AS p95_fast_confirmation_ms,
    toUInt32(ifNotFinite(round(quantileExactIf(0.99)(fast_confirmed_slot_start_diff, confirmed)), 0)) AS p99_fast_confirmation_ms,
    toUInt32(ifNull(maxIf(fast_confirmed_slot_start_diff, confirmed), 0)) AS max_fast_confirmation_ms,
    toUInt32(ifNotFinite(round(avgIf(finalized_slot_start_diff, finalized)), 0)) AS avg_finality_ms,
    toUInt32(ifNull(minIf(finalized_slot_start_diff, finalized), 0)) AS min_finality_ms,
    toUInt32(ifNotFinite(round(quantileExactIf(0.50)(finalized_slot_start_diff, finalized)), 0)) AS p50_finality_ms,
    toUInt32(ifNotFinite(round(quantileExactIf(0.95)(finalized_slot_start_diff, finalized)), 0)) AS p95_finality_ms,
    toUInt32(ifNull(maxIf(finalized_slot_start_diff, finalized), 0)) AS max_finality_ms,
    toFloat32(ifNotFinite(round(quantileExactIf(0.50)(finalized_slot_start_diff / fast_confirmed_slot_start_diff, confirmed AND finalized AND fast_confirmed_slot_start_diff > 0), 2), 0)) AS p50_speedup
FROM observations
GROUP BY day_start_date, meta_consensus_implementation
