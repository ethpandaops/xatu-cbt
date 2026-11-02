---
table: fct_engine_get_blobs_duration_chunked_50ms
type: incremental
interval:
  type: slot
  max: 384
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - engine_api
  - get_blobs
  - chunked
dependencies:
  - "{{transformation}}.int_engine_get_blobs"
---
-- Built from int_engine_get_blobs, so it shares its 300s lateness cutoff,
-- and its slot attribution (each call goes to the block that held its blobs).
-- Each run re-aggregates slots from 6 minutes before its bounds. CBT runs these bounds only
-- once int_engine_get_blobs covers them, so the last run touching a slot sees every observation of
-- it. Live runs, re-runs and late re-runs therefore write the same rows.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_root,
    node_class,
    toInt64(floor(duration_ms / 50) * 50) AS chunk_duration_ms,
    COUNT(*) AS observation_count,
    countIf(status = 'SUCCESS') AS success_count,
    countIf(status = 'PARTIAL') AS partial_count,
    countIf(status = 'EMPTY') AS empty_count,
    countIf(status = 'ERROR' OR status = 'UNSUPPORTED') AS error_count
FROM {{ index .dep "{{transformation}}" "int_engine_get_blobs" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 6 MINUTE
    AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_root, node_class, chunk_duration_ms
