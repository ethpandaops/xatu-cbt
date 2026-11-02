---
table: fct_engine_new_payload_duration_chunked_50ms
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
  - new_payload
  - chunked
dependencies:
  - "{{transformation}}.int_engine_new_payload"
---
-- Built from int_engine_new_payload, so it shares its 300s lateness cutoff.
-- Each run re-aggregates slots from 6 minutes before its bounds. CBT runs these bounds only
-- once int_engine_new_payload covers them, so the last run touching a slot sees every observation of
-- it. Live runs, re-runs and late re-runs therefore write the same rows.
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    block_hash,
    node_class,
    toInt64(floor(duration_ms / 50) * 50) AS chunk_duration_ms,
    COUNT(*) AS observation_count,
    countIf(status = 'VALID') AS valid_count,
    countIf(status = 'INVALID' OR status = 'INVALID_BLOCK_HASH') AS invalid_count
FROM {{ index .dep "{{transformation}}" "int_engine_new_payload" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 6 MINUTE
    AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot, slot_start_date_time, epoch, epoch_start_date_time, block_hash, node_class, chunk_duration_ms
