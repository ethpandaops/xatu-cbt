---
table: fct_engine_get_blobs_by_el_client
type: incremental
interval:
  type: slot
  max: 50000
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 30s"
tags:
  - slot
  - engine_api
  - get_blobs
  - el_client
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
    argMin(slot, duration_ms) AS slot,
    slot_start_date_time,
    argMin(epoch, duration_ms) AS epoch,
    argMin(epoch_start_date_time, duration_ms) AS epoch_start_date_time,
    block_root,
    meta_execution_implementation,
    meta_execution_version,
    status,
    node_class,
    -- Observation counts
    COUNT(*) AS observation_count,
    COUNT(DISTINCT meta_client_name) AS unique_node_count,
    -- Request/Response
    MAX(requested_count) AS max_requested_count,
    round(AVG(returned_count), 2) AS avg_returned_count,
    -- Duration statistics
    round(AVG(duration_ms)) AS avg_duration_ms,
    round(quantile(0.5)(duration_ms)) AS median_duration_ms,
    MIN(duration_ms) AS min_duration_ms,
    MAX(duration_ms) AS max_duration_ms,
    round(quantile(0.95)(duration_ms)) AS p95_duration_ms
FROM {{ index .dep "{{transformation}}" "int_engine_get_blobs" "helpers" "from" }} FINAL
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 6 MINUTE
    AND fromUnixTimestamp({{ .bounds.end }})
    AND meta_execution_implementation != ''
GROUP BY slot_start_date_time, block_root, meta_execution_implementation, meta_execution_version, status, node_class
-- Merge each key across shards before inserting instead of each shard inserting its partial aggregate.
SETTINGS parallel_distributed_insert_select = 0
