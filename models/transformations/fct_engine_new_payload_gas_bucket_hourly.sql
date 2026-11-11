---
table: fct_engine_new_payload_gas_bucket_hourly
type: incremental
interval:
  type: slot
  max: 25200
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30s"
tags:
  - hourly
  - engine_api
  - new_payload
  - performance
dependencies:
  - "{{transformation}}.fct_engine_new_payload_by_el_client"
---
-- engine_newPayload timings of VALID payloads per hour, node scope, execution client and 5 Mgas block size bucket,
-- as counts and sums. The sums of x (block Mgas), y (milliseconds), x², y² and x·y let a reader fit the
-- least-squares cost model ms = a + b·Mgas exactly over any range, and Σgas over Σms gives throughput.
-- Rewrites every hour the bounds touch so partial ones are recounted in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    period_bounds AS (
        SELECT
            toStartOfHour(fromUnixTimestamp({{ .bounds.start }})) AS min_period,
            toStartOfHour(fromUnixTimestamp({{ .bounds.end }})) AS max_period
    ),
    -- One row per block and client, versions folded together weighted by observations as Lab does. Reference nodes
    -- (node_class eip7870-block-builder) count in both scopes, other Xatu nodes only in all.
    per_block AS (
        SELECT
            toStartOfHour(slot_start_date_time) AS hour_start_date_time,
            node_scope,
            client,
            slot,
            any(block_hash) AS block_hash,
            any(slot_start_date_time) AS slot_time,
            any(gas_used) / 1e6 AS gas_mgas,
            sum(observation_count * avg_duration_ms) / sum(observation_count) AS avg_ms,
            sum(observation_count * median_duration_ms) / sum(observation_count) AS p50_ms,
            sum(observation_count * p95_duration_ms) / sum(observation_count) AS p95_ms
        FROM (
            SELECT
                slot,
                slot_start_date_time,
                block_hash,
                gas_used,
                observation_count,
                avg_duration_ms,
                median_duration_ms,
                p95_duration_ms,
                if(lower(meta_execution_implementation) = 'go-ethereum', 'geth', lower(meta_execution_implementation)) AS client,
                node_class
            FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_by_el_client" "helpers" "from" }} FINAL
            WHERE slot_start_date_time >= (SELECT min_period FROM period_bounds)
              AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 HOUR
              AND status = 'VALID'
              AND gas_used > 0
              AND median_duration_ms > 0
        )
        ARRAY JOIN if(node_class = 'eip7870-block-builder', ['reference', 'all'], ['all']) AS node_scope
        GROUP BY hour_start_date_time, node_scope, client, slot
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    hour_start_date_time,
    node_scope,
    client,
    toUInt16(intDiv(toUInt32(gas_mgas), 5) * 5) AS gas_bucket_mgas,
    toUInt32(count()) AS block_count,
    sum(gas_mgas) AS gas_mgas_sum,
    sum(gas_mgas * gas_mgas) AS gas_mgas_sq_sum,
    sum(avg_ms) AS avg_ms_sum,
    sum(p50_ms) AS p50_ms_sum,
    sum(p50_ms * p50_ms) AS p50_ms_sq_sum,
    sum(gas_mgas * p50_ms) AS gas_p50_product_sum,
    sum(p95_ms) AS p95_ms_sum,
    sum(p95_ms * p95_ms) AS p95_ms_sq_sum,
    sum(gas_mgas * p95_ms) AS gas_p95_product_sum
FROM per_block
GROUP BY hour_start_date_time, node_scope, client, gas_bucket_mgas
-- Merge across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0
