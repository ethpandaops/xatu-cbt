---
table: fct_engine_new_payload_throughput_histogram_daily
type: incremental
interval:
  type: slot
  max: 86400
schedules:
  forwardfill: "@every 1h"
  backfill: "@every 30s"
tags:
  - daily
  - engine_api
  - new_payload
  - performance
dependencies:
  - "{{transformation}}.fct_engine_new_payload_by_el_client"
---
-- Per-block engine_newPayload throughput (block Mgas over the block's median duration across Xatu nodes, in Mgas/s)
-- of VALID payloads per UTC day, node scope and execution client, as a histogram with buckets about 4% wide
-- (bucket k covers 1.04^k to 1.04^(k+1) Mgas/s). Blocks under 5 Mgas are left out because fixed overhead
-- dominates them. Summed over any range it gives the range's throughput percentiles within one bucket.
-- Rewrites every day the bounds touch so partial ones are recounted in full.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
    period_bounds AS (
        SELECT
            toDate(fromUnixTimestamp({{ .bounds.start }})) AS min_period,
            toDate(fromUnixTimestamp({{ .bounds.end }})) AS max_period
    ),
    -- One row per block and client, versions folded together weighted by observations as Lab does. Reference nodes
    -- (node_class eip7870-block-builder) count in both scopes, other Xatu nodes only in all.
    per_block AS (
        SELECT
            toDate(slot_start_date_time) AS day_start_date,
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
              AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 DAY
              AND status = 'VALID'
              AND gas_used > 0
              AND median_duration_ms > 0
        )
        ARRAY JOIN if(node_class = 'eip7870-block-builder', ['reference', 'all'], ['all']) AS node_scope
        GROUP BY day_start_date, node_scope, client, slot
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    node_scope,
    client,
    bucket_index,
    toFloat32(pow(1.04, bucket_index)) AS bucket_lower_mgas_per_second,
    toFloat32(pow(1.04, bucket_index + 1)) AS bucket_upper_mgas_per_second,
    toUInt32(count()) AS block_count
FROM (
    SELECT
        day_start_date,
        node_scope,
        client,
        gas_mgas / (p50_ms / 1000) AS mgas_per_second,
        toInt16(floor(log(mgas_per_second) / log(1.04))) AS k0,
        -- The float log can land one bucket off at a bucket edge, so step to the bucket whose bounds hold the value
        toInt16(multiIf(pow(1.04, k0 + 1) <= mgas_per_second, k0 + 1, pow(1.04, k0) > mgas_per_second, k0 - 1, k0)) AS bucket_index
    FROM per_block
    WHERE gas_mgas >= 5
)
GROUP BY day_start_date, node_scope, client, bucket_index
-- Merge across shards before inserting instead of each shard inserting its partial counts.
SETTINGS parallel_distributed_insert_select = 0
