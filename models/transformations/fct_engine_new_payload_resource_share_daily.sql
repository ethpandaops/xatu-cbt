---
table: fct_engine_new_payload_resource_share_daily
type: incremental
interval:
  type: slot
  max: 86400
fill:
  # int_block_resource_gas trails the chain head (about 95 minutes on mainnet, 2026-11-06). Without a buffer the range
  # at the head is always newer than it, the guard below fails every run, and the model never starts. If the lag
  # grows past the buffer the guard holds the range until it catches up.
  buffer: 14400
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
  - "{{transformation}}.fct_engine_new_payload_by_slot"
---
-- engine_newPayload throughput of VALID payloads per UTC day, node scope, execution client, gas resource and the
-- share of the block's resource gas that resource took, in fixed 10% buckets. Shows how a client's throughput moves as
-- blocks lean on one resource. Only blocks of at least 10 Mgas count, and only blocks int_block_resource_gas covers.
--
-- int_block_resource_gas has block intervals, so it can't be a declared dependency of this slot-interval model. It is
-- read directly, and this first statement fails the task (CBT retries it) until that table has reached the last
-- block in the range.
SELECT throwIf(
    (SELECT max(block_number) FROM `{{ .self.database }}`.`int_block_resource_gas`) < (
        SELECT max(block_number)
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_by_slot" "helpers" "from" }} FINAL
        WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
    ),
    'int_block_resource_gas has not reached the end of this slot range yet'
);

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
    ),
    -- The block number of each block, for the resource gas lookup
    block_numbers AS (
        SELECT slot_start_date_time, block_hash, any(block_number) AS block_number
        FROM {{ index .dep "{{transformation}}" "fct_engine_new_payload_by_slot" "helpers" "from" }} FINAL
        WHERE slot_start_date_time >= (SELECT min_period FROM period_bounds)
          AND slot_start_date_time < (SELECT max_period FROM period_bounds) + INTERVAL 1 DAY
        GROUP BY slot_start_date_time, block_hash
    ),
    shares AS (
        SELECT
            block_number,
            gas_compute + gas_memory + gas_address_access + gas_state_growth + gas_history + gas_bloom_topics + gas_block_size AS total,
            [gas_compute, gas_memory, gas_address_access, gas_state_growth, gas_history, gas_bloom_topics, gas_block_size] AS parts
        FROM `{{ .self.database }}`.`int_block_resource_gas` FINAL
        WHERE block_number GLOBAL IN (SELECT block_number FROM block_numbers)
    )
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    day_start_date,
    node_scope,
    client,
    resource,
    toUInt8(least(floor(share * 10), 9) * 10) AS share_bucket_pct,
    toUInt32(count()) AS block_count,
    sum(gas_mgas) AS gas_mgas_sum,
    sum(p50_ms) AS p50_ms_sum
FROM (
    SELECT
        b.day_start_date AS day_start_date,
        b.node_scope AS node_scope,
        b.client AS client,
        b.gas_mgas AS gas_mgas,
        b.p50_ms AS p50_ms,
        s.total AS total,
        s.parts AS parts
    FROM per_block AS b
    GLOBAL INNER JOIN block_numbers AS n ON b.slot_time = n.slot_start_date_time AND b.block_hash = n.block_hash
    GLOBAL INNER JOIN shares AS s ON n.block_number = s.block_number
    WHERE b.gas_mgas >= 10 AND s.total > 0
)
ARRAY JOIN
    ['compute', 'memory', 'address_access', 'state_growth', 'history', 'bloom_topics', 'block_size'] AS resource,
    arrayMap(p -> p / total, parts) AS share
GROUP BY day_start_date, node_scope, client, resource, share_bucket_pct
-- Merge across shards before inserting instead of each shard inserting its partial sums.
SETTINGS parallel_distributed_insert_select = 0
