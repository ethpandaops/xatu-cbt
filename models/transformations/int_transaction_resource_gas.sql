---
table: int_transaction_resource_gas
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 10s"
  backfill: "@every 30s"
tags:
  - execution
  - gas
  - resource
dependencies:
  - "{{transformation}}.int_transaction_call_frame_opcode_resource_gas"
  - "{{transformation}}.int_transaction_call_frame"
  - "{{external}}.canonical_execution_transaction"
---
-- Per-transaction resource gas totals.
--
-- Combines three gas sources:
--   1. Opcode resources  - SUM from per-opcode resource decomposition
--   2. Precompile gas    - Synthetic frames with opcode_count=0 (all Compute)
--   3. Intrinsic gas     - Decomposed into resource categories using fixed constants
--
-- =============================================================================
-- INTRINSIC GAS DECOMPOSITION
-- =============================================================================
--
-- Base cost decomposition (from researcher's model):
--
--   Regular tx (21,000 base):
--     Compute=8500, History=6500, Access=300, Block Size=5700
--
--   Contract creation tx (53,000 base):
--     Compute=9500, History=13200, State Growth=21800, Access=2800, Block Size=5700
--
-- Calldata cost decomposition (per byte):
--   Zero byte (4 gas):     History=0.5, Block Size=3.5
--   Nonzero byte (16 gas): History=5,   Block Size=11
--
-- Since History has fractional per-byte costs (0.5 for zero bytes), we use
-- batch rounding: intDiv(n_zero * 1 + n_nonzero * 10, 2) to get an integer
-- History cost. Block Size absorbs the rounding residual as the remainder.
-- This keeps all columns UInt64 while preserving exact calldata totals.
--
-- Access list cost: all to Address Access.
-- EIP-3860 initcode word cost: all to Compute.
--
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH

-- =============================================================================
-- CTE 1: Opcode-level resource totals per transaction
-- =============================================================================
opcode_resources AS (
    SELECT
        block_number,
        transaction_hash,
        sum(gas) AS total_opcode_gas,
        sum(gas_compute) AS gas_compute,
        sum(gas_memory) AS gas_memory,
        sum(gas_address_access) AS gas_address_access,
        sum(gas_state_growth) AS gas_state_growth,
        sum(gas_history) AS gas_history,
        sum(gas_bloom_topics) AS gas_bloom_topics,
        sum(gas_block_size) AS gas_block_size
    FROM {{ index .dep "{{transformation}}" "int_transaction_call_frame_opcode_resource_gas" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
    GROUP BY block_number, transaction_hash
),

-- =============================================================================
-- CTE 2: Precompile gas per transaction (synthetic frames → all Compute)
-- =============================================================================
precompile_gas AS (
    SELECT
        block_number,
        transaction_hash,
        sum(gas) AS gas
    FROM {{ index .dep "{{transformation}}" "int_transaction_call_frame" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND opcode_count = 0
    GROUP BY block_number, transaction_hash
    HAVING gas > 0
),

-- =============================================================================
-- CTE 3: Intrinsic gas decomposition from root frames + transaction data
-- =============================================================================
intrinsic AS (
    SELECT
        cf.block_number,
        cf.transaction_hash,
        cf.transaction_index,
        cf.intrinsic_gas,
        cf.gas_cumulative,
        cf.gas_refund,
        tx.to_address,
        tx.n_input_zero_bytes,
        tx.n_input_nonzero_bytes,
        -- Is this a contract creation transaction?
        tx.to_address IS NULL AS is_create,
        -- Input data gas cost
        toUInt64(tx.n_input_zero_bytes) * toUInt64(4)
            + toUInt64(tx.n_input_nonzero_bytes) * toUInt64(16) AS input_data_cost,
        -- Base cost (21000 regular, 53000 contract creation)
        if(tx.to_address IS NULL, toUInt64(53000), toUInt64(21000)) AS base_cost,
        -- EIP-3860 initcode word cost for CREATE transactions
        -- 2 gas per 32-byte word of input, ceil(input_length / 32)
        if(tx.to_address IS NULL AND tx.n_input_bytes > 0,
            toUInt64(2) * toUInt64(intDiv(toUInt64(tx.n_input_bytes) + 31, 32)),
            toUInt64(0)) AS initcode_word_cost,
        -- Input length for EIP-3860
        tx.n_input_bytes
    FROM {{ index .dep "{{transformation}}" "int_transaction_call_frame" "helpers" "from" }} AS cf FINAL
    GLOBAL LEFT ANY JOIN (
        SELECT
            block_number,
            transaction_hash,
            to_address,
            n_input_bytes,
            n_input_zero_bytes,
            n_input_nonzero_bytes
        FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }}
        WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
            AND meta_network_name = '{{ .env.NETWORK }}'
    ) tx
        ON cf.block_number = tx.block_number
        AND cf.transaction_hash = tx.transaction_hash
    WHERE cf.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND cf.call_frame_id = 0  -- Root frame only
        AND cf.intrinsic_gas IS NOT NULL  -- Successful transactions only
)

-- =============================================================================
-- FINAL: Combine all three sources
-- =============================================================================
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    i.block_number,
    i.transaction_hash,
    i.transaction_index,

    -- gas_compute: opcode compute + precompile gas + intrinsic compute + initcode word cost
    coalesce(o.gas_compute, toUInt64(0))
        + coalesce(p.gas, toUInt64(0))
        + if(i.is_create, toUInt64(9500), toUInt64(8500))
        + i.initcode_word_cost
        AS gas_compute,

    -- gas_memory: opcode memory only (no intrinsic memory component)
    coalesce(o.gas_memory, toUInt64(0)) AS gas_memory,

    -- gas_address_access: opcode access + intrinsic access + access list cost
    -- Access list cost = intrinsic_gas - base_cost - input_data_cost - initcode_word_cost
    coalesce(o.gas_address_access, toUInt64(0))
        + if(i.is_create, toUInt64(2800), toUInt64(300))
        + toUInt64(i.intrinsic_gas - i.base_cost - i.input_data_cost - i.initcode_word_cost)
        AS gas_address_access,

    -- gas_state_growth: opcode state + intrinsic state (CREATE only) + code deposit
    -- Code deposit (200 gas/byte of deployed code) is charged post-execution for
    -- CREATE txs. Computed as residual: gas_cumulative - opcode_gas - precompile_gas.
    coalesce(o.gas_state_growth, toUInt64(0))
        + if(i.is_create, toUInt64(21800), toUInt64(0))
        + if(i.is_create,
            toUInt64(i.gas_cumulative
                - coalesce(o.total_opcode_gas, toUInt64(0))
                - coalesce(p.gas, toUInt64(0))),
            toUInt64(0))
        AS gas_state_growth,

    -- gas_history: opcode history + intrinsic history + calldata history
    -- Calldata history uses batch rounding: intDiv(n_zero * 1 + n_nonzero * 10, 2)
    coalesce(o.gas_history, toUInt64(0))
        + if(i.is_create, toUInt64(13200), toUInt64(6500))
        + toUInt64(intDiv(
            toUInt64(i.n_input_zero_bytes) * toUInt64(1)
                + toUInt64(i.n_input_nonzero_bytes) * toUInt64(10),
            2))
        AS gas_history,

    -- gas_bloom_topics: opcode bloom only
    coalesce(o.gas_bloom_topics, toUInt64(0)) AS gas_bloom_topics,

    -- gas_block_size: intrinsic block size + calldata block size
    -- Calldata block size = input_data_cost - history_calldata (absorbs rounding residual)
    toUInt64(5700)
        + toUInt64(i.input_data_cost
            - toUInt64(intDiv(
                toUInt64(i.n_input_zero_bytes) * toUInt64(1)
                    + toUInt64(i.n_input_nonzero_bytes) * toUInt64(10),
                2)))
        AS gas_block_size,

    -- gas_refund
    coalesce(i.gas_refund, toUInt64(0)) AS gas_refund,

    '{{ .env.NETWORK }}' AS meta_network_name

FROM intrinsic i
LEFT JOIN opcode_resources o
    ON i.block_number = o.block_number
    AND i.transaction_hash = o.transaction_hash
LEFT JOIN precompile_gas p
    ON i.block_number = p.block_number
    AND i.transaction_hash = p.transaction_hash
SETTINGS
    max_threads = 8,
    max_bytes_before_external_group_by = 10000000000,
    distributed_aggregation_memory_efficient = 1;
