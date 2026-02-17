---
table: int_transaction_call_frame_opcode_resource_gas
type: incremental
interval:
  type: block
  max: 500
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 10s"
  backfill: "@every 30s"
tags:
  - execution
  - opcode
  - gas
  - resource
  - call_frame
dependencies:
  - "{{transformation}}.int_transaction_call_frame_opcode_gas"
---
-- Decomposes per-frame per-opcode gas into 7 resource categories.
--
-- =============================================================================
-- RESOURCE CATEGORIES
-- =============================================================================
--
-- Every opcode's gas is split into exactly 7 buckets that sum to gas:
--   gas_compute        - Pure computation (arithmetic, stack, control flow)
--   gas_memory         - Memory expansion cost
--   gas_address_access - Cold address/storage access premium (EIP-2929)
--   gas_state_growth   - New storage slot / contract creation cost
--   gas_history        - LOG data storage cost
--   gas_bloom_topics   - LOG bloom filter topic indexing
--   gas_block_size     - Always 0 at opcode level (only at transaction level)
--
-- =============================================================================
-- MEMORY EXPANSION GAS FORMULA
-- =============================================================================
--
-- The EVM charges for memory expansion using:
--   memory_cost = 3 * words + words^2 / 512
--
-- For aggregated per-opcode data, the expansion cost across all executions is:
--   intDiv(sq_sum_after - sq_sum_before, 512) + 3 * (sum_after - sum_before)
--
-- where sq_sum = SUM(words^2) and sum = SUM(words) across all executions.
-- Note: intDiv rounding may cause ±1-3 gas variance vs exact per-opcode costs.
--
-- =============================================================================
-- OPCODE CLASSIFICATION RULES
-- =============================================================================
--
-- Pure Compute:  ~60 stack/arithmetic/env ops - all gas is compute
-- Memory ops:    MLOAD, MSTORE, etc - compute = gas - mem_exp, memory = mem_exp
-- Access:        BALANCE, SLOAD, etc - compute = warm_cost * count, access = cold premium
-- SSTORE:        compute = 100 * count, access = cold * 2100, state = remainder
-- CALL family:   compute = gas - cold*2500 - mem_exp, access = cold*2500, memory = mem_exp
-- EXTCODECOPY:   compute = gas - cold*2500 - mem_exp, access = cold*2500, memory = mem_exp
-- LOG0-LOG4:     compute = 20 * count, memory = mem_exp, bloom = 250*N*count, history = remainder
-- CREATE family: compute = 1000*count, memory = mem_exp, access = 250*count,
--                state = gas - 1000*count - 6700*count - 250*count - mem_exp, history = 6700*count
-- SELFDESTRUCT:  compute = 5000 * count, access = cold * 2600, state = remainder
--
-- =============================================================================
-- UNDERFLOW PROTECTION (Priority-Based Clamping)
-- =============================================================================
--
-- In reverted frames, opcodes may be partially charged (gas < full cost).
-- To prevent UInt64 underflow when subtracting fixed components, we use
-- priority-based clamping: each resource category takes min(formula, remaining)
-- where remaining decreases after each allocation. This ensures:
--   1. All 7 columns are non-negative
--   2. All 7 columns still sum exactly to gas
--
-- Affected opcodes: BALANCE, EXTCODESIZE, EXTCODEHASH, SLOAD, SSTORE,
--                   LOG0-LOG4, SELFDESTRUCT
--
-- =============================================================================
-- TYPE SAFETY NOTE
-- =============================================================================
--
-- In ClickHouse 25.x, UInt64 - UInt64 returns Int64 (not UInt64). All
-- subtractions must be wrapped in toUInt64() to prevent type mismatches
-- in least() and multiIf() which require homogeneous argument types.
--
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
source AS (
    SELECT
        block_number,
        transaction_hash,
        transaction_index,
        call_frame_id,
        opcode,
        count,
        gas,
        -- Memory expansion gas reconstructed from sq_sum/sum columns.
        -- All subtractions wrapped in toUInt64() because UInt64 - UInt64 = Int64
        -- in ClickHouse 25.x, and EVM memory only grows so after >= before.
        intDiv(toUInt64(memory_words_sq_sum_after - memory_words_sq_sum_before), toUInt64(512))
            + toUInt64(3) * toUInt64(memory_words_sum_after - memory_words_sum_before)
            AS mem_exp,
        cold_access_count AS cold,
        meta_network_name,

        -- === Priority-clamped values for underflow protection ===

        -- Access opcodes (BALANCE, EXTCODESIZE, EXTCODEHASH):
        -- compute(100*count) → access(cold*2500) absorbed by remainder
        least(toUInt64(100) * count, gas) AS access_account_compute,
        least(
            cold_access_count * toUInt64(2500),
            toUInt64(gas - least(toUInt64(100) * count, gas))
        ) AS access_account_access,

        -- SLOAD: compute(100*count) → access(cold*2000) absorbed by remainder
        least(toUInt64(100) * count, gas) AS sload_compute,
        least(
            cold_access_count * toUInt64(2000),
            toUInt64(gas - least(toUInt64(100) * count, gas))
        ) AS sload_access,

        -- SSTORE: compute(100*count) → access(cold*2100) → state(remainder)
        least(toUInt64(100) * count, gas) AS sstore_compute,
        least(
            cold_access_count * toUInt64(2100),
            toUInt64(gas - least(toUInt64(100) * count, gas))
        ) AS sstore_access,

        -- LOGn: compute(20*count) → memory(mem_exp) → bloom/history(remainder)
        least(toUInt64(20) * count, gas) AS log_compute,
        least(
            intDiv(toUInt64(memory_words_sq_sum_after - memory_words_sq_sum_before), toUInt64(512))
                + toUInt64(3) * toUInt64(memory_words_sum_after - memory_words_sum_before),
            toUInt64(gas - least(toUInt64(20) * count, gas))
        ) AS log_memory,
        toUInt64(
            toUInt64(gas - least(toUInt64(20) * count, gas))
            - least(
                intDiv(toUInt64(memory_words_sq_sum_after - memory_words_sq_sum_before), toUInt64(512))
                    + toUInt64(3) * toUInt64(memory_words_sum_after - memory_words_sum_before),
                toUInt64(gas - least(toUInt64(20) * count, gas))
            )
        ) AS log_remaining,

        -- SELFDESTRUCT: compute(5000*count) → access(cold*2600) → state(remainder)
        least(toUInt64(5000) * count, gas) AS sd_compute,
        least(
            cold_access_count * toUInt64(2600),
            toUInt64(gas - least(toUInt64(5000) * count, gas))
        ) AS sd_access
    FROM {{ index .dep "{{transformation}}" "int_transaction_call_frame_opcode_gas" "helpers" "from" }} FINAL
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
        AND meta_network_name = '{{ .env.NETWORK }}'
)

SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    transaction_hash,
    transaction_index,
    call_frame_id,
    opcode,
    count,
    gas,

    -- =========================================================================
    -- RESOURCE DECOMPOSITION
    -- =========================================================================

    -- gas_compute
    CASE
        -- Pure compute opcodes: all gas is compute
        WHEN opcode IN (
            'STOP', 'ADD', 'MUL', 'SUB', 'DIV', 'SDIV', 'MOD', 'SMOD',
            'ADDMOD', 'MULMOD', 'EXP', 'SIGNEXTEND',
            'LT', 'GT', 'SLT', 'SGT', 'EQ', 'ISZERO', 'AND', 'OR', 'XOR',
            'NOT', 'BYTE', 'SHL', 'SHR', 'SAR',
            'ADDRESS', 'ORIGIN', 'CALLER', 'CALLVALUE', 'CALLDATALOAD',
            'CALLDATASIZE', 'CODESIZE', 'GASPRICE', 'RETURNDATASIZE',
            'COINBASE', 'TIMESTAMP', 'NUMBER', 'PREVRANDAO', 'GASLIMIT',
            'CHAINID', 'SELFBALANCE', 'BASEFEE', 'BLOBBASEFEE',
            'POP', 'JUMP', 'JUMPI', 'PC', 'MSIZE', 'GAS', 'JUMPDEST',
            'TLOAD', 'TSTORE', 'PUSH0', 'BLOBHASH',
            'PUSH1', 'PUSH2', 'PUSH3', 'PUSH4', 'PUSH5', 'PUSH6', 'PUSH7',
            'PUSH8', 'PUSH9', 'PUSH10', 'PUSH11', 'PUSH12', 'PUSH13',
            'PUSH14', 'PUSH15', 'PUSH16', 'PUSH17', 'PUSH18', 'PUSH19',
            'PUSH20', 'PUSH21', 'PUSH22', 'PUSH23', 'PUSH24', 'PUSH25',
            'PUSH26', 'PUSH27', 'PUSH28', 'PUSH29', 'PUSH30', 'PUSH31', 'PUSH32',
            'DUP1', 'DUP2', 'DUP3', 'DUP4', 'DUP5', 'DUP6', 'DUP7', 'DUP8',
            'DUP9', 'DUP10', 'DUP11', 'DUP12', 'DUP13', 'DUP14', 'DUP15', 'DUP16',
            'SWAP1', 'SWAP2', 'SWAP3', 'SWAP4', 'SWAP5', 'SWAP6', 'SWAP7', 'SWAP8',
            'SWAP9', 'SWAP10', 'SWAP11', 'SWAP12', 'SWAP13', 'SWAP14', 'SWAP15', 'SWAP16',
            'INVALID'
        ) THEN gas

        -- Memory ops: compute = gas - mem_exp
        WHEN opcode IN (
            'MLOAD', 'MSTORE', 'MSTORE8', 'RETURN', 'REVERT',
            'MCOPY', 'KECCAK256', 'CALLDATACOPY', 'CODECOPY', 'RETURNDATACOPY'
        ) THEN toUInt64(gas - mem_exp)

        -- Access opcodes: compute = min(100 * count, gas)
        WHEN opcode IN ('BALANCE', 'EXTCODESIZE', 'EXTCODEHASH')
        THEN access_account_compute

        -- SLOAD: compute = min(100 * count, gas)
        WHEN opcode = 'SLOAD' THEN sload_compute

        -- SSTORE: compute = min(100 * count, gas)
        WHEN opcode = 'SSTORE' THEN sstore_compute

        -- CALL family: compute = gas - cold*2500 - mem_exp
        -- (value transfer + account creation gas goes to compute implicitly)
        WHEN opcode IN ('CALL', 'STATICCALL', 'DELEGATECALL', 'CALLCODE')
        THEN toUInt64(gas - cold * toUInt64(2500) - mem_exp)

        -- EXTCODECOPY: compute = gas - cold*2500 - mem_exp
        WHEN opcode = 'EXTCODECOPY' THEN toUInt64(gas - cold * toUInt64(2500) - mem_exp)

        -- LOG0-LOG4: compute = min(20 * count, gas)
        WHEN opcode IN ('LOG0', 'LOG1', 'LOG2', 'LOG3', 'LOG4')
        THEN log_compute

        -- CREATE family: compute = 1000 * count
        WHEN opcode IN ('CREATE', 'CREATE2') THEN toUInt64(1000) * count

        -- SELFDESTRUCT: compute = min(5000 * count, gas)
        WHEN opcode = 'SELFDESTRUCT' THEN sd_compute

        -- Fallback: all gas is compute (unknown opcodes)
        ELSE gas
    END AS gas_compute,

    -- gas_memory
    CASE
        WHEN opcode IN (
            'MLOAD', 'MSTORE', 'MSTORE8', 'RETURN', 'REVERT',
            'MCOPY', 'KECCAK256', 'CALLDATACOPY', 'CODECOPY', 'RETURNDATACOPY'
        ) THEN mem_exp

        WHEN opcode IN ('CALL', 'STATICCALL', 'DELEGATECALL', 'CALLCODE')
        THEN mem_exp

        WHEN opcode = 'EXTCODECOPY' THEN mem_exp

        -- LOGn: memory = min(mem_exp, gas - compute)
        WHEN opcode IN ('LOG0', 'LOG1', 'LOG2', 'LOG3', 'LOG4')
        THEN log_memory

        WHEN opcode IN ('CREATE', 'CREATE2') THEN mem_exp

        ELSE toUInt64(0)
    END AS gas_memory,

    -- gas_address_access
    CASE
        -- Access opcodes: access = min(cold * 2500, gas - compute)
        WHEN opcode IN ('BALANCE', 'EXTCODESIZE', 'EXTCODEHASH')
        THEN access_account_access

        -- SLOAD: access = min(cold * 2000, gas - compute)
        WHEN opcode = 'SLOAD' THEN sload_access

        -- SSTORE: access = min(cold * 2100, gas - compute)
        WHEN opcode = 'SSTORE' THEN sstore_access

        -- CALL family: cold * 2500
        WHEN opcode IN ('CALL', 'STATICCALL', 'DELEGATECALL', 'CALLCODE')
        THEN cold * toUInt64(2500)

        -- EXTCODECOPY: cold * 2500
        WHEN opcode = 'EXTCODECOPY' THEN cold * toUInt64(2500)

        -- CREATE family: 250 * count (address allocation)
        WHEN opcode IN ('CREATE', 'CREATE2') THEN toUInt64(250) * count

        -- SELFDESTRUCT: access = min(cold * 2600, gas - compute)
        WHEN opcode = 'SELFDESTRUCT' THEN sd_access

        ELSE toUInt64(0)
    END AS gas_address_access,

    -- gas_state_growth
    CASE
        -- SSTORE: remainder after compute + access (safe: sum = gas by construction)
        WHEN opcode = 'SSTORE'
        THEN toUInt64(gas - sstore_compute - sstore_access)

        -- CREATE family: state = gas - compute(1000) - history(6700) - access(250) - mem_exp
        WHEN opcode IN ('CREATE', 'CREATE2')
        THEN toUInt64(gas - toUInt64(1000) * count - toUInt64(6700) * count - toUInt64(250) * count - mem_exp)

        -- SELFDESTRUCT: remainder after compute + access (safe: sum = gas by construction)
        WHEN opcode = 'SELFDESTRUCT'
        THEN toUInt64(gas - sd_compute - sd_access)

        ELSE toUInt64(0)
    END AS gas_state_growth,

    -- gas_history
    CASE
        -- LOG0: history = remainder after compute + memory (no bloom)
        WHEN opcode = 'LOG0' THEN toUInt64(log_remaining)

        -- LOG1-LOG4: history = remainder after compute + memory + bloom
        WHEN opcode = 'LOG1'
        THEN toUInt64(log_remaining - least(toUInt64(250) * count, log_remaining))
        WHEN opcode = 'LOG2'
        THEN toUInt64(log_remaining - least(toUInt64(500) * count, log_remaining))
        WHEN opcode = 'LOG3'
        THEN toUInt64(log_remaining - least(toUInt64(750) * count, log_remaining))
        WHEN opcode = 'LOG4'
        THEN toUInt64(log_remaining - least(toUInt64(1000) * count, log_remaining))

        -- CREATE family: 6700 * count
        WHEN opcode IN ('CREATE', 'CREATE2') THEN toUInt64(6700) * count

        ELSE toUInt64(0)
    END AS gas_history,

    -- gas_bloom_topics
    CASE
        -- LOG1-LOG4: bloom = min(250*N*count, remaining after compute + memory)
        WHEN opcode = 'LOG1' THEN least(toUInt64(250) * count, log_remaining)
        WHEN opcode = 'LOG2' THEN least(toUInt64(500) * count, log_remaining)
        WHEN opcode = 'LOG3' THEN least(toUInt64(750) * count, log_remaining)
        WHEN opcode = 'LOG4' THEN least(toUInt64(1000) * count, log_remaining)
        ELSE toUInt64(0)
    END AS gas_bloom_topics,

    -- gas_block_size: always 0 at opcode level
    toUInt64(0) AS gas_block_size,

    meta_network_name

FROM source
SETTINGS
    max_threads = 8,
    do_not_merge_across_partitions_select_final = 1;
