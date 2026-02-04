---
table: int_transaction_call_frame
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
  - call
  - gas
dependencies:
  - "{{external}}.canonical_execution_transaction"
  - "{{external}}.canonical_execution_transaction_structlog_agg"
  - "{{external}}.canonical_execution_traces"
---
-- Aggregates structlog opcodes into per-call-frame records for transaction call tree analysis.
-- Each row represents one call frame within a transaction, with aggregated metrics.
--
-- This model includes ALL transactions:
--   - Transactions WITH structlogs: Full call frame hierarchy from EVM execution
--   - Transactions WITHOUT structlogs: Synthetic root frame (simple ETH transfers)
--
-- =============================================================================
-- DATA SOURCE: canonical_execution_transaction_structlog_agg
-- =============================================================================
--
-- This transformation uses the pre-aggregated structlog_agg table instead of
-- computing aggregations from raw structlogs. The structlog_agg table contains:
--
--   Summary rows (operation = ''):
--     - Frame-level metadata: call_type, target_address, depth
--     - Frame-level gas: gas (self), gas_cumulative (self + children)
--     - Root-only fields: gas_refund, intrinsic_gas
--
--   Per-opcode rows (operation != ''):
--     - Aggregated per (frame, opcode) combination
--     - Used by int_transaction_call_frame_opcode_gas transformation
--
-- This transformation only uses summary rows (operation = '').
--
-- =============================================================================
-- GAS MODEL EXPLANATION
-- =============================================================================
--
-- Transaction gas has three components:
--
--   1. INTRINSIC GAS (derived from receipt - includes ALL components)
--      - Base cost: 21,000 gas
--      - Calldata: 4 gas/zero byte + 16 gas/non-zero byte
--      - Contract creation: +32,000 gas (if to_address is NULL)
--      - Access list: 2,400/address + 1,900/storage key (EIP-2930)
--      We derive this from receipt_gas rather than calculating, so access lists are included.
--
--   2. EVM EXECUTION GAS (this table - root frame's gas_cumulative)
--      - All opcode execution costs
--      - Includes CALL overhead and child frame gas
--      - For simple transfers: 0 (no EVM execution)
--
--   3. GAS REFUND (from SSTORE operations clearing storage)
--      - Accumulated during execution
--      - Capped at 20% of total gas consumed (EIP-3529)
--
-- =============================================================================
-- CONTRACT CREATION CODE DEPOSIT COST
-- =============================================================================
--
-- For CREATE/CREATE2 transactions, there's an additional gas cost NOT captured
-- in structlog_agg: the CODE DEPOSIT COST (200 gas per byte of deployed bytecode).
--
-- This cost is a STATE TRANSITION charge applied when the constructor successfully
-- returns (RETURN opcode), not an opcode execution cost. The EVM charges it after
-- all opcodes complete, so debug_traceTransaction's structlog doesn't include it.
--
-- The structlog tracer reports opcode-by-opcode gas. Code deposit isn't an opcode -
-- it's charged as part of CREATE/CREATE2 result processing in the EVM state transition.
--
-- This transformation corrects for this by:
--   1. Detecting CREATE transactions (action_type = 'create' in traces)
--   2. Getting deployed bytecode from traces.result_code
--   3. Adding code_deposit = len(bytecode) * 200 to gas and gas_cumulative
--   4. Recalculating intrinsic_gas with the corrected gas_cumulative
--
-- Without this correction, the code deposit cost would incorrectly appear as
-- intrinsic gas (since intrinsic = receipt - gas_cumulative + refund).
--
-- =============================================================================
-- COLUMN DEFINITIONS
-- =============================================================================
--
--   gas:            Gas consumed by THIS frame only (excludes child frames).
--                   This is what flame graphs show as "self" gas.
--                   sum(gas) for all frames = total EVM execution gas.
--                   For simple transfers: 0
--
--   gas_cumulative: Gas consumed by this frame AND all descendants.
--                   For root frame (call_frame_id=0), this is total EVM execution gas.
--                   Relationship: parent.gas_cumulative = parent.gas + sum(children.gas_cumulative)
--                   For simple transfers: 0
--
--   gas_refund:     Total accumulated refund for the transaction.
--                   Only populated for root frame (call_frame_id=0).
--                   Refund is applied once at transaction end, not per-frame.
--                   For simple transfers: NULL
--
--   intrinsic_gas:  Transaction intrinsic cost (only populated for root frame).
--                   Derived from: receipt_gas, gas_cumulative, gas_refund
--                   Formula handles both capped and uncapped refund cases.
--                   INCLUDES access list costs (derived, not calculated).
--                   NULL for failed transactions (refund not applied, formula invalid).
--                   For simple transfers: equals receipt gas_used (all gas is intrinsic)
--
--   receipt_gas_used: Actual gas used from transaction receipt (only for root frame).
--                     This is the source of truth for "Total Gas Used" display.
--                     For successful txs: intrinsic + gas_cumulative - gas_refund = receipt_gas_used
--                     For failed txs: all gas consumed (formula doesn't apply)
--
-- =============================================================================
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- =============================================================================
-- CTEs: Simple transfer identification via key exclusion pattern
-- =============================================================================

-- All transactions in the block range
all_transactions AS (
  SELECT
    block_number,
    transaction_hash,
    transaction_index,
    gas_used as receipt_gas_used,
    to_address,
    input,
    success
  FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }}
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
),

-- =============================================================================
-- PART 1: Identify transactions without structlogs (simple transfers)
-- =============================================================================

structlog_tx_keys AS (
  SELECT DISTINCT block_number, transaction_hash
  FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog_agg" "helpers" "from" }}
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND operation = ''
),

simple_transfers AS (
  SELECT
    t.block_number,
    t.transaction_hash,
    t.transaction_index,
    t.receipt_gas_used,
    t.to_address,
    t.input,
    t.success
  FROM all_transactions t
  LEFT JOIN structlog_tx_keys s
    ON t.block_number = s.block_number
    AND t.transaction_hash = s.transaction_hash
  WHERE s.transaction_hash IS NULL  -- No structlog data
),

-- =============================================================================
-- PART 2: Process transactions WITH structlogs (GLOBAL LEFT ANY JOIN)
-- =============================================================================

structlog_frames AS (
  SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    agg.block_number,
    agg.transaction_hash,
    agg.transaction_index,
    agg.call_frame_id,
    agg.parent_call_frame_id,
    agg.depth,
    -- For CREATE transactions at root, use result_address (the created contract)
    -- Otherwise use the normal target_address from structlog
    if(agg.call_frame_id = 0 AND tr.action_type = 'create',
       tr.result_address,
       agg.target_address) as target_address,
    -- For root frames: use 'CREATE' if it's a contract creation, else empty
    -- For non-root frames: use the call_type from structlog
    if(agg.call_frame_id = 0,
       if(tr.action_type = 'create', 'CREATE', ''),
       agg.call_type) as call_type,
    tr.function_selector,
    agg.opcode_count,
    agg.error_count,
    -- CODE DEPOSIT CORRECTION (see "CONTRACT CREATION CODE DEPOSIT COST" section above)
    -- For CREATE: add code_deposit = len(deployed_bytecode) * 200 to gas fields
    -- result_code is hex with '0x' prefix, so byte_count = (length - 2) / 2
    -- Cast to UInt64 to avoid Int64 promotion from subtraction
    if(agg.call_frame_id = 0 AND tr.action_type = 'create' AND tr.result_code IS NOT NULL AND length(tr.result_code) > 2,
       agg.gas + toUInt64(intDiv(length(tr.result_code) - 2, 2) * 200),
       agg.gas) as gas,
    if(agg.call_frame_id = 0 AND tr.action_type = 'create' AND tr.result_code IS NOT NULL AND length(tr.result_code) > 2,
       agg.gas_cumulative + toUInt64(intDiv(length(tr.result_code) - 2, 2) * 200),
       agg.gas_cumulative) as gas_cumulative,
    agg.gas_refund,
    -- Recalculate intrinsic_gas with corrected gas_cumulative for CREATE transactions
    -- Use Int64 arithmetic to avoid UInt64 subtraction promotion issues, then cast result
    if(agg.call_frame_id = 0,
       if(tr.action_type = 'create' AND tr.result_code IS NOT NULL AND length(tr.result_code) > 2,
          CAST(
            toInt64(tx.receipt_gas_used)
            - toInt64(agg.gas_cumulative)
            - toInt64(intDiv(length(tr.result_code) - 2, 2) * 200)
            + toInt64(coalesce(agg.gas_refund, toUInt64(0)))
          AS Nullable(UInt64)),
          agg.intrinsic_gas),
       NULL) as intrinsic_gas,
    -- Receipt gas used from transaction table (only for root frame)
    if(agg.call_frame_id = 0, tx.receipt_gas_used, NULL) as receipt_gas_used
  FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog_agg" "helpers" "from" }} agg
  -- GLOBAL LEFT ANY JOIN: evaluates subquery once, broadcasts result, uses first match only
  GLOBAL LEFT ANY JOIN (
    SELECT
      block_number,
      transaction_hash,
      toUInt32(internal_index - 1) as call_frame_id,
      substring(action_input, 1, 10) as function_selector,
      action_type,
      result_address,
      result_code
    FROM {{ index .dep "{{external}}" "canonical_execution_traces" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
  ) tr
    ON agg.block_number = tr.block_number
    AND agg.transaction_hash = tr.transaction_hash
    AND tr.call_frame_id = agg.call_frame_id
  GLOBAL LEFT ANY JOIN (
    SELECT
      block_number,
      transaction_hash,
      gas_used as receipt_gas_used
    FROM {{ index .dep "{{external}}" "canonical_execution_transaction" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
  ) tx
    ON agg.block_number = tx.block_number
    AND agg.transaction_hash = tx.transaction_hash
  WHERE agg.block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND agg.meta_network_name = '{{ .env.NETWORK }}'
    AND agg.operation = ''  -- Summary rows only
),

-- =============================================================================
-- PART 3: Build synthetic root frames for simple transfers (no structlogs)
-- =============================================================================

simple_transfer_frames AS (
  SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    st.block_number,
    st.transaction_hash,
    st.transaction_index,
    toUInt32(0) as call_frame_id,
    CAST(NULL AS Nullable(UInt32)) as parent_call_frame_id,
    toUInt32(0) as depth,
    st.to_address as target_address,
    '' as call_type,
    -- Function selector from input (first 10 chars = '0x' + 4 bytes)
    -- For pure ETH transfers, input is '0x' or empty, so this will be NULL/empty
    if(
      st.input IS NOT NULL AND length(st.input) >= 10,
      substring(st.input, 1, 10),
      NULL
    ) as function_selector,
    toUInt64(0) as opcode_count,
    -- If transaction failed (success=false), count as 1 error
    toUInt64(if(st.success, 0, 1)) as error_count,
    -- No EVM execution for simple transfers
    toUInt64(0) as gas,
    toUInt64(0) as gas_cumulative,
    -- No refund for simple transfers
    CAST(NULL AS Nullable(UInt64)) as gas_refund,
    -- For simple transfers, all gas is intrinsic (only if success)
    if(st.success, toUInt64(st.receipt_gas_used), NULL) as intrinsic_gas,
    -- Receipt gas used (always populated for root frame)
    toUInt64(st.receipt_gas_used) as receipt_gas_used
  FROM simple_transfers st
)

-- =============================================================================
-- COMBINE: All call frames from structlogs + synthetic frames for simple transfers
-- =============================================================================

SELECT * FROM structlog_frames
UNION ALL
SELECT * FROM simple_transfer_frames
SETTINGS
  max_bytes_before_external_group_by = 10000000000,
  distributed_aggregation_memory_efficient = 1;
