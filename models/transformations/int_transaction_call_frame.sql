---
table: int_transaction_call_frame
type: incremental
interval:
  type: block
  max: 1000
fill:
  direction: "tail"
  allow_gap_skipping: false
schedules:
  forwardfill: "@every 2s"
  backfill: "@every 2s"
tags:
  - execution
  - call
  - gas
dependencies:
  - "{{external}}.canonical_execution_transaction"
  - "{{external}}.canonical_execution_transaction_structlog"
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
-- To calculate receipt gas_used (what Etherscan/Phalcon show):
--
--   total_consumed = intrinsic_gas + root.total_gas_used
--   capped_refund  = min(root.gas_refund, total_consumed / 5)
--   receipt_gas    = total_consumed - capped_refund
--
-- Example (deriving intrinsic from receipt):
--   receipt_gas_used  = 646,559  (from structlog.transaction_gas)
--   gas_cumulative    = 761,334  (root frame EVM execution)
--   gas_refund        = 276,700  (accumulated)
--
--   Is refund capped? gas_refund (276,700) >= receipt/4 (161,639)? YES
--   So: intrinsic = receipt * 1.25 - EVM
--                 = 646,559 * 1.25 - 761,334
--                 = 808,198 - 761,334
--                 = 46,864 (exact, includes any access list costs)
--
-- =============================================================================
-- COLUMN DEFINITIONS
-- =============================================================================
--
--   gas:            Gas consumed by THIS frame only (excludes child frames).
--                   Calculated as: gas_cumulative - sum(children.gas_cumulative)
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
-- =============================================================================
--
-- Key insight: To find what CALL opcode created a frame, we look at the opcode
-- immediately before the first opcode in that frame. That preceding opcode
-- (in the parent frame) is the CALL that initiated this frame.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- =============================================================================
-- PART 1: Identify transactions with and without structlogs
-- =============================================================================

-- All transactions from canonical_execution_transaction
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

-- Simple transfers: transactions WITHOUT structlog data
-- Note: Uses NOT IN instead of LEFT JOIN due to FixedString comparison issues
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
  WHERE (t.block_number, t.transaction_hash) NOT IN (
    SELECT block_number, transaction_hash
    FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog" "helpers" "from" }}
    WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
      AND meta_network_name = '{{ .env.NETWORK }}'
  )
),

-- =============================================================================
-- PART 2: Process transactions WITH structlogs (existing logic)
-- =============================================================================

-- First, aggregate metrics per frame and find the first opcode index in each frame
--
-- EOA (Externally Owned Account) Call Frames:
-- When a contract CALLs an EOA, the EVM creates a call frame but no opcodes execute
-- (EOAs have no code). execution-processor emits a synthetic structlog row for these
-- frames with operation = '' to maintain alignment with canonical_execution_traces.
-- This ensures call_frame_id = internal_index - 1 works for all frames.
-- See: ai_plans/003_eoa_call_frames.md for full context.
frame_metrics AS (
  SELECT
    block_number,
    transaction_hash,
    transaction_index,
    call_frame_id,
    any(call_frame_path) as call_frame_path,
    -- Count only real opcodes, not synthetic EOA rows (operation = '')
    -- EOA frames will have opcode_count = 0
    countIf(operation != '') as opcode_count,
    -- Gas cumulative: (gas before first opcode) - (gas after last opcode)
    -- argMin(gas, index) = gas at first opcode (min index = first)
    -- argMax(gas, index) = gas at last opcode (max index = last)
    -- gas_after_last = argMax(gas, index) - argMax(gas_used, index)
    -- gas_cumulative = first_gas - gas_after_last
    argMin(gas, index) - argMax(gas, index) + argMax(gas_used, index) as gas_cumulative,
    -- Gas refund: cumulative refund counter at end of frame
    max(refund) as gas_refund,
    countIf(error IS NOT NULL AND error != '') as error_count,
    min(index) as first_opcode_index,
    -- Receipt gas_used (same for all rows in transaction, from trace)
    any(transaction_gas) as receipt_gas_used
  FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog" "helpers" "from" }}
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
  GROUP BY
    block_number,
    transaction_hash,
    transaction_index,
    call_frame_id
),

-- Find the CALL opcodes that initiate new frames
-- These are identified by: the next opcode (by index) is in a different (child) frame
initiating_calls AS (
  SELECT
    block_number,
    transaction_hash,
    index + 1 as child_first_index,  -- The first opcode of the child frame
    call_to_address,
    operation as call_type
  FROM {{ index .dep "{{external}}" "canonical_execution_transaction_structlog" "helpers" "from" }}
  WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
    AND meta_network_name = '{{ .env.NETWORK }}'
    AND operation IN ('CALL', 'DELEGATECALL', 'STATICCALL', 'CALLCODE', 'CREATE', 'CREATE2')
),

-- Calculate sum of direct children's gas_cumulative for each parent frame
-- This is used to derive gas = gas_cumulative - children_total_gas
children_gas AS (
  SELECT
    block_number,
    transaction_hash,
    call_frame_path[length(call_frame_path) - 1] as parent_call_frame_id,
    sum(gas_cumulative) as children_total_gas
  FROM frame_metrics
  WHERE length(call_frame_path) > 1  -- Only non-root frames have parents
  GROUP BY
    block_number,
    transaction_hash,
    parent_call_frame_id
),

-- =============================================================================
-- PART 3: Build output for transactions WITH structlogs
-- =============================================================================

structlog_frames AS (
  SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    fm.block_number,
    fm.transaction_hash,
    fm.transaction_index,
    fm.call_frame_id,
    -- Parent frame is second-to-last in path, NULL for root frame
    if(
      length(fm.call_frame_path) > 1,
      fm.call_frame_path[length(fm.call_frame_path) - 1],
      NULL
    ) as parent_call_frame_id,
    -- Depth is path length - 1 (root is depth 0)
    toUInt32(length(fm.call_frame_path) - 1) as depth,
    -- Target address: from initiating CALL opcode, or from traces for EOA/root frames
    -- EOA frames may not match initiating_calls join (synthetic row index issue)
    -- Root frame has no initiating CALL, so falls back to traces (transaction's to_address)
    coalesce(ic.call_to_address, tr.action_to) as target_address,
    -- Call type: opcode that started this frame (empty for root)
    -- For non-root frames, fall back to traces if initiating_calls join fails
    -- traces uses lowercase with underscores (e.g., 'delegate_call'), normalize to uppercase
    if(
      fm.call_frame_id = 0,
      '',  -- Root frame has no initiating CALL
      coalesce(nullIf(ic.call_type, ''), upper(replaceAll(tr.action_call_type, '_', '')), '')
    ) as call_type,
    -- Function selector from traces.action_input (first 4 bytes = 10 chars including 0x prefix)
    -- traces.internal_index = call_frame_id + 1 (traces are 1-indexed, call_frame_id is 0-indexed)
    substring(tr.action_input, 1, 10) as function_selector,
    fm.opcode_count,
    fm.error_count,
    -- Gas consumed by this frame only (excludes children)
    toUInt64(fm.gas_cumulative - coalesce(cg.children_total_gas, 0)) as gas,
    toUInt64(fm.gas_cumulative) as gas_cumulative,
    -- Gas refund only for root frame (refund applied once at transaction end)
    if(fm.call_frame_id = 0, fm.gas_refund, NULL) as gas_refund,
    -- Intrinsic gas only for root frame, derived from receipt gas
    -- Formula: if refund was capped (>= receipt/4), intrinsic = receipt*1.25 - EVM
    --          else intrinsic = receipt - EVM + refund
    -- NULL for failed transactions (error_count > 0) since refund isn't applied
    -- Note: coalesce gas_refund to 0 for transactions with no SSTORE refunds
    if(
      fm.call_frame_id = 0 AND fm.error_count = 0,
      toUInt64(
        if(
          coalesce(fm.gas_refund, 0) >= intDiv(fm.receipt_gas_used, 4),
          -- Capped case: refund = 20% of total, so total = receipt * 1.25
          intDiv(fm.receipt_gas_used * 5, 4) - fm.gas_cumulative,
          -- Uncapped case: refund fully applied
          fm.receipt_gas_used - fm.gas_cumulative + coalesce(fm.gas_refund, 0)
        )
      ),
      NULL
    ) as intrinsic_gas
  FROM frame_metrics fm
  LEFT JOIN initiating_calls ic
    ON fm.block_number = ic.block_number
    AND fm.transaction_hash = ic.transaction_hash
    AND fm.first_opcode_index = ic.child_first_index
  LEFT JOIN children_gas cg
    ON fm.block_number = cg.block_number
    AND fm.transaction_hash = cg.transaction_hash
    AND fm.call_frame_id = cg.parent_call_frame_id
  -- Join traces for:
  --   - target_address fallback (for EOA/root frames where initiating_calls join fails)
  --   - call_type fallback (for EOA frames)
  --   - function_selector (from action_input)
  -- traces.internal_index is 1-indexed, call_frame_id is 0-indexed
  LEFT JOIN {{ index .dep "{{external}}" "canonical_execution_traces" "helpers" "from" }} tr
    ON fm.block_number = tr.block_number
    AND fm.transaction_hash = tr.transaction_hash
    AND tr.internal_index = fm.call_frame_id + 1
    AND tr.meta_network_name = '{{ .env.NETWORK }}'
),

-- =============================================================================
-- PART 4: Build synthetic root frames for simple transfers (no structlogs)
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
    if(st.success, toUInt64(st.receipt_gas_used), NULL) as intrinsic_gas
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
