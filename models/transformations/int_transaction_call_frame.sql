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
  - "{{external}}.canonical_execution_transaction_structlog"
---
-- Aggregates structlog opcodes into per-call-frame records for transaction call tree analysis.
-- Each row represents one call frame within a transaction, with aggregated metrics.
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
--
--   gas_cumulative: Gas consumed by this frame AND all descendants.
--                   For root frame (call_frame_id=0), this is total EVM execution gas.
--                   Relationship: parent.gas_cumulative = parent.gas + sum(children.gas_cumulative)
--
--   gas_refund:     Total accumulated refund for the transaction.
--                   Only populated for root frame (call_frame_id=0).
--                   Refund is applied once at transaction end, not per-frame.
--
--   intrinsic_gas:  Transaction intrinsic cost (only populated for root frame).
--                   Derived from: receipt_gas, gas_cumulative, gas_refund
--                   Formula handles both capped and uncapped refund cases.
--                   INCLUDES access list costs (derived, not calculated).
--                   NULL for failed transactions (refund not applied, formula invalid).
--
-- =============================================================================
--
-- Key insight: To find what CALL opcode created a frame, we look at the opcode
-- immediately before the first opcode in that frame. That preceding opcode
-- (in the parent frame) is the CALL that initiated this frame.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
WITH
-- First, aggregate metrics per frame and find the first opcode index in each frame
frame_metrics AS (
  SELECT
    block_number,
    transaction_hash,
    transaction_index,
    call_frame_id,
    any(call_frame_path) as call_frame_path,
    count(*) as opcode_count,
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
)
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
  -- Target address comes from the CALL opcode that created this frame (NULL for root)
  ic.call_to_address as target_address,
  -- Target name from dim_contract_owner lookup (NULL if not found)
  dc.contract_name as target_name,
  -- Call type is the opcode that started this frame (empty for root)
  coalesce(ic.call_type, '') as call_type,
  fm.opcode_count,
  fm.error_count,
  -- Gas consumed by this frame only (excludes children)
  fm.gas_cumulative - coalesce(cg.children_total_gas, 0) as gas,
  fm.gas_cumulative,
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
LEFT JOIN `{{ .self.database }}`.dim_contract_owner dc FINAL
  ON lower(ic.call_to_address) = dc.contract_address
SETTINGS
  max_bytes_before_external_group_by = 10000000000,
  distributed_aggregation_memory_efficient = 1;
