---
table: int_block_transaction_summary
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
  - block
dependencies:
  - "{{transformation}}.int_transaction_call_frame"
---
-- One row per transaction with its call tree summarised: gas from the root frame (call_frame_id 0), and the
-- number of frames, deepest call and target from the whole tree.
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT * EXCEPT (root_frame_call_type, root_target_address)
FROM (
SELECT
    fromUnixTimestamp({{ .task.start }}) AS updated_date_time,
    block_number,
    transaction_hash,
    any(transaction_index) AS transaction_index,
    anyIf(receipt_gas_used, call_frame_id = 0) AS receipt_gas_used,
    anyIf(intrinsic_gas, call_frame_id = 0) AS intrinsic_gas,
    ifNull(anyIf(gas_refund, call_frame_id = 0), 0) AS gas_refund,
    anyIf(gas_cumulative, call_frame_id = 0) AS evm_gas,
    toUInt32(count()) AS call_frame_count,
    max(depth) AS max_depth,
    anyIf(error_count, call_frame_id = 0) > 0 AS has_errors,
    anyIf(call_type, call_frame_id = 0) AS root_frame_call_type,
    anyIf(target_address, call_frame_id = 0) AS root_target_address,
    -- A plain call has an empty root call type. A root frame without a target created a contract.
    multiIf(
        root_frame_call_type = 'CREATE' OR (root_target_address IS NULL AND root_frame_call_type = ''), 'CREATE',
        root_frame_call_type = '', 'CALL',
        root_frame_call_type
    ) AS root_call_type,
    root_target_address AS target_address
FROM {{ index .dep "{{transformation}}" "int_transaction_call_frame" "helpers" "from" }} FINAL
WHERE block_number BETWEEN {{ .bounds.start }} AND {{ .bounds.end }}
GROUP BY block_number, transaction_hash
)
